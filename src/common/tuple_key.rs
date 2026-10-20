//! Multi-column heap keys: a [`TupleKey`] encodes to one canonical byte string,
//! the ASAPv1 tuple key (`docs/asapv1_wire_format.md`, Section 2), which a
//! heap-carrying sketch keeps as a `bytes` key.
//!
//! ```
//! use asap_sketchlib::{CMSHeap, DataInput, KeyColumn, TupleKey};
//!
//! let key = TupleKey::new(vec![KeyColumn::Str("us-east".into()), KeyColumn::I64(443)]);
//! let bytes = key.to_bytes().unwrap();
//! let mut sketch: CMSHeap = CMSHeap::new(3, 64, 8);
//! sketch.update(&DataInput::Bytes(&bytes));
//! assert_eq!(TupleKey::from_bytes(&bytes).unwrap(), key);
//! ```

use std::hash::{Hash, Hasher};

const TAG_NULL: u8 = 0x00;
const TAG_FALSE: u8 = 0x01;
const TAG_TRUE: u8 = 0x02;
const TAG_I64: u8 = 0x03;
const TAG_F64: u8 = 0x04;
const TAG_STR: u8 = 0x05;

/// The one `NaN` bit pattern a tuple key carries.
const CANONICAL_NAN: u64 = 0x7ff8_0000_0000_0000;

/// One column of a [`TupleKey`]. Equality and hashing follow the canonical
/// encoding: `-0.0` equals `0.0`, and every `NaN` equals every `NaN`.
#[derive(Clone, Debug)]
pub enum KeyColumn {
    /// SQL `NULL`.
    Null,
    /// A boolean.
    Bool(bool),
    /// A signed 64-bit integer.
    I64(i64),
    /// A 64-bit float. `-0.0` encodes as `0.0`, and every `NaN` as one `NaN`.
    F64(f64),
    /// A UTF-8 string.
    Str(String),
}

impl PartialEq for KeyColumn {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::I64(a), Self::I64(b)) => a == b,
            (Self::F64(a), Self::F64(b)) => canonical_f64_bits(*a) == canonical_f64_bits(*b),
            (Self::Str(a), Self::Str(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for KeyColumn {}

impl Hash for KeyColumn {
    fn hash<S: Hasher>(&self, state: &mut S) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Null => {}
            Self::Bool(value) => value.hash(state),
            Self::I64(value) => value.hash(state),
            Self::F64(value) => canonical_f64_bits(*value).hash(state),
            Self::Str(value) => value.hash(state),
        }
    }
}

/// A key made of typed columns, encoded into one canonical byte string. Two
/// tuples are equal exactly when their encodings are.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TupleKey(Vec<KeyColumn>);

impl TupleKey {
    /// A tuple of `columns`, in order.
    pub fn new(columns: Vec<KeyColumn>) -> Self {
        Self(columns)
    }

    /// The columns, in order.
    pub fn columns(&self) -> &[KeyColumn] {
        &self.0
    }

    /// The canonical encoding: each column's tag byte and value, concatenated.
    /// Fails on a string longer than `u32::MAX` bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        let mut out = Vec::with_capacity(9 * self.0.len());
        for column in &self.0 {
            match column {
                KeyColumn::Null => out.push(TAG_NULL),
                KeyColumn::Bool(false) => out.push(TAG_FALSE),
                KeyColumn::Bool(true) => out.push(TAG_TRUE),
                KeyColumn::I64(value) => {
                    out.push(TAG_I64);
                    out.extend_from_slice(&value.to_be_bytes());
                }
                KeyColumn::F64(value) => {
                    out.push(TAG_F64);
                    out.extend_from_slice(&canonical_f64_bits(*value).to_be_bytes());
                }
                KeyColumn::Str(value) => {
                    let len = u32::try_from(value.len()).map_err(|_| {
                        format!(
                            "tuple key string of {} bytes exceeds the u32 length prefix",
                            value.len()
                        )
                    })?;
                    out.push(TAG_STR);
                    out.extend_from_slice(&len.to_be_bytes());
                    out.extend_from_slice(value.as_bytes());
                }
            }
        }
        Ok(out)
    }

    /// Decodes a canonical encoding. Rejects an unknown tag, a value cut short,
    /// a string that is not UTF-8, and an `f64` written as `-0.0` or as a `NaN`
    /// other than `0x7ff8000000000000`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let mut columns = Vec::new();
        let mut rest = bytes;
        while let Some((&tag, tail)) = rest.split_first() {
            rest = tail;
            let column = match tag {
                TAG_NULL => KeyColumn::Null,
                TAG_FALSE => KeyColumn::Bool(false),
                TAG_TRUE => KeyColumn::Bool(true),
                TAG_I64 => KeyColumn::I64(i64::from_be_bytes(take(&mut rest)?)),
                TAG_F64 => {
                    let bits = u64::from_be_bytes(take(&mut rest)?);
                    let value = f64::from_bits(bits);
                    if canonical_f64_bits(value) != bits {
                        return Err(format!("tuple key f64 bits {bits:#018x} are not canonical"));
                    }
                    KeyColumn::F64(value)
                }
                TAG_STR => {
                    let len = u32::from_be_bytes(take(&mut rest)?) as usize;
                    if rest.len() < len {
                        return Err(format!(
                            "tuple key string of {len} bytes is cut short at {}",
                            rest.len()
                        ));
                    }
                    let (text, tail) = rest.split_at(len);
                    rest = tail;
                    let text = std::str::from_utf8(text)
                        .map_err(|error| format!("tuple key string is not UTF-8: {error}"))?;
                    KeyColumn::Str(text.to_string())
                }
                other => return Err(format!("tuple key tag {other:#04x} is unknown")),
            };
            columns.push(column);
        }
        Ok(Self(columns))
    }
}

/// `value`'s bits, with `-0.0` written as `0.0` and every `NaN` as one `NaN`.
fn canonical_f64_bits(value: f64) -> u64 {
    if value.is_nan() {
        CANONICAL_NAN
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

/// Splits `N` bytes off the front of `rest`.
fn take<const N: usize>(rest: &mut &[u8]) -> Result<[u8; N], String> {
    if rest.len() < N {
        return Err(format!(
            "tuple key value of {N} bytes is cut short at {}",
            rest.len()
        ));
    }
    let (head, tail) = rest.split_at(N);
    *rest = tail;
    Ok(head.try_into().expect("N bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(columns: Vec<KeyColumn>) -> TupleKey {
        TupleKey::new(columns)
    }

    fn encode(columns: Vec<KeyColumn>) -> Vec<u8> {
        key(columns).to_bytes().expect("encode")
    }

    /// The byte layout of every column kind, written out by hand.
    #[test]
    fn each_column_has_its_tag_and_value() {
        let bytes = encode(vec![
            KeyColumn::Null,
            KeyColumn::Bool(false),
            KeyColumn::Bool(true),
            KeyColumn::I64(-2),
            KeyColumn::F64(1.5),
            KeyColumn::Str("é".into()),
        ]);
        let mut expected = vec![0x00, 0x01, 0x02, 0x03];
        expected.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe]);
        expected.push(0x04);
        expected.extend_from_slice(&[0x3f, 0xf8, 0, 0, 0, 0, 0, 0]);
        expected.extend_from_slice(&[0x05, 0, 0, 0, 2, 0xc3, 0xa9]);
        assert_eq!(bytes, expected);
        assert_eq!(encode(vec![]), Vec::<u8>::new());
    }

    #[test]
    fn round_trip_restores_every_column() {
        let columns = vec![
            KeyColumn::Str(String::new()),
            KeyColumn::I64(i64::MIN),
            KeyColumn::I64(i64::MAX),
            KeyColumn::Null,
            KeyColumn::F64(-1e300),
            KeyColumn::F64(f64::INFINITY),
            KeyColumn::Bool(true),
            KeyColumn::Str("us-east-1 😀".into()),
        ];
        let original = key(columns);
        let bytes = original.to_bytes().expect("encode");
        let decoded = TupleKey::from_bytes(&bytes).expect("decode");
        assert_eq!(decoded, original);
        assert_eq!(decoded.to_bytes().expect("re-encode"), bytes);
        assert_eq!(TupleKey::from_bytes(&[]).expect("empty"), key(vec![]));
    }

    /// `-0.0` and `0.0` are one key, and so is every `NaN`.
    #[test]
    fn floats_have_one_encoding() {
        assert_eq!(
            encode(vec![KeyColumn::F64(-0.0)]),
            encode(vec![KeyColumn::F64(0.0)])
        );
        let quiet = encode(vec![KeyColumn::F64(f64::NAN)]);
        for bits in [0x7ff0_0000_0000_0001, 0xfff8_0000_0000_0000, u64::MAX] {
            assert_eq!(encode(vec![KeyColumn::F64(f64::from_bits(bits))]), quiet);
        }
        assert_eq!(&quiet[1..], &CANONICAL_NAN.to_be_bytes());
        let decoded = TupleKey::from_bytes(&quiet).expect("decode");
        let KeyColumn::F64(value) = decoded.columns()[0] else {
            panic!("expected an f64 column");
        };
        assert_eq!(value.to_bits(), CANONICAL_NAN);
    }

    /// Equality and hashing agree with the bytes: `-0.0 == 0.0`, `NaN == NaN`.
    #[test]
    fn equality_and_hash_follow_the_canonical_bytes() {
        use std::collections::HashSet;
        let pairs = [
            (KeyColumn::F64(-0.0), KeyColumn::F64(0.0)),
            (
                KeyColumn::F64(f64::NAN),
                KeyColumn::F64(f64::from_bits(0xfff8_0000_0000_0001)),
            ),
        ];
        for (a, b) in pairs {
            let (a, b) = (
                key(vec![KeyColumn::I64(1), a]),
                key(vec![KeyColumn::I64(1), b]),
            );
            assert_eq!(a.to_bytes().unwrap(), b.to_bytes().unwrap());
            assert_eq!(a, b);
            assert_eq!(HashSet::from([a.clone(), b.clone()]).len(), 1);
        }
        let distinct = [
            key(vec![KeyColumn::F64(1.0)]),
            key(vec![KeyColumn::F64(-1.0)]),
            key(vec![KeyColumn::I64(0)]),
            key(vec![KeyColumn::F64(0.0)]),
            key(vec![KeyColumn::Null]),
        ];
        for (i, a) in distinct.iter().enumerate() {
            for b in &distinct[i + 1..] {
                assert_ne!(a, b);
                assert_ne!(a.to_bytes().unwrap(), b.to_bytes().unwrap());
            }
        }
        assert_eq!(
            HashSet::<&TupleKey>::from_iter(&distinct).len(),
            distinct.len()
        );
    }

    /// Distinct tuples are distinct bytes, column boundaries included.
    #[test]
    fn distinct_tuples_are_distinct_bytes() {
        let tuples = [
            vec![KeyColumn::Str("ab".into())],
            vec![KeyColumn::Str("a".into()), KeyColumn::Str("b".into())],
            vec![KeyColumn::Null],
            vec![KeyColumn::Bool(false)],
            vec![KeyColumn::I64(0)],
            vec![KeyColumn::F64(0.0)],
            vec![KeyColumn::Str(String::new())],
            vec![KeyColumn::I64(0), KeyColumn::Null],
            vec![KeyColumn::Null, KeyColumn::I64(0)],
        ];
        let encoded: Vec<Vec<u8>> = tuples.iter().cloned().map(encode).collect();
        for (i, a) in encoded.iter().enumerate() {
            for b in &encoded[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }

    #[test]
    fn non_canonical_bytes_are_rejected() {
        let mut negative_zero = vec![TAG_F64];
        negative_zero.extend_from_slice(&(-0.0f64).to_bits().to_be_bytes());
        let mut other_nan = vec![TAG_F64];
        other_nan.extend_from_slice(&0x7ff0_0000_0000_0001u64.to_be_bytes());
        let cases: [(&[u8], &str); 7] = [
            (&negative_zero, "not canonical"),
            (&other_nan, "not canonical"),
            (&[0x06], "unknown"),
            (&[TAG_I64, 0, 0, 0], "cut short"),
            (&[TAG_STR, 0, 0, 0], "cut short"),
            (&[TAG_STR, 0, 0, 0, 3, b'a', b'b'], "cut short"),
            (&[TAG_STR, 0, 0, 0, 1, 0xff], "not UTF-8"),
        ];
        for (bytes, expected) in cases {
            let error = TupleKey::from_bytes(bytes).expect_err("must be rejected");
            assert!(error.contains(expected), "{bytes:02x?}: {error}");
        }
    }
}
