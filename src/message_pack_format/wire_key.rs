//! The msgpack form of a byte-array key and of a string key.
//!
//! ASAPv1's `key_type` names the exact key variant and the decoder reads the
//! `keys` array **as** that type (`docs/asapv1_wire_format.md` §3.5). msgpack's
//! own decode does not separate `bin` from `str` in either direction:
//! rmp-serde forwards `deserialize_bytes`, `deserialize_str` and
//! `deserialize_string` to `deserialize_any`, which dispatches on the marker
//! alone, and serde's built-in visitors then take the other family — `Vec<u8>`
//! accepts a `str`, and `String` accepts any `bin` holding valid UTF-8. A
//! `Bytes` key and a `String` key are different keys, so a relabelled payload
//! would decode into the wrong variant and then answer `0` for every key it
//! holds.
//!
//! [`WireBytes`] and [`WireString`] close both halves: each writes its own
//! msgpack family and its visitor refuses the other, so neither a `str`-keyed
//! payload relabelled `"bytes"` nor a UTF-8 `bin`-keyed payload relabelled
//! `"string"` decodes.
//!
//! [`exact`] and [`exact_vec`] read an `f64` from float64 (`0xcb`) alone and an
//! `f32` from float32 (`0xca`) alone (`docs/asapv1_wire_format.md` Section 4,
//! Float fields).

use std::fmt;

use serde::de::{Deserialize, Deserializer, Error, Unexpected, Visitor};
use serde::ser::{Serialize, Serializer};

/// A byte-array key on the wire: msgpack `bin`, and `bin` only.
#[derive(Debug)]
pub(crate) struct WireBytes(pub(crate) Vec<u8>);

impl WireBytes {
    /// Takes the bytes back out.
    pub(crate) fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

impl Serialize for WireBytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for WireBytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BytesOnly;

        impl<'de> Visitor<'de> for BytesOnly {
            type Value = WireBytes;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a msgpack bin value")
            }

            fn visit_bytes<E: Error>(self, value: &[u8]) -> Result<Self::Value, E> {
                Ok(WireBytes(value.to_vec()))
            }

            fn visit_byte_buf<E: Error>(self, value: Vec<u8>) -> Result<Self::Value, E> {
                Ok(WireBytes(value))
            }

            fn visit_str<E: Error>(self, _: &str) -> Result<Self::Value, E> {
                Err(E::custom(
                    "ASAPv1: a bytes key must be msgpack bin, not str",
                ))
            }
        }

        deserializer.deserialize_bytes(BytesOnly)
    }
}

/// A text key on the wire: msgpack `str`, and `str` only.
#[derive(Debug)]
pub(crate) struct WireString(pub(crate) String);

impl WireString {
    /// Takes the string back out.
    pub(crate) fn into_string(self) -> String {
        self.0
    }
}

impl Serialize for WireString {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for WireString {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct StrOnly;

        impl<'de> Visitor<'de> for StrOnly {
            type Value = WireString;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a msgpack str value")
            }

            fn visit_str<E: Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(WireString(value.to_string()))
            }

            fn visit_string<E: Error>(self, value: String) -> Result<Self::Value, E> {
                Ok(WireString(value))
            }

            fn visit_bytes<E: Error>(self, _: &[u8]) -> Result<Self::Value, E> {
                Err(E::custom(
                    "ASAPv1: a string key must be msgpack str, not bin",
                ))
            }

            fn visit_byte_buf<E: Error>(self, _: Vec<u8>) -> Result<Self::Value, E> {
                Err(E::custom(
                    "ASAPv1: a string key must be msgpack str, not bin",
                ))
            }
        }

        deserializer.deserialize_str(StrOnly)
    }
}

/// Deserializes a bare `f64` from msgpack float64 alone and a bare `f32` from
/// float32 alone; any other `T`, floats inside an `Option`, tuple or sequence
/// included, deserializes unchanged.
pub(crate) fn exact<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(ExactFloats(deserializer))
}

/// [`exact`] for every element of an array.
pub(crate) fn exact_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    let elements = Vec::<Exact<T>>::deserialize(deserializer)?;
    Ok(elements.into_iter().map(|element| element.0).collect())
}

/// One array element read through [`exact`].
struct Exact<T>(T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Exact<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        exact(deserializer).map(Exact)
    }
}

/// Forwards every request to the wrapped deserializer, and narrows the `f64`
/// and `f32` requests to the matching msgpack float type.
struct ExactFloats<D>(D);

macro_rules! forward {
    ($($method:ident($($arg:ident: $ty:ty),*);)*) => {$(
        fn $method<V: Visitor<'de>>(self, $($arg: $ty,)* visitor: V) -> Result<V::Value, D::Error> {
            self.0.$method($($arg,)* visitor)
        }
    )*};
}

impl<'de, D: Deserializer<'de>> Deserializer<'de> for ExactFloats<D> {
    type Error = D::Error;

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        self.0.deserialize_f64(Float64Only(visitor))
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        self.0.deserialize_f32(Float32Only(visitor))
    }

    fn is_human_readable(&self) -> bool {
        self.0.is_human_readable()
    }

    forward! {
        deserialize_any();
        deserialize_bool();
        deserialize_i8();
        deserialize_i16();
        deserialize_i32();
        deserialize_i64();
        deserialize_i128();
        deserialize_u8();
        deserialize_u16();
        deserialize_u32();
        deserialize_u64();
        deserialize_u128();
        deserialize_char();
        deserialize_str();
        deserialize_string();
        deserialize_bytes();
        deserialize_byte_buf();
        deserialize_option();
        deserialize_unit();
        deserialize_unit_struct(name: &'static str);
        deserialize_newtype_struct(name: &'static str);
        deserialize_seq();
        deserialize_tuple(len: usize);
        deserialize_tuple_struct(name: &'static str, len: usize);
        deserialize_map();
        deserialize_struct(name: &'static str, fields: &'static [&'static str]);
        deserialize_enum(name: &'static str, variants: &'static [&'static str]);
        deserialize_identifier();
        deserialize_ignored_any();
    }
}

/// Passes a float64 to the wrapped visitor and refuses every other value.
struct Float64Only<V>(V);

impl<'de, V: Visitor<'de>> Visitor<'de> for Float64Only<V> {
    type Value = V::Value;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a msgpack float64 value")
    }

    fn visit_f64<E: Error>(self, value: f64) -> Result<Self::Value, E> {
        self.0.visit_f64(value)
    }

    fn visit_f32<E: Error>(self, value: f32) -> Result<Self::Value, E> {
        Err(E::invalid_type(Unexpected::Float(value.into()), &self))
    }
}

/// Passes a float32 to the wrapped visitor and refuses every other value.
struct Float32Only<V>(V);

impl<'de, V: Visitor<'de>> Visitor<'de> for Float32Only<V> {
    type Value = V::Value;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a msgpack float32 value")
    }

    fn visit_f32<E: Error>(self, value: f32) -> Result<Self::Value, E> {
        self.0.visit_f32(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::HydraCounter;
    use crate::message_pack_format::envelope::{self, hex};
    use crate::*;

    fn float64(value: f64) -> Vec<u8> {
        [&[0xcb][..], &value.to_be_bytes()].concat()
    }

    fn float32(value: f32) -> Vec<u8> {
        [&[0xca][..], &value.to_be_bytes()].concat()
    }

    fn read<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, rmp_serde::decode::Error> {
        exact(&mut rmp_serde::Deserializer::new(bytes))
    }

    #[test]
    fn exact_reads_each_float_width_from_its_own_marker() {
        assert_eq!(read::<f64>(&float64(1.5)).expect("float64"), 1.5);
        assert!(read::<f64>(&float32(1.5)).is_err());
        assert!(read::<f64>(&[0x01]).is_err());
        assert_eq!(read::<f32>(&float32(1.5)).expect("float32"), 1.5);
        assert!(read::<f32>(&float64(1.5)).is_err());
        assert!(read::<f32>(&[0x01]).is_err());
        assert_eq!(read::<i64>(&[0x01]).expect("integer"), 1);
    }

    /// The msgpack form a float is rewritten into.
    #[derive(Clone, Copy, Debug)]
    enum Form {
        /// The value as a fixint, in place of its float64.
        Int,
        /// The value as a float32, in place of its float64.
        F32,
        /// The value as a float64, in place of its float32.
        F64,
    }
    use Form::{F32, F64, Int};

    /// A label, a valid encoding, the same encoding with one float rewritten,
    /// and whether bytes decode.
    type Row = (String, Vec<u8>, Vec<u8>, fn(&[u8]) -> bool);

    /// Re-frames `bytes` with the first encoding of `value` in the metadata
    /// block, or else in the payload block, rewritten into `form`.
    fn row(label: &str, bytes: &[u8], value: f64, form: Form, decodes: fn(&[u8]) -> bool) -> Row {
        let (from, to) = match form {
            Int => (float64(value), vec![value as i8 as u8]),
            F32 => (float64(value), float32(value as f32)),
            F64 => (float32(value as f32), float64(value)),
        };
        let (kind_id, metadata, payload) = envelope::split(bytes).expect("split");
        let find = |block: &[u8]| block.windows(from.len()).position(|window| window == from);
        let swap =
            |block: &[u8], at: usize| [&block[..at], &to, &block[at + from.len()..]].concat();
        let rewritten = match (find(metadata), find(payload)) {
            (Some(at), _) => envelope::encode(kind_id, &swap(metadata, at), payload),
            (None, Some(at)) => envelope::encode(kind_id, metadata, &swap(payload, at)),
            (None, None) => panic!("{label}: {value} is not in the frame"),
        };
        (
            format!("{label} as {form:?}"),
            bytes.to_vec(),
            rewritten,
            decodes,
        )
    }

    #[test]
    fn float_fields_reject_an_integer_and_the_other_float_width() {
        type CmsF64 = CountMin<Vector2D<f64>, FastPath>;
        type HeapF64 = CMSHeap<Vector2D<f64>, RegularPath>;
        let mut positive = DDSketch::new(0.25);
        let mut signed = DDSketch::new(0.25);
        let mut kll = KLL::<f64>::init_kll_with_seed(200, 1);
        let mut heap = HeapF64::from_storage(Vector2D::from_fn(2, 4, |r, c| (r * 4 + c) as f64), 4);
        let mut space_saving: SpaceSaving = SpaceSaving::with_capacity(4);
        let mut univmon = UnivMon::init_univmon(4, 2, 16, 3);
        let mut hydra = Hydra::with_schema(2, 8, ["region"], HydraCounter::KLL(KLL::default()))
            .expect("schema");
        for value in [1.0, 2.0] {
            positive.add(&value);
            signed.add(&-value);
        }
        for value in [1.0, 2.5, 7.0] {
            kll.update(&value);
        }
        heap.heap_mut().update(&DataInput::F32(1.5), 11);
        heap.heap_mut().update(&DataInput::F32(3.0), 5);
        space_saving.insert(&DataInput::F64(2.5));
        univmon.insert(&DataInput::F64(2.5), 2);
        hydra
            .update(&["eu"], &DataInput::F64(7.0), None)
            .expect("arity");

        let dd = &positive.serialize_to_bytes().unwrap();
        let sdd = &signed.serialize_to_bytes().unwrap();
        let hip = &hex(include_str!("../../asapv1_golden/hll_hip_p12.hex"));
        let cms = &hex(include_str!("../../asapv1_golden/cms_f64_fast_2x3.hex"));
        let cms_delta = &hex(include_str!(
            "../../asapv1_golden/cms_f64_fast_2x3_delta.hex"
        ));
        let cs = &hex(include_str!("../../asapv1_golden/cs_f64_fast_2x4.hex"));
        let cs_delta = &hex(include_str!(
            "../../asapv1_golden/cs_f64_fast_2x4_delta.hex"
        ));
        let kll = &kll.serialize_to_bytes().unwrap();
        let heap = &heap.serialize_to_bytes().unwrap();
        let ss = &space_saving.serialize_to_bytes().unwrap();
        let um = &univmon.serialize_to_bytes().unwrap();
        let hy = &hydra.serialize_to_bytes().unwrap();

        let dd_ok = |b: &[u8]| DDSketch::deserialize_from_bytes(b).is_ok();
        let hip_ok = |b: &[u8]| HyperLogLogHIPP12::deserialize_from_bytes(b).is_ok();
        let cms_ok = |b: &[u8]| CmsF64::deserialize_from_bytes(b).is_ok();
        let delta_ok = |b: &[u8]| CountMinDelta::<f64, FastPath>::deserialize_from_bytes(b).is_ok();
        let cs_ok = |b: &[u8]| Count::<Vector2D<f64>, FastPath>::deserialize_from_bytes(b).is_ok();
        let cs_delta_ok = |b: &[u8]| {
            sketches::CountSketchDelta::<f64, FastPath>::deserialize_from_bytes(b).is_ok()
        };
        let kll_ok = |b: &[u8]| KLL::<f64>::deserialize_from_bytes(b).is_ok();
        let heap_ok = |b: &[u8]| HeapF64::deserialize_from_bytes(b).is_ok();
        let ss_ok = |b: &[u8]| SpaceSaving::<DefaultXxHasher>::deserialize_from_bytes(b).is_ok();
        let um_ok = |b: &[u8]| UnivMon::deserialize_from_bytes(b).is_ok();
        let hy_ok = |b: &[u8]| Hydra::deserialize_from_bytes(b).is_ok();

        let rows = vec![
            row("DDSketch alpha", dd, 0.25, F32, dd_ok),
            row("DDSketch sum", dd, 3.0, Int, dd_ok),
            row("DDSketch min", dd, 1.0, Int, dd_ok),
            row("DDSketch max", dd, 2.0, F32, dd_ok),
            row("DDSketch signed sum", sdd, -3.0, Int, dd_ok),
            row("DDSketch signed min", sdd, -2.0, Int, dd_ok),
            row("DDSketch signed max", sdd, -1.0, F32, dd_ok),
            row("HLL hip_kxq0", hip, 1.5, F32, hip_ok),
            row("HLL hip_kxq1", hip, 2.5, F32, hip_ok),
            row("HLL hip_est", hip, 3.0, Int, hip_ok),
            row("CMS f64 count", cms, 1.5, F32, cms_ok),
            row("CMS f64 delta", cms_delta, 2.5, Int, delta_ok),
            row("CS f64 count", cs, -2.25, F32, cs_ok),
            row("CS f64 delta", cs_delta, 2.25, Int, cs_delta_ok),
            row("KLL f64 item", kll, 7.0, Int, kll_ok),
            row("CMSHeap f64 count", heap, 1.0, Int, heap_ok),
            row("CMSHeap f32 key", heap, 1.5, F64, heap_ok),
            row("SpaceSaving f64 key", ss, 2.5, F32, ss_ok),
            row("UnivMon f64 key", um, 2.5, F32, um_ok),
            row("Hydra KLL item", hy, 7.0, Int, hy_ok),
        ];
        #[cfg(feature = "experimental")]
        let rows = {
            let mut rows = rows;
            let mut uniform = UniformSampling::with_seed(1.0, 7);
            uniform.update(2.5);
            let us = &uniform.serialize_to_bytes().unwrap();
            let us_ok = |b: &[u8]| UniformSampling::deserialize_from_bytes(b).is_ok();
            rows.push(row("Uniform sample_rate", us, 1.0, Int, us_ok));
            rows.push(row("Uniform value", us, 2.5, F32, us_ok));
            rows
        };

        for (label, bytes, rewritten, decodes) in rows {
            assert!(decodes(&bytes), "{label}: the valid encoding must decode");
            assert!(!decodes(&rewritten), "{label}: must be rejected");
        }
    }
}
