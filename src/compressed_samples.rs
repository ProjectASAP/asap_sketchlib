//! Compressed samples: every sample of one time series in one window, as one
//! block in a portable byte format whose first byte names its codec.
//!
//! - **Codec 0, Gorilla (XOR).** Timestamps as deltas of deltas, values as
//!   the XOR of their `f64` bits with the previous value's. Lossless on any
//!   bit pattern (NaN payloads, signed zeros, infinities).
//! - **Codec 1, decimal + zstd.** For blocks whose values are all short
//!   decimals `m / 10^D`: zigzag LEB128 varints of the timestamps' deltas of
//!   deltas and of the mantissas' deltas (or deltas of deltas), in one zstd
//!   frame.
//!
//! A block being built ([`CompressedSamples::push`]) is always codec 0, one
//! sample appended at a time. [`CompressedSamples::seal`] re-encodes it with
//! the shorter codec. The bytes ([`CompressedSamples::to_bytes`]) are what a
//! store keeps and a reader decodes; their layout is fixed to the bit so that
//! every implementation writes the same bytes:
//!
//! - codec 0 header, 25 bytes, little-endian: codec `0` (`u8`), sample count
//!   `u32`, first timestamp `i64` (ms), first value's `f64` bits `u64`,
//!   payload length in bits `u32`; then the payload, most significant bit
//!   first, the last byte zero-padded. For each sample after the first, its
//!   timestamp code then its value code:
//!   - timestamp: `d = t - t_prev` (the first delta taken against
//!     `d_prev = 0`), `dod = d - d_prev`: `0` -> `0`; [-64, 63] -> `10` + 7
//!     bits; [-256, 255] -> `110` + 9; [-2048, 2047] -> `1110` + 12;
//!     otherwise `1111` + 64; two's complement;
//!   - value: `x = bits(v) ^ bits(v_prev)`: `0` -> `0`; else
//!     `lead = min(leading_zeros(x), 31)`, `trail = trailing_zeros(x)`; if a
//!     window `(L, T)` exists with `lead >= L` and `trail >= T` -> `10` + the
//!     `64 - L - T` bits `x >> T`; else `11` + `lead` (5 bits) + `width - 1`
//!     (6 bits, `width = 64 - lead - trail`) + the `width` bits `x >> trail`,
//!     and `(lead, trail)` becomes the window.
//!
//!   An empty block is 25 zero bytes.
//! - codec 1 header, 19 bytes: codec `1`, sample count `u32`, first timestamp
//!   `i64`, digits `D` (`u8`), mantissa order (`u8`: 1 deltas, 2 deltas of
//!   deltas), frame length `u32`; then one zstd frame (written at level 1,
//!   read at any level) of the varint of each timestamp's delta of delta
//!   after the first (as codec 0 computes it), then the varint of every
//!   sample's mantissa delta (order 1, from `m_prev = 0`) or delta of delta
//!   (order 2, from a previous delta of 0). A block is eligible when every
//!   value is finite, not -0, and equals `m / 10^D` exactly (`f64` division)
//!   for integers `|m| < 2^53` and one `D <= 17`, `D` the largest of the
//!   values' smallest digit counts; the shorter order is written, order 1 on
//!   a tie.
//!
//! Samples are kept in timestamp order: a pushed sample older than the newest
//! is inserted in place (the block is re-encoded), and one whose timestamp is
//! already present replaces that sample's value (the last write wins).

use std::fmt;

/// A block's codec: its first byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SampleCodec {
    /// Delta-of-delta timestamps and XOR values, bit-packed. Holds any
    /// `f64` bit pattern.
    Gorilla = 0,
    /// Varint deltas of decimal mantissas in a zstd frame. Holds only blocks
    /// of short decimals (finite, not -0, `m / 10^D` exactly).
    DecimalZstd = 1,
}

impl SampleCodec {
    /// The header's length in bytes: 25 for `Gorilla`, 19 for
    /// `DecimalZstd`.
    pub const fn header_bytes(self) -> usize {
        match self {
            SampleCodec::Gorilla => GORILLA_HEADER_BYTES,
            SampleCodec::DecimalZstd => DECIMAL_HEADER_BYTES,
        }
    }
}

/// Why bytes are not a compressed-samples block. Always bad input: no block
/// [`CompressedSamples::to_bytes`] writes is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressedSamplesFormatError {
    /// Fewer bytes than the codec's header (or none at all).
    Truncated,
    /// The bytes after the header are not as long as the header says, or a
    /// codec-0 payload's codes do not use exactly its stated bits.
    PayloadLengthMismatch,
    /// A sample count of zero with payload bits or a frame.
    PayloadWithoutSamples,
    /// A first byte that names no codec.
    UnknownCodec,
    /// The payload does not decode to a block: not a zstd frame, malformed
    /// codes or varints, a value code without a previous window, digits over
    /// 17, an order other than 1 or 2, or timestamps not strictly increasing.
    InvalidFrame,
}

impl fmt::Display for CompressedSamplesFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = match self {
            Self::Truncated => "shorter than its codec's header",
            Self::PayloadLengthMismatch => "payload length disagrees with the header",
            Self::PayloadWithoutSamples => "payload without samples",
            Self::UnknownCodec => "unknown codec",
            Self::InvalidFrame => "payload does not decode to a block",
        };
        write!(f, "compressed samples: {reason}")
    }
}

impl std::error::Error for CompressedSamplesFormatError {}

const GORILLA_HEADER_BYTES: usize = 25;
const DECIMAL_HEADER_BYTES: usize = 19;
/// The zstd level codec 1 is written at.
const ZSTD_LEVEL: i32 = 1;
/// The largest number of decimal digits codec 1 holds.
const MAX_DIGITS: u8 = 17;
/// Mantissas must be integers exactly representable in an `f64`.
const MAX_MANTISSA: f64 = 9_007_199_254_740_992.0; // 2^53
/// Powers of ten up to 10^17, all exact in an `f64`.
const POW10: [f64; 18] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17,
];

/// Every sample of one series in one window, as one block of either codec.
#[derive(Debug, Clone, PartialEq)]
pub struct CompressedSamples {
    codec: SampleCodec,
    len: u32,
    first_timestamp_ms: i64,
    body: Body,
}

/// A block's codec-specific part.
#[derive(Debug, Clone, PartialEq)]
enum Body {
    Gorilla(GorillaBlock),
    Decimal(DecimalBlock),
}

/// A codec-0 block: its first value, its bit payload and the state the next
/// appended sample is coded against.
#[derive(Debug, Clone, PartialEq)]
struct GorillaBlock {
    first_value_bits: u64,
    payload: BitWriter,
    last_timestamp_ms: i64,
    last_delta_ms: i64,
    last_value_bits: u64,
    /// `(leading, trailing)` zeros of the last new XOR window.
    window: Option<(u32, u32)>,
}

/// A codec-1 block: its digits, mantissa order and zstd frame.
#[derive(Debug, Clone, PartialEq)]
struct DecimalBlock {
    digits: u8,
    order: u8,
    frame: Vec<u8>,
}

impl Default for CompressedSamples {
    fn default() -> Self {
        Self::new()
    }
}

impl CompressedSamples {
    /// An empty codec-0 block.
    pub fn new() -> Self {
        Self {
            codec: SampleCodec::Gorilla,
            len: 0,
            first_timestamp_ms: 0,
            body: Body::Gorilla(GorillaBlock {
                first_value_bits: 0,
                payload: BitWriter::default(),
                last_timestamp_ms: 0,
                last_delta_ms: 0,
                last_value_bits: 0,
                window: None,
            }),
        }
    }

    /// Appends a sample to the block, which stays (or becomes) codec 0. A
    /// sample newer than the newest is appended in O(1); an older one, or
    /// one whose timestamp is already present (its value replaced), re-encodes
    /// the block in O(n).
    pub fn push(&mut self, timestamp_ms: i64, value: f64) {
        if let Body::Gorilla(block) = &mut self.body {
            if self.len == 0 {
                block.first_value_bits = value.to_bits();
                block.last_value_bits = value.to_bits();
                block.last_timestamp_ms = timestamp_ms;
                self.first_timestamp_ms = timestamp_ms;
                self.len = 1;
                return;
            }
            if timestamp_ms > block.last_timestamp_ms {
                block.append(timestamp_ms, value.to_bits());
                self.len += 1;
                return;
            }
        }
        let mut samples: Vec<(i64, f64)> = self.iter().collect();
        match samples.binary_search_by_key(&timestamp_ms, |&(t, _)| t) {
            Ok(index) => samples[index].1 = value,
            Err(index) => samples.insert(index, (timestamp_ms, value)),
        }
        *self = encode_gorilla(&samples);
    }

    /// The block re-encoded with the shorter codec: codec 0, or codec 1 when
    /// the block is eligible and strictly shorter.
    pub fn seal(&self) -> Self {
        let samples: Vec<(i64, f64)> = self.iter().collect();
        seal_samples(&samples)
    }

    /// The block re-encoded with `codec`; `None` when the samples are not
    /// eligible for it (codec 1: empty, or not all short decimals).
    pub fn encoded_with(&self, codec: SampleCodec) -> Option<Self> {
        let samples: Vec<(i64, f64)> = self.iter().collect();
        match codec {
            SampleCodec::Gorilla => Some(encode_gorilla(&samples)),
            SampleCodec::DecimalZstd => encode_decimal(&samples),
        }
    }

    /// The block's codec.
    pub fn codec(&self) -> SampleCodec {
        self.codec
    }

    /// The number of samples.
    pub fn len(&self) -> u32 {
        self.len
    }

    /// Whether the block holds no sample.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Bits after the header: the exact payload bits for codec 0, eight per
    /// frame byte for codec 1.
    pub fn payload_bits(&self) -> u64 {
        match &self.body {
            Body::Gorilla(block) => block.payload.bits,
            Body::Decimal(block) => 8 * block.frame.len() as u64,
        }
    }

    /// The block's length in bytes, `codec().header_bytes() +
    /// ceil(payload_bits / 8)`: the length of [`Self::to_bytes`].
    pub fn encoded_bytes(&self) -> usize {
        self.codec.header_bytes() + self.payload_bits().div_ceil(8) as usize
    }

    /// The samples in timestamp order. Codec 0 decodes as it goes; codec 1
    /// decompresses its frame once when the iterator is made.
    pub fn iter(&self) -> SamplesIter<'_> {
        match &self.body {
            Body::Gorilla(block) => {
                SamplesIter(SamplesIterInner::Gorilla(GorillaIter::new(self, block)))
            }
            Body::Decimal(block) => {
                SamplesIter(SamplesIterInner::Decimal(DecimalIter::new(self, block)))
            }
        }
    }

    /// The samples with `start_exclusive_ms < t <= end_inclusive_ms`, in
    /// timestamp order.
    pub fn samples_in(
        &self,
        start_exclusive_ms: i64,
        end_inclusive_ms: i64,
    ) -> impl Iterator<Item = (i64, f64)> + '_ {
        self.iter()
            .skip_while(move |&(t, _)| t <= start_exclusive_ms)
            .take_while(move |&(t, _)| t <= end_inclusive_ms)
    }

    /// The sealed block of both blocks' samples; on an equal timestamp the
    /// value of `other` wins. O(n + m) plus the seal.
    pub fn merge(&self, other: &Self) -> Self {
        let mut first = self.iter().peekable();
        let mut second = other.iter().peekable();
        let mut union = Vec::with_capacity((self.len + other.len) as usize);
        loop {
            match (first.peek(), second.peek()) {
                (Some(&a), Some(&b)) => {
                    if a.0 < b.0 {
                        union.push(a);
                        first.next();
                    } else {
                        if a.0 == b.0 {
                            first.next();
                        }
                        union.push(b);
                        second.next();
                    }
                }
                (Some(_), None) => union.extend(first.by_ref()),
                (None, Some(_)) => union.extend(second.by_ref()),
                (None, None) => break,
            }
        }
        seal_samples(&union)
    }

    /// The block's portable bytes (the module doc's layout).
    ///
    /// # Panics
    ///
    /// If a codec-0 payload exceeds `u32::MAX` bits or a codec-1 frame
    /// `u32::MAX` bytes (a block of hundreds of millions of samples).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.encoded_bytes());
        bytes.push(self.codec as u8);
        bytes.extend(self.len.to_le_bytes());
        bytes.extend(self.first_timestamp_ms.to_le_bytes());
        match &self.body {
            Body::Gorilla(block) => {
                bytes.extend(block.first_value_bits.to_le_bytes());
                let bits = u32::try_from(block.payload.bits).expect("payload over u32::MAX bits");
                bytes.extend(bits.to_le_bytes());
                bytes.extend(&block.payload.bytes);
            }
            Body::Decimal(block) => {
                bytes.push(block.digits);
                bytes.push(block.order);
                let length = u32::try_from(block.frame.len()).expect("frame over u32::MAX bytes");
                bytes.extend(length.to_le_bytes());
                bytes.extend(&block.frame);
            }
        }
        bytes
    }

    /// Reads a block from its bytes, validating all of them: the codec, the
    /// lengths, and that the payload decodes to exactly the stated samples
    /// in strictly increasing time. Never panics; O(n).
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CompressedSamplesFormatError> {
        use CompressedSamplesFormatError as E;
        let codec = match bytes.first() {
            None => return Err(E::Truncated),
            Some(0) => SampleCodec::Gorilla,
            Some(1) => SampleCodec::DecimalZstd,
            Some(_) => return Err(E::UnknownCodec),
        };
        if bytes.len() < codec.header_bytes() {
            return Err(E::Truncated);
        }
        let len = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
        let first_timestamp_ms = i64::from_le_bytes(bytes[5..13].try_into().unwrap());
        match codec {
            SampleCodec::Gorilla => {
                let first_value_bits = u64::from_le_bytes(bytes[13..21].try_into().unwrap());
                let bits = u64::from(u32::from_le_bytes(bytes[21..25].try_into().unwrap()));
                let payload = &bytes[GORILLA_HEADER_BYTES..];
                if payload.len() as u64 != bits.div_ceil(8) {
                    return Err(E::PayloadLengthMismatch);
                }
                if len == 0 {
                    return if bits > 0 {
                        Err(E::PayloadWithoutSamples)
                    } else {
                        Ok(Self::new())
                    };
                }
                let block =
                    validate_gorilla(len, first_timestamp_ms, first_value_bits, payload, bits)?;
                Ok(Self {
                    codec,
                    len,
                    first_timestamp_ms,
                    body: Body::Gorilla(block),
                })
            }
            SampleCodec::DecimalZstd => {
                let digits = bytes[13];
                let order = bytes[14];
                let frame_length = u32::from_le_bytes(bytes[15..19].try_into().unwrap()) as usize;
                let frame = &bytes[DECIMAL_HEADER_BYTES..];
                if frame.len() != frame_length {
                    return Err(E::PayloadLengthMismatch);
                }
                if len == 0 {
                    return Err(E::PayloadWithoutSamples);
                }
                if digits > MAX_DIGITS || !(1..=2).contains(&order) {
                    return Err(E::InvalidFrame);
                }
                let block = DecimalBlock {
                    digits,
                    order,
                    frame: frame.to_vec(),
                };
                let content = decompress(len, &block.frame).ok_or(E::InvalidFrame)?;
                let mut reader = VarintReader::new(&content);
                let mut timestamps = TimestampDecoder::new(first_timestamp_ms);
                let mut previous = first_timestamp_ms;
                for _ in 1..len {
                    let t = timestamps.next(reader.read().ok_or(E::InvalidFrame)?);
                    if t <= previous {
                        return Err(E::InvalidFrame);
                    }
                    previous = t;
                }
                for _ in 0..len {
                    reader.read().ok_or(E::InvalidFrame)?;
                }
                if !reader.at_end() {
                    return Err(E::InvalidFrame);
                }
                Ok(Self {
                    codec,
                    len,
                    first_timestamp_ms,
                    body: Body::Decimal(block),
                })
            }
        }
    }
}

impl<'a> IntoIterator for &'a CompressedSamples {
    type Item = (i64, f64);
    type IntoIter = SamplesIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// The samples of a block, in timestamp order ([`CompressedSamples::iter`]).
pub struct SamplesIter<'a>(SamplesIterInner<'a>);

enum SamplesIterInner<'a> {
    Gorilla(GorillaIter<'a>),
    Decimal(DecimalIter),
}

impl Iterator for SamplesIter<'_> {
    type Item = (i64, f64);

    fn next(&mut self) -> Option<(i64, f64)> {
        match &mut self.0 {
            SamplesIterInner::Gorilla(iter) => iter.next(),
            SamplesIterInner::Decimal(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.0 {
            SamplesIterInner::Gorilla(iter) => iter.size_hint(),
            SamplesIterInner::Decimal(iter) => iter.size_hint(),
        }
    }
}

// ---------------------------------------------------------------------------
// Codec 0.

/// A growable MSB-first bit string.
#[derive(Debug, Clone, Default, PartialEq)]
struct BitWriter {
    bytes: Vec<u8>,
    bits: u64,
}

impl BitWriter {
    /// Appends the low `count` (at most 64) bits of `value`, most significant
    /// first: the partial last byte and the new bits are packed in one `u128`
    /// and written out a byte at a time.
    fn write(&mut self, value: u64, count: u32) {
        if count == 0 {
            return;
        }
        let value = if count == 64 {
            value
        } else {
            value & ((1u64 << count) - 1)
        };
        let used = (self.bits % 8) as u32;
        let mut packed = u128::from(value) << (128 - used - count);
        if used > 0 {
            packed |= u128::from(self.bytes.pop().unwrap_or(0)) << 120;
        }
        let bytes = (used + count).div_ceil(8) as usize;
        self.bytes.extend_from_slice(&packed.to_be_bytes()[..bytes]);
        self.bits += u64::from(count);
    }
}

/// Reads an MSB-first bit string; reading past its end yields `None`.
struct BitReader<'a> {
    bytes: &'a [u8],
    bits: u64,
    position: u64,
}

impl<'a> BitReader<'a> {
    fn new(bytes: &'a [u8], bits: u64) -> Self {
        Self {
            bytes,
            bits,
            position: 0,
        }
    }

    /// The next `count` (at most 64) bits, from the (up to) 9 bytes they
    /// span loaded as one big-endian `u128`.
    fn read(&mut self, count: u32) -> Option<u64> {
        if count == 0 {
            return Some(0);
        }
        if self.position + u64::from(count) > self.bits {
            return None;
        }
        let start = (self.position / 8) as usize;
        let offset = (self.position % 8) as u32;
        let word = match self.bytes.get(start..start + 16) {
            Some(window) => u128::from_be_bytes(window.try_into().unwrap()),
            None => {
                let mut window = [0u8; 16];
                let tail = &self.bytes[start..];
                window[..tail.len()].copy_from_slice(tail);
                u128::from_be_bytes(window)
            }
        };
        self.position += u64::from(count);
        Some(((word << offset) >> (128 - count)) as u64)
    }

    fn bit(&mut self) -> Option<bool> {
        self.read(1).map(|bit| bit == 1)
    }
}

/// Sign-extends the low `bits` bits of `value`.
fn sign_extend(value: u64, bits: u32) -> i64 {
    let shift = 64 - bits;
    ((value << shift) as i64) >> shift
}

impl GorillaBlock {
    /// Codes one sample newer than the last.
    fn append(&mut self, timestamp_ms: i64, value_bits: u64) {
        let delta = timestamp_ms.wrapping_sub(self.last_timestamp_ms);
        let dod = delta.wrapping_sub(self.last_delta_ms);
        let out = &mut self.payload;
        match dod {
            0 => out.write(0, 1),
            -64..=63 => {
                out.write(0b10, 2);
                out.write(dod as u64, 7);
            }
            -256..=255 => {
                out.write(0b110, 3);
                out.write(dod as u64, 9);
            }
            -2048..=2047 => {
                out.write(0b1110, 4);
                out.write(dod as u64, 12);
            }
            _ => {
                out.write(0b1111, 4);
                out.write(dod as u64, 64);
            }
        }
        let x = value_bits ^ self.last_value_bits;
        if x == 0 {
            out.write(0, 1);
        } else {
            let lead = x.leading_zeros().min(31);
            let trail = x.trailing_zeros();
            match self.window {
                Some((l, t)) if lead >= l && trail >= t => {
                    out.write(0b10, 2);
                    out.write(x >> t, 64 - l - t);
                }
                _ => {
                    let width = 64 - lead - trail;
                    out.write(0b11, 2);
                    out.write(u64::from(lead), 5);
                    out.write(u64::from(width - 1), 6);
                    out.write(x >> trail, width);
                    self.window = Some((lead, trail));
                }
            }
        }
        self.last_timestamp_ms = timestamp_ms;
        self.last_delta_ms = delta;
        self.last_value_bits = value_bits;
    }
}

/// Decodes a codec-0 payload one sample at a time.
struct GorillaDecoder<'a> {
    reader: BitReader<'a>,
    timestamp_ms: i64,
    delta_ms: i64,
    value_bits: u64,
    window: Option<(u32, u32)>,
}

impl<'a> GorillaDecoder<'a> {
    fn new(payload: &'a [u8], bits: u64, first_timestamp_ms: i64, first_value_bits: u64) -> Self {
        Self {
            reader: BitReader::new(payload, bits),
            timestamp_ms: first_timestamp_ms,
            delta_ms: 0,
            value_bits: first_value_bits,
            window: None,
        }
    }

    /// The next sample; `None` when the codes are malformed or run out.
    fn next_sample(&mut self) -> Option<(i64, u64)> {
        let r = &mut self.reader;
        let dod = if !r.bit()? {
            0
        } else if !r.bit()? {
            sign_extend(r.read(7)?, 7)
        } else if !r.bit()? {
            sign_extend(r.read(9)?, 9)
        } else if !r.bit()? {
            sign_extend(r.read(12)?, 12)
        } else {
            r.read(64)? as i64
        };
        self.delta_ms = self.delta_ms.wrapping_add(dod);
        self.timestamp_ms = self.timestamp_ms.wrapping_add(self.delta_ms);
        if r.bit()? {
            if r.bit()? {
                let lead = r.read(5)? as u32;
                let width = r.read(6)? as u32 + 1;
                if lead + width > 64 {
                    return None;
                }
                let trail = 64 - lead - width;
                self.value_bits ^= r.read(width)? << trail;
                self.window = Some((lead, trail));
            } else {
                let (lead, trail) = self.window?;
                self.value_bits ^= r.read(64 - lead - trail)? << trail;
            }
        }
        Some((self.timestamp_ms, self.value_bits))
    }
}

/// Checks a codec-0 payload decodes to `len` samples in strictly increasing
/// time using exactly `bits` bits, and returns the block with its append
/// state.
fn validate_gorilla(
    len: u32,
    first_timestamp_ms: i64,
    first_value_bits: u64,
    payload: &[u8],
    bits: u64,
) -> Result<GorillaBlock, CompressedSamplesFormatError> {
    use CompressedSamplesFormatError as E;
    let mut decoder = GorillaDecoder::new(payload, bits, first_timestamp_ms, first_value_bits);
    let mut previous = first_timestamp_ms;
    for _ in 1..len {
        let (t, _) = decoder.next_sample().ok_or(E::PayloadLengthMismatch)?;
        if t <= previous {
            return Err(E::InvalidFrame);
        }
        previous = t;
    }
    if decoder.reader.position != bits {
        return Err(E::PayloadLengthMismatch);
    }
    // Padding bits must be zero, so the bytes are the ones `to_bytes` writes.
    if bits % 8 != 0 {
        let last = *payload.last().unwrap();
        if last & ((1u8 << (8 - bits % 8)) - 1) != 0 {
            return Err(E::PayloadLengthMismatch);
        }
    }
    Ok(GorillaBlock {
        first_value_bits,
        payload: BitWriter {
            bytes: payload.to_vec(),
            bits,
        },
        last_timestamp_ms: decoder.timestamp_ms,
        last_delta_ms: decoder.delta_ms,
        last_value_bits: decoder.value_bits,
        window: decoder.window,
    })
}

/// Decodes a codec-0 block's samples lazily.
struct GorillaIter<'a> {
    first: Option<(i64, f64)>,
    decoder: GorillaDecoder<'a>,
    remaining: u32,
}

impl<'a> GorillaIter<'a> {
    fn new(samples: &'a CompressedSamples, block: &'a GorillaBlock) -> Self {
        let first = (samples.len > 0).then(|| {
            (
                samples.first_timestamp_ms,
                f64::from_bits(block.first_value_bits),
            )
        });
        Self {
            first,
            decoder: GorillaDecoder::new(
                &block.payload.bytes,
                block.payload.bits,
                samples.first_timestamp_ms,
                block.first_value_bits,
            ),
            remaining: samples.len.saturating_sub(1),
        }
    }
}

impl Iterator for GorillaIter<'_> {
    type Item = (i64, f64);

    fn next(&mut self) -> Option<(i64, f64)> {
        if let Some(first) = self.first.take() {
            return Some(first);
        }
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        // Blocks are valid by construction, so the decoder does not run out.
        let (t, bits) = self.decoder.next_sample()?;
        Some((t, f64::from_bits(bits)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.remaining as usize + usize::from(self.first.is_some());
        (n, Some(n))
    }
}

/// The codec-0 block of `samples`, in timestamp order without repeats.
fn encode_gorilla(samples: &[(i64, f64)]) -> CompressedSamples {
    let mut block = CompressedSamples::new();
    for &(t, v) in samples {
        block.push(t, v);
    }
    block
}

// ---------------------------------------------------------------------------
// Codec 1.

/// The varint (zigzag LEB128) of `value`, appended to `out`.
fn write_varint(out: &mut Vec<u8>, value: i64) {
    let mut zigzag = ((value << 1) ^ (value >> 63)) as u64;
    loop {
        let byte = (zigzag & 0x7f) as u8;
        zigzag >>= 7;
        if zigzag == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Reads zigzag LEB128 varints; a malformed or truncated one yields `None`.
struct VarintReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> VarintReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn read(&mut self) -> Option<i64> {
        let mut zigzag = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = *self.bytes.get(self.position)?;
            self.position += 1;
            if shift == 63 && byte > 1 {
                return None;
            }
            zigzag |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Some(((zigzag >> 1) as i64) ^ -((zigzag & 1) as i64));
            }
        }
        None
    }

    fn at_end(&self) -> bool {
        self.position == self.bytes.len()
    }
}

/// Turns timestamp deltas of deltas back into timestamps.
struct TimestampDecoder {
    timestamp_ms: i64,
    delta_ms: i64,
}

impl TimestampDecoder {
    fn new(first_timestamp_ms: i64) -> Self {
        Self {
            timestamp_ms: first_timestamp_ms,
            delta_ms: 0,
        }
    }

    fn next(&mut self, dod: i64) -> i64 {
        self.delta_ms = self.delta_ms.wrapping_add(dod);
        self.timestamp_ms = self.timestamp_ms.wrapping_add(self.delta_ms);
        self.timestamp_ms
    }
}

/// The fewest digits `d <= 17` with `value == m / 10^d` exactly for an
/// integer `|m| < 2^53`; `None` when there is none, or for NaN, ±inf, -0.
fn decimal_digits(value: f64) -> Option<u8> {
    if !value.is_finite() || value.to_bits() == (-0.0f64).to_bits() {
        return None;
    }
    (0..=MAX_DIGITS).find(|&d| mantissa(value, d).is_some())
}

/// `m` with `value == m / 10^digits` exactly and `|m| < 2^53`, if any.
fn mantissa(value: f64, digits: u8) -> Option<i64> {
    let scale = POW10[digits as usize];
    let m = (value * scale).round();
    (m.abs() < MAX_MANTISSA && m / scale == value).then_some(m as i64)
}

/// The codec-1 block of `samples` (timestamp order, no repeats); `None` when
/// empty or a value is not a short decimal.
fn encode_decimal(samples: &[(i64, f64)]) -> Option<CompressedSamples> {
    let &(first_timestamp_ms, _) = samples.first()?;
    let mut digits = 0;
    for &(_, v) in samples {
        digits = digits.max(decimal_digits(v)?);
    }
    let mantissas = samples
        .iter()
        .map(|&(_, v)| mantissa(v, digits))
        .collect::<Option<Vec<i64>>>()?;
    let mut content = Vec::with_capacity(samples.len() * 4);
    let mut previous_delta = 0i64;
    for pair in samples.windows(2) {
        let delta = pair[1].0.wrapping_sub(pair[0].0);
        write_varint(&mut content, delta.wrapping_sub(previous_delta));
        previous_delta = delta;
    }
    let mut order_1 = Vec::with_capacity(samples.len() * 2);
    let mut order_2 = Vec::with_capacity(samples.len() * 2);
    let (mut previous, mut previous_delta) = (0i64, 0i64);
    for &m in &mantissas {
        let delta = m.wrapping_sub(previous);
        write_varint(&mut order_1, delta);
        write_varint(&mut order_2, delta.wrapping_sub(previous_delta));
        previous = m;
        previous_delta = delta;
    }
    let order = if order_1.len() <= order_2.len() { 1 } else { 2 };
    content.extend(if order == 1 { order_1 } else { order_2 });
    let frame = zstd::bulk::compress(&content, ZSTD_LEVEL).ok()?;
    Some(CompressedSamples {
        codec: SampleCodec::DecimalZstd,
        len: u32::try_from(samples.len()).ok()?,
        first_timestamp_ms,
        body: Body::Decimal(DecimalBlock {
            digits,
            order,
            frame,
        }),
    })
}

/// The frame's content, at most the longest content `len` samples can have
/// (ten bytes per varint); `None` when it is not a zstd frame within it.
fn decompress(len: u32, frame: &[u8]) -> Option<Vec<u8>> {
    let capacity = (2 * len as usize).saturating_mul(10);
    zstd::bulk::decompress(frame, capacity).ok()
}

/// Decodes a codec-1 block's samples from its decompressed content.
struct DecimalIter {
    content: Vec<u8>,
    /// Where the mantissa varints start.
    mantissa_position: usize,
    timestamp_position: usize,
    first_timestamp_ms: i64,
    timestamps: TimestampDecoder,
    scale: f64,
    order: u8,
    mantissa: i64,
    mantissa_delta: i64,
    produced: u32,
    len: u32,
}

impl DecimalIter {
    fn new(samples: &CompressedSamples, block: &DecimalBlock) -> Self {
        // Blocks are valid by construction or by `from_bytes`.
        let content = decompress(samples.len, &block.frame).unwrap_or_default();
        let mut reader = VarintReader::new(&content);
        for _ in 1..samples.len {
            reader.read();
        }
        let mantissa_position = reader.position;
        Self {
            content,
            mantissa_position,
            timestamp_position: 0,
            first_timestamp_ms: samples.first_timestamp_ms,
            timestamps: TimestampDecoder::new(samples.first_timestamp_ms),
            scale: POW10[block.digits as usize],
            order: block.order,
            mantissa: 0,
            mantissa_delta: 0,
            produced: 0,
            len: samples.len,
        }
    }
}

impl Iterator for DecimalIter {
    type Item = (i64, f64);

    fn next(&mut self) -> Option<(i64, f64)> {
        if self.produced == self.len {
            return None;
        }
        let timestamp_ms = if self.produced == 0 {
            self.first_timestamp_ms
        } else {
            let mut reader = VarintReader {
                bytes: &self.content,
                position: self.timestamp_position,
            };
            let dod = reader.read()?;
            self.timestamp_position = reader.position;
            self.timestamps.next(dod)
        };
        let mut reader = VarintReader {
            bytes: &self.content,
            position: self.mantissa_position,
        };
        let code = reader.read()?;
        self.mantissa_position = reader.position;
        let delta = if self.order == 1 {
            code
        } else {
            self.mantissa_delta.wrapping_add(code)
        };
        self.mantissa_delta = delta;
        self.mantissa = self.mantissa.wrapping_add(delta);
        self.produced += 1;
        Some((timestamp_ms, self.mantissa as f64 / self.scale))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = (self.len - self.produced) as usize;
        (n, Some(n))
    }
}

/// The block of `samples` with the shorter codec, codec 0 on a tie.
fn seal_samples(samples: &[(i64, f64)]) -> CompressedSamples {
    let gorilla = encode_gorilla(samples);
    match encode_decimal(samples) {
        Some(decimal) if decimal.encoded_bytes() < gorilla.encoded_bytes() => decimal,
        _ => gorilla,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Codec 0's dod buckets are two's complement: the edges of each bucket
    /// round-trip, and one past an edge moves to the next bucket's code.
    #[test]
    fn delta_of_delta_bucket_edges_round_trip() {
        let mut t = 0i64;
        let mut delta = 0i64;
        let mut samples = vec![(t, 0.0)];
        for dod in [
            10_000, -64, 63, 64, -65, -256, 255, 256, -2048, 2047, 2048, -2049,
        ] {
            delta += dod;
            t += delta;
            samples.push((t, 0.0));
        }
        let block = encode_gorilla(&samples);
        let decoded: Vec<(i64, f64)> = block.iter().collect();
        assert_eq!(decoded, samples);
    }

    /// Varints round-trip at the extremes and refuse an over-long encoding.
    #[test]
    fn varints_round_trip_and_refuse_overflow() {
        let mut out = Vec::new();
        for v in [0, 1, -1, 63, -64, i64::MAX, i64::MIN] {
            write_varint(&mut out, v);
        }
        let mut reader = VarintReader::new(&out);
        for v in [0, 1, -1, 63, -64, i64::MAX, i64::MIN] {
            assert_eq!(reader.read(), Some(v));
        }
        assert!(reader.at_end());
        let mut overlong = VarintReader::new(&[0xff; 11]);
        assert_eq!(overlong.read(), None);
    }

    /// Short decimals get their fewest digits; others none.
    #[test]
    fn decimal_digits_are_the_fewest() {
        assert_eq!(decimal_digits(12.34), Some(2));
        assert_eq!(decimal_digits(1000.0), Some(0));
        assert_eq!(decimal_digits(0.1), Some(1));
        assert_eq!(decimal_digits(-0.0), None);
        assert_eq!(decimal_digits(f64::NAN), None);
        assert_eq!(decimal_digits(1e300), None);
        assert_eq!(decimal_digits(std::f64::consts::PI), Some(15));
    }
}
