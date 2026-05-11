//! Wire-format-aligned KLL sketch type for cross-language interop.
//!
//! `KllSketch` is a thin facade over the pure-Rust [`crate::sketches::kll::KLL`]
//! (parity already locked in by the existing
//! `test_update_then_envelope_matches_sketchlib_go_bytes` in this file
//! against `sketchlib-go::KLLSketch.SerializePortable`). The wire shape
//! also serves as a nested field for
//! [`crate::message_pack_format::portable::hydra_kll`].

use serde::{Deserialize, Serialize};

use crate::message_pack_format::{Error as MsgPackError, MessagePackCodec};
use crate::sketches::kll::KLL;

/// Concrete KLL type backing the wire-format `KllSketch`.
pub type SketchlibKll = KLL<f64>;

/// Creates a fresh sketchlib KLL sketch with the requested accuracy
/// parameter `k`.
pub fn new_sketchlib_kll(k: u16) -> SketchlibKll {
    KLL::init_kll(k as i32)
}

/// Updates a sketchlib KLL with one numeric observation.
pub fn sketchlib_kll_update(inner: &mut SketchlibKll, value: f64) {
    inner.update(&value);
}

/// Queries a sketchlib KLL for the value at the requested quantile.
pub fn sketchlib_kll_quantile(inner: &SketchlibKll, q: f64) -> f64 {
    inner.quantile(q)
}

/// Merges `src` into `dst`.
pub fn sketchlib_kll_merge(dst: &mut SketchlibKll, src: &SketchlibKll) {
    dst.merge(src);
}

/// Serializes a sketchlib KLL into MessagePack bytes.
pub fn bytes_from_sketchlib_kll(inner: &SketchlibKll) -> Vec<u8> {
    inner.serialize_to_bytes().unwrap()
}

/// Deserializes a sketchlib KLL from MessagePack bytes.
pub fn sketchlib_kll_from_bytes(bytes: &[u8]) -> Result<SketchlibKll, Box<dyn std::error::Error>> {
    Ok(KLL::deserialize_from_bytes(bytes)?)
}

/// Wire-format KLL sketch — a thin facade over [`SketchlibKll`] with a
/// fixed `k` parameter and msgpack codec via [`KllSketchData`].
pub struct KllSketch {
    pub k: u16,
    pub(crate) backend: SketchlibKll,
}

impl KllSketch {
    pub fn new(k: u16) -> Self {
        Self {
            k,
            backend: new_sketchlib_kll(k),
        }
    }

    /// The configured `k` parameter (compactor capacity).
    pub fn k(&self) -> u16 {
        self.k
    }

    /// Returns the raw sketch bytes (for JSON serialization, etc.).
    pub fn sketch_bytes(&self) -> Vec<u8> {
        bytes_from_sketchlib_kll(&self.backend)
    }

    pub fn update(&mut self, value: f64) {
        sketchlib_kll_update(&mut self.backend, value);
    }

    pub fn count(&self) -> u64 {
        self.backend.count() as u64
    }

    /// Estimate the value at the given quantile `q ∈ [0, 1]`.
    pub fn quantile(&self, q: f64) -> f64 {
        if self.count() == 0 {
            return 0.0;
        }
        sketchlib_kll_quantile(&self.backend, q)
    }

    /// Merge another KllSketch into self in place. Both operands must
    /// have identical `k`.
    pub fn merge(
        &mut self,
        other: &KllSketch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.k != other.k {
            return Err(format!("KllSketch k mismatch: self={}, other={}", self.k, other.k).into());
        }
        sketchlib_kll_merge(&mut self.backend, &other.backend);
        Ok(())
    }

    /// Merge from references without cloning.
    pub fn merge_refs(
        sketches: &[&Self],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if sketches.is_empty() {
            return Err("No sketches to merge".into());
        }
        let k = sketches[0].k;
        for s in sketches {
            if s.k != k {
                return Err("Cannot merge KllSketch with different k values".into());
            }
        }
        let mut merged = Self::new(k);
        for s in sketches {
            sketchlib_kll_merge(&mut merged.backend, &s.backend);
        }
        Ok(merged)
    }

    /// One-shot aggregation: build a sketch from a slice of values.
    pub fn aggregate_kll(k: u16, values: &[f64]) -> Option<Vec<u8>> {
        if values.is_empty() {
            return None;
        }
        let mut sketch = Self::new(k);
        for &value in values {
            sketch.update(value);
        }
        sketch.to_msgpack().ok()
    }
}

impl Clone for KllSketch {
    fn clone(&self) -> Self {
        let bytes = bytes_from_sketchlib_kll(&self.backend);
        Self {
            k: self.k,
            backend: sketchlib_kll_from_bytes(&bytes).unwrap(),
        }
    }
}

impl std::fmt::Debug for KllSketch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KllSketch")
            .field("k", &self.k)
            .field("sketch_n", &self.count())
            .finish()
    }
}

// Thread safety: sketchlib's KLL is used in a single-threaded context per
// accumulator instance and only shares read-only operations across threads.
unsafe impl Send for KllSketch {}
unsafe impl Sync for KllSketch {}

// ----- Wire format -----

/// Wire DTO for [`KllSketch`]. Also referenced as a nested field by
/// [`crate::message_pack_format::portable::hydra_kll::HydraKllSketchWire`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KllSketchData {
    pub k: u16,
    pub sketch_bytes: Vec<u8>,
}

impl MessagePackCodec for KllSketch {
    fn to_msgpack(&self) -> Result<Vec<u8>, MsgPackError> {
        let wire = KllSketchData {
            k: self.k,
            sketch_bytes: self.sketch_bytes(),
        };
        Ok(rmp_serde::to_vec(&wire)?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, MsgPackError> {
        let wire: KllSketchData = rmp_serde::from_slice(bytes)?;
        let backend = KLL::deserialize_from_bytes(&wire.sketch_bytes)?;
        Ok(Self { k: wire.k, backend })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kll_creation() {
        let kll = KllSketch::new(200);
        assert_eq!(kll.count(), 0);
        assert_eq!(kll.k, 200);
    }

    #[test]
    fn test_kll_update() {
        let mut kll = KllSketch::new(200);
        kll.update(10.0);
        kll.update(20.0);
        kll.update(15.0);
        assert_eq!(kll.count(), 3);
    }

    #[test]
    fn test_kll_quantile() {
        let mut kll = KllSketch::new(200);
        for i in 1..=10 {
            kll.update(i as f64);
        }
        assert_eq!(kll.quantile(0.0), 1.0);
        assert_eq!(kll.quantile(1.0), 10.0);
        let median = kll.quantile(0.5);
        assert!(
            (5.0..=6.0).contains(&median),
            "median should be between 5 and 6; got {median}"
        );
    }

    #[test]
    fn test_kll_merge() {
        let mut kll1 = KllSketch::new(200);
        let mut kll2 = KllSketch::new(200);

        for i in 1..=5 {
            kll1.update(i as f64);
        }
        for i in 6..=10 {
            kll2.update(i as f64);
        }

        kll1.merge(&kll2).unwrap();
        assert_eq!(kll1.count(), 10);
        assert_eq!(kll1.quantile(0.0), 1.0);
        assert_eq!(kll1.quantile(1.0), 10.0);
    }

    #[test]
    fn test_msgpack_round_trip() {
        let mut kll = KllSketch::new(200);
        for i in 1..=5 {
            kll.update(i as f64);
        }

        let bytes = kll.to_msgpack().unwrap();
        let deserialized = KllSketch::from_msgpack(&bytes).unwrap();

        assert_eq!(deserialized.k, 200);
        assert_eq!(deserialized.count(), 5);
        assert_eq!(deserialized.quantile(0.0), 1.0);
        assert_eq!(deserialized.quantile(1.0), 5.0);
    }

    #[test]
    fn test_aggregate_kll() {
        let values = [1.0, 2.0, 3.0, 4.0, 5.0];
        let bytes = KllSketch::aggregate_kll(200, &values).unwrap();
        let kll = KllSketch::from_msgpack(&bytes).unwrap();
        assert_eq!(kll.count(), 5);
        assert_eq!(kll.quantile(0.0), 1.0);
        assert_eq!(kll.quantile(1.0), 5.0);
    }

    #[test]
    fn test_aggregate_kll_empty() {
        assert!(KllSketch::aggregate_kll(200, &[]).is_none());
    }

    /// Cross-language byte-parity guard against `sketchlib-go`'s
    /// `KLLSketch.SerializePortable` output for the deterministic
    /// input `(1..=50)` with `seed=42` and `k=200`.
    #[test]
    fn test_update_then_envelope_matches_sketchlib_go_bytes() {
        use crate::proto::sketchlib::{
            CoinState, KllState, SketchEnvelope, sketch_envelope::SketchState,
        };
        use prost::Message;

        let mut sk: KLL<f64> = KLL::init_kll_with_seed(200, 42);
        for i in 1..=50 {
            sk.update(&(i as f64));
        }

        let (state, bit_cache, remaining_bits) = sk.wire_coin();
        let kll_state = KllState {
            k: sk.wire_k(),
            m: sk.wire_m(),
            num_levels: sk.wire_num_levels(),
            levels: sk.wire_levels(),
            items: sk.wire_items(),
            coin: Some(CoinState {
                state,
                bit_cache,
                remaining_bits,
            }),
        };
        let envelope = SketchEnvelope {
            format_version: 1,
            producer: None,
            hash_spec: None,
            sketch_state: Some(SketchState::Kll(kll_state)),
        };
        let mut got = Vec::with_capacity(envelope.encoded_len());
        envelope.encode(&mut got).expect("prost encode");

        const GOLDEN_HEX: &str = "08016aa20308c80110081801220200322a9003000000000000f03f000000000000004000000000000008400000000000001040000000000000144000000000000018400000000000001c40000000000000204000000000000022400000000000002440000000000000264000000000000028400000000000002a400000000000002c400000000000002e4000000000000030400000000000003140000000000000324000000000000033400000000000003440000000000000354000000000000036400000000000003740000000000000384000000000000039400000000000003a400000000000003b400000000000003c400000000000003d400000000000003e400000000000003f4000000000000040400000000000804040000000000000414000000000008041400000000000004240000000000080424000000000000043400000000000804340000000000000444000000000008044400000000000004540000000000080454000000000000046400000000000804640000000000000474000000000008047400000000000004840000000000080484000000000000049403202082a";
        let want = decode_hex(GOLDEN_HEX);
        assert_eq!(
            got, want,
            "KLL envelope bytes diverge from sketchlib-go golden \
             ({} bytes got vs {} bytes want)",
            got.len(),
            want.len(),
        );
    }

    fn decode_hex(s: &str) -> Vec<u8> {
        s.as_bytes()
            .chunks(2)
            .map(|pair| {
                let high = hex_nibble(pair[0]);
                let low = hex_nibble(pair[1]);
                (high << 4) | low
            })
            .collect()
    }

    fn hex_nibble(c: u8) -> u8 {
        match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            b'A'..=b'F' => c - b'A' + 10,
            _ => panic!("non-hex byte {}", c as char),
        }
    }
}
