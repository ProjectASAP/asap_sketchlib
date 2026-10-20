//! Wire-format-aligned KLL sketch type for cross-language interop.
//!
//! `KllSketch` is a thin facade over the pure-Rust [`crate::sketches::kll::Kll`].
//! The wire shape
//! also serves as a nested field for
//! [`crate::message_pack_format::portable::hydra_kll`].

use serde::{Deserialize, Serialize};

use crate::message_pack_format::{Error as MsgPackError, MessagePackCodec};
use crate::sketches::kll::Kll;

/// Concrete KLL type backing the wire-format `KllSketch`.
pub type SketchlibKll = Kll<f64>;

/// Creates a fresh sketchlib KLL sketch with the requested accuracy
/// parameter `k`.
pub fn new_sketchlib_kll(k: u16) -> SketchlibKll {
    Kll::with_k(k as i32)
}

/// [`new_sketchlib_kll`] with an explicit compaction-coin seed, so the sketch
/// replays identically across runs.
pub fn new_sketchlib_kll_with_seed(k: u16, seed: u64) -> SketchlibKll {
    Kll::with_k_and_seed(k as i32, seed)
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
    inner.to_asapv1().unwrap()
}

/// Deserializes a sketchlib KLL from MessagePack bytes.
pub fn sketchlib_kll_from_bytes(bytes: &[u8]) -> Result<SketchlibKll, Box<dyn std::error::Error>> {
    Ok(Kll::from_asapv1(bytes)?)
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

    /// [`KllSketch::new`] with an explicit compaction-coin seed.
    ///
    /// `new` seeds the coin from the wall clock, so two runs over the same
    /// input produce different sketches. Pass a seed when the result has to be
    /// reproducible — replaying a stream, comparing two processes, or
    /// asserting an accuracy bound in a test.
    pub fn with_seed(k: u16, seed: u64) -> Self {
        Self {
            k,
            backend: new_sketchlib_kll_with_seed(k, seed),
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
        // Structural clone: the backing `Kll<f64>` derives `Clone` (its fields
        // are `Box<[..]>`/`Vec`/scalars), so copy it directly instead of a full
        // msgpack serialize -> deserialize -> rebuild round-trip. Produces an
        // identical sketch far more cheaply (no rmp encode/decode, far fewer
        // allocations).
        Self {
            k: self.k,
            backend: self.backend.clone(),
        }
    }
}

impl KllSketch {
    /// Reconstruct directly from portable wire state (k + level-ordered items +
    /// level boundaries) without replaying items through `update()`. Bit-exact.
    pub fn from_portable_state(
        k: u16,
        items: &[f64],
        levels: &[usize],
        num_levels: usize,
    ) -> Result<Self, String> {
        Ok(Self {
            k,
            backend: SketchlibKll::from_portable_state(k as usize, items, levels, num_levels)?,
        })
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
        let backend = Kll::from_asapv1(&wire.sketch_bytes)?;
        Ok(Self { k: wire.k, backend })
    }
}

impl MessagePackCodec for KllSketchData {
    /// Encodes the `(k, sketch_bytes)` DTO directly, byte-identical to
    /// [`KllSketch::to_msgpack`] (which serializes this same struct). Lets
    /// producers that hold a raw sketchlib KLL backend (via
    /// `to_asapv1`) emit the portable KLL msgpack form without
    /// constructing a [`KllSketch`] facade.
    fn to_msgpack(&self) -> Result<Vec<u8>, MsgPackError> {
        Ok(rmp_serde::to_vec(self)?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, MsgPackError> {
        Ok(rmp_serde::from_slice(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kll_data_msgpack_matches_kll_sketch() {
        let mut sk = KllSketch::new(200);
        for i in 0..500 {
            sk.update(i as f64);
        }
        let via_sketch = sk.to_msgpack().unwrap();
        let data = KllSketchData {
            k: sk.k(),
            sketch_bytes: sk.sketch_bytes(),
        };
        let via_data = data.to_msgpack().unwrap();
        assert_eq!(
            via_sketch, via_data,
            "KllSketchData codec must byte-match KllSketch"
        );
        let rt = KllSketchData::from_msgpack(&via_data).unwrap();
        assert_eq!(rt.k, data.k);
        assert_eq!(rt.sketch_bytes, data.sketch_bytes);
    }

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
}
