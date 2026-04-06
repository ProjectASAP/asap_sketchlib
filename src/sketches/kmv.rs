//! KMV distinct-count sketch.
//!
//! Reference:
//! - "On synopses for distinct-value estimation under multiset operations"
//!   https://dl.acm.org/doi/10.1145/1247480.1247504

use crate::{
    CANONICAL_HASH_SEED, CommonHeap, DefaultXxHasher, KeepLargest, SketchHasher, SketchInput,
};

use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use rmp_serde::{
    decode::Error as RmpDecodeError, encode::Error as RmpEncodeError, from_slice, to_vec_named,
};

// expect error bound to be less than 2%
const KMV_DEFAULT_LENGTH: usize = 4096_usize;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct KMV<H: SketchHasher = DefaultXxHasher> {
    pub k: usize,
    pub k_vals: CommonHeap<u64, KeepLargest>,
    #[serde(skip)]
    _hasher: PhantomData<H>,
}

impl Default for KMV {
    fn default() -> Self {
        Self::new(KMV_DEFAULT_LENGTH)
    }
}

impl<H: SketchHasher> KMV<H> {
    pub fn new(k: usize) -> Self {
        Self {
            k,
            k_vals: CommonHeap::new_max(k),
            _hasher: PhantomData,
        }
    }

    pub fn insert(&mut self, item: &SketchInput) {
        let hashed = H::hash64_seeded(CANONICAL_HASH_SEED, item);
        self.insert_by_hash(hashed);
    }

    pub fn insert_by_hash(&mut self, hash_value: u64) {
        if self.k_vals.iter().any(|value| *value == hash_value) {
            return;
        }
        self.k_vals.push(hash_value);
    }

    pub fn estimate(&mut self) -> f64 {
        if self.k_vals.len() < self.k {
            return self.k_vals.len() as f64;
        }
        let largest = *self
            .k_vals
            .peek()
            .expect("k_vals should be non-empty when len >= k");
        const DIVISOR: f64 = 1.0 / (1u64 << 53) as f64;
        let mapped: f64 = (largest >> 11) as f64 * DIVISOR;
        (self.k - 1) as f64 / mapped
    }

    pub fn merge(&mut self, other: &mut KMV<H>) {
        assert_eq!(
            self.k, other.k,
            "Two KMV sketch have different k size, not mergeable"
        );
        for &value in other.k_vals.iter() {
            self.insert_by_hash(value);
        }
    }

    /// Serializes the sketch into MessagePack bytes.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        to_vec_named(self)
    }

    /// Deserializes a sketch from MessagePack bytes.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::SketchInput;

    // takes too long for 10_000_000
    // const TARGETS: [usize; 7] = [10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000];
    const TARGETS: [usize; 6] = [10, 100, 1_000, 10_000, 100_000, 1_000_000];
    const ERROR_TOLERANCE: f64 = 0.02;
    const SERDE_SAMPLE: usize = 100_000;

    #[test]
    fn assert_accuracy() {
        let mut sketch: KMV = KMV::default();
        let mut inserted: usize = 0;

        for &target in TARGETS.iter() {
            while inserted < target {
                let input = SketchInput::U64(inserted as u64);
                sketch.insert(&input);
                inserted += 1;
            }

            let truth = target as f64;
            let estimate = sketch.estimate();
            let error = if truth == 0.0 {
                0.0
            } else {
                (estimate - truth).abs() / truth
            };
            assert!(
                error <= ERROR_TOLERANCE,
                "KMV accuracy error {error:.4} exceeded {ERROR_TOLERANCE} (truth {truth}, estimate {estimate})"
            );
        }
    }

    #[test]
    fn assert_merge_accuracy() {
        let mut left: KMV = KMV::default();
        let mut right: KMV = KMV::default();
        let mut next_even: usize = 0;
        let mut next_odd: usize = 1;

        for &target in TARGETS.iter() {
            while next_even < target {
                let input = SketchInput::U64(next_even as u64);
                left.insert(&input);
                next_even += 2;
            }

            while next_odd < target {
                let input = SketchInput::U64(next_odd as u64);
                right.insert(&input);
                next_odd += 2;
            }

            let mut merged = left.clone();
            merged.merge(&mut right);

            let truth = target as f64;
            let estimate = merged.estimate();
            let error = if truth == 0.0 {
                0.0
            } else {
                (estimate - truth).abs() / truth
            };
            assert!(
                error <= ERROR_TOLERANCE,
                "KMV merge error {error:.4} exceeded {ERROR_TOLERANCE} (truth {truth}, estimate {estimate})"
            );
        }
    }

    #[test]
    fn assert_serialization_round_trip() {
        let mut sketch: KMV = KMV::default();
        for value in 0..SERDE_SAMPLE {
            let input = SketchInput::U64(value as u64);
            sketch.insert(&input);
        }

        let encoded = sketch
            .serialize_to_bytes()
            .unwrap_or_else(|err| panic!("KMV serialize_to_bytes failed: {err}"));
        assert!(
            !encoded.is_empty(),
            "KMV serialization output should not be empty"
        );

        let mut decoded: KMV = KMV::deserialize_from_bytes(&encoded)
            .unwrap_or_else(|err| panic!("KMV deserialize_from_bytes failed: {err}"));

        let reencoded = decoded
            .serialize_to_bytes()
            .unwrap_or_else(|err| panic!("KMV re-serialize failed: {err}"));

        assert_eq!(
            encoded, reencoded,
            "KMV serialized bytes differed after round trip"
        );

        let original_est = sketch.estimate();
        let decoded_est = decoded.estimate();
        assert!(
            (original_est - decoded_est).abs() <= ERROR_TOLERANCE * original_est.max(1.0),
            "KMV estimate mismatch after round trip: before {original_est}, after {decoded_est}"
        );
    }
}
