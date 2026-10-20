//! KMV distinct-count sketch.
//!
//! Reference:
//! - "On synopses for distinct-value estimation under multiset operations"
//!   <https://dl.acm.org/doi/10.1145/1247480.1247504>

use crate::{
    CommonHeap, DEFAULT_CANONICAL_SEED_INDEX, DataInput, DefaultXxHasher, RetainSmallest,
    SketchHasher,
};

use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

mod wire;

// expect error bound to be less than 2%
const KMV_DEFAULT_K: usize = 4096_usize;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct Kmv<H: SketchHasher = DefaultXxHasher> {
    pub k: usize,
    pub retained_hashes: CommonHeap<u64, RetainSmallest>,
    #[serde(skip)]
    _hasher: PhantomData<H>,
}

impl Default for Kmv {
    fn default() -> Self {
        Self::new(KMV_DEFAULT_K)
    }
}

impl<H: SketchHasher> Kmv<H> {
    pub fn new(k: usize) -> Self {
        Self {
            k,
            retained_hashes: CommonHeap::new_max(k),
            _hasher: PhantomData,
        }
    }

    pub fn update(&mut self, item: &DataInput) {
        let hashed = H::hash64_seeded(DEFAULT_CANONICAL_SEED_INDEX, item);
        self.update_with_hash(hashed);
    }

    pub fn update_with_hash(&mut self, hash_value: u64) {
        if self
            .retained_hashes
            .iter()
            .any(|value| *value == hash_value)
        {
            return;
        }
        self.retained_hashes.push(hash_value);
    }

    /// `(k - 1) / U(k)`, where `U(k)` is the k-th smallest hash's position in
    /// the 64-bit range, mapped into (0, 1].
    pub fn estimate(&mut self) -> f64 {
        if self.retained_hashes.len() < self.k {
            return self.retained_hashes.len() as f64;
        }
        let largest = *self
            .retained_hashes
            .peek()
            .expect("retained_hashes should be non-empty when len >= k");
        const HASH_RANGE: f64 = 18_446_744_073_709_551_616.0;
        let mapped: f64 = (largest as f64 + 1.0) / HASH_RANGE;
        (self.k - 1) as f64 / mapped
    }

    pub fn merge(&mut self, other: &mut Kmv<H>) {
        assert_eq!(
            self.k, other.k,
            "Two KMV sketch have different k size, not mergeable"
        );
        for &value in other.retained_hashes.iter() {
            self.update_with_hash(value);
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::DataInput;

    const ERROR_TOLERANCE: f64 = 0.02;
    const SERDE_SAMPLE: usize = 100_000;

    #[test]
    fn assert_serialization_round_trip() {
        let mut sketch: Kmv = Kmv::default();
        for value in 0..SERDE_SAMPLE {
            let input = DataInput::U64(value as u64);
            sketch.update(&input);
        }

        let encoded = sketch
            .to_asapv1()
            .unwrap_or_else(|err| panic!("KMV to_asapv1 failed: {err}"));
        assert!(
            !encoded.is_empty(),
            "KMV serialization output should not be empty"
        );

        let mut decoded: Kmv = Kmv::from_asapv1(&encoded)
            .unwrap_or_else(|err| panic!("KMV from_asapv1 failed: {err}"));

        let reencoded = decoded
            .to_asapv1()
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
