//! KMV distinct-count sketch.
//!
//! Reference:
//! - "On synopses for distinct-value estimation under multiset operations"
//!   <https://dl.acm.org/doi/10.1145/1247480.1247504>

use crate::{
    CANONICAL_HASH_SEED, CommonHeap, DataInput, DefaultXxHasher, KeepLargest, SketchHasher,
};

use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

mod wire;

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

    pub fn insert(&mut self, item: &DataInput) {
        let hashed = H::hash64_seeded(CANONICAL_HASH_SEED, item);
        self.insert_by_hash(hashed);
    }

    pub fn insert_by_hash(&mut self, hash_value: u64) {
        // A full sketch keeps only hashes below its largest retained one, so
        // most inserts are turned away here without scanning the k hashes.
        if self.k_vals.is_full() && self.k_vals.peek().is_some_and(|max| hash_value >= *max) {
            return;
        }
        if self.k_vals.iter().any(|value| *value == hash_value) {
            return;
        }
        self.k_vals.push(hash_value);
    }

    /// `(k - 1) / U(k)`, where `U(k)` is the k-th smallest hash's position in
    /// the 64-bit range, mapped into (0, 1].
    pub fn estimate(&self) -> f64 {
        if self.k_vals.len() < self.k {
            return self.k_vals.len() as f64;
        }
        let largest = *self
            .k_vals
            .peek()
            .expect("k_vals should be non-empty when len >= k");
        const HASH_RANGE: f64 = 18_446_744_073_709_551_616.0;
        let mapped: f64 = (largest as f64 + 1.0) / HASH_RANGE;
        (self.k - 1) as f64 / mapped
    }

    pub fn merge(&mut self, other: &KMV<H>) {
        assert_eq!(
            self.k, other.k,
            "Two KMV sketch have different k size, not mergeable"
        );
        for &value in other.k_vals.iter() {
            self.insert_by_hash(value);
        }
    }

    /// Approximate in-memory footprint in bytes: the struct plus the hash
    /// slots its heap has allocated. The heap reserves up to 1024 slots at
    /// construction and grows toward `k` as distinct hashes arrive.
    pub fn approx_memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(self.k_vals.allocated_slots().saturating_mul(8))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::DataInput;

    const ERROR_TOLERANCE: f64 = 0.02;
    const SERDE_SAMPLE: usize = 100_000;

    fn build(k: usize, items: std::ops::Range<u64>) -> KMV {
        let mut sketch = KMV::new(k);
        for item in items {
            sketch.insert(&DataInput::U64(item));
        }
        sketch
    }

    /// Reading an estimate and merging in another sketch borrow their inputs
    /// shared, so neither needs a mutable binding or a copy, and the merged
    /// estimate equals the union build's.
    #[test]
    fn estimate_and_merge_borrow_inputs_shared() {
        let left = build(64, 0..600);
        let right = build(64, 400..1_000);
        let union = build(64, 0..1_000);
        let mut merged = left.clone();
        merged.merge(&right);
        assert_eq!(merged.estimate(), union.estimate());
        assert_eq!(left.estimate(), build(64, 0..600).estimate());
    }

    /// The memory report covers the hashes the heap holds, grows as it fills,
    /// and is the same after a duplicate or a hash past the retained k.
    #[test]
    fn memory_report_covers_the_retained_hashes() {
        let k = 3_000;
        let empty = KMV::new(k);
        let full = build(k, 0..20_000);
        assert_eq!(full.k_vals.len(), k);
        for sketch in [&empty, &full] {
            assert!(
                sketch.approx_memory_bytes()
                    >= std::mem::size_of::<KMV>() + sketch.k_vals.len() * 8
            );
        }
        assert!(full.approx_memory_bytes() > empty.approx_memory_bytes());
        let mut again = full.clone();
        let before = again.approx_memory_bytes();
        again.insert(&DataInput::U64(7));
        again.insert_by_hash(u64::MAX);
        assert_eq!(again.approx_memory_bytes(), before);
        assert_eq!(again.estimate(), full.estimate());
    }

    #[test]
    fn assert_serialization_round_trip() {
        let mut sketch: KMV = KMV::default();
        for value in 0..SERDE_SAMPLE {
            let input = DataInput::U64(value as u64);
            sketch.insert(&input);
        }

        let encoded = sketch
            .serialize_to_bytes()
            .unwrap_or_else(|err| panic!("KMV serialize_to_bytes failed: {err}"));
        assert!(
            !encoded.is_empty(),
            "KMV serialization output should not be empty"
        );

        let decoded: KMV = KMV::deserialize_from_bytes(&encoded)
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
