//! KMV distinct-count sketch.
//!
//! Reference:
//! - "On synopses for distinct-value estimation under multiset operations"
//!   <https://dl.acm.org/doi/10.1145/1247480.1247504>

use crate::common::heap::vec_slots_after_pushes;
use crate::common::structures::heap::PREALLOCATED_SLOTS;
use crate::{
    CommonHeap, DEFAULT_CANONICAL_SEED_INDEX, DataInput, DefaultXxHasher, RetainSmallest,
    SketchHasher,
};

use std::marker::PhantomData;

mod wire;

// expect error bound to be less than 2%
const KMV_DEFAULT_K: usize = 4096_usize;

#[derive(Clone, Debug)]
pub struct Kmv<H: SketchHasher = DefaultXxHasher> {
    pub k: usize,
    pub retained_hashes: CommonHeap<u64, RetainSmallest>,
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
        // A full sketch keeps only hashes below its largest retained one.
        if self.retained_hashes.is_full()
            && self
                .retained_hashes
                .peek()
                .is_some_and(|max| hash_value >= *max)
        {
            return;
        }
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
    pub fn estimate(&self) -> f64 {
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

    pub fn merge(&mut self, other: &Kmv<H>) {
        assert_eq!(
            self.k, other.k,
            "Two KMV sketch have different k size, not mergeable"
        );
        for &value in other.retained_hashes.iter() {
            self.update_with_hash(value);
        }
    }

    /// Bytes this sketch holds: the struct plus the hash slots its heap has
    /// allocated (up to 1024 at construction, growing toward `k`).
    pub fn memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.retained_hashes.allocated_slots() * std::mem::size_of::<u64>()
    }
}

// Memory estimate; the size is the same for every hasher.
impl Kmv {
    /// Bytes a sketch of `k`, built, decoded or cloned, holds after `distinct`
    /// distinct inputs, merges included: [`Self::memory_bytes`] exactly, and an
    /// upper bound when fewer arrive. The heap reserves `min(k, 1024)` slots
    /// and doubles toward `k`. Saturates at `usize::MAX`.
    pub fn estimated_memory_bytes(k: usize, distinct: usize) -> usize {
        let slots = vec_slots_after_pushes(
            k.min(PREALLOCATED_SLOTS),
            k.min(distinct),
            std::mem::size_of::<u64>(),
        );
        std::mem::size_of::<Self>().saturating_add(slots.saturating_mul(std::mem::size_of::<u64>()))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::DataInput;

    const ERROR_TOLERANCE: f64 = 0.02;
    const SERDE_SAMPLE: usize = 100_000;

    fn build(k: usize, items: std::ops::Range<u64>) -> Kmv {
        let mut sketch = Kmv::new(k);
        for item in items {
            sketch.update(&DataInput::U64(item));
        }
        sketch
    }

    /// Estimating and merging borrow shared; the merged estimate equals the
    /// union build's.
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

    /// Memory covers the retained hashes, grows as the heap fills, and stays
    /// put after a duplicate or a hash past the k-th smallest.
    #[test]
    fn memory_bytes_covers_the_retained_hashes() {
        let k = 3_000;
        let empty: Kmv = Kmv::new(k);
        let full = build(k, 0..20_000);
        assert_eq!(full.retained_hashes.len(), k);
        assert_eq!(empty.memory_bytes(), std::mem::size_of::<Kmv>() + 1024 * 8);
        assert!(full.memory_bytes() >= std::mem::size_of::<Kmv>() + k * 8);
        let mut again = full.clone();
        let before = again.memory_bytes();
        again.update(&DataInput::U64(7));
        again.update_with_hash(u64::MAX);
        assert_eq!(again.memory_bytes(), before);
        assert_eq!(again.estimate(), full.estimate());
    }

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

        let decoded: Kmv = Kmv::from_asapv1(&encoded)
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
