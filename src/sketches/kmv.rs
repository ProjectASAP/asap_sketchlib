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

mod retained_index;
mod wire;

use retained_index::RetainedIndex;

// expect error bound to be less than 2%
const KMV_DEFAULT_K: usize = 4096_usize;

/// Index slots per heap slot: the index keeps twice the heap's allocated
/// slots, so its load stays at most one half and a lookup probes about 1.5
/// slots on average.
const INDEX_SLOTS_PER_HEAP_SLOT: usize = 2;

/// Keeps the `k` smallest distinct 64-bit hashes of its inputs.
///
/// The hashes sit twice: in a bounded max-heap, whose root is the largest
/// retained hash (the estimate's `U(k)` and the one evicted next), and in an
/// index that answers "is this hash already retained" in expected `O(1)`. An
/// update therefore costs `O(1)` plus one heap sift, `O(log k)`, and a merge
/// `O(k)`. Both are private so they cannot drift apart; read the heap through
/// [`Self::retained_hashes`].
#[derive(Clone, Debug)]
pub struct Kmv<H: SketchHasher = DefaultXxHasher> {
    pub k: usize,
    retained_hashes: CommonHeap<u64, RetainSmallest>,
    retained_index: RetainedIndex,
    _hasher: PhantomData<H>,
}

impl Default for Kmv {
    fn default() -> Self {
        Self::new(KMV_DEFAULT_K)
    }
}

impl<H: SketchHasher> Kmv<H> {
    pub fn new(k: usize) -> Self {
        Self::from_heap(CommonHeap::new_max(k))
    }

    /// Wraps a heap of retained hashes (distinct, at most its capacity) and
    /// indexes them.
    fn from_heap(retained_hashes: CommonHeap<u64, RetainSmallest>) -> Self {
        let retained_index = RetainedIndex::with_slots(
            INDEX_SLOTS_PER_HEAP_SLOT * retained_hashes.allocated_slots(),
            retained_hashes.as_slice(),
        );
        Self {
            k: retained_hashes.capacity(),
            retained_hashes,
            retained_index,
            _hasher: PhantomData,
        }
    }

    /// The retained hashes, the largest at the root. Read-only: the sketch
    /// keeps an index beside the heap that every change must update.
    pub fn retained_hashes(&self) -> &CommonHeap<u64, RetainSmallest> {
        &self.retained_hashes
    }

    pub fn update(&mut self, item: &DataInput) {
        let hashed = H::hash64_seeded(DEFAULT_CANONICAL_SEED_INDEX, item);
        self.update_with_hash(hashed);
    }

    pub fn update_with_hash(&mut self, hash_value: u64) {
        let full = self.retained_hashes.is_full();
        // A full sketch keeps only hashes below its largest retained one.
        if full {
            match self.retained_hashes.peek() {
                Some(max) => {
                    if hash_value >= *max {
                        return;
                    }
                }
                // A bound of 0 keeps nothing.
                None => return,
            }
        }
        if self.retained_index.contains(hash_value) {
            return;
        }
        if full {
            let evicted = self
                .retained_hashes
                .replace_root_with(hash_value, &mut |_, _| {});
            self.retained_index.remove(evicted);
            self.retained_index.insert(hash_value);
        } else {
            self.retained_hashes
                .push_back_with(hash_value, &mut |_, _| {});
            self.index_pushed(hash_value);
        }
    }

    /// Indexes `hash_value`, just pushed onto the heap. When the push grew the
    /// heap's allocation, the index grows with it (a rebuild, amortized `O(1)`
    /// per push since the heap doubles), so its size depends only on the
    /// heap's and the memory stays exact.
    fn index_pushed(&mut self, hash_value: u64) {
        let slot_count = INDEX_SLOTS_PER_HEAP_SLOT * self.retained_hashes.allocated_slots();
        if slot_count == self.retained_index.slot_count() {
            self.retained_index.insert(hash_value);
        } else {
            self.retained_index =
                RetainedIndex::with_slots(slot_count, self.retained_hashes.as_slice());
        }
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

    /// Keeps the `k` smallest distinct hashes of both sketches, in `O(k)`:
    /// the other sketch's hashes that are new and below this one's bound join
    /// this sketch's, the `k` smallest are selected, heapified and indexed.
    pub fn merge(&mut self, other: &Kmv<H>) {
        assert_eq!(
            self.k, other.k,
            "Two KMV sketch have different k size, not mergeable"
        );
        let bound = if self.retained_hashes.is_full() {
            self.retained_hashes.peek().copied()
        } else {
            None
        };
        let mut union: Vec<u64> = Vec::new();
        for &value in other.retained_hashes.iter() {
            if bound.is_some_and(|max| value >= max) || self.retained_index.contains(value) {
                continue;
            }
            if union.is_empty() {
                union.reserve(self.retained_hashes.len() + other.retained_hashes.len());
                union.extend_from_slice(self.retained_hashes.as_slice());
            }
            union.push(value);
        }
        if union.is_empty() {
            return;
        }
        if union.len() > self.k {
            union.select_nth_unstable(self.k - 1);
            union.truncate(self.k);
        }
        *self = Self::from_heap(CommonHeap::from_unordered_vec(
            union,
            self.k,
            RetainSmallest,
        ));
    }

    /// Bytes this sketch holds: the struct, the hash slots its heap has
    /// allocated (up to 1024 at construction, growing toward `k`), and the
    /// index's slots, twice the heap's.
    pub fn memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + (self.retained_hashes.allocated_slots() + self.retained_index.slot_count())
                * std::mem::size_of::<u64>()
    }
}

// Memory estimate; the size is the same for every hasher.
impl Kmv {
    /// Bytes a sketch of `k`, built, decoded or cloned, holds after `distinct`
    /// distinct inputs, merges included: [`Self::memory_bytes`] exactly, and an
    /// upper bound when fewer arrive. The heap reserves `min(k, 1024)` slots
    /// and doubles toward `k`; the index holds twice the heap's slots.
    /// Saturates at `usize::MAX`.
    pub fn theoretical_memory_bytes(k: usize, distinct: usize) -> usize {
        let slots = vec_slots_after_pushes(
            k.min(PREALLOCATED_SLOTS),
            k.min(distinct),
            std::mem::size_of::<u64>(),
        );
        let heap_and_index_slots = slots.saturating_mul(1 + INDEX_SLOTS_PER_HEAP_SLOT);
        std::mem::size_of::<Self>()
            .saturating_add(heap_and_index_slots.saturating_mul(std::mem::size_of::<u64>()))
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
        // 1,024 heap slots and twice as many index slots, 8 bytes each.
        assert_eq!(
            empty.memory_bytes(),
            std::mem::size_of::<Kmv>() + (1024 + 2 * 1024) * 8
        );
        assert!(full.memory_bytes() >= std::mem::size_of::<Kmv>() + 3 * k * 8);
        // Built, merged or decoded, the same retained set holds the same bytes,
        // the theoretical figure.
        let mut merged = build(k, 0..12_000);
        merged.merge(&build(k, 8_000..20_000));
        let decoded = Kmv::from_asapv1(&full.to_asapv1().unwrap()).unwrap();
        for sketch in [&full, &merged, &decoded, &full.clone()] {
            assert_eq!(
                sketch.memory_bytes(),
                Kmv::theoretical_memory_bytes(k, 20_000)
            );
        }
        let mut again = full.clone();
        let before = again.memory_bytes();
        again.update(&DataInput::U64(7));
        again.update_with_hash(u64::MAX);
        assert_eq!(again.memory_bytes(), before);
        assert_eq!(again.estimate(), full.estimate());
    }

    /// The KMV algorithm as it was before the retained-hash index, kept as the
    /// oracle the index must reproduce: a linear duplicate scan per update, and
    /// a merge that re-inserts the other sketch's hashes one at a time.
    struct ScanKmv {
        k: usize,
        retained: CommonHeap<u64, RetainSmallest>,
    }

    impl ScanKmv {
        fn new(k: usize) -> Self {
            Self {
                k,
                retained: CommonHeap::new_max(k),
            }
        }

        fn update_with_hash(&mut self, hash_value: u64) {
            if self.retained.is_full() && self.retained.peek().is_some_and(|max| hash_value >= *max)
            {
                return;
            }
            if self.retained.iter().any(|value| *value == hash_value) {
                return;
            }
            self.retained.push(hash_value);
        }

        fn merge(&mut self, other: &ScanKmv) {
            for &value in other.retained.iter() {
                self.update_with_hash(value);
            }
        }

        fn sorted(&self) -> Vec<u64> {
            let mut hashes: Vec<u64> = self.retained.iter().copied().collect();
            hashes.sort_unstable();
            hashes
        }

        fn estimate(&self) -> f64 {
            if self.retained.len() < self.k {
                return self.retained.len() as f64;
            }
            let largest = *self.retained.peek().expect("full");
            (self.k - 1) as f64 / ((largest as f64 + 1.0) / 18_446_744_073_709_551_616.0)
        }
    }

    /// The retained hashes, ascending.
    fn sorted_hashes(sketch: &Kmv) -> Vec<u64> {
        let mut hashes: Vec<u64> = sketch.retained_hashes.iter().copied().collect();
        hashes.sort_unstable();
        hashes
    }

    /// Asserts `sketch` keeps exactly the oracle's hashes and estimates
    /// bit-for-bit alike, and that its heap still holds the largest at the
    /// root and the bound `k`.
    #[track_caller]
    fn assert_same_as_scan(sketch: &Kmv, oracle: &ScanKmv, what: &str) {
        assert_eq!(
            sorted_hashes(sketch),
            oracle.sorted(),
            "{what}: retained set"
        );
        assert_eq!(
            sketch.estimate().to_bits(),
            oracle.estimate().to_bits(),
            "{what}: estimate"
        );
        assert_eq!(
            sketch.retained_hashes.peek(),
            oracle.sorted().last(),
            "{what}: heap root"
        );
        assert_eq!(sketch.retained_hashes.capacity(), oracle.k, "{what}: bound");
    }

    /// Hash streams with duplicates: a domain smaller than, around and far
    /// above `k`, plus the extreme hashes 0 and `u64::MAX`.
    fn hash_streams(rng: &mut rand::rngs::StdRng, k: usize) -> Vec<Vec<u64>> {
        use rand::Rng;
        let mut streams = Vec::new();
        for domain in [k as u64 / 2 + 1, k as u64 + 1, 4 * k as u64 + 7, u64::MAX] {
            let len = rng.random_range(0..6 * k + 20);
            let mut stream: Vec<u64> = (0..len)
                .map(|_| {
                    if domain == u64::MAX {
                        rng.random()
                    } else {
                        // Spread the domain over the hash range so it is
                        // not only the low end.
                        rng.random_range(0..domain)
                            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                    }
                })
                .collect();
            stream.extend([0, u64::MAX, 0, u64::MAX]);
            streams.push(stream);
        }
        streams
    }

    /// Update by update, the indexed sketch keeps the same hashes and the same
    /// estimate as the linear scan, across bounds from 1 up, every stream
    /// shape and keyed updates.
    #[test]
    fn updates_match_the_linear_scan() {
        use rand::SeedableRng;
        let mut rng = rand::rngs::StdRng::seed_from_u64(0x6b6d76);
        for k in [1usize, 2, 3, 8, 64, 1_000, 3_000] {
            for (index, stream) in hash_streams(&mut rng, k).into_iter().enumerate() {
                let mut sketch = Kmv::new(k);
                let mut oracle = ScanKmv::new(k);
                for (step, &hash) in stream.iter().enumerate() {
                    sketch.update_with_hash(hash);
                    oracle.update_with_hash(hash);
                    if k <= 8 || step % 997 == 0 {
                        assert_same_as_scan(&sketch, &oracle, &format!("k={k} stream {index}"));
                    }
                }
                assert_same_as_scan(&sketch, &oracle, &format!("k={k} stream {index} end"));
            }
            let mut keyed = Kmv::new(k);
            let mut oracle = ScanKmv::new(k);
            for id in (0..3 * k as u64).chain(0..k as u64) {
                keyed.update(&DataInput::U64(id));
                oracle.update_with_hash(DefaultXxHasher::hash64_seeded(
                    DEFAULT_CANONICAL_SEED_INDEX,
                    &DataInput::U64(id),
                ));
            }
            assert_same_as_scan(&keyed, &oracle, &format!("k={k} keyed"));
        }
    }

    /// Merging parts in any order, sequentially or as a tree, keeps the same
    /// hashes and estimate as the linear scan's merge and as one sketch built
    /// over the whole stream; inputs are left unchanged.
    #[test]
    fn merges_in_any_order_match_the_linear_scan_and_the_union_build() {
        use rand::seq::SliceRandom;
        use rand::{Rng, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(0x6d657267);
        for k in [1usize, 2, 8, 64, 1_000] {
            for (index, stream) in hash_streams(&mut rng, k).into_iter().enumerate() {
                let parts_count = rng.random_range(1..6);
                let mut parts: Vec<(Kmv, ScanKmv)> = (0..parts_count)
                    .map(|_| (Kmv::new(k), ScanKmv::new(k)))
                    .collect();
                let mut whole = Kmv::new(k);
                for &hash in &stream {
                    let part = rng.random_range(0..parts_count);
                    parts[part].0.update_with_hash(hash);
                    parts[part].1.update_with_hash(hash);
                    whole.update_with_hash(hash);
                }
                parts.shuffle(&mut rng);
                let what = format!("k={k} stream {index} parts {parts_count}");

                let mut sequential = Kmv::new(k);
                let mut oracle = ScanKmv::new(k);
                for (part, part_oracle) in &parts {
                    let before = sorted_hashes(part);
                    sequential.merge(part);
                    oracle.merge(part_oracle);
                    assert_eq!(sorted_hashes(part), before, "{what}: input changed");
                    assert_same_as_scan(&sequential, &oracle, &what);
                }
                assert_eq!(
                    sorted_hashes(&sequential),
                    sorted_hashes(&whole),
                    "{what}: union"
                );

                // A tree: merge neighbours pairwise until one sketch is left.
                let mut level: Vec<Kmv> = parts.iter().map(|(part, _)| part.clone()).collect();
                while level.len() > 1 {
                    level = level
                        .chunks(2)
                        .map(|pair| {
                            let mut merged = pair[0].clone();
                            if let Some(right) = pair.get(1) {
                                merged.merge(right);
                            }
                            merged
                        })
                        .collect();
                }
                assert_same_as_scan(&level[0], &oracle, &format!("{what} tree"));

                // Merging a sketch into itself and merging an empty sketch
                // either way change nothing.
                let mut twice = sequential.clone();
                twice.merge(&sequential);
                twice.merge(&Kmv::new(k));
                assert_same_as_scan(&twice, &oracle, &format!("{what} self"));
                let mut from_empty = Kmv::new(k);
                from_empty.merge(&sequential);
                assert_same_as_scan(&from_empty, &oracle, &format!("{what} into empty"));
            }
        }
    }

    /// Edge cases: an empty sketch, a bound of 1, and a stream of one hash
    /// repeated, in updates and merges.
    #[test]
    fn edge_cases_match_the_linear_scan() {
        let empty = Kmv::new(16);
        assert_same_as_scan(&empty, &ScanKmv::new(16), "empty");
        let mut merged_empties = Kmv::new(16);
        merged_empties.merge(&Kmv::new(16));
        assert_same_as_scan(&merged_empties, &ScanKmv::new(16), "empty merge");

        let mut one = Kmv::new(1);
        let mut one_oracle = ScanKmv::new(1);
        for hash in [9u64, 9, 4, 12, 4, 0, 0, u64::MAX] {
            one.update_with_hash(hash);
            one_oracle.update_with_hash(hash);
            assert_same_as_scan(&one, &one_oracle, "k=1");
        }
        assert_eq!(sorted_hashes(&one), vec![0]);

        for k in [1usize, 5, 2_000] {
            let mut same = Kmv::new(k);
            let mut same_oracle = ScanKmv::new(k);
            for _ in 0..10_000 {
                same.update_with_hash(42);
                same_oracle.update_with_hash(42);
            }
            assert_same_as_scan(&same, &same_oracle, &format!("k={k} all duplicates"));
            // One distinct hash: counted exactly below the bound; at k = 1 the
            // estimate's numerator k - 1 is zero.
            assert_eq!(same.estimate(), if k == 1 { 0.0 } else { 1.0 });
            let mut doubled = same.clone();
            doubled.merge(&same);
            assert_same_as_scan(&doubled, &same_oracle, &format!("k={k} duplicates merged"));
        }
    }

    /// The bytes do not depend on how the retained set was reached: built,
    /// merged or decoded, the same set serializes the same, and a decoded
    /// sketch keeps updating like the original.
    #[test]
    fn serialization_is_unchanged_by_the_index() {
        let k = 500;
        let mut built: Kmv = Kmv::new(k);
        let (mut left, mut right) = (
            Kmv::<DefaultXxHasher>::new(k),
            Kmv::<DefaultXxHasher>::new(k),
        );
        for id in 0..5_000u64 {
            let hash = id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            built.update_with_hash(hash);
            if id % 3 == 0 {
                left.update_with_hash(hash);
            } else {
                right.update_with_hash(hash);
            }
        }
        left.merge(&right);
        let bytes = built.to_asapv1().expect("serialize");
        assert_eq!(left.to_asapv1().expect("serialize merged"), bytes);
        let mut decoded: Kmv = Kmv::from_asapv1(&bytes).expect("decode");
        assert_eq!(decoded.to_asapv1().expect("re-serialize"), bytes);
        for id in 5_000..6_000u64 {
            let hash = id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            decoded.update_with_hash(hash);
            built.update_with_hash(hash);
        }
        // Re-inserting a retained hash after the decode is still a duplicate.
        let retained = sorted_hashes(&decoded);
        for &hash in &retained {
            decoded.update_with_hash(hash);
        }
        assert_eq!(sorted_hashes(&decoded), sorted_hashes(&built));
        assert_eq!(decoded.to_asapv1().unwrap(), built.to_asapv1().unwrap());
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
