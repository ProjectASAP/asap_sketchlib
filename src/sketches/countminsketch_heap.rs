//! CountMinSketchWithHeap: a convenient wrapper that pairs a [`CountMinSketch`] sketch with an
//! [`TopKHeap`] for automatic top-k heavy-hitter tracking.
//!
//! Every insertion updates both the frequency sketch and the heap, mirroring
//! the pattern used by `CountMinSketchFold` but without folding complexity.

use crate::{
    CountMinSketch, DataInput, DefaultMatrixI32, DefaultMatrixI64, DefaultMatrixI128,
    DefaultXxHasher, FastPath, FixedMatrix, HeapCounter, MatrixStorage, QuickMatrixI64,
    QuickMatrixI128, RegularPath, SketchHasher, TopKHeap, Vector2D, heap_item_to_sketch_input,
};

mod delta;
pub(crate) mod heap_wire;
mod wire;

pub use delta::CountMinSketchWithHeapDelta;

/// The heap count type of a `CountMinSketchWithHeap` over storage `S`.
type HeapCountOf<S> = <<S as MatrixStorage>::Counter as HeapCounter>::HeapCount;

const DEFAULT_HEAP_SIZE: usize = 32;

/// A Count-Min Sketch paired with a top-k heavy-hitter heap.
///
/// Generic over the same type parameters as [`CountMinSketch`]. The heap's counts are
/// `f64` over `f64` counters and `i64` otherwise ([`HeapCounter`]).
#[derive(Clone)]
pub struct CountMinSketchWithHeap<
    S: MatrixStorage = Vector2D<i64>,
    Mode = RegularPath,
    H: SketchHasher = DefaultXxHasher,
> where
    S::Counter: HeapCounter,
{
    count_min_sketch: CountMinSketch<S, Mode, H>,
    heap: TopKHeap<HeapCountOf<S>>,
}

// -- Construction for Vector2D-backed storage --------------------------------

impl<T, M, H: SketchHasher> CountMinSketchWithHeap<Vector2D<T>, M, H>
where
    T: Copy + Default + std::ops::AddAssign + HeapCounter,
{
    /// Creates a new `CountMinSketchWithHeap` with the given CMS dimensions and heap capacity.
    pub fn new(rows: usize, cols: usize, heap_size: usize) -> Self {
        CountMinSketchWithHeap {
            count_min_sketch: CountMinSketch::with_dimensions(rows, cols),
            heap: TopKHeap::new(heap_size),
        }
    }
}

// -- Construction from any MatrixStorage -------------------------------------

impl<S: MatrixStorage, M, H: SketchHasher> CountMinSketchWithHeap<S, M, H>
where
    S::Counter: HeapCounter,
{
    /// Creates a `CountMinSketchWithHeap` from a pre-built storage backend.
    pub fn from_storage(storage: S, heap_size: usize) -> Self {
        CountMinSketchWithHeap {
            count_min_sketch: CountMinSketch::from_storage(storage),
            heap: TopKHeap::new(heap_size),
        }
    }
}

// -- Default impls -----------------------------------------------------------
//
// Default is available for Vector2D-backed sketches and fixed-size matrix
// backends (Quick/Default/Fixed families). Use `from_storage(...)` when you
// want explicit backend control with a custom `top_k`.

impl Default for CountMinSketchWithHeap<Vector2D<i64>, RegularPath> {
    fn default() -> Self {
        Self::new(3, 4096, DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountMinSketchWithHeap<Vector2D<i64>, FastPath> {
    fn default() -> Self {
        Self::new(3, 4096, DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountMinSketchWithHeap<Vector2D<i32>, RegularPath> {
    fn default() -> Self {
        Self::new(3, 4096, DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountMinSketchWithHeap<Vector2D<i32>, FastPath> {
    fn default() -> Self {
        Self::new(3, 4096, DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountMinSketchWithHeap<FixedMatrix, RegularPath> {
    fn default() -> Self {
        Self::from_storage(FixedMatrix::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountMinSketchWithHeap<FixedMatrix, FastPath> {
    fn default() -> Self {
        Self::from_storage(FixedMatrix::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountMinSketchWithHeap<DefaultMatrixI32, RegularPath> {
    fn default() -> Self {
        Self::from_storage(DefaultMatrixI32::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountMinSketchWithHeap<DefaultMatrixI32, FastPath> {
    fn default() -> Self {
        Self::from_storage(DefaultMatrixI32::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountMinSketchWithHeap<QuickMatrixI64, RegularPath> {
    fn default() -> Self {
        Self::from_storage(QuickMatrixI64::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountMinSketchWithHeap<QuickMatrixI64, FastPath> {
    fn default() -> Self {
        Self::from_storage(QuickMatrixI64::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountMinSketchWithHeap<QuickMatrixI128, RegularPath> {
    fn default() -> Self {
        Self::from_storage(QuickMatrixI128::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountMinSketchWithHeap<QuickMatrixI128, FastPath> {
    fn default() -> Self {
        Self::from_storage(QuickMatrixI128::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountMinSketchWithHeap<DefaultMatrixI64, RegularPath> {
    fn default() -> Self {
        Self::from_storage(DefaultMatrixI64::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountMinSketchWithHeap<DefaultMatrixI64, FastPath> {
    fn default() -> Self {
        Self::from_storage(DefaultMatrixI64::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountMinSketchWithHeap<DefaultMatrixI128, RegularPath> {
    fn default() -> Self {
        Self::from_storage(DefaultMatrixI128::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountMinSketchWithHeap<DefaultMatrixI128, FastPath> {
    fn default() -> Self {
        Self::from_storage(DefaultMatrixI128::default(), DEFAULT_HEAP_SIZE)
    }
}

// -- Shared accessors (all storage types) ------------------------------------

impl<S: MatrixStorage, M, H: SketchHasher> CountMinSketchWithHeap<S, M, H>
where
    S::Counter: HeapCounter,
{
    /// Returns a reference to the internal CMS.
    pub fn count_min_sketch(&self) -> &CountMinSketch<S, M, H> {
        &self.count_min_sketch
    }

    /// Returns a mutable reference to the internal CMS.
    pub fn count_min_sketch_mut(&mut self) -> &mut CountMinSketch<S, M, H> {
        &mut self.count_min_sketch
    }

    /// Returns a reference to the heavy-hitter heap.
    pub fn heap(&self) -> &TopKHeap<HeapCountOf<S>> {
        &self.heap
    }

    /// Returns a mutable reference to the heavy-hitter heap.
    pub fn heap_mut(&mut self) -> &mut TopKHeap<HeapCountOf<S>> {
        &mut self.heap
    }

    /// Number of rows in the underlying CMS.
    #[inline(always)]
    pub fn rows(&self) -> usize {
        self.count_min_sketch.rows()
    }

    /// Number of columns in the underlying CMS.
    #[inline(always)]
    pub fn cols(&self) -> usize {
        self.count_min_sketch.cols()
    }

    /// Clears both the CMS counters (by rebuilding) and the heap.
    pub fn clear_heap(&mut self) {
        self.heap.clear();
    }
}

// -- RegularPath insert / estimate / merge -----------------------------------

impl<S: MatrixStorage, H: SketchHasher> CountMinSketchWithHeap<S, RegularPath, H>
where
    S::Counter: Copy + PartialOrd + From<i32> + std::ops::AddAssign + HeapCounter,
    S::Counter: Into<HeapCountOf<S>>,
{
    /// Inserts a single observation and updates the top-k heap.
    #[inline]
    pub fn update(&mut self, key: &DataInput) {
        self.count_min_sketch.update(key);
        let est = self.count_min_sketch.estimate(key);
        self.heap.update(key, est.into());
    }

    /// Inserts an observation with the given count and updates the top-k heap.
    #[inline]
    pub fn update_weighted(&mut self, key: &DataInput, weight: S::Counter) {
        self.count_min_sketch.update_weighted(key, weight);
        let est = self.count_min_sketch.estimate(key);
        self.heap.update(key, est.into());
    }

    /// Inserts a batch of observations, updating the heap after each.
    pub fn update_batch(&mut self, values: &[DataInput]) {
        for value in values {
            self.update(value);
        }
    }

    /// Returns the CMS frequency estimate for the given key.
    #[inline]
    pub fn estimate(&self, key: &DataInput) -> S::Counter {
        self.count_min_sketch.estimate(key)
    }

    /// Merges another `CountMinSketchWithHeap` into `self`.
    ///
    /// After merging the CMS counters, all heap items from both sources are
    /// re-queried against the merged sketch to reconcile the top-k heap.
    pub fn merge(&mut self, other: &Self) {
        self.count_min_sketch.merge(&other.count_min_sketch);
        let mut candidate_keys = Vec::with_capacity(self.heap.len() + other.heap.len());
        for item in self.heap.heap() {
            candidate_keys.push(item.key.clone());
        }
        for item in other.heap.heap() {
            candidate_keys.push(item.key.clone());
        }
        self.heap.clear();
        for key in candidate_keys {
            let key_ref = heap_item_to_sketch_input(&key);
            let est = self.count_min_sketch.estimate(&key_ref);
            self.heap.update(&key_ref, est.into());
        }
    }
}

// -- FastPath insert / estimate / merge --------------------------------------

impl<S, H: SketchHasher> CountMinSketchWithHeap<S, FastPath, H>
where
    S: MatrixStorage + crate::FastPathHasher<H>,
    S::Counter: Copy + PartialOrd + From<i32> + std::ops::AddAssign + HeapCounter,
    S::Counter: Into<HeapCountOf<S>>,
{
    /// Inserts a single observation using fast-path hashing and updates the heap.
    #[inline]
    pub fn update(&mut self, key: &DataInput) {
        self.count_min_sketch.update(key);
        let est = self.count_min_sketch.estimate(key);
        self.heap.update(key, est.into());
    }

    /// Inserts an observation with the given count using fast-path hashing.
    #[inline]
    pub fn update_weighted(&mut self, key: &DataInput, weight: S::Counter) {
        self.count_min_sketch.update_weighted(key, weight);
        let est = self.count_min_sketch.estimate(key);
        self.heap.update(key, est.into());
    }

    /// Inserts a batch of observations using fast-path hashing.
    pub fn update_batch(&mut self, values: &[DataInput]) {
        for value in values {
            self.update(value);
        }
    }

    /// Returns the CMS frequency estimate using fast-path hashing.
    #[inline]
    pub fn estimate(&self, key: &DataInput) -> S::Counter {
        self.count_min_sketch.estimate(key)
    }

    /// Merges another `CountMinSketchWithHeap` into `self`.
    pub fn merge(&mut self, other: &Self) {
        self.count_min_sketch.merge(&other.count_min_sketch);
        let mut candidate_keys = Vec::with_capacity(self.heap.len() + other.heap.len());
        for item in self.heap.heap() {
            candidate_keys.push(item.key.clone());
        }
        for item in other.heap.heap() {
            candidate_keys.push(item.key.clone());
        }
        self.heap.clear();
        for key in candidate_keys {
            let key_ref = heap_item_to_sketch_input(&key);
            let est = self.count_min_sketch.estimate(&key_ref);
            self.heap.update(&key_ref, est.into());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DataInput;
    use crate::test_utils::sample_zipf_u64;
    use std::collections::{HashMap, HashSet};

    fn heap_count_for_key<C: crate::HeapCount>(heap: &TopKHeap<C>, key: &DataInput) -> Option<C> {
        heap.heap()
            .iter()
            .find(|item| heap_item_to_sketch_input(&item.key) == *key)
            .map(|item| item.count)
    }

    fn run_zipf_stream_regular(
        rows: usize,
        cols: usize,
        top_k: usize,
        domain: usize,
        exponent: f64,
        samples: usize,
        seed: u64,
    ) -> (
        CountMinSketchWithHeap<Vector2D<i64>, RegularPath>,
        HashMap<u64, i64>,
    ) {
        let mut truth = HashMap::<u64, i64>::new();
        let mut sketch =
            CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(rows, cols, top_k);
        for value in sample_zipf_u64(domain, exponent, samples, seed) {
            let key = DataInput::U64(value);
            sketch.update(&key);
            *truth.entry(value).or_insert(0) += 1;
        }
        (sketch, truth)
    }

    fn run_zipf_stream_fast(
        rows: usize,
        cols: usize,
        top_k: usize,
        domain: usize,
        exponent: f64,
        samples: usize,
        seed: u64,
    ) -> (
        CountMinSketchWithHeap<Vector2D<i64>, FastPath>,
        HashMap<u64, i64>,
    ) {
        let mut truth = HashMap::<u64, i64>::new();
        let mut sketch = CountMinSketchWithHeap::<Vector2D<i64>, FastPath>::new(rows, cols, top_k);
        for value in sample_zipf_u64(domain, exponent, samples, seed) {
            let key = DataInput::U64(value);
            sketch.update(&key);
            *truth.entry(value).or_insert(0) += 1;
        }
        (sketch, truth)
    }

    fn top_k_truth_keys(truth: &HashMap<u64, i64>, k: usize) -> HashSet<u64> {
        let mut entries: Vec<(u64, i64)> =
            truth.iter().map(|(key, count)| (*key, *count)).collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        entries.into_iter().take(k).map(|(key, _)| key).collect()
    }

    fn top_k_heap_keys(heap: &TopKHeap) -> HashSet<u64> {
        heap.heap()
            .iter()
            .map(|item| match heap_item_to_sketch_input(&item.key) {
                DataInput::U64(v) => v,
                other => panic!("expected U64 key in zipf tests, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn insert_and_estimate() {
        // Verifies single-key inserts update both CMS estimate and visible frequency.
        let mut sh = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 64, 10);
        let key = DataInput::Str("hello");
        for _ in 0..5 {
            sh.update(&key);
        }
        assert_eq!(sh.estimate(&key), 5);
    }

    #[test]
    fn heap_tracks_top_k() {
        // Verifies heap keeps the highest-frequency keys within top-k capacity.
        let mut sh = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 256, 3);

        // Insert 5 distinct keys with different frequencies.
        for i in 1..=5u64 {
            let key = DataInput::U64(i);
            for _ in 0..(i * 10) {
                sh.update(&key);
            }
        }

        // Heap should contain at most 3 items (top-3).
        assert!(sh.heap().len() <= 3);

        // The top-3 should be keys 3, 4, 5 (counts 30, 40, 50).
        let mut counts: Vec<i64> = sh.heap().heap().iter().map(|item| item.count).collect();
        counts.sort_unstable();
        assert_eq!(counts, vec![30, 40, 50]);
    }

    #[test]
    fn merge_reconciles_heaps() {
        // Verifies merge combines CMS counters and refreshes heap counts accordingly.
        let mut a = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 256, 5);
        let mut b = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 256, 5);

        let key = DataInput::Str("merge_key");
        for _ in 0..10 {
            a.update(&key);
        }
        for _ in 0..20 {
            b.update(&key);
        }

        a.merge(&b);

        // After merge the estimate should be the sum.
        assert_eq!(a.estimate(&key), 30);

        // The heap should reflect the merged estimate.
        let heap_item = a
            .heap()
            .heap()
            .iter()
            .find(|item| {
                let k = heap_item_to_sketch_input(&item.key);
                k == key
            })
            .expect("key should be in heap");
        assert_eq!(heap_item.count, 30);
    }

    /// An `f64` sketch's heap holds the unrounded estimate after an insert and
    /// after a merge, on both paths.
    #[test]
    fn f64_heap_counts_are_the_unrounded_estimate() {
        let key = DataInput::Str("flow");
        let mut regular = CountMinSketchWithHeap::<Vector2D<f64>, RegularPath>::new(3, 64, 4);
        regular.update_weighted(&key, 2.25);
        assert_eq!(heap_count_for_key(regular.heap(), &key), Some(2.25));
        regular.merge(&regular.clone());
        assert_eq!(heap_count_for_key(regular.heap(), &key), Some(4.5));

        let mut fast = CountMinSketchWithHeap::<Vector2D<f64>, FastPath>::new(3, 64, 4);
        fast.update_weighted(&key, 2.25);
        assert_eq!(heap_count_for_key(fast.heap(), &key), Some(2.25));
        fast.merge(&fast.clone());
        assert_eq!(heap_count_for_key(fast.heap(), &key), Some(4.5));
    }

    #[test]
    fn insert_many_updates_estimate_and_heap() {
        // Verifies batched insert_many updates both estimate and heap entry count.
        let mut sh = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 1024, 4);
        let key = DataInput::Str("many");
        sh.update_weighted(&key, 11);

        assert_eq!(sh.estimate(&key), 11);
        assert_eq!(heap_count_for_key(sh.heap(), &key), Some(11));
    }

    #[test]
    fn bulk_insert_updates_multiple_keys() {
        // Verifies update_batch processes streams correctly across multiple keys.
        let mut sh = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 1024, 4);
        let values = vec![
            DataInput::U64(7),
            DataInput::U64(8),
            DataInput::U64(7),
            DataInput::U64(9),
            DataInput::U64(7),
        ];
        sh.update_batch(&values);

        assert_eq!(sh.estimate(&DataInput::U64(7)), 3);
        assert_eq!(sh.estimate(&DataInput::U64(8)), 1);
        assert_eq!(sh.estimate(&DataInput::U64(9)), 1);
        assert_eq!(heap_count_for_key(sh.heap(), &DataInput::U64(7)), Some(3));
    }

    #[test]
    fn clear_heap_keeps_cms_counters() {
        // Verifies clearing heap does not erase CMS counters or future heap rebuilds.
        let mut sh = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 1024, 2);
        let key = DataInput::Str("persist");
        sh.update_weighted(&key, 5);

        sh.clear_heap();
        assert!(sh.heap().is_empty());
        assert_eq!(sh.estimate(&key), 5);

        sh.update(&key);
        assert_eq!(heap_count_for_key(sh.heap(), &key), Some(6));
    }

    #[test]
    fn from_storage_uses_storage_dimensions() {
        // Verifies from_storage preserves backend dimensions and requested heap capacity.
        let storage = Vector2D::<i64>::init(4, 128);
        let sh = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::from_storage(storage, 9);

        assert_eq!(sh.rows(), 4);
        assert_eq!(sh.cols(), 128);
        assert_eq!(sh.heap().capacity(), 9);
    }

    #[test]
    fn merge_refreshes_existing_self_heap_entries() {
        // Verifies merge updates existing self heap keys to merged sketch estimates.
        let mut a = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 2048, 2);
        let mut b = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 2048, 1);
        let a_key = DataInput::Str("a-key");
        let c_key = DataInput::Str("c-key");
        let b_key = DataInput::Str("b-key");

        a.update_weighted(&a_key, 10);
        a.update_weighted(&c_key, 9);

        b.update_weighted(&a_key, 5);
        b.update_weighted(&b_key, 50);

        a.merge(&b);

        assert_eq!(a.estimate(&a_key), 15);
        assert_eq!(heap_count_for_key(a.heap(), &a_key), Some(15));
    }

    #[test]
    fn fast_path_insert_and_estimate() {
        // Verifies FastPath insert and estimate agree on repeated updates.
        let mut sh = CountMinSketchWithHeap::<Vector2D<i64>, FastPath>::new(3, 64, 10);
        let key = DataInput::Str("fast");
        for _ in 0..7 {
            sh.update(&key);
        }
        assert_eq!(sh.estimate(&key), 7);
    }

    #[test]
    fn fast_path_insert_many_and_bulk_insert() {
        // Verifies FastPath batched APIs keep estimate and heap counts in sync.
        let mut sh = CountMinSketchWithHeap::<Vector2D<i64>, FastPath>::new(3, 1024, 4);
        let key = DataInput::Str("fast-many");
        sh.update_weighted(&key, 6);
        sh.update_batch(&[
            DataInput::Str("fast-many"),
            DataInput::Str("another"),
            DataInput::Str("fast-many"),
        ]);

        assert_eq!(sh.estimate(&key), 8);
        assert_eq!(heap_count_for_key(sh.heap(), &key), Some(8));
    }

    #[test]
    fn fast_path_heap_tracks_top_k() {
        // Verifies FastPath heap still retains top-k counts after weighted inserts.
        let mut sh = CountMinSketchWithHeap::<Vector2D<i64>, FastPath>::new(3, 512, 3);

        for i in 1..=5u64 {
            let key = DataInput::U64(i);
            sh.update_weighted(&key, (i as i64) * 10);
        }

        let mut counts: Vec<i64> = sh.heap().heap().iter().map(|item| item.count).collect();
        counts.sort_unstable();
        assert_eq!(counts, vec![30, 40, 50]);
    }

    #[test]
    fn fast_path_merge_refreshes_existing_self_heap_entries() {
        // Verifies FastPath merge refreshes self heap entries using merged counts.
        let mut a = CountMinSketchWithHeap::<Vector2D<i64>, FastPath>::new(3, 2048, 2);
        let mut b = CountMinSketchWithHeap::<Vector2D<i64>, FastPath>::new(3, 2048, 1);
        let a_key = DataInput::Str("a-fast");
        let c_key = DataInput::Str("c-fast");
        let b_key = DataInput::Str("b-fast");

        a.update_weighted(&a_key, 10);
        a.update_weighted(&c_key, 9);

        b.update_weighted(&a_key, 5);
        b.update_weighted(&b_key, 50);

        a.merge(&b);

        assert_eq!(a.estimate(&a_key), 15);
        assert_eq!(heap_count_for_key(a.heap(), &a_key), Some(15));
    }

    #[test]
    fn default_construction() {
        // Verifies default CountMinSketchWithHeap uses expected dimensions and heap capacity.
        let sh = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::default();
        assert_eq!(sh.rows(), 3);
        assert_eq!(sh.cols(), 4096);
        assert_eq!(sh.heap().capacity(), DEFAULT_HEAP_SIZE);
    }

    #[test]
    fn default_construction_fixed_backends_parity() {
        // Verifies default constructors across backends maintain intended dimensions/capacity.
        let fixed_regular = CountMinSketchWithHeap::<FixedMatrix, RegularPath>::default();
        assert_eq!(fixed_regular.rows(), 5);
        assert_eq!(fixed_regular.cols(), 2048);
        assert_eq!(fixed_regular.heap().capacity(), DEFAULT_HEAP_SIZE);

        let fixed_fast = CountMinSketchWithHeap::<FixedMatrix, FastPath>::default();
        assert_eq!(fixed_fast.rows(), 5);
        assert_eq!(fixed_fast.cols(), 2048);
        assert_eq!(fixed_fast.heap().capacity(), DEFAULT_HEAP_SIZE);

        let dm_i32_regular = CountMinSketchWithHeap::<DefaultMatrixI32, RegularPath>::default();
        assert_eq!(dm_i32_regular.rows(), 3);
        assert_eq!(dm_i32_regular.cols(), 4096);
        assert_eq!(dm_i32_regular.heap().capacity(), DEFAULT_HEAP_SIZE);

        let dm_i32_fast = CountMinSketchWithHeap::<DefaultMatrixI32, FastPath>::default();
        assert_eq!(dm_i32_fast.rows(), 3);
        assert_eq!(dm_i32_fast.cols(), 4096);
        assert_eq!(dm_i32_fast.heap().capacity(), DEFAULT_HEAP_SIZE);

        let qm_i64_regular = CountMinSketchWithHeap::<QuickMatrixI64, RegularPath>::default();
        assert_eq!(qm_i64_regular.rows(), 5);
        assert_eq!(qm_i64_regular.cols(), 2048);
        assert_eq!(qm_i64_regular.heap().capacity(), DEFAULT_HEAP_SIZE);

        let qm_i64_fast = CountMinSketchWithHeap::<QuickMatrixI64, FastPath>::default();
        assert_eq!(qm_i64_fast.rows(), 5);
        assert_eq!(qm_i64_fast.cols(), 2048);
        assert_eq!(qm_i64_fast.heap().capacity(), DEFAULT_HEAP_SIZE);

        let qm_i128_regular = CountMinSketchWithHeap::<QuickMatrixI128, RegularPath>::default();
        assert_eq!(qm_i128_regular.rows(), 5);
        assert_eq!(qm_i128_regular.cols(), 2048);
        assert_eq!(qm_i128_regular.heap().capacity(), DEFAULT_HEAP_SIZE);

        let qm_i128_fast = CountMinSketchWithHeap::<QuickMatrixI128, FastPath>::default();
        assert_eq!(qm_i128_fast.rows(), 5);
        assert_eq!(qm_i128_fast.cols(), 2048);
        assert_eq!(qm_i128_fast.heap().capacity(), DEFAULT_HEAP_SIZE);

        let dm_i64_regular = CountMinSketchWithHeap::<DefaultMatrixI64, RegularPath>::default();
        assert_eq!(dm_i64_regular.rows(), 3);
        assert_eq!(dm_i64_regular.cols(), 4096);
        assert_eq!(dm_i64_regular.heap().capacity(), DEFAULT_HEAP_SIZE);

        let dm_i64_fast = CountMinSketchWithHeap::<DefaultMatrixI64, FastPath>::default();
        assert_eq!(dm_i64_fast.rows(), 3);
        assert_eq!(dm_i64_fast.cols(), 4096);
        assert_eq!(dm_i64_fast.heap().capacity(), DEFAULT_HEAP_SIZE);

        let dm_i128_regular = CountMinSketchWithHeap::<DefaultMatrixI128, RegularPath>::default();
        assert_eq!(dm_i128_regular.rows(), 3);
        assert_eq!(dm_i128_regular.cols(), 4096);
        assert_eq!(dm_i128_regular.heap().capacity(), DEFAULT_HEAP_SIZE);

        let dm_i128_fast = CountMinSketchWithHeap::<DefaultMatrixI128, FastPath>::default();
        assert_eq!(dm_i128_fast.rows(), 3);
        assert_eq!(dm_i128_fast.cols(), 4096);
        assert_eq!(dm_i128_fast.heap().capacity(), DEFAULT_HEAP_SIZE);
    }

    #[test]
    #[should_panic(expected = "dimension mismatch while merging CountMin sketches")]
    fn merge_requires_matching_dimensions_panics() {
        // Verifies merge rejects sketches with incompatible dimensions.
        let mut left = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 256, 4);
        let right = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(4, 256, 4);
        left.merge(&right);
    }

    #[test]
    fn heap_entries_match_cms_estimates_after_mutations() {
        // Verifies every heap entry count equals current CMS estimate after updates/merge.
        let mut sh = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 2048, 4);
        sh.update_weighted(&DataInput::Str("a"), 10);
        sh.update_weighted(&DataInput::Str("b"), 7);
        sh.update_batch(&[
            DataInput::Str("a"),
            DataInput::Str("c"),
            DataInput::Str("a"),
            DataInput::Str("d"),
        ]);

        for item in sh.heap().heap() {
            let key = heap_item_to_sketch_input(&item.key);
            assert_eq!(item.count, sh.estimate(&key));
        }

        let mut other = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 2048, 4);
        other.update_weighted(&DataInput::Str("b"), 9);
        other.update_weighted(&DataInput::Str("e"), 20);
        sh.merge(&other);

        for item in sh.heap().heap() {
            let key = heap_item_to_sketch_input(&item.key);
            assert_eq!(item.count, sh.estimate(&key));
        }
    }

    #[test]
    fn bulk_insert_equivalent_to_repeated_insert() {
        // Verifies update_batch yields same estimates and heap counts as repeated insert.
        let values = vec![
            DataInput::U64(1),
            DataInput::U64(2),
            DataInput::U64(1),
            DataInput::U64(3),
            DataInput::U64(2),
            DataInput::U64(1),
            DataInput::U64(4),
            DataInput::U64(2),
            DataInput::U64(5),
        ];

        let mut via_bulk = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 4096, 3);
        via_bulk.update_batch(&values);

        let mut via_repeat = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 4096, 3);
        for value in &values {
            via_repeat.update(value);
        }

        for key in [1_u64, 2, 3, 4, 5] {
            let k = DataInput::U64(key);
            assert_eq!(via_bulk.estimate(&k), via_repeat.estimate(&k));
            assert_eq!(
                heap_count_for_key(via_bulk.heap(), &k),
                heap_count_for_key(via_repeat.heap(), &k)
            );
        }
    }

    #[test]
    fn regular_vs_fast_equivalence_on_same_stream() {
        // Verifies regular and fast wrapper paths are consistent on identical short streams.
        let values = vec![
            DataInput::Str("alpha"),
            DataInput::Str("beta"),
            DataInput::Str("alpha"),
            DataInput::Str("gamma"),
            DataInput::Str("beta"),
            DataInput::Str("alpha"),
            DataInput::Str("delta"),
            DataInput::Str("gamma"),
            DataInput::Str("epsilon"),
            DataInput::Str("alpha"),
        ];

        let mut regular = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 4096, 3);
        let mut fast = CountMinSketchWithHeap::<Vector2D<i64>, FastPath>::new(3, 4096, 3);
        for value in &values {
            regular.update(value);
            fast.update(value);
        }

        for key in ["alpha", "beta", "gamma", "delta", "epsilon"] {
            let k = DataInput::Str(key);
            assert_eq!(regular.estimate(&k), fast.estimate(&k));
            assert_eq!(
                heap_count_for_key(regular.heap(), &k),
                heap_count_for_key(fast.heap(), &k)
            );
        }
    }

    #[test]
    fn merge_with_empty_other_and_empty_self() {
        // Verifies merge behaves correctly when either source sketch is empty.
        let mut non_empty = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 1024, 3);
        non_empty.update_weighted(&DataInput::Str("x"), 11);
        non_empty.update_weighted(&DataInput::Str("y"), 5);

        let empty = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 1024, 3);
        let before_len = non_empty.heap().len();
        let before_x = non_empty.estimate(&DataInput::Str("x"));
        non_empty.merge(&empty);
        assert_eq!(non_empty.heap().len(), before_len);
        assert_eq!(non_empty.estimate(&DataInput::Str("x")), before_x);

        let mut empty_self = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 1024, 3);
        empty_self.merge(&non_empty);
        assert_eq!(empty_self.estimate(&DataInput::Str("x")), before_x);
        assert!(heap_count_for_key(empty_self.heap(), &DataInput::Str("x")).is_some());
    }

    #[test]
    fn duplicate_candidate_keys_during_merge_do_not_corrupt_heap() {
        // Verifies duplicate merge candidates do not create duplicate heap entries.
        let mut left = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 2048, 4);
        let mut right = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 2048, 4);

        left.update_weighted(&DataInput::Str("dup"), 10);
        left.update_weighted(&DataInput::Str("left-only"), 7);

        right.update_weighted(&DataInput::Str("dup"), 9);
        right.update_weighted(&DataInput::Str("right-only"), 6);

        left.merge(&right);

        let dup_count = heap_count_for_key(left.heap(), &DataInput::Str("dup"));
        assert_eq!(dup_count, Some(19));
        assert_eq!(left.estimate(&DataInput::Str("dup")), 19);
        assert!(left.heap().len() <= left.heap().capacity());

        let dup_entries = left
            .heap()
            .heap()
            .iter()
            .filter(|item| heap_item_to_sketch_input(&item.key) == DataInput::Str("dup"))
            .count();
        assert_eq!(dup_entries, 1);
    }

    #[test]
    fn zipf_stream_top_k_recall_regular_fast_budget() {
        // Verifies regular-path heap captures most true heavy hitters under a Zipf stream.
        let rows = 3;
        let cols = 4096;
        let top_k = 16;
        let (sketch, truth) =
            run_zipf_stream_regular(rows, cols, top_k, 1024, 1.1, 20_000, 0x5eed_c0de);

        assert!(sketch.heap().len() <= top_k);
        for item in sketch.heap().heap() {
            let key = heap_item_to_sketch_input(&item.key);
            assert_eq!(item.count, sketch.estimate(&key));
        }

        let truth_top = top_k_truth_keys(&truth, top_k);
        let heap_top = top_k_heap_keys(sketch.heap());
        let recall_hits = truth_top.intersection(&heap_top).count();
        assert!(
            recall_hits >= 15,
            "top-k recall too low: hits={recall_hits}, truth_top={truth_top:?}, heap_top={heap_top:?}"
        );
    }

    #[test]
    fn zipf_stream_top_k_recall_fast_path_fast_budget() {
        // Verifies fast-path heap captures most true heavy hitters under a Zipf stream.
        let rows = 3;
        let cols = 4096;
        let top_k = 16;
        let (sketch, truth) =
            run_zipf_stream_fast(rows, cols, top_k, 1024, 1.1, 20_000, 0x5eed_c0de);

        assert!(sketch.heap().len() <= top_k);
        for item in sketch.heap().heap() {
            let key = heap_item_to_sketch_input(&item.key);
            assert_eq!(item.count, sketch.estimate(&key));
        }

        let truth_top = top_k_truth_keys(&truth, top_k);
        let heap_top = top_k_heap_keys(sketch.heap());
        let recall_hits = truth_top.intersection(&heap_top).count();
        assert!(
            recall_hits >= 15,
            "top-k recall too low: hits={recall_hits}, truth_top={truth_top:?}, heap_top={heap_top:?}"
        );
    }

    #[test]
    fn zipf_stream_regular_fast_heap_overlap() {
        // Verifies regular and fast paths produce highly overlapping top-k heaps on Zipf input.
        let rows = 3;
        let cols = 4096;
        let top_k = 16;
        let stream = sample_zipf_u64(1024, 1.1, 20_000, 0xABCD_1234);

        let mut regular =
            CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(rows, cols, top_k);
        let mut fast = CountMinSketchWithHeap::<Vector2D<i64>, FastPath>::new(rows, cols, top_k);
        for value in &stream {
            let key = DataInput::U64(*value);
            regular.update(&key);
            fast.update(&key);
        }

        let regular_heap_keys = top_k_heap_keys(regular.heap());
        let fast_heap_keys = top_k_heap_keys(fast.heap());
        let overlap = regular_heap_keys.intersection(&fast_heap_keys).count();
        assert!(
            (overlap as f64) / (top_k as f64) >= 0.8,
            "heap overlap too low: overlap={overlap}, regular={regular_heap_keys:?}, fast={fast_heap_keys:?}"
        );
    }
}
