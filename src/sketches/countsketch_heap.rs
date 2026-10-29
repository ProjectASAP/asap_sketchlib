//! Top-k heavy-hitter variant of the Count Sketch family.
//!
//! [`CountSketchWithHeap`] pairs a [`CountSketch`] sketch with a [`TopKHeap`] for automatic top-k tracking.
//! Every insertion updates both the frequency sketch and the heap, mirroring
//! the pattern used by `CountMinSketchWithHeap` but with Count Sketch (median estimator).

use crate::sketches::countsketch::CountSketchCounter;
use crate::{
    CountSketch, DataInput, DefaultMatrixI32, DefaultMatrixI64, DefaultMatrixI128, DefaultXxHasher,
    FastPath, FixedMatrix, HeapCounter, MatrixStorage, QuickMatrixI64, QuickMatrixI128,
    RegularPath, SketchHasher, TopKHeap, Vector2D,
};

mod delta;
mod wire;

pub use delta::CountSketchWithHeapDelta;

/// The count a `CountSketchWithHeap` entry over integer counters carries for a Count Sketch
/// estimate.
///
/// `CountSketch::estimate` returns the row **median** as `f64`, because a Count
/// Sketch estimate is signed and is a median rather than a counter read. The
/// integer counters' heap stores `i64`, so a conversion is unavoidable and its
/// semantics are part of the API rather than an accident of a cast:
///
/// - **Saturating**, not wrapping. Rust's `f64 as i64` saturates at
///   `i64::MIN`/`i64::MAX` and maps `NaN` to `0`; this function names that
///   choice so it cannot be silently changed. An `i128`-backed sketch really
///   can hold counts past `i64::MAX` — `CountSketchWithHeap<QuickMatrixI128>` and friends
///   accept `update_weighted(key, i128)` — and those clamp here. A wrapped negative
///   count would corrupt the heap's ordering, which is exactly what saturation
///   prevents.
/// - **Truncating toward zero** on the fractional part a median of an even row
///   count can produce.
///
/// Above `2^53` the estimate has already lost precision in `f64` inside
/// `CountSketch::estimate`, so an `i128` sketch's heap entries are exact only up to
/// that magnitude. Both limits are asserted in
/// `tests/e2e/matrix_instances.rs`.
#[inline]
pub(crate) fn heap_count_from_estimate(estimate: f64) -> i64 {
    // `as` on a float is a saturating cast in Rust (since 1.45); this is the
    // documented behaviour, not a fallback.
    estimate as i64
}

/// The heap count type of a `CountSketchWithHeap` over storage `S`.
type HeapCountOf<S> = <<S as MatrixStorage>::Counter as HeapCounter>::HeapCount;

/// A Count Sketch counter type a `CountSketchWithHeap` keeps a heap beside: the heap count
/// of a median estimate is the median cast to `i64` (saturating, truncating toward zero)
/// for `i32`, `i64` and `i128`, and the unrounded median for `f64`.
pub trait CountSketchHeapCounter: CountSketchCounter + HeapCounter {
    /// The heap count of a median `estimate`.
    fn heap_count(estimate: f64) -> Self::HeapCount;
}

impl CountSketchHeapCounter for i32 {
    fn heap_count(estimate: f64) -> i64 {
        heap_count_from_estimate(estimate)
    }
}

impl CountSketchHeapCounter for i64 {
    fn heap_count(estimate: f64) -> i64 {
        heap_count_from_estimate(estimate)
    }
}

impl CountSketchHeapCounter for i128 {
    fn heap_count(estimate: f64) -> i64 {
        heap_count_from_estimate(estimate)
    }
}

impl CountSketchHeapCounter for f64 {
    fn heap_count(estimate: f64) -> f64 {
        estimate
    }
}

use crate::common::heap::DEFAULT_HEAP_SIZE;

/// A Count Sketch paired with a top-k heavy-hitter heap.
///
/// Generic over the same type parameters as [`CountSketch`]. The heap's counts are
/// `f64` over `f64` counters and `i64` otherwise ([`CountSketchHeapCounter`]).
#[derive(Clone)]
pub struct CountSketchWithHeap<
    S: MatrixStorage = Vector2D<i64>,
    Mode = RegularPath,
    H: SketchHasher = DefaultXxHasher,
> where
    S::Counter: HeapCounter,
{
    count_sketch: CountSketch<S, Mode, H>,
    heap: TopKHeap<HeapCountOf<S>>,
}

// -- Construction for Vector2D-backed storage --------------------------------

impl<T, M, H: SketchHasher> CountSketchWithHeap<Vector2D<T>, M, H>
where
    T: CountSketchHeapCounter,
{
    /// Creates a new `CountSketchWithHeap` with the given CS dimensions and heap capacity.
    ///
    /// Panics when [`Self::check_shape`] refuses `rows` x `cols`.
    pub fn new(rows: usize, cols: usize, heap_size: usize) -> Self {
        CountSketchWithHeap {
            count_sketch: CountSketch::with_dimensions(rows, cols),
            heap: TopKHeap::new(heap_size),
        }
    }

    /// Bytes a sketch of `rows` x `cols` and `heap_size` holds once its heap is
    /// full of keys shaped like `key`, whether built by [`Self::new`], decoded,
    /// cloned or given a [`CountSketchWithHeapDelta`] through `merge_delta`: the struct,
    /// the counters and [`TopKHeap::theoretical_memory_bytes`].
    pub fn theoretical_memory_bytes(
        rows: usize,
        cols: usize,
        heap_size: usize,
        key: &DataInput,
    ) -> usize {
        (size_of::<Self>() - size_of::<TopKHeap<T::HeapCount>>())
            .saturating_add(rows.saturating_mul(cols).saturating_mul(size_of::<T>()))
            .saturating_add(TopKHeap::<T::HeapCount>::theoretical_memory_bytes(
                heap_size, key,
            ))
    }
}

// -- Construction from any MatrixStorage -------------------------------------

impl<S: MatrixStorage, M, H: SketchHasher> CountSketchWithHeap<S, M, H>
where
    S::Counter: CountSketchHeapCounter,
{
    /// Creates a `CountSketchWithHeap` from a pre-built storage backend.
    ///
    /// Panics when [`Self::check_shape`] refuses the storage's shape.
    pub fn from_storage(storage: S, heap_size: usize) -> Self {
        CountSketchWithHeap {
            count_sketch: CountSketch::from_storage(storage),
            heap: TopKHeap::new(heap_size),
        }
    }
}

// -- Default impls -----------------------------------------------------------
//
// Default is available for Vector2D-backed sketches and fixed-size matrix
// backends (Quick/Default/Fixed families). Use `from_storage(...)` when you
// want explicit backend control with a custom `top_k`.

impl Default for CountSketchWithHeap<Vector2D<i64>, RegularPath> {
    fn default() -> Self {
        Self::new(3, 4096, DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountSketchWithHeap<Vector2D<i64>, FastPath> {
    fn default() -> Self {
        Self::new(3, 4096, DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountSketchWithHeap<Vector2D<i32>, RegularPath> {
    fn default() -> Self {
        Self::new(3, 4096, DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountSketchWithHeap<Vector2D<i32>, FastPath> {
    fn default() -> Self {
        Self::new(3, 4096, DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountSketchWithHeap<FixedMatrix, RegularPath> {
    fn default() -> Self {
        Self::from_storage(FixedMatrix::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountSketchWithHeap<FixedMatrix, FastPath> {
    fn default() -> Self {
        Self::from_storage(FixedMatrix::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountSketchWithHeap<DefaultMatrixI32, RegularPath> {
    fn default() -> Self {
        Self::from_storage(DefaultMatrixI32::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountSketchWithHeap<DefaultMatrixI32, FastPath> {
    fn default() -> Self {
        Self::from_storage(DefaultMatrixI32::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountSketchWithHeap<QuickMatrixI64, RegularPath> {
    fn default() -> Self {
        Self::from_storage(QuickMatrixI64::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountSketchWithHeap<QuickMatrixI64, FastPath> {
    fn default() -> Self {
        Self::from_storage(QuickMatrixI64::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountSketchWithHeap<QuickMatrixI128, RegularPath> {
    fn default() -> Self {
        Self::from_storage(QuickMatrixI128::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountSketchWithHeap<QuickMatrixI128, FastPath> {
    fn default() -> Self {
        Self::from_storage(QuickMatrixI128::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountSketchWithHeap<DefaultMatrixI64, RegularPath> {
    fn default() -> Self {
        Self::from_storage(DefaultMatrixI64::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountSketchWithHeap<DefaultMatrixI64, FastPath> {
    fn default() -> Self {
        Self::from_storage(DefaultMatrixI64::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountSketchWithHeap<DefaultMatrixI128, RegularPath> {
    fn default() -> Self {
        Self::from_storage(DefaultMatrixI128::default(), DEFAULT_HEAP_SIZE)
    }
}

impl Default for CountSketchWithHeap<DefaultMatrixI128, FastPath> {
    fn default() -> Self {
        Self::from_storage(DefaultMatrixI128::default(), DEFAULT_HEAP_SIZE)
    }
}

// -- Shared accessors (all storage types) ------------------------------------

impl<S: MatrixStorage, M, H: SketchHasher> CountSketchWithHeap<S, M, H>
where
    S::Counter: CountSketchHeapCounter,
{
    /// Returns a reference to the internal Count Sketch.
    pub fn count_sketch(&self) -> &CountSketch<S, M, H> {
        &self.count_sketch
    }

    /// Returns a mutable reference to the internal Count Sketch.
    pub fn count_sketch_mut(&mut self) -> &mut CountSketch<S, M, H> {
        &mut self.count_sketch
    }

    /// Returns a reference to the heavy-hitter heap.
    pub fn heap(&self) -> &TopKHeap<HeapCountOf<S>> {
        &self.heap
    }

    /// Returns a mutable reference to the heavy-hitter heap.
    pub fn heap_mut(&mut self) -> &mut TopKHeap<HeapCountOf<S>> {
        &mut self.heap
    }

    /// Number of rows in the underlying CS.
    #[inline(always)]
    pub fn rows(&self) -> usize {
        self.count_sketch.rows()
    }

    /// Number of columns in the underlying CS.
    #[inline(always)]
    pub fn cols(&self) -> usize {
        self.count_sketch.cols()
    }

    /// Empties the heavy-hitter heap; the counters are kept.
    pub fn clear_heap(&mut self) {
        self.heap.clear();
    }

    /// Zeroes the counters and empties the heap, keeping the shape and the
    /// allocations.
    pub fn clear(&mut self) {
        self.count_sketch.clear();
        self.heap.clear();
    }

    /// Bytes this sketch holds: the struct, its `rows * cols` counters and
    /// [`TopKHeap::memory_bytes`].
    pub fn memory_bytes(&self) -> usize {
        size_of::<Self>() - size_of::<TopKHeap<HeapCountOf<S>>>()
            + self.rows() * self.cols() * size_of::<S::Counter>()
            + self.heap.memory_bytes()
    }
}

// -- Checked f64 updates ------------------------------------------------------

impl<H: SketchHasher> CountSketchWithHeap<Vector2D<f64>, RegularPath, H> {
    /// [`CountSketch::check_weight`]: Ok for a finite weight, the weights
    /// [`Self::try_update_weighted`] accepts.
    pub fn check_weight(weight: f64) -> Result<(), String> {
        CountSketch::<Vector2D<f64>, RegularPath, H>::check_weight(weight)
    }

    /// [`Self::update_weighted`] through [`CountSketch::try_update_weighted`]:
    /// rejects a non-finite weight and a counter overflow, leaving the
    /// counters and the heap unchanged on an error.
    pub fn try_update_weighted(&mut self, key: &DataInput, weight: f64) -> Result<(), String> {
        self.count_sketch.try_update_weighted(key, weight)?;
        let est = self.count_sketch.estimate(key);
        self.heap.update(key, est);
        Ok(())
    }
}

// -- RegularPath insert / estimate / merge -----------------------------------

impl<S: MatrixStorage, H: SketchHasher> CountSketchWithHeap<S, RegularPath, H>
where
    S::Counter: CountSketchHeapCounter,
{
    /// Inserts a single observation and updates the top-k heap.
    #[inline]
    pub fn update(&mut self, key: &DataInput) {
        self.count_sketch.update(key);
        let est = self.count_sketch.estimate(key);
        self.heap.update(key, S::Counter::heap_count(est));
    }

    /// Inserts an observation with the given count and updates the top-k heap.
    #[inline]
    pub fn update_weighted(&mut self, key: &DataInput, weight: S::Counter) {
        self.count_sketch.update_weighted(key, weight);
        let est = self.count_sketch.estimate(key);
        self.heap.update(key, S::Counter::heap_count(est));
    }

    /// Inserts a batch of observations, updating the heap after each.
    pub fn update_batch(&mut self, values: &[DataInput]) {
        for value in values {
            self.update(value);
        }
    }

    /// Returns the CS frequency estimate (median) for the given key.
    #[inline]
    pub fn estimate(&self, key: &DataInput) -> f64 {
        self.count_sketch.estimate(key)
    }

    /// Merges another `CountSketchWithHeap` into `self`.
    ///
    /// After merging the CS counters, all heap items from both sources are
    /// re-queried against the merged sketch to reconcile the top-k heap.
    pub fn merge(&mut self, other: &Self) {
        self.count_sketch.merge(&other.count_sketch);
        let mut candidate_keys = Vec::with_capacity(self.heap.len() + other.heap.len());
        for item in self.heap.heap() {
            candidate_keys.push(item.key.clone());
        }
        for item in other.heap.heap() {
            candidate_keys.push(item.key.clone());
        }
        crate::analytics::ops::add(candidate_keys.len());
        self.heap.clear();
        for key in candidate_keys {
            crate::analytics::ops::add(1);
            let key_ref = key.as_data_input();
            let est = self.count_sketch.estimate(&key_ref);
            self.heap.update(&key_ref, S::Counter::heap_count(est));
        }
    }

    /// Re-reads every heap resident's count from the counters, keeping the
    /// same residents. An update rescores only the key it touches, so a
    /// resident sharing a cell with it keeps its earlier count; a caller that
    /// needs every resident's count to equal its current estimate calls this
    /// before reading the heap. Costs one estimate per resident.
    pub fn rescore_heap(&mut self) {
        let rescored = self
            .heap
            .heap()
            .iter()
            .map(|item| {
                let est = self.count_sketch.estimate(&item.key.as_data_input());
                (item.key.clone(), S::Counter::heap_count(est))
            })
            .collect::<Vec<_>>();
        for (key, count) in rescored {
            self.heap.update_owned(&key, count);
        }
    }
}

// -- FastPath insert / estimate / merge --------------------------------------

impl<S, H: SketchHasher> CountSketchWithHeap<S, FastPath, H>
where
    S: MatrixStorage + crate::FastPathHasher<H>,
    S::Counter: CountSketchHeapCounter,
{
    /// Inserts a single observation using fast-path hashing and updates the heap.
    #[inline]
    pub fn update(&mut self, key: &DataInput) {
        self.count_sketch.update(key);
        let est = self.count_sketch.estimate(key);
        self.heap.update(key, S::Counter::heap_count(est));
    }

    /// Inserts an observation with the given count using fast-path hashing.
    #[inline]
    pub fn update_weighted(&mut self, key: &DataInput, weight: S::Counter) {
        self.count_sketch.update_weighted(key, weight);
        let est = self.count_sketch.estimate(key);
        self.heap.update(key, S::Counter::heap_count(est));
    }

    /// Inserts a batch of observations using fast-path hashing.
    pub fn update_batch(&mut self, values: &[DataInput]) {
        for value in values {
            self.update(value);
        }
    }

    /// Returns the CS frequency estimate (median) using fast-path hashing.
    #[inline]
    pub fn estimate(&self, key: &DataInput) -> f64 {
        self.count_sketch.estimate(key)
    }

    /// Merges another `CountSketchWithHeap` into `self`.
    pub fn merge(&mut self, other: &Self) {
        self.count_sketch.merge(&other.count_sketch);
        let mut candidate_keys = Vec::with_capacity(self.heap.len() + other.heap.len());
        for item in self.heap.heap() {
            candidate_keys.push(item.key.clone());
        }
        for item in other.heap.heap() {
            candidate_keys.push(item.key.clone());
        }
        crate::analytics::ops::add(candidate_keys.len());
        self.heap.clear();
        for key in candidate_keys {
            crate::analytics::ops::add(1);
            let key_ref = key.as_data_input();
            let est = self.count_sketch.estimate(&key_ref);
            self.heap.update(&key_ref, S::Counter::heap_count(est));
        }
    }
}

// Shape check; the rule is the same for every storage, path and hasher.
impl CountSketchWithHeap {
    /// The shapes this sketch builds; see [`CountSketch::check_shape`].
    pub fn check_shape(rows: usize, cols: usize) -> Result<(), String> {
        CountSketch::check_shape(rows, cols)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DataInput;
    use crate::test_utils::sample_zipf_u64;
    use std::collections::{HashMap, HashSet};

    /// `clear` zeroes the counters and empties the heap, keeping the shape and
    /// the memory held; `check_shape` is the matrix's.
    #[test]
    fn clear_empties_counters_and_heap_and_keeps_the_allocation() {
        let mut sketch = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 64, 8);
        for i in 0..2_000u64 {
            sketch.update(&DataInput::U64(i % 50));
        }
        assert_eq!(sketch.heap().len(), 8);
        let held = sketch.memory_bytes();
        sketch.clear();
        assert_eq!((sketch.rows(), sketch.cols()), (3, 64));
        assert!(sketch.heap().is_empty());
        assert!(
            sketch
                .count_sketch()
                .as_storage()
                .as_slice()
                .iter()
                .all(|&c| c == 0)
        );
        assert_eq!(sketch.memory_bytes(), held);
        assert!(CountSketchWithHeap::check_shape(21, 64).is_ok());
        assert!(CountSketchWithHeap::check_shape(0, 64).is_err());
    }

    fn heap_count_for_key<C: crate::HeapCount>(heap: &TopKHeap<C>, key: &DataInput) -> Option<C> {
        heap.heap()
            .iter()
            .find(|item| item.key.as_data_input() == *key)
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
        CountSketchWithHeap<Vector2D<i64>, RegularPath>,
        HashMap<u64, i64>,
    ) {
        let mut truth = HashMap::<u64, i64>::new();
        let mut sketch = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(rows, cols, top_k);
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
        CountSketchWithHeap<Vector2D<i64>, FastPath>,
        HashMap<u64, i64>,
    ) {
        let mut truth = HashMap::<u64, i64>::new();
        let mut sketch = CountSketchWithHeap::<Vector2D<i64>, FastPath>::new(rows, cols, top_k);
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
            .map(|item| match item.key.as_data_input() {
                DataInput::U64(v) => v,
                other => panic!("expected U64 key in zipf tests, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn update_and_estimate() {
        // Verifies single-key inserts update both CS estimate and wrapper behavior.
        let mut sh = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 256, 10);
        let key = DataInput::Str("hello");
        for _ in 0..5 {
            sh.update(&key);
        }
        assert!((sh.estimate(&key) - 5.0).abs() < 1e-9);
    }

    #[test]
    fn heap_tracks_top_k() {
        // Verifies heap retains only the highest-frequency keys under top-k limit.
        let mut sh = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 1024, 3);

        // Insert 5 distinct keys with different frequencies.
        for i in 1..=5u64 {
            let key = DataInput::U64(i);
            for _ in 0..(i * 100) {
                sh.update(&key);
            }
        }

        // Heap should contain at most 3 items (top-3).
        assert!(sh.heap().len() <= 3);

        // The top-3 counts should be 300, 400, 500.
        let mut counts: Vec<i64> = sh.heap().heap().iter().map(|item| item.count).collect();
        counts.sort_unstable();
        assert_eq!(counts, vec![300, 400, 500]);
    }

    #[test]
    fn merge_reconciles_heaps() {
        // Verifies merge combines sketches and refreshes heap counts from merged state.
        let mut a = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 256, 5);
        let mut b = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 256, 5);

        let key = DataInput::Str("merge_key");
        for _ in 0..10 {
            a.update(&key);
        }
        for _ in 0..20 {
            b.update(&key);
        }

        a.merge(&b);

        // After merge the estimate should be the sum.
        assert!((a.estimate(&key) - 30.0).abs() < 1e-9);

        // The heap should reflect the merged estimate.
        let heap_item = a
            .heap()
            .heap()
            .iter()
            .find(|item| {
                let k = item.key.as_data_input();
                k == key
            })
            .expect("key should be in heap");
        assert_eq!(heap_item.count, 30);
    }

    /// An `f64` sketch's heap holds the unrounded median after an insert and
    /// after a merge, on both paths.
    #[test]
    fn f64_heap_counts_are_the_unrounded_median() {
        let key = DataInput::Str("flow");
        let mut regular = CountSketchWithHeap::<Vector2D<f64>, RegularPath>::new(3, 64, 4);
        regular.update_weighted(&key, -2.25);
        assert_eq!(heap_count_for_key(regular.heap(), &key), Some(-2.25));
        regular.merge(&regular.clone());
        assert_eq!(heap_count_for_key(regular.heap(), &key), Some(-4.5));

        let mut fast = CountSketchWithHeap::<Vector2D<f64>, FastPath>::new(3, 64, 4);
        fast.update_weighted(&key, -2.25);
        assert_eq!(heap_count_for_key(fast.heap(), &key), Some(-2.25));
        fast.merge(&fast.clone());
        assert_eq!(heap_count_for_key(fast.heap(), &key), Some(-4.5));
    }

    /// An integer sketch's heap holds its median truncated toward zero: rows
    /// reading `-2` and `-3` give `-2.5`, held as `-2`.
    #[test]
    fn integer_heap_counts_truncate_the_median_toward_zero() {
        let key = DataInput::Str("flow");
        let mut sketch = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(2, 64, 4);
        sketch.update_weighted(&key, -2);
        let row = &mut sketch.count_sketch_mut().as_storage_mut()[1];
        let cell = row.iter_mut().find(|cell| **cell != 0).expect("key cell");
        *cell = *cell / 2 * 3;
        sketch.update_weighted(&key, 0);
        assert_eq!(sketch.estimate(&key), -2.5);
        assert_eq!(heap_count_for_key(sketch.heap(), &key), Some(-2));
    }

    #[test]
    fn update_weighted_updates_estimate_and_heap() {
        // Verifies update_weighted updates estimate and heap count consistently.
        let mut sh = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 2048, 4);
        let key = DataInput::Str("many");
        sh.update_weighted(&key, 17);

        let estimate = sh.estimate(&key);
        assert!((estimate - 17.0).abs() < 1e-9);
        assert_eq!(heap_count_for_key(sh.heap(), &key), Some(estimate as i64));
    }

    #[test]
    fn update_batch_updates_multiple_keys() {
        // Verifies bulk stream ingestion updates multiple keys and heap tracking.
        let mut sh = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 2048, 4);
        let values = vec![
            DataInput::U64(7),
            DataInput::U64(8),
            DataInput::U64(7),
            DataInput::U64(9),
            DataInput::U64(7),
        ];
        sh.update_batch(&values);

        let key = DataInput::U64(7);
        assert!((sh.estimate(&key) - 3.0).abs() < 1e-9);
        assert_eq!(
            heap_count_for_key(sh.heap(), &key),
            Some(sh.estimate(&key) as i64)
        );
    }

    #[test]
    fn clear_heap_keeps_cs_counters() {
        // Verifies clearing heap preserves sketch counters for future updates.
        let mut sh = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 2048, 2);
        let key = DataInput::Str("persist");
        sh.update_weighted(&key, 5);

        sh.clear_heap();
        assert!(sh.heap().is_empty());
        assert!((sh.estimate(&key) - 5.0).abs() < 1e-9);

        sh.update(&key);
        assert_eq!(
            heap_count_for_key(sh.heap(), &key),
            Some(sh.estimate(&key) as i64)
        );
    }

    #[test]
    fn from_storage_uses_storage_dimensions() {
        // Verifies from_storage keeps backend dimensions and heap capacity.
        let storage = Vector2D::<i64>::unfilled(4, 128);
        let sh = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::from_storage(storage, 9);

        assert_eq!(sh.rows(), 4);
        assert_eq!(sh.cols(), 128);
        assert_eq!(sh.heap().capacity(), 9);
    }

    #[test]
    fn merge_refreshes_existing_self_heap_entries() {
        // Verifies merge refreshes pre-existing self heap keys to merged estimates.
        let mut a = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 4096, 2);
        let mut b = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 4096, 1);
        let a_key = DataInput::Str("a-key");
        let c_key = DataInput::Str("c-key");
        let b_key = DataInput::Str("b-key");

        a.update_weighted(&a_key, 120);
        a.update_weighted(&c_key, 10);

        b.update_weighted(&a_key, 40);
        b.update_weighted(&b_key, 400);

        a.merge(&b);

        let merged_estimate = a.estimate(&a_key) as i64;
        assert_eq!(heap_count_for_key(a.heap(), &a_key), Some(merged_estimate));
    }

    #[test]
    fn fast_path_update_and_estimate() {
        // Verifies FastPath insert and estimate stay coherent for repeated keys.
        let mut sh = CountSketchWithHeap::<Vector2D<i64>, FastPath>::new(5, 256, 10);
        let key = DataInput::Str("fast");
        for _ in 0..7 {
            sh.update(&key);
        }
        assert!((sh.estimate(&key) - 7.0).abs() < 1e-9);
    }

    #[test]
    fn fast_path_update_weighted_and_update_batch() {
        // Verifies FastPath batched APIs maintain estimate/heap consistency.
        let mut sh = CountSketchWithHeap::<Vector2D<i64>, FastPath>::new(5, 2048, 4);
        let key = DataInput::Str("fast-many");
        sh.update_weighted(&key, 6);
        sh.update_batch(&[
            DataInput::Str("fast-many"),
            DataInput::Str("another"),
            DataInput::Str("fast-many"),
        ]);

        let estimate = sh.estimate(&key);
        assert!((estimate - 8.0).abs() < 1e-9);
        assert_eq!(heap_count_for_key(sh.heap(), &key), Some(estimate as i64));
    }

    #[test]
    fn fast_path_heap_tracks_top_k() {
        // Verifies FastPath top-k maintenance under weighted updates.
        let mut sh = CountSketchWithHeap::<Vector2D<i64>, FastPath>::new(5, 4096, 3);

        for i in 1..=5u64 {
            let key = DataInput::U64(i);
            sh.update_weighted(&key, (i as i64) * 100);
        }

        let mut counts: Vec<i64> = sh.heap().heap().iter().map(|item| item.count).collect();
        counts.sort_unstable();
        assert_eq!(counts, vec![300, 400, 500]);
    }

    #[test]
    fn fast_path_merge_refreshes_existing_self_heap_entries() {
        // Verifies FastPath merge refreshes self heap counts from merged sketch.
        let mut a = CountSketchWithHeap::<Vector2D<i64>, FastPath>::new(5, 4096, 2);
        let mut b = CountSketchWithHeap::<Vector2D<i64>, FastPath>::new(5, 4096, 1);
        let a_key = DataInput::Str("a-fast");
        let c_key = DataInput::Str("c-fast");
        let b_key = DataInput::Str("b-fast");

        a.update_weighted(&a_key, 120);
        a.update_weighted(&c_key, 10);

        b.update_weighted(&a_key, 40);
        b.update_weighted(&b_key, 400);

        a.merge(&b);

        let merged_estimate = a.estimate(&a_key) as i64;
        assert_eq!(heap_count_for_key(a.heap(), &a_key), Some(merged_estimate));
    }

    #[test]
    fn default_construction() {
        // Verifies default CountSketchWithHeap dimensions and default top-k capacity.
        let sh = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::default();
        assert_eq!(sh.rows(), 3);
        assert_eq!(sh.cols(), 4096);
        assert_eq!(sh.heap().capacity(), DEFAULT_HEAP_SIZE);
    }

    #[test]
    fn default_construction_fixed_backends_parity() {
        // Verifies default construction parity across all supported backends.
        let fixed_regular = CountSketchWithHeap::<FixedMatrix, RegularPath>::default();
        assert_eq!(fixed_regular.rows(), 5);
        assert_eq!(fixed_regular.cols(), 2048);
        assert_eq!(fixed_regular.heap().capacity(), DEFAULT_HEAP_SIZE);

        let fixed_fast = CountSketchWithHeap::<FixedMatrix, FastPath>::default();
        assert_eq!(fixed_fast.rows(), 5);
        assert_eq!(fixed_fast.cols(), 2048);
        assert_eq!(fixed_fast.heap().capacity(), DEFAULT_HEAP_SIZE);

        let dm_i32_regular = CountSketchWithHeap::<DefaultMatrixI32, RegularPath>::default();
        assert_eq!(dm_i32_regular.rows(), 3);
        assert_eq!(dm_i32_regular.cols(), 4096);
        assert_eq!(dm_i32_regular.heap().capacity(), DEFAULT_HEAP_SIZE);

        let dm_i32_fast = CountSketchWithHeap::<DefaultMatrixI32, FastPath>::default();
        assert_eq!(dm_i32_fast.rows(), 3);
        assert_eq!(dm_i32_fast.cols(), 4096);
        assert_eq!(dm_i32_fast.heap().capacity(), DEFAULT_HEAP_SIZE);

        let qm_i64_regular = CountSketchWithHeap::<QuickMatrixI64, RegularPath>::default();
        assert_eq!(qm_i64_regular.rows(), 5);
        assert_eq!(qm_i64_regular.cols(), 2048);
        assert_eq!(qm_i64_regular.heap().capacity(), DEFAULT_HEAP_SIZE);

        let qm_i64_fast = CountSketchWithHeap::<QuickMatrixI64, FastPath>::default();
        assert_eq!(qm_i64_fast.rows(), 5);
        assert_eq!(qm_i64_fast.cols(), 2048);
        assert_eq!(qm_i64_fast.heap().capacity(), DEFAULT_HEAP_SIZE);

        let qm_i128_regular = CountSketchWithHeap::<QuickMatrixI128, RegularPath>::default();
        assert_eq!(qm_i128_regular.rows(), 5);
        assert_eq!(qm_i128_regular.cols(), 2048);
        assert_eq!(qm_i128_regular.heap().capacity(), DEFAULT_HEAP_SIZE);

        let qm_i128_fast = CountSketchWithHeap::<QuickMatrixI128, FastPath>::default();
        assert_eq!(qm_i128_fast.rows(), 5);
        assert_eq!(qm_i128_fast.cols(), 2048);
        assert_eq!(qm_i128_fast.heap().capacity(), DEFAULT_HEAP_SIZE);

        let dm_i64_regular = CountSketchWithHeap::<DefaultMatrixI64, RegularPath>::default();
        assert_eq!(dm_i64_regular.rows(), 3);
        assert_eq!(dm_i64_regular.cols(), 4096);
        assert_eq!(dm_i64_regular.heap().capacity(), DEFAULT_HEAP_SIZE);

        let dm_i64_fast = CountSketchWithHeap::<DefaultMatrixI64, FastPath>::default();
        assert_eq!(dm_i64_fast.rows(), 3);
        assert_eq!(dm_i64_fast.cols(), 4096);
        assert_eq!(dm_i64_fast.heap().capacity(), DEFAULT_HEAP_SIZE);

        let dm_i128_regular = CountSketchWithHeap::<DefaultMatrixI128, RegularPath>::default();
        assert_eq!(dm_i128_regular.rows(), 3);
        assert_eq!(dm_i128_regular.cols(), 4096);
        assert_eq!(dm_i128_regular.heap().capacity(), DEFAULT_HEAP_SIZE);

        let dm_i128_fast = CountSketchWithHeap::<DefaultMatrixI128, FastPath>::default();
        assert_eq!(dm_i128_fast.rows(), 3);
        assert_eq!(dm_i128_fast.cols(), 4096);
        assert_eq!(dm_i128_fast.heap().capacity(), DEFAULT_HEAP_SIZE);
    }

    #[test]
    #[should_panic(expected = "dimension mismatch while merging Count Sketches")]
    fn merge_requires_matching_dimensions_panics() {
        // Verifies merge panics when dimensions differ.
        let mut left = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 256, 4);
        let right = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(6, 256, 4);
        left.merge(&right);
    }

    #[test]
    fn heap_entries_match_cs_estimates_after_mutations() {
        // Verifies heap entries always match current sketch estimates after mutations.
        let mut sh = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 4096, 4);
        sh.update_weighted(&DataInput::Str("a"), 100);
        sh.update_weighted(&DataInput::Str("b"), 70);
        sh.update_batch(&[
            DataInput::Str("a"),
            DataInput::Str("c"),
            DataInput::Str("a"),
            DataInput::Str("d"),
        ]);

        for item in sh.heap().heap() {
            let key = item.key.as_data_input();
            assert_eq!(item.count, sh.estimate(&key) as i64);
        }

        let mut other = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 4096, 4);
        other.update_weighted(&DataInput::Str("b"), 90);
        other.update_weighted(&DataInput::Str("e"), 200);
        sh.merge(&other);

        for item in sh.heap().heap() {
            let key = item.key.as_data_input();
            assert_eq!(item.count, sh.estimate(&key) as i64);
        }
    }

    #[test]
    fn update_batch_equivalent_to_repeated_update() {
        // Verifies update_batch behavior matches repeated insert behavior.
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

        let mut via_bulk = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 4096, 3);
        via_bulk.update_batch(&values);

        let mut via_repeat = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 4096, 3);
        for value in &values {
            via_repeat.update(value);
        }

        for key in [1_u64, 2, 3, 4, 5] {
            let k = DataInput::U64(key);
            assert!((via_bulk.estimate(&k) - via_repeat.estimate(&k)).abs() < 1e-9);
            assert_eq!(
                heap_count_for_key(via_bulk.heap(), &k),
                heap_count_for_key(via_repeat.heap(), &k)
            );
        }
    }

    #[test]
    fn regular_vs_fast_equivalence_on_same_stream() {
        // Verifies regular and fast wrapper paths match on a short deterministic stream.
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

        let mut regular = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 4096, 3);
        let mut fast = CountSketchWithHeap::<Vector2D<i64>, FastPath>::new(5, 4096, 3);
        for value in &values {
            regular.update(value);
            fast.update(value);
        }

        for key in ["alpha", "beta", "gamma", "delta", "epsilon"] {
            let k = DataInput::Str(key);
            assert!((regular.estimate(&k) - fast.estimate(&k)).abs() < 1e-9);
            assert_eq!(
                heap_count_for_key(regular.heap(), &k),
                heap_count_for_key(fast.heap(), &k)
            );
        }
    }

    #[test]
    fn merge_with_empty_other_and_empty_self() {
        // Verifies merge behavior is stable when one side is empty.
        let mut non_empty = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 2048, 3);
        non_empty.update_weighted(&DataInput::Str("x"), 110);
        non_empty.update_weighted(&DataInput::Str("y"), 50);

        let empty = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 2048, 3);
        let before_len = non_empty.heap().len();
        let before_x = non_empty.estimate(&DataInput::Str("x"));
        non_empty.merge(&empty);
        assert_eq!(non_empty.heap().len(), before_len);
        assert!((non_empty.estimate(&DataInput::Str("x")) - before_x).abs() < 1e-9);

        let mut empty_self = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 2048, 3);
        empty_self.merge(&non_empty);
        assert!((empty_self.estimate(&DataInput::Str("x")) - before_x).abs() < 1e-9);
        assert!(heap_count_for_key(empty_self.heap(), &DataInput::Str("x")).is_some());
    }

    #[test]
    fn duplicate_candidate_keys_during_merge_do_not_corrupt_heap() {
        // Verifies duplicate merge candidates do not duplicate heap entries.
        let mut left = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 4096, 4);
        let mut right = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 4096, 4);

        left.update_weighted(&DataInput::Str("dup"), 100);
        left.update_weighted(&DataInput::Str("left-only"), 70);

        right.update_weighted(&DataInput::Str("dup"), 90);
        right.update_weighted(&DataInput::Str("right-only"), 60);

        left.merge(&right);

        let merged_estimate = left.estimate(&DataInput::Str("dup")) as i64;
        let dup_count = heap_count_for_key(left.heap(), &DataInput::Str("dup"));
        assert_eq!(dup_count, Some(merged_estimate));
        assert!(left.heap().len() <= left.heap().capacity());

        let dup_entries = left
            .heap()
            .heap()
            .iter()
            .filter(|item| item.key.as_data_input() == DataInput::Str("dup"))
            .count();
        assert_eq!(dup_entries, 1);
    }

    #[test]
    fn zipf_stream_top_k_recall_regular_fast_budget() {
        // Verifies regular-path heap captures most true heavy hitters under a Zipf stream.
        let rows = 5;
        let cols = 4096;
        let top_k = 16;
        let (sketch, truth) =
            run_zipf_stream_regular(rows, cols, top_k, 1024, 1.1, 20_000, 0x5eed_c0de);

        assert!(sketch.heap().len() <= top_k);
        for item in sketch.heap().heap() {
            let key = item.key.as_data_input();
            assert_eq!(item.count, sketch.estimate(&key) as i64);
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
        let rows = 5;
        let cols = 4096;
        let top_k = 16;
        let (sketch, truth) =
            run_zipf_stream_fast(rows, cols, top_k, 1024, 1.1, 20_000, 0x5eed_c0de);

        assert!(sketch.heap().len() <= top_k);
        for item in sketch.heap().heap() {
            let key = item.key.as_data_input();
            assert_eq!(item.count, sketch.estimate(&key) as i64);
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
        let rows = 5;
        let cols = 4096;
        let top_k = 16;
        let stream = sample_zipf_u64(1024, 1.1, 20_000, 0xABCD_1234);

        let mut regular = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(rows, cols, top_k);
        let mut fast = CountSketchWithHeap::<Vector2D<i64>, FastPath>::new(rows, cols, top_k);
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

    fn f64_heap_entries(
        sketch: &CountSketchWithHeap<Vector2D<f64>, RegularPath>,
    ) -> Vec<(crate::OwnedDataInput, f64)> {
        let mut entries = sketch
            .heap()
            .heap()
            .iter()
            .map(|item| (item.key.clone(), item.count))
            .collect::<Vec<_>>();
        entries.sort_by(|a, b| a.1.total_cmp(&b.1));
        entries
    }

    // A checked f64 update adds signed fractional weights, rejects a
    // non-finite weight and a counter overflow in either direction, and on an
    // error leaves the counters and the heap as they were.
    #[test]
    fn f64_checked_updates_are_all_or_nothing() {
        let mut sketch = CountSketchWithHeap::<Vector2D<f64>, RegularPath>::new(5, 4096, 4);
        let key = DataInput::I64(7);
        sketch.try_update_weighted(&key, -0.125).unwrap();
        sketch.try_update_weighted(&key, -0.25).unwrap();
        assert_eq!(sketch.estimate(&key), -0.375);
        let resident = sketch
            .heap()
            .heap()
            .iter()
            .find(|item| item.key.as_data_input() == key)
            .map(|item| item.count);
        assert_eq!(resident, Some(-0.375));
        sketch
            .try_update_weighted(&DataInput::I64(8), -f64::MAX)
            .unwrap();

        let cells = sketch.count_sketch().as_storage().as_slice().to_vec();
        let heap = f64_heap_entries(&sketch);
        for weight in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(
                CountSketchWithHeap::<Vector2D<f64>, RegularPath>::check_weight(weight).is_err()
            );
            assert!(sketch.try_update_weighted(&key, weight).is_err());
        }
        assert!(
            sketch
                .try_update_weighted(&DataInput::I64(8), -f64::MAX)
                .is_err()
        );
        assert_eq!(sketch.count_sketch().as_storage().as_slice(), cells);
        assert_eq!(f64_heap_entries(&sketch), heap);
    }

    // Under heavy collisions an update moves cells other residents share, so
    // their heap counts go stale; `rescore_heap` re-reads every resident and
    // leaves each count equal to its current estimate.
    #[test]
    fn rescore_heap_rereads_every_resident() {
        let mut sketch = CountSketchWithHeap::<Vector2D<f64>, RegularPath>::new(3, 8, 4);
        let mut stale = 0;
        for step in 0..60i64 {
            sketch
                .try_update_weighted(&DataInput::I64(step % 7), 0.5 + (step % 3) as f64)
                .unwrap();
            stale += sketch
                .heap()
                .heap()
                .iter()
                .filter(|item| item.count != sketch.estimate(&item.key.as_data_input()))
                .count();
            sketch.rescore_heap();
            assert!(sketch.heap().len() <= 4);
            for item in sketch.heap().heap() {
                assert_eq!(item.count, sketch.estimate(&item.key.as_data_input()));
            }
        }
        assert!(stale > 0, "8 columns must leave some resident stale");
    }
}
