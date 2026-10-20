//! Top-k heavy-hitter variants of the Count Sketch family.
//!
//! - [`CountSketchWithHeap`]: pairs a [`CountSketch`] sketch with an [`TopKHeap`] for automatic top-k tracking.
//!   Every insertion updates both the frequency sketch and the heap, mirroring
//!   the pattern used by `CountMinSketchWithHeap` but with Count Sketch (median estimator).
//! - [`CountSketchWithL2Norm`]: Count Sketch augmented with per-row L2 norm tracking for
//!   heavy-hitter detection.

use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use crate::common::structures::matrix_storage::cols_mask_bits;
use crate::octo_delta::CountSketchCellDelta;
use crate::sketches::countsketch::CountSketchCounter;
use crate::{
    CountSketch, DataInput, DefaultMatrixI32, DefaultMatrixI64, DefaultMatrixI128, DefaultXxHasher,
    FastPath, FixedMatrix, HeapCounter, MatrixStorage, QuickMatrixI64, QuickMatrixI128,
    RegularPath, SketchHasher, TopKHeap, Vector1D, Vector2D, compute_median_inline_f64,
    heap_item_to_sketch_input,
};

mod delta;
pub(crate) mod l2hh_wire;
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
pub fn heap_count_from_estimate(estimate: f64) -> i64 {
    // `as` on a float is a saturating cast in Rust (since 1.45); this is the
    // documented behaviour, not a fallback.
    estimate as i64
}

/// The heap count type of a `CountSketchWithHeap` over storage `S`.
type HeapCountOf<S> = <<S as MatrixStorage>::Counter as HeapCounter>::HeapCount;

/// A Count Sketch counter type a `CountSketchWithHeap` keeps a heap beside: the heap count
/// of a median estimate is [`heap_count_from_estimate`]'s for `i32`, `i64` and `i128`, and
/// the unrounded median for `f64`.
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

const DEFAULT_HEAP_SIZE: usize = 32;
const DEFAULT_ROWS: usize = 3;
const DEFAULT_COLS: usize = 4096;

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
    cs: CountSketch<S, Mode, H>,
    heap: TopKHeap<HeapCountOf<S>>,
}

// -- Construction for Vector2D-backed storage --------------------------------

impl<T, M, H: SketchHasher> CountSketchWithHeap<Vector2D<T>, M, H>
where
    T: CountSketchHeapCounter,
{
    /// Creates a new `CountSketchWithHeap` with the given CS dimensions and heap capacity.
    pub fn new(rows: usize, cols: usize, top_k: usize) -> Self {
        CountSketchWithHeap {
            cs: CountSketch::with_dimensions(rows, cols),
            heap: TopKHeap::new(top_k),
        }
    }
}

// -- Construction from any MatrixStorage -------------------------------------

impl<S: MatrixStorage, M, H: SketchHasher> CountSketchWithHeap<S, M, H>
where
    S::Counter: CountSketchHeapCounter,
{
    /// Creates a `CountSketchWithHeap` from a pre-built storage backend.
    pub fn from_storage(storage: S, top_k: usize) -> Self {
        CountSketchWithHeap {
            cs: CountSketch::from_storage(storage),
            heap: TopKHeap::new(top_k),
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
    pub fn cs(&self) -> &CountSketch<S, M, H> {
        &self.cs
    }

    /// Returns a mutable reference to the internal Count Sketch.
    pub fn cs_mut(&mut self) -> &mut CountSketch<S, M, H> {
        &mut self.cs
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
        self.cs.rows()
    }

    /// Number of columns in the underlying CS.
    #[inline(always)]
    pub fn cols(&self) -> usize {
        self.cs.cols()
    }

    /// Clears the heap.
    pub fn clear_heap(&mut self) {
        self.heap.clear();
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
        self.cs.update(key);
        let est = self.cs.estimate(key);
        self.heap.update(key, S::Counter::heap_count(est));
    }

    /// Inserts an observation with the given count and updates the top-k heap.
    #[inline]
    pub fn update_weighted(&mut self, key: &DataInput, weight: S::Counter) {
        self.cs.update_weighted(key, weight);
        let est = self.cs.estimate(key);
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
        self.cs.estimate(key)
    }

    /// Merges another `CountSketchWithHeap` into `self`.
    ///
    /// After merging the CS counters, all heap items from both sources are
    /// re-queried against the merged sketch to reconcile the top-k heap.
    pub fn merge(&mut self, other: &Self) {
        self.cs.merge(&other.cs);
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
            let est = self.cs.estimate(&key_ref);
            self.heap.update(&key_ref, S::Counter::heap_count(est));
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
        self.cs.update(key);
        let est = self.cs.estimate(key);
        self.heap.update(key, S::Counter::heap_count(est));
    }

    /// Inserts an observation with the given count using fast-path hashing.
    #[inline]
    pub fn update_weighted(&mut self, key: &DataInput, weight: S::Counter) {
        self.cs.update_weighted(key, weight);
        let est = self.cs.estimate(key);
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
        self.cs.estimate(key)
    }

    /// Merges another `CountSketchWithHeap` into `self`.
    pub fn merge(&mut self, other: &Self) {
        self.cs.merge(&other.cs);
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
            let est = self.cs.estimate(&key_ref);
            self.heap.update(&key_ref, S::Counter::heap_count(est));
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
            .map(|item| match heap_item_to_sketch_input(&item.key) {
                DataInput::U64(v) => v,
                other => panic!("expected U64 key in zipf tests, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn insert_and_estimate() {
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
                let k = heap_item_to_sketch_input(&item.key);
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
        let row = &mut sketch.cs_mut().as_storage_mut()[1];
        let cell = row.iter_mut().find(|cell| **cell != 0).expect("key cell");
        *cell = *cell / 2 * 3;
        sketch.update_weighted(&key, 0);
        assert_eq!(sketch.estimate(&key), -2.5);
        assert_eq!(heap_count_for_key(sketch.heap(), &key), Some(-2));
    }

    #[test]
    fn insert_many_updates_estimate_and_heap() {
        // Verifies insert_many updates estimate and heap count consistently.
        let mut sh = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 2048, 4);
        let key = DataInput::Str("many");
        sh.update_weighted(&key, 17);

        let estimate = sh.estimate(&key);
        assert!((estimate - 17.0).abs() < 1e-9);
        assert_eq!(heap_count_for_key(sh.heap(), &key), Some(estimate as i64));
    }

    #[test]
    fn bulk_insert_updates_multiple_keys() {
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
        let storage = Vector2D::<i64>::init(4, 128);
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
    fn fast_path_insert_and_estimate() {
        // Verifies FastPath insert and estimate stay coherent for repeated keys.
        let mut sh = CountSketchWithHeap::<Vector2D<i64>, FastPath>::new(5, 256, 10);
        let key = DataInput::Str("fast");
        for _ in 0..7 {
            sh.update(&key);
        }
        assert!((sh.estimate(&key) - 7.0).abs() < 1e-9);
    }

    #[test]
    fn fast_path_insert_many_and_bulk_insert() {
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
    #[should_panic(expected = "dimension mismatch while merging CountMin sketches")]
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
            let key = heap_item_to_sketch_input(&item.key);
            assert_eq!(item.count, sh.estimate(&key) as i64);
        }

        let mut other = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 4096, 4);
        other.update_weighted(&DataInput::Str("b"), 90);
        other.update_weighted(&DataInput::Str("e"), 200);
        sh.merge(&other);

        for item in sh.heap().heap() {
            let key = heap_item_to_sketch_input(&item.key);
            assert_eq!(item.count, sh.estimate(&key) as i64);
        }
    }

    #[test]
    fn bulk_insert_equivalent_to_repeated_insert() {
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
            .filter(|item| heap_item_to_sketch_input(&item.key) == DataInput::Str("dup"))
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
            let key = heap_item_to_sketch_input(&item.key);
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
            let key = heap_item_to_sketch_input(&item.key);
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
}

// =====================================================================
// CountSketchWithL2Norm — Count Sketch with per-row L2 norm tracking for
// heavy-hitter detection. Moved here from `count.rs` so the algorithm
// file stays focused on plain Count Sketch.
// =====================================================================

/// Checks that a `rows` x `cols` `CountSketchWithL2Norm` fits its one 128-bit key hash.
///
/// Row `r` reads its column from bits `[r * mask_bits, (r + 1) * mask_bits)`
/// and its sign from bit `127 - r`, where `mask_bits = cols_mask_bits(cols)`.
/// Each row therefore claims `mask_bits + 1` bits, and the fields are in range
/// and pairwise disjoint (so every column and sign is an independent hash
/// draw) exactly while `rows * (mask_bits + 1) <= 128`. Past that, deep rows'
/// columns reuse shallow rows' sign bits, and from `rows * mask_bits >= 128`
/// the shift runs off the hash.
pub(crate) fn check_l2hh_hash_bits(rows: usize, cols: usize) -> Result<(), String> {
    let mask_bits = cols_mask_bits(cols.max(1)) as usize;
    let bits_required = mask_bits.saturating_add(1).saturating_mul(rows);
    if bits_required > 128 {
        return Err(format!(
            "CountL2HH rows={rows}, cols={cols} needs {bits_required} hash bits \
             ({mask_bits} column bits + 1 sign bit per row), past the 128 the key hash carries"
        ));
    }
    Ok(())
}

/// Column and sign `CountSketchWithL2Norm` uses for `row`, given its 128-bit key hash.
///
/// Shared with the OctoSketch worker sketches so a worker and its parent can
/// never disagree about which cell a key lands in.
#[inline(always)]
pub fn l2hh_cell_for_row(
    hashed_val: u128,
    row: usize,
    cols: usize,
    mask_bits: u32,
) -> (usize, i64) {
    let mask = (1u128 << mask_bits) - 1;
    let hashed = (hashed_val >> (row * mask_bits as usize)) & mask;
    let col = (hashed as usize) % cols;
    let bit = ((hashed_val >> (127 - row)) & 1) as i64;
    (col, -(1 - 2 * bit))
}

/// Count Sketch augmented with per-row L2 norm tracking for heavy-hitter detection.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(bound = "")]
pub struct CountSketchWithL2Norm<H: SketchHasher = DefaultXxHasher> {
    counts: Vector2D<i64>,
    l2: Vector1D<i64>,
    row: usize,
    col: usize,
    seed_index: usize,
    #[serde(skip)]
    _hasher: PhantomData<H>,
}

// Default CountSketchWithL2Norm configuration.
impl Default for CountSketchWithL2Norm {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROWS, DEFAULT_COLS)
    }
}

// CountSketchWithL2Norm constructors and operations.
impl<H: SketchHasher> CountSketchWithL2Norm<H> {
    /// Creates a sketch with the requested number of rows and columns.
    pub fn with_dimensions(rows: usize, cols: usize) -> Self {
        Self::with_dimensions_and_seed_index(rows, cols, 0)
    }

    /// Creates a sketch with the requested dimensions and a custom seed offset.
    ///
    /// # Panics
    ///
    /// When `rows * (cols_mask_bits(cols) + 1) > 128`: the rows' column and
    /// sign fields would overlap in the one 128-bit key hash.
    pub fn with_dimensions_and_seed_index(rows: usize, cols: usize, seed_index: usize) -> Self {
        if let Err(problem) = check_l2hh_hash_bits(rows, cols) {
            panic!("{problem}");
        }
        let mut sk = CountSketchWithL2Norm {
            counts: Vector2D::init(rows, cols),
            l2: Vector1D::filled(rows, 0),
            row: rows,
            col: cols,
            seed_index,
            _hasher: PhantomData,
        };
        sk.counts.fill(0);
        sk
    }

    /// Number of rows in the sketch.
    pub fn rows(&self) -> usize {
        self.row
    }

    /// Number of columns in the sketch.
    pub fn cols(&self) -> usize {
        self.col
    }

    /// Exposes the backing matrix for inspection/testing.
    pub fn as_storage(&self) -> &Vector2D<i64> {
        &self.counts
    }

    /// Mutable access used internally for testing scenarios.
    pub fn as_storage_mut(&mut self) -> &mut Vector2D<i64> {
        &mut self.counts
    }

    /// Merges another sketch while asserting compatible dimensions.
    pub fn merge(&mut self, other: &Self) {
        assert_eq!(
            (self.row, self.col),
            (other.row, other.col),
            "dimension mismatch while merging CountL2HH sketches"
        );
        assert_eq!(
            self.seed_index, other.seed_index,
            "seed mismatch while merging CountL2HH sketches"
        );

        for i in 0..self.row {
            let mut row_l2 = 0_i128;
            for j in 0..self.col {
                self.counts[i][j] += other.counts[i][j];
                let counter = self.counts[i][j] as i128;
                row_l2 = row_l2.saturating_add(counter.saturating_mul(counter));
            }
            self.l2[i] = row_l2.min(i64::MAX as i128) as i64;
        }
    }

    /// Resets all counters and L2 accumulators to zero without reallocating.
    pub fn clear(&mut self) {
        self.counts.fill(0);
        self.l2.fill(0);
    }

    /// Inserts with hash optimization - computes hash once and reuses it.
    /// due to the limitation of seeds, use fast_insert only
    pub fn update_weighted(&mut self, val: &DataInput, c: i64) {
        let hashed_val = H::hash128_seeded(self.seed_index, val);
        self.update_weighted_with_hash(hashed_val, c);
    }

    /// Inserts with hash optimization using precomputed hash value.
    pub fn update_weighted_with_hash(&mut self, hashed_val: u128, c: i64) {
        let mask_bits = self.counts.mask_bits();
        for i in 0..self.row {
            let (idx, sign_bit) = l2hh_cell_for_row(hashed_val, i, self.col, mask_bits);
            let old_value = self.counts.query_one_counter(i, idx);
            self.write_counter(i, idx, old_value + sign_bit * c);
        }
    }

    /// Writes one counter and carries the row's L2 accumulator with it.
    ///
    /// Saturating i128 intermediate so extreme counts degrade gracefully
    /// instead of wrapping (matches merge-path semantics, which also clamps to
    /// [0, i64::MAX]: L2^2 can never go negative, even when a decrement follows
    /// prior saturation).
    #[inline(always)]
    fn write_counter(&mut self, row: usize, col: usize, new_value: i64) {
        let old_value = self.counts.query_one_counter(row, col);
        self.counts[row][col] = new_value;
        let old_l2 = self.l2.as_slice()[row] as i128;
        let new_l2 = old_l2 + (new_value as i128) * (new_value as i128)
            - (old_value as i128) * (old_value as i128);
        self.l2[row] = new_l2.clamp(0, i64::MAX as i128) as i64;
    }

    /// Seed offset this sketch hashes with; UnivMon gives each layer its own.
    pub fn seed_index(&self) -> usize {
        self.seed_index
    }

    /// Column-mask width used to slice the 128-bit hash into per-row indices.
    pub fn mask_bits(&self) -> u32 {
        self.counts.mask_bits()
    }

    /// Applies a counter delta promoted by an OctoSketch worker.
    ///
    /// The row's L2 accumulator is carried along exactly, so a parent fed only
    /// deltas reports the same `l2` as one fed the raw stream.
    pub fn apply_delta(&mut self, delta: CountSketchCellDelta) {
        let (row, col) = (delta.row as usize, delta.col as usize);
        let new_value = self.counts.query_one_counter(row, col) + delta.value as i64;
        self.write_counter(row, col, new_value);
    }

    /// Inserts without L2 update using precomputed hash value.
    pub fn update_weighted_with_hash_without_l2(&mut self, hashed_val: u128, c: i64) {
        let mask_bits = self.counts.mask_bits() as usize;
        let mask = (1u128 << mask_bits) - 1;
        let mut shift_amount = 0;
        let mut sign_bit_pos = 127;

        for i in 0..self.row {
            let hashed = (hashed_val >> shift_amount) & mask;
            let idx = (hashed as usize) % self.col;
            let bit = ((hashed_val >> sign_bit_pos) & 1) as i64;
            let sign_bit = -(1 - 2 * bit);

            self.counts[i][idx] += sign_bit * c;

            shift_amount += mask_bits;
            sign_bit_pos -= 1;
        }
    }

    /// Update and estimate with hash optimization.
    /// due to the limitation of seeds, use fast_insert only
    pub fn update_weighted_and_estimate(&mut self, val: &DataInput, c: i64) -> f64 {
        let hashed_val = H::hash128_seeded(self.seed_index, val);
        self.update_weighted_with_hash(hashed_val, c);
        self.estimate_with_hash(hashed_val)
    }

    /// Update and estimate without L2 with hash optimization.
    /// due to the limitation of seeds, use fast_insert only
    pub fn fast_update_and_est_without_l2(&mut self, val: &DataInput, c: i64) -> f64 {
        let hashed_val = H::hash128_seeded(self.seed_index, val);
        self.update_weighted_with_hash_without_l2(hashed_val, c);
        self.estimate_with_hash(hashed_val)
    }

    /// Returns the median estimate of the squared L2 norm across rows.
    pub fn l2_squared(&self) -> f64 {
        let mut values: Vec<f64> = self.l2.as_slice()[..self.row]
            .iter()
            .map(|&v| v as f64)
            .collect();
        compute_median_inline_f64(&mut values)
    }

    /// Returns the estimated L2 norm (square root of `l2_squared`).
    pub fn l2(&self) -> f64 {
        let l2 = self.l2_squared();
        l2.sqrt()
    }

    /// Returns the frequency estimate with hash optimization.
    /// due to the limitation of seeds, use fast_insert only
    pub fn estimate(&self, val: &DataInput) -> f64 {
        let hashed_val = H::hash128_seeded(self.seed_index, val);
        self.estimate_with_hash(hashed_val)
    }

    /// Returns the frequency estimate using precomputed hash value.
    /// due to the limitation of seeds, use fast_insert only
    pub fn estimate_with_hash(&self, hashed_val: u128) -> f64 {
        let mask_bits = self.counts.mask_bits() as usize;
        let mask = (1u128 << mask_bits) - 1;
        let mut lst = Vec::with_capacity(self.row);
        let mut shift_amount = 0;
        let mut sign_bit_pos = 127;

        for i in 0..self.row {
            let hashed = (hashed_val >> shift_amount) & mask;
            let idx = (hashed as usize) % self.col;
            let bit = ((hashed_val >> sign_bit_pos) & 1) as i64;
            let sign_bit = -(1 - 2 * bit);
            let counter = self.counts.query_one_counter(i, idx);
            lst.push((sign_bit * counter) as f64);

            shift_amount += mask_bits;
            sign_bit_pos -= 1;
        }
        compute_median_inline_f64(&mut lst[..])
    }
}

#[cfg(test)]
mod tests_count_l2_hh {
    use super::*;

    /// 11 rows x (11 column bits + 1 sign bit) = 132 bits of one 128-bit
    /// hash: the deep rows' columns would reuse the shallow rows' signs.
    #[test]
    #[should_panic(expected = "hash bits")]
    fn countl2hh_rejects_column_bits_that_reach_the_sign_bits() {
        let _: CountSketchWithL2Norm =
            CountSketchWithL2Norm::with_dimensions_and_seed_index(11, 2048, 0);
    }

    /// 10 x 2048 spends 120 bits and is accepted.
    #[test]
    fn countl2hh_accepts_the_widest_disjoint_geometry() {
        let sketch: CountSketchWithL2Norm =
            CountSketchWithL2Norm::with_dimensions_and_seed_index(10, 2048, 0);
        assert_eq!((sketch.rows(), sketch.cols()), (10, 2048));
    }

    #[test]
    fn countl2hh_estimates_and_l2_are_consistent() {
        let mut sketch: CountSketchWithL2Norm = CountSketchWithL2Norm::with_dimensions(3, 32);
        let key = DataInput::Str("gamma");

        let est_after_first = sketch.update_weighted_and_estimate(&key, 5);
        assert_eq!(est_after_first, 5.0);

        let est_after_second = sketch.update_weighted_and_estimate(&key, -2);
        assert_eq!(est_after_second, 3.0);

        let l2 = sketch.l2();
        assert!(l2 >= 3.0, "expected non-trivial l2, got {l2}");
    }

    #[test]
    fn countl2hh_merge_combines_frequency_vectors() {
        let mut left: CountSketchWithL2Norm = CountSketchWithL2Norm::with_dimensions(3, 32);
        let mut right: CountSketchWithL2Norm = CountSketchWithL2Norm::with_dimensions(3, 32);
        let key = DataInput::U32(42);

        left.update_weighted(&key, 4);
        assert_eq!(left.estimate(&key), 4.0);
        right.update_weighted(&key, 9);
        assert_eq!(right.estimate(&key), 9.0);

        left.merge(&right);
        assert_eq!(left.estimate(&key), 13.0);
        assert_eq!(left.l2(), 13.0);
    }

    #[test]
    fn countl2hh_round_trip_serialization() {
        let mut sketch: CountSketchWithL2Norm =
            CountSketchWithL2Norm::with_dimensions_and_seed_index(3, 32, 7);
        let key = DataInput::Str("serialize");

        sketch.update_weighted(&key, 11);
        sketch.update_weighted(&key, -3);
        let base_est = sketch.estimate(&key);
        let base_l2 = sketch.l2();

        let encoded = sketch
            .to_asapv1()
            .expect("serialize CountL2HH into MessagePack");
        assert!(!encoded.is_empty(), "serialized bytes should not be empty");
        let data = encoded.clone();

        let decoded: CountSketchWithL2Norm = CountSketchWithL2Norm::from_asapv1(&data)
            .expect("deserialize CountL2HH from MessagePack");

        assert_eq!(sketch.rows(), decoded.rows());
        assert_eq!(sketch.cols(), decoded.cols());
        assert!(
            (decoded.estimate(&key) - base_est).abs() < f64::EPSILON,
            "estimate changed after round trip"
        );
        assert!(
            (decoded.l2() - base_l2).abs() < f64::EPSILON,
            "L2 changed after round trip"
        );
    }
}
