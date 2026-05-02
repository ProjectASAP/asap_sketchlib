//! CMSHeap: a convenient wrapper that pairs a [`CountMin`] sketch with an
//! [`HHHeap`] for automatic top-k heavy-hitter tracking.
//!
//! Every insertion updates both the frequency sketch and the heap, mirroring
//! the pattern used by `FoldCMS` but without folding complexity.

use crate::{
    CountMin, DataInput, DefaultMatrixI32, DefaultMatrixI64, DefaultMatrixI128, DefaultXxHasher,
    FastPath, FixedMatrix, HHHeap, MatrixStorage, QuickMatrixI64, QuickMatrixI128, RegularPath,
    SketchHasher, Vector2D, heap_item_to_sketch_input,
};

const DEFAULT_TOP_K: usize = 32;

/// A Count-Min Sketch paired with a top-k heavy-hitter heap.
///
/// Generic over the same type parameters as [`CountMin`].
pub struct CMSHeap<
    S: MatrixStorage = Vector2D<i64>,
    Mode = RegularPath,
    H: SketchHasher = DefaultXxHasher,
> {
    cms: CountMin<S, Mode, H>,
    heap: HHHeap,
}

// -- Construction for Vector2D-backed storage --------------------------------

impl<T, M, H: SketchHasher> CMSHeap<Vector2D<T>, M, H>
where
    T: Copy + Default + std::ops::AddAssign,
{
    /// Creates a new `CMSHeap` with the given CMS dimensions and heap capacity.
    pub fn new(rows: usize, cols: usize, top_k: usize) -> Self {
        CMSHeap {
            cms: CountMin::with_dimensions(rows, cols),
            heap: HHHeap::new(top_k),
        }
    }
}

// -- Construction from any MatrixStorage -------------------------------------

impl<S: MatrixStorage, M, H: SketchHasher> CMSHeap<S, M, H> {
    /// Creates a `CMSHeap` from a pre-built storage backend.
    pub fn from_storage(storage: S, top_k: usize) -> Self {
        CMSHeap {
            cms: CountMin::from_storage(storage),
            heap: HHHeap::new(top_k),
        }
    }
}

// -- Default impls -----------------------------------------------------------
//
// Default is available for Vector2D-backed sketches and fixed-size matrix
// backends (Quick/Default/Fixed families). Use `from_storage(...)` when you
// want explicit backend control with a custom `top_k`.

impl Default for CMSHeap<Vector2D<i64>, RegularPath> {
    fn default() -> Self {
        Self::new(3, 4096, DEFAULT_TOP_K)
    }
}

impl Default for CMSHeap<Vector2D<i64>, FastPath> {
    fn default() -> Self {
        Self::new(3, 4096, DEFAULT_TOP_K)
    }
}

impl Default for CMSHeap<Vector2D<i32>, RegularPath> {
    fn default() -> Self {
        Self::new(3, 4096, DEFAULT_TOP_K)
    }
}

impl Default for CMSHeap<Vector2D<i32>, FastPath> {
    fn default() -> Self {
        Self::new(3, 4096, DEFAULT_TOP_K)
    }
}

impl Default for CMSHeap<FixedMatrix, RegularPath> {
    fn default() -> Self {
        Self::from_storage(FixedMatrix::default(), DEFAULT_TOP_K)
    }
}

impl Default for CMSHeap<FixedMatrix, FastPath> {
    fn default() -> Self {
        Self::from_storage(FixedMatrix::default(), DEFAULT_TOP_K)
    }
}

impl Default for CMSHeap<DefaultMatrixI32, RegularPath> {
    fn default() -> Self {
        Self::from_storage(DefaultMatrixI32::default(), DEFAULT_TOP_K)
    }
}

impl Default for CMSHeap<DefaultMatrixI32, FastPath> {
    fn default() -> Self {
        Self::from_storage(DefaultMatrixI32::default(), DEFAULT_TOP_K)
    }
}

impl Default for CMSHeap<QuickMatrixI64, RegularPath> {
    fn default() -> Self {
        Self::from_storage(QuickMatrixI64::default(), DEFAULT_TOP_K)
    }
}

impl Default for CMSHeap<QuickMatrixI64, FastPath> {
    fn default() -> Self {
        Self::from_storage(QuickMatrixI64::default(), DEFAULT_TOP_K)
    }
}

impl Default for CMSHeap<QuickMatrixI128, RegularPath> {
    fn default() -> Self {
        Self::from_storage(QuickMatrixI128::default(), DEFAULT_TOP_K)
    }
}

impl Default for CMSHeap<QuickMatrixI128, FastPath> {
    fn default() -> Self {
        Self::from_storage(QuickMatrixI128::default(), DEFAULT_TOP_K)
    }
}

impl Default for CMSHeap<DefaultMatrixI64, RegularPath> {
    fn default() -> Self {
        Self::from_storage(DefaultMatrixI64::default(), DEFAULT_TOP_K)
    }
}

impl Default for CMSHeap<DefaultMatrixI64, FastPath> {
    fn default() -> Self {
        Self::from_storage(DefaultMatrixI64::default(), DEFAULT_TOP_K)
    }
}

impl Default for CMSHeap<DefaultMatrixI128, RegularPath> {
    fn default() -> Self {
        Self::from_storage(DefaultMatrixI128::default(), DEFAULT_TOP_K)
    }
}

impl Default for CMSHeap<DefaultMatrixI128, FastPath> {
    fn default() -> Self {
        Self::from_storage(DefaultMatrixI128::default(), DEFAULT_TOP_K)
    }
}

// -- Shared accessors (all storage types) ------------------------------------

impl<S: MatrixStorage, M, H: SketchHasher> CMSHeap<S, M, H> {
    /// Returns a reference to the internal CMS.
    pub fn cms(&self) -> &CountMin<S, M, H> {
        &self.cms
    }

    /// Returns a mutable reference to the internal CMS.
    pub fn cms_mut(&mut self) -> &mut CountMin<S, M, H> {
        &mut self.cms
    }

    /// Returns a reference to the heavy-hitter heap.
    pub fn heap(&self) -> &HHHeap {
        &self.heap
    }

    /// Returns a mutable reference to the heavy-hitter heap.
    pub fn heap_mut(&mut self) -> &mut HHHeap {
        &mut self.heap
    }

    /// Number of rows in the underlying CMS.
    #[inline(always)]
    pub fn rows(&self) -> usize {
        self.cms.rows()
    }

    /// Number of columns in the underlying CMS.
    #[inline(always)]
    pub fn cols(&self) -> usize {
        self.cms.cols()
    }

    /// Clears both the CMS counters (by rebuilding) and the heap.
    pub fn clear_heap(&mut self) {
        self.heap.clear();
    }
}

// -- RegularPath insert / estimate / merge -----------------------------------

impl<S: MatrixStorage, H: SketchHasher> CMSHeap<S, RegularPath, H>
where
    S::Counter: Copy + Ord + From<i32> + Into<i64> + std::ops::AddAssign,
{
    /// Inserts a single observation and updates the top-k heap.
    #[inline]
    pub fn insert(&mut self, key: &DataInput) {
        self.cms.insert(key);
        let est = self.cms.estimate(key);
        self.heap.update(key, est.into());
    }

    /// Inserts an observation with the given count and updates the top-k heap.
    #[inline]
    pub fn insert_many(&mut self, key: &DataInput, many: S::Counter) {
        self.cms.insert_many(key, many);
        let est = self.cms.estimate(key);
        self.heap.update(key, est.into());
    }

    /// Inserts a batch of observations, updating the heap after each.
    pub fn bulk_insert(&mut self, values: &[DataInput]) {
        for value in values {
            self.insert(value);
        }
    }

    /// Returns the CMS frequency estimate for the given key.
    #[inline]
    pub fn estimate(&self, key: &DataInput) -> S::Counter {
        self.cms.estimate(key)
    }

    /// Merges another `CMSHeap` into `self`.
    ///
    /// After merging the CMS counters, all heap items from both sources are
    /// re-queried against the merged sketch to reconcile the top-k heap.
    pub fn merge(&mut self, other: &Self) {
        self.cms.merge(&other.cms);
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
            let est = self.cms.estimate(&key_ref);
            self.heap.update(&key_ref, est.into());
        }
    }
}

// -- FastPath insert / estimate / merge --------------------------------------

impl<S, H: SketchHasher> CMSHeap<S, FastPath, H>
where
    S: MatrixStorage + crate::FastPathHasher<H>,
    S::Counter: Copy + Ord + From<i32> + Into<i64> + std::ops::AddAssign,
{
    /// Inserts a single observation using fast-path hashing and updates the heap.
    #[inline]
    pub fn insert(&mut self, key: &DataInput) {
        self.cms.insert(key);
        let est = self.cms.estimate(key);
        self.heap.update(key, est.into());
    }

    /// Inserts an observation with the given count using fast-path hashing.
    #[inline]
    pub fn insert_many(&mut self, key: &DataInput, many: S::Counter) {
        self.cms.insert_many(key, many);
        let est = self.cms.estimate(key);
        self.heap.update(key, est.into());
    }

    /// Inserts a batch of observations using fast-path hashing.
    pub fn bulk_insert(&mut self, values: &[DataInput]) {
        for value in values {
            self.insert(value);
        }
    }

    /// Returns the CMS frequency estimate using fast-path hashing.
    #[inline]
    pub fn estimate(&self, key: &DataInput) -> S::Counter {
        self.cms.estimate(key)
    }

    /// Merges another `CMSHeap` into `self`.
    pub fn merge(&mut self, other: &Self) {
        self.cms.merge(&other.cms);
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
            let est = self.cms.estimate(&key_ref);
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

    fn heap_count_for_key(heap: &HHHeap, key: &DataInput) -> Option<i64> {
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
    ) -> (CMSHeap<Vector2D<i64>, RegularPath>, HashMap<u64, i64>) {
        let mut truth = HashMap::<u64, i64>::new();
        let mut sketch = CMSHeap::<Vector2D<i64>, RegularPath>::new(rows, cols, top_k);
        for value in sample_zipf_u64(domain, exponent, samples, seed) {
            let key = DataInput::U64(value);
            sketch.insert(&key);
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
    ) -> (CMSHeap<Vector2D<i64>, FastPath>, HashMap<u64, i64>) {
        let mut truth = HashMap::<u64, i64>::new();
        let mut sketch = CMSHeap::<Vector2D<i64>, FastPath>::new(rows, cols, top_k);
        for value in sample_zipf_u64(domain, exponent, samples, seed) {
            let key = DataInput::U64(value);
            sketch.insert(&key);
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

    fn top_k_heap_keys(heap: &HHHeap) -> HashSet<u64> {
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
        let mut sh = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 64, 10);
        let key = DataInput::Str("hello");
        for _ in 0..5 {
            sh.insert(&key);
        }
        assert_eq!(sh.estimate(&key), 5);
    }

    #[test]
    fn heap_tracks_top_k() {
        // Verifies heap keeps the highest-frequency keys within top-k capacity.
        let mut sh = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 256, 3);

        // Insert 5 distinct keys with different frequencies.
        for i in 1..=5u64 {
            let key = DataInput::U64(i);
            for _ in 0..(i * 10) {
                sh.insert(&key);
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
        let mut a = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 256, 5);
        let mut b = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 256, 5);

        let key = DataInput::Str("merge_key");
        for _ in 0..10 {
            a.insert(&key);
        }
        for _ in 0..20 {
            b.insert(&key);
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

    #[test]
    fn insert_many_updates_estimate_and_heap() {
        // Verifies batched insert_many updates both estimate and heap entry count.
        let mut sh = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 1024, 4);
        let key = DataInput::Str("many");
        sh.insert_many(&key, 11);

        assert_eq!(sh.estimate(&key), 11);
        assert_eq!(heap_count_for_key(sh.heap(), &key), Some(11));
    }

    #[test]
    fn bulk_insert_updates_multiple_keys() {
        // Verifies bulk_insert processes streams correctly across multiple keys.
        let mut sh = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 1024, 4);
        let values = vec![
            DataInput::U64(7),
            DataInput::U64(8),
            DataInput::U64(7),
            DataInput::U64(9),
            DataInput::U64(7),
        ];
        sh.bulk_insert(&values);

        assert_eq!(sh.estimate(&DataInput::U64(7)), 3);
        assert_eq!(sh.estimate(&DataInput::U64(8)), 1);
        assert_eq!(sh.estimate(&DataInput::U64(9)), 1);
        assert_eq!(heap_count_for_key(sh.heap(), &DataInput::U64(7)), Some(3));
    }

    #[test]
    fn clear_heap_keeps_cms_counters() {
        // Verifies clearing heap does not erase CMS counters or future heap rebuilds.
        let mut sh = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 1024, 2);
        let key = DataInput::Str("persist");
        sh.insert_many(&key, 5);

        sh.clear_heap();
        assert!(sh.heap().is_empty());
        assert_eq!(sh.estimate(&key), 5);

        sh.insert(&key);
        assert_eq!(heap_count_for_key(sh.heap(), &key), Some(6));
    }

    #[test]
    fn from_storage_uses_storage_dimensions() {
        // Verifies from_storage preserves backend dimensions and requested heap capacity.
        let storage = Vector2D::<i64>::init(4, 128);
        let sh = CMSHeap::<Vector2D<i64>, RegularPath>::from_storage(storage, 9);

        assert_eq!(sh.rows(), 4);
        assert_eq!(sh.cols(), 128);
        assert_eq!(sh.heap().capacity(), 9);
    }

    #[test]
    fn merge_refreshes_existing_self_heap_entries() {
        // Verifies merge updates existing self heap keys to merged sketch estimates.
        let mut a = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 2048, 2);
        let mut b = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 2048, 1);
        let a_key = DataInput::Str("a-key");
        let c_key = DataInput::Str("c-key");
        let b_key = DataInput::Str("b-key");

        a.insert_many(&a_key, 10);
        a.insert_many(&c_key, 9);

        b.insert_many(&a_key, 5);
        b.insert_many(&b_key, 50);

        a.merge(&b);

        assert_eq!(a.estimate(&a_key), 15);
        assert_eq!(heap_count_for_key(a.heap(), &a_key), Some(15));
    }

    #[test]
    fn fast_path_insert_and_estimate() {
        // Verifies FastPath insert and estimate agree on repeated updates.
        let mut sh = CMSHeap::<Vector2D<i64>, FastPath>::new(3, 64, 10);
        let key = DataInput::Str("fast");
        for _ in 0..7 {
            sh.insert(&key);
        }
        assert_eq!(sh.estimate(&key), 7);
    }

    #[test]
    fn fast_path_insert_many_and_bulk_insert() {
        // Verifies FastPath batched APIs keep estimate and heap counts in sync.
        let mut sh = CMSHeap::<Vector2D<i64>, FastPath>::new(3, 1024, 4);
        let key = DataInput::Str("fast-many");
        sh.insert_many(&key, 6);
        sh.bulk_insert(&[
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
        let mut sh = CMSHeap::<Vector2D<i64>, FastPath>::new(3, 512, 3);

        for i in 1..=5u64 {
            let key = DataInput::U64(i);
            sh.insert_many(&key, (i as i64) * 10);
        }

        let mut counts: Vec<i64> = sh.heap().heap().iter().map(|item| item.count).collect();
        counts.sort_unstable();
        assert_eq!(counts, vec![30, 40, 50]);
    }

    #[test]
    fn fast_path_merge_refreshes_existing_self_heap_entries() {
        // Verifies FastPath merge refreshes self heap entries using merged counts.
        let mut a = CMSHeap::<Vector2D<i64>, FastPath>::new(3, 2048, 2);
        let mut b = CMSHeap::<Vector2D<i64>, FastPath>::new(3, 2048, 1);
        let a_key = DataInput::Str("a-fast");
        let c_key = DataInput::Str("c-fast");
        let b_key = DataInput::Str("b-fast");

        a.insert_many(&a_key, 10);
        a.insert_many(&c_key, 9);

        b.insert_many(&a_key, 5);
        b.insert_many(&b_key, 50);

        a.merge(&b);

        assert_eq!(a.estimate(&a_key), 15);
        assert_eq!(heap_count_for_key(a.heap(), &a_key), Some(15));
    }

    #[test]
    fn default_construction() {
        // Verifies default CMSHeap uses expected dimensions and heap capacity.
        let sh = CMSHeap::<Vector2D<i64>, RegularPath>::default();
        assert_eq!(sh.rows(), 3);
        assert_eq!(sh.cols(), 4096);
        assert_eq!(sh.heap().capacity(), DEFAULT_TOP_K);
    }

    #[test]
    fn default_construction_fixed_backends_parity() {
        // Verifies default constructors across backends maintain intended dimensions/capacity.
        let fixed_regular = CMSHeap::<FixedMatrix, RegularPath>::default();
        assert_eq!(fixed_regular.rows(), 5);
        assert_eq!(fixed_regular.cols(), 2048);
        assert_eq!(fixed_regular.heap().capacity(), DEFAULT_TOP_K);

        let fixed_fast = CMSHeap::<FixedMatrix, FastPath>::default();
        assert_eq!(fixed_fast.rows(), 5);
        assert_eq!(fixed_fast.cols(), 2048);
        assert_eq!(fixed_fast.heap().capacity(), DEFAULT_TOP_K);

        let dm_i32_regular = CMSHeap::<DefaultMatrixI32, RegularPath>::default();
        assert_eq!(dm_i32_regular.rows(), 3);
        assert_eq!(dm_i32_regular.cols(), 4096);
        assert_eq!(dm_i32_regular.heap().capacity(), DEFAULT_TOP_K);

        let dm_i32_fast = CMSHeap::<DefaultMatrixI32, FastPath>::default();
        assert_eq!(dm_i32_fast.rows(), 3);
        assert_eq!(dm_i32_fast.cols(), 4096);
        assert_eq!(dm_i32_fast.heap().capacity(), DEFAULT_TOP_K);

        let qm_i64_regular = CMSHeap::<QuickMatrixI64, RegularPath>::default();
        assert_eq!(qm_i64_regular.rows(), 5);
        assert_eq!(qm_i64_regular.cols(), 2048);
        assert_eq!(qm_i64_regular.heap().capacity(), DEFAULT_TOP_K);

        let qm_i64_fast = CMSHeap::<QuickMatrixI64, FastPath>::default();
        assert_eq!(qm_i64_fast.rows(), 5);
        assert_eq!(qm_i64_fast.cols(), 2048);
        assert_eq!(qm_i64_fast.heap().capacity(), DEFAULT_TOP_K);

        let qm_i128_regular = CMSHeap::<QuickMatrixI128, RegularPath>::default();
        assert_eq!(qm_i128_regular.rows(), 5);
        assert_eq!(qm_i128_regular.cols(), 2048);
        assert_eq!(qm_i128_regular.heap().capacity(), DEFAULT_TOP_K);

        let qm_i128_fast = CMSHeap::<QuickMatrixI128, FastPath>::default();
        assert_eq!(qm_i128_fast.rows(), 5);
        assert_eq!(qm_i128_fast.cols(), 2048);
        assert_eq!(qm_i128_fast.heap().capacity(), DEFAULT_TOP_K);

        let dm_i64_regular = CMSHeap::<DefaultMatrixI64, RegularPath>::default();
        assert_eq!(dm_i64_regular.rows(), 3);
        assert_eq!(dm_i64_regular.cols(), 4096);
        assert_eq!(dm_i64_regular.heap().capacity(), DEFAULT_TOP_K);

        let dm_i64_fast = CMSHeap::<DefaultMatrixI64, FastPath>::default();
        assert_eq!(dm_i64_fast.rows(), 3);
        assert_eq!(dm_i64_fast.cols(), 4096);
        assert_eq!(dm_i64_fast.heap().capacity(), DEFAULT_TOP_K);

        let dm_i128_regular = CMSHeap::<DefaultMatrixI128, RegularPath>::default();
        assert_eq!(dm_i128_regular.rows(), 3);
        assert_eq!(dm_i128_regular.cols(), 4096);
        assert_eq!(dm_i128_regular.heap().capacity(), DEFAULT_TOP_K);

        let dm_i128_fast = CMSHeap::<DefaultMatrixI128, FastPath>::default();
        assert_eq!(dm_i128_fast.rows(), 3);
        assert_eq!(dm_i128_fast.cols(), 4096);
        assert_eq!(dm_i128_fast.heap().capacity(), DEFAULT_TOP_K);
    }

    #[test]
    #[should_panic(expected = "dimension mismatch while merging CountMin sketches")]
    fn merge_requires_matching_dimensions_panics() {
        // Verifies merge rejects sketches with incompatible dimensions.
        let mut left = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 256, 4);
        let right = CMSHeap::<Vector2D<i64>, RegularPath>::new(4, 256, 4);
        left.merge(&right);
    }

    #[test]
    fn heap_entries_match_cms_estimates_after_mutations() {
        // Verifies every heap entry count equals current CMS estimate after updates/merge.
        let mut sh = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 2048, 4);
        sh.insert_many(&DataInput::Str("a"), 10);
        sh.insert_many(&DataInput::Str("b"), 7);
        sh.bulk_insert(&[
            DataInput::Str("a"),
            DataInput::Str("c"),
            DataInput::Str("a"),
            DataInput::Str("d"),
        ]);

        for item in sh.heap().heap() {
            let key = heap_item_to_sketch_input(&item.key);
            assert_eq!(item.count, sh.estimate(&key));
        }

        let mut other = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 2048, 4);
        other.insert_many(&DataInput::Str("b"), 9);
        other.insert_many(&DataInput::Str("e"), 20);
        sh.merge(&other);

        for item in sh.heap().heap() {
            let key = heap_item_to_sketch_input(&item.key);
            assert_eq!(item.count, sh.estimate(&key));
        }
    }

    #[test]
    fn bulk_insert_equivalent_to_repeated_insert() {
        // Verifies bulk_insert yields same estimates and heap counts as repeated insert.
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

        let mut via_bulk = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 4096, 3);
        via_bulk.bulk_insert(&values);

        let mut via_repeat = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 4096, 3);
        for value in &values {
            via_repeat.insert(value);
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

        let mut regular = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 4096, 3);
        let mut fast = CMSHeap::<Vector2D<i64>, FastPath>::new(3, 4096, 3);
        for value in &values {
            regular.insert(value);
            fast.insert(value);
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
        let mut non_empty = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 1024, 3);
        non_empty.insert_many(&DataInput::Str("x"), 11);
        non_empty.insert_many(&DataInput::Str("y"), 5);

        let empty = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 1024, 3);
        let before_len = non_empty.heap().len();
        let before_x = non_empty.estimate(&DataInput::Str("x"));
        non_empty.merge(&empty);
        assert_eq!(non_empty.heap().len(), before_len);
        assert_eq!(non_empty.estimate(&DataInput::Str("x")), before_x);

        let mut empty_self = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 1024, 3);
        empty_self.merge(&non_empty);
        assert_eq!(empty_self.estimate(&DataInput::Str("x")), before_x);
        assert!(heap_count_for_key(empty_self.heap(), &DataInput::Str("x")).is_some());
    }

    #[test]
    fn duplicate_candidate_keys_during_merge_do_not_corrupt_heap() {
        // Verifies duplicate merge candidates do not create duplicate heap entries.
        let mut left = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 2048, 4);
        let mut right = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 2048, 4);

        left.insert_many(&DataInput::Str("dup"), 10);
        left.insert_many(&DataInput::Str("left-only"), 7);

        right.insert_many(&DataInput::Str("dup"), 9);
        right.insert_many(&DataInput::Str("right-only"), 6);

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

        let mut regular = CMSHeap::<Vector2D<i64>, RegularPath>::new(rows, cols, top_k);
        let mut fast = CMSHeap::<Vector2D<i64>, FastPath>::new(rows, cols, top_k);
        for value in &stream {
            let key = DataInput::U64(*value);
            regular.insert(&key);
            fast.insert(&key);
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
// asap_sketchlib wire-format-aligned variant.
//
// `CountMinSketchWithHeap` and `CmsHeapItem` below are the
// public-field, proto-decode-friendly types consumed by the ASAP query
// engine accumulators, backed by `asap_sketchlib`'s in-tree CMSHeap.
// All hashing is delegated to the `SketchlibCMSHeap` backend (which
// uses `DefaultXxHasher`), so producer and consumer always agree on
// bucket assignments. The high-throughput in-process variant above
// (`CMSHeap`) keeps its original design. Note: the wire-format
// heap-item type was renamed `HeapItem` -> `CmsHeapItem` to avoid
// collision with `common::input::HeapItem` (the polymorphic key type
// used by the generic in-process heap).
// =====================================================================

use serde::{Deserialize, Serialize};

// ----- asap_sketchlib-backed CMSHeap helpers -----
// Used below by `CountMinSketchWithHeap`. Lives in this file so the
// wire-format type and its backend share a single home.

/// Wire-format heap item (key, value) used by the dispatch helpers below.
pub struct WireHeapItem {
    pub key: String,
    pub value: f64,
}

/// Concrete Count-Min-with-Heap type backing the wire-format `CountMinSketchWithHeap`.
pub type SketchlibCMSHeap = CMSHeap<Vector2D<i64>, RegularPath>;

/// Creates a fresh CMSHeap with the given dimensions and heap capacity.
pub fn new_sketchlib_cms_heap(
    row_num: usize,
    col_num: usize,
    heap_size: usize,
) -> SketchlibCMSHeap {
    CMSHeap::new(row_num, col_num, heap_size)
}

/// Builds a CMSHeap from an existing sketch matrix and optional heap items.
/// Used when deserializing wire-format state.
pub fn sketchlib_cms_heap_from_matrix_and_heap(
    row_num: usize,
    col_num: usize,
    heap_size: usize,
    sketch: &[Vec<f64>],
    topk_heap: &[WireHeapItem],
) -> SketchlibCMSHeap {
    let matrix = Vector2D::from_fn(row_num, col_num, |r, c| {
        sketch
            .get(r)
            .and_then(|row| row.get(c))
            .copied()
            .unwrap_or(0.0)
            .round() as i64
    });
    let mut cms_heap = CMSHeap::from_storage(matrix, heap_size);

    // Populate the heap from wire-format topk_heap
    for item in topk_heap {
        let count = item.value.round() as i64;
        if count > 0 {
            let input = DataInput::Str(&item.key);
            cms_heap.heap_mut().update(&input, count);
        }
    }

    cms_heap
}

/// Converts a CMSHeap's storage into a `Vec<Vec<f64>>` matrix.
pub fn matrix_from_sketchlib_cms_heap(cms_heap: &SketchlibCMSHeap) -> Vec<Vec<f64>> {
    let storage = cms_heap.cms().as_storage();
    let rows = storage.rows();
    let cols = storage.cols();
    let mut sketch = vec![vec![0.0; cols]; rows];

    for (r, row) in sketch.iter_mut().enumerate().take(rows) {
        for (c, cell) in row.iter_mut().enumerate().take(cols) {
            if let Some(v) = storage.get(r, c) {
                *cell = *v as f64;
            }
        }
    }

    sketch
}

/// Converts sketchlib HHHeap items to wire-format (key, value) pairs.
pub fn heap_to_wire(cms_heap: &SketchlibCMSHeap) -> Vec<WireHeapItem> {
    cms_heap
        .heap()
        .heap()
        .iter()
        .map(|hh_item| {
            let key = match &hh_item.key {
                crate::HeapItem::String(s) => s.clone(),
                other => format!("{:?}", other),
            };
            WireHeapItem {
                key,
                value: hh_item.count as f64,
            }
        })
        .collect()
}

/// Updates a CMSHeap with a weighted key. Automatically updates the heap.
pub fn sketchlib_cms_heap_update(cms_heap: &mut SketchlibCMSHeap, key: &str, value: f64) {
    let many = value.round() as i64;
    if many <= 0 {
        return;
    }
    let input = DataInput::String(key.to_owned());
    cms_heap.insert_many(&input, many);
}

/// Queries a CMSHeap for a key's frequency estimate.
pub fn sketchlib_cms_heap_query(cms_heap: &SketchlibCMSHeap, key: &str) -> f64 {
    let input = DataInput::String(key.to_owned());
    cms_heap.estimate(&input) as f64
}

/// Item in the top-k heap representing a key-value pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CmsHeapItem {
    pub key: String,
    pub value: f64,
}

/// Helper struct matching the nested wire serialization format (inner CMS).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CmsData {
    sketch: Vec<Vec<f64>>,
    #[serde(rename = "row_num")]
    rows: usize,
    #[serde(rename = "col_num")]
    cols: usize,
}

/// Helper struct matching the wire serialization format (outer wrapper).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CountMinSketchWithHeapSerialized {
    sketch: CmsData,
    topk_heap: Vec<CmsHeapItem>,
    heap_size: usize,
}

/// Count-Min Sketch with Heap for top-k tracking.
/// Combines probabilistic frequency counting with efficient top-k maintenance.
pub struct CountMinSketchWithHeap {
    pub rows: usize,
    pub cols: usize,
    pub heap_size: usize,
    pub(crate) backend: SketchlibCMSHeap,
}

impl std::fmt::Debug for CountMinSketchWithHeap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CountMinSketchWithHeap")
            .field("rows", &self.rows)
            .field("cols", &self.cols)
            .field("heap_size", &self.heap_size)
            .finish()
    }
}

impl Clone for CountMinSketchWithHeap {
    fn clone(&self) -> Self {
        let sketch = matrix_from_sketchlib_cms_heap(&self.backend);
        let wire_heap: Vec<WireHeapItem> = heap_to_wire(&self.backend);
        Self {
            rows: self.rows,
            cols: self.cols,
            heap_size: self.heap_size,
            backend: sketchlib_cms_heap_from_matrix_and_heap(
                self.rows,
                self.cols,
                self.heap_size,
                &sketch,
                &wire_heap,
            ),
        }
    }
}

impl CountMinSketchWithHeap {
    pub fn new(rows: usize, cols: usize, heap_size: usize) -> Self {
        Self {
            rows,
            cols,
            heap_size,
            backend: new_sketchlib_cms_heap(rows, cols, heap_size),
        }
    }

    /// Number of hash rows in the sketch matrix.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Number of columns (width) in the sketch matrix.
    pub fn cols(&self) -> usize {
        self.cols
    }

    /// Create from a sketch matrix and heap (e.g. from JSON deserialization).
    pub fn from_legacy_matrix(
        sketch: Vec<Vec<f64>>,
        topk_heap: Vec<CmsHeapItem>,
        rows: usize,
        cols: usize,
        heap_size: usize,
    ) -> Self {
        let wire_heap: Vec<WireHeapItem> = topk_heap
            .into_iter()
            .map(|h| WireHeapItem {
                key: h.key,
                value: h.value,
            })
            .collect();
        Self {
            rows,
            cols,
            heap_size,
            backend: sketchlib_cms_heap_from_matrix_and_heap(
                rows, cols, heap_size, &sketch, &wire_heap,
            ),
        }
    }

    /// Get the top-k heap items.
    pub fn topk_heap_items(&self) -> Vec<CmsHeapItem> {
        heap_to_wire(&self.backend)
            .into_iter()
            .map(|w| CmsHeapItem {
                key: w.key,
                value: w.value,
            })
            .collect()
    }

    /// Get the sketch matrix.
    pub fn sketch_matrix(&self) -> Vec<Vec<f64>> {
        matrix_from_sketchlib_cms_heap(&self.backend)
    }

    pub fn update(&mut self, key: &str, value: f64) {
        sketchlib_cms_heap_update(&mut self.backend, key, value);
    }

    /// Estimate the frequency of `key` (CountMin point query).
    pub fn estimate(&self, key: &str) -> f64 {
        sketchlib_cms_heap_query(&self.backend, key)
    }

    /// Merge another CountMinSketchWithHeap into self in place. Both
    /// operands must have identical dimensions; the resulting heap_size
    /// is the minimum of the two.
    pub fn merge(
        &mut self,
        other: &CountMinSketchWithHeap,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.rows != other.rows || self.cols != other.cols {
            return Err(format!(
                "CountMinSketchWithHeap dimension mismatch: self={}x{}, other={}x{}",
                self.rows, self.cols, other.rows, other.cols
            )
            .into());
        }
        self.backend.merge(&other.backend);
        self.heap_size = self.heap_size.min(other.heap_size);
        Ok(())
    }

    /// Merge from references, returning a new sketch — convenience
    /// for batch reduction at API edges. The resulting heap_size is
    /// the minimum across all inputs.
    pub fn merge_refs(
        inputs: &[&CountMinSketchWithHeap],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let first = inputs
            .first()
            .ok_or("CountMinSketchWithHeap::merge_refs called with empty input")?;
        let mut merged = (*first).clone();
        for h in inputs.iter().skip(1) {
            merged.merge(h)?;
        }
        Ok(merged)
    }

    pub fn serialize_msgpack(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        let (sketch, topk_heap) = (self.sketch_matrix(), self.topk_heap_items());

        let serialized = CountMinSketchWithHeapSerialized {
            sketch: CmsData {
                sketch,
                rows: self.rows,
                cols: self.cols,
            },
            topk_heap,
            heap_size: self.heap_size,
        };

        let mut buf = Vec::new();
        serialized.serialize(&mut rmp_serde::Serializer::new(&mut buf))?;
        Ok(buf)
    }

    pub fn deserialize_msgpack(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let serialized: CountMinSketchWithHeapSerialized = rmp_serde::from_slice(buffer).map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("Failed to deserialize CountMinSketchWithHeap from MessagePack: {e}").into()
            },
        )?;

        let mut sorted_topk_heap = serialized.topk_heap;
        sorted_topk_heap.sort_by(|a, b| b.value.partial_cmp(&a.value).unwrap());

        let wire_heap: Vec<WireHeapItem> = sorted_topk_heap
            .iter()
            .map(|h| WireHeapItem {
                key: h.key.clone(),
                value: h.value,
            })
            .collect();
        let backend = sketchlib_cms_heap_from_matrix_and_heap(
            serialized.sketch.rows,
            serialized.sketch.cols,
            serialized.heap_size,
            &serialized.sketch.sketch,
            &wire_heap,
        );

        Ok(Self {
            rows: serialized.sketch.rows,
            cols: serialized.sketch.cols,
            heap_size: serialized.heap_size,
            backend,
        })
    }

    pub fn aggregate_topk(
        rows: usize,
        cols: usize,
        heap_size: usize,
        keys: &[&str],
        values: &[f64],
    ) -> Option<Vec<u8>> {
        if keys.is_empty() {
            return None;
        }
        let mut sketch = Self::new(rows, cols, heap_size);
        for (key, &value) in keys.iter().zip(values.iter()) {
            sketch.update(key, value);
        }
        sketch.serialize_msgpack().ok()
    }
}

#[cfg(test)]
mod tests_wire_cms_heap {
    use super::*;

    #[test]
    fn test_creation() {
        let cms = CountMinSketchWithHeap::new(4, 1000, 20);
        assert_eq!(cms.rows, 4);
        assert_eq!(cms.cols, 1000);
        assert_eq!(cms.heap_size, 20);
        assert_eq!(cms.sketch_matrix().len(), 4);
        assert_eq!(cms.sketch_matrix()[0].len(), 1000);
        assert_eq!(cms.topk_heap_items().len(), 0);
    }

    #[test]
    fn test_query_empty() {
        let cms = CountMinSketchWithHeap::new(2, 10, 5);
        assert_eq!(cms.estimate("anything"), 0.0);
    }

    #[test]
    fn test_merge() {
        let mut sketch1 = vec![vec![0.0; 10]; 2];
        sketch1[0][0] = 10.0;
        sketch1[1][1] = 20.0;
        let mut cms1 = CountMinSketchWithHeap::from_legacy_matrix(
            sketch1,
            vec![
                CmsHeapItem {
                    key: "key1".to_string(),
                    value: 100.0,
                },
                CmsHeapItem {
                    key: "key2".to_string(),
                    value: 50.0,
                },
            ],
            2,
            10,
            5,
        );
        let mut sketch2 = vec![vec![0.0; 10]; 2];
        sketch2[0][0] = 5.0;
        sketch2[1][1] = 15.0;
        let cms2 = CountMinSketchWithHeap::from_legacy_matrix(
            sketch2,
            vec![
                CmsHeapItem {
                    key: "key3".to_string(),
                    value: 75.0,
                },
                CmsHeapItem {
                    key: "key1".to_string(),
                    value: 80.0,
                },
            ],
            2,
            10,
            3,
        );

        cms1.merge(&cms2).unwrap();

        assert_eq!(cms1.sketch_matrix()[0][0], 15.0);
        assert_eq!(cms1.sketch_matrix()[1][1], 35.0);
        assert_eq!(cms1.heap_size, 3);
        assert!(cms1.topk_heap_items().len() <= 3);
    }

    #[test]
    fn test_merge_dimension_mismatch() {
        let mut cms1 = CountMinSketchWithHeap::new(2, 10, 5);
        let cms2 = CountMinSketchWithHeap::new(3, 10, 5);
        assert!(cms1.merge(&cms2).is_err());
    }

    #[test]
    fn test_msgpack_round_trip() {
        let mut cms = CountMinSketchWithHeap::new(4, 128, 3);
        cms.update("hot", 100.0);
        cms.update("cold", 1.0);

        let bytes = cms.serialize_msgpack().unwrap();
        let deserialized = CountMinSketchWithHeap::deserialize_msgpack(&bytes).unwrap();

        assert_eq!(deserialized.rows, 4);
        assert_eq!(deserialized.cols, 128);
        assert_eq!(deserialized.heap_size, 3);
        let items = deserialized.topk_heap_items();
        assert!(!items.is_empty());
        let hot = items
            .iter()
            .find(|item| item.key == "hot")
            .expect("'hot' should be in the heap");
        assert!(hot.value >= 100.0);
        assert!(deserialized.estimate("hot") >= 100.0);
        assert!(deserialized.estimate("cold") >= 1.0);
    }

    #[test]
    fn test_aggregate_topk() {
        let keys = ["a", "b", "a", "c"];
        let values = [1.0, 2.0, 3.0, 0.5];
        let bytes = CountMinSketchWithHeap::aggregate_topk(4, 100, 2, &keys, &values).unwrap();
        let cms = CountMinSketchWithHeap::deserialize_msgpack(&bytes).unwrap();
        assert_eq!(cms.heap_size, 2);
        assert!(cms.topk_heap_items().len() <= 2);
    }

    #[test]
    fn test_aggregate_topk_empty() {
        assert!(CountMinSketchWithHeap::aggregate_topk(4, 100, 10, &[], &[]).is_none());
    }
}
