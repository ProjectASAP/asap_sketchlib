//! CSSpaceSaving: a [`Count`] sketch beside a [`SpaceSaving`] summary. Every
//! update reaches both; top-k ranks the summary's keys by their Count Sketch
//! estimate. The summary's `capacity` is independent of `k`.

use crate::sketches::countminsketch_space_saving::{SpaceSavingCounter, rank_candidates};
use crate::sketches::countsketch::CountSketchCounter;
use crate::{
    Count, DataInput, DefaultXxHasher, HeapItem, MatrixStorage, RegularPath, SketchHasher,
    SpaceSaving, Vector2D,
};

mod wire;

/// A Count Sketch paired with a Space-Saving candidate summary that hashes with
/// the same `H`.
#[derive(Clone)]
pub struct CSSpaceSaving<
    S: MatrixStorage = Vector2D<i64>,
    Mode = RegularPath,
    H: SketchHasher = DefaultXxHasher,
> {
    cs: Count<S, Mode, H>,
    summary: SpaceSaving<H>,
}

impl<T, M, H: SketchHasher> CSSpaceSaving<Vector2D<T>, M, H>
where
    T: CountSketchCounter,
{
    /// Creates a `rows x cols` sketch whose summary monitors at most
    /// `capacity` keys.
    pub fn new(rows: usize, cols: usize, capacity: usize) -> Self {
        CSSpaceSaving {
            cs: Count::with_dimensions(rows, cols),
            summary: SpaceSaving::with_capacity(capacity),
        }
    }
}

impl<S: MatrixStorage, M, H: SketchHasher> CSSpaceSaving<S, M, H>
where
    S::Counter: CountSketchCounter,
{
    /// Pairs an existing matrix with an existing summary.
    pub fn from_parts(cs: Count<S, M, H>, space_saving: SpaceSaving<H>) -> Self {
        CSSpaceSaving {
            cs,
            summary: space_saving,
        }
    }

    /// The Count Sketch matrix.
    pub fn cs(&self) -> &Count<S, M, H> {
        &self.cs
    }

    /// The candidate summary.
    pub fn space_saving(&self) -> &SpaceSaving<H> {
        &self.summary
    }

    /// Number of rows in the matrix.
    pub fn rows(&self) -> usize {
        self.cs.rows()
    }

    /// Number of columns in the matrix.
    pub fn cols(&self) -> usize {
        self.cs.cols()
    }

    /// Keys the summary can monitor.
    pub fn capacity(&self) -> usize {
        self.summary.capacity()
    }

    /// Adds `other`'s matrix to this one and merges the summaries. Panics when
    /// the dimensions, the admission rates or the summary capacities differ.
    pub fn merge(&mut self, other: &Self) {
        assert_eq!(
            self.summary.capacity(),
            other.summary.capacity(),
            "Space-Saving capacity mismatch while merging CSSpaceSaving sketches"
        );
        self.cs.merge(&other.cs);
        self.summary.merge(&other.summary);
    }
}

impl<S: MatrixStorage, H: SketchHasher> CSSpaceSaving<S, RegularPath, H>
where
    S::Counter: CountSketchCounter + SpaceSavingCounter,
{
    /// Records one occurrence of `key`.
    #[inline]
    pub fn update(&mut self, key: &DataInput) {
        self.cs.update(key);
        self.summary.update(key);
    }

    /// Adds `±many` to `key`'s cells and its
    /// [`SpaceSavingCounter::summary_weight`] to the summary.
    #[inline]
    pub fn update_weighted(&mut self, key: &DataInput, weight: S::Counter) {
        self.cs.update_weighted(key, weight);
        self.summary.update_weighted(key, weight.summary_weight());
    }

    /// Records every key in `values`.
    pub fn update_batch(&mut self, values: &[DataInput]) {
        for value in values {
            self.update(value);
        }
    }

    /// The Count Sketch estimate (row median) for `key`.
    #[inline]
    pub fn estimate(&self, key: &DataInput) -> f64 {
        self.cs.estimate(key)
    }

    /// The `k` monitored keys with the largest Count Sketch estimates, highest
    /// first by `f64::total_cmp`, ties broken by heap key order.
    pub fn top_k(&self, k: usize) -> Vec<(HeapItem, f64)> {
        rank_candidates(&self.summary, k, |key| self.cs.estimate(key))
    }
}

impl<S, H: SketchHasher> CSSpaceSaving<S, crate::FastPath, H>
where
    S: MatrixStorage + crate::FastPathHasher<H>,
    S::Counter: CountSketchCounter + SpaceSavingCounter,
{
    /// Records one occurrence of `key`.
    #[inline]
    pub fn update(&mut self, key: &DataInput) {
        self.cs.update(key);
        self.summary.update(key);
    }

    /// Adds `±many` to `key`'s cells and its
    /// [`SpaceSavingCounter::summary_weight`] to the summary.
    #[inline]
    pub fn update_weighted(&mut self, key: &DataInput, weight: S::Counter) {
        self.cs.update_weighted(key, weight);
        self.summary.update_weighted(key, weight.summary_weight());
    }

    /// Records every key in `values`.
    pub fn update_batch(&mut self, values: &[DataInput]) {
        for value in values {
            self.update(value);
        }
    }

    /// The Count Sketch estimate (row median) for `key`.
    #[inline]
    pub fn estimate(&self, key: &DataInput) -> f64 {
        self.cs.estimate(key)
    }

    /// The `k` monitored keys with the largest Count Sketch estimates, highest
    /// first by `f64::total_cmp`, ties broken by heap key order.
    pub fn top_k(&self, k: usize) -> Vec<(HeapItem, f64)> {
        rank_candidates(&self.summary, k, |key| self.cs.estimate(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CSHeap, FastPath};

    fn ranked(top: &[(HeapItem, f64)]) -> Vec<(u64, f64)> {
        top.iter()
            .map(|(key, count)| match key {
                HeapItem::U64(v) => (*v, *count),
                other => panic!("expected a U64 key, got {other:?}"),
            })
            .collect()
    }

    /// `3` displaces `2` in a two-counter summary and carries `2`'s count as
    /// error, yet `top_k` reports the matrix's `1` for it and never reports `2`.
    #[test]
    fn top_k_ranks_the_candidates_by_their_matrix_estimate() {
        let mut sk = CSSpaceSaving::<Vector2D<i64>, RegularPath>::new(3, 1024, 2);
        sk.update_weighted(&DataInput::U64(1), 5);
        sk.update(&DataInput::U64(2));
        sk.update(&DataInput::U64(3));
        assert_eq!(sk.estimate(&DataInput::U64(2)), 1.0);
        assert_eq!(sk.estimate(&DataInput::U64(3)), 1.0);
        assert_eq!(sk.space_saving().estimate(&DataInput::U64(3)), 2);
        assert_eq!(ranked(&sk.top_k(2)), vec![(1, 5.0), (3, 1.0)]);
        assert_eq!((sk.rows(), sk.cols(), sk.capacity()), (3, 1024, 2));
    }

    /// The shared key `3` is the heaviest overall but second in each source: a
    /// one-entry heap per source drops it, a three-counter summary keeps it.
    #[test]
    fn merge_keeps_a_candidate_no_local_heap_held() {
        let mut heap_a = CSHeap::<Vector2D<i64>, FastPath>::new(3, 1024, 1);
        let mut heap_b = heap_a.clone();
        let mut ss_a = CSSpaceSaving::<Vector2D<i64>, FastPath>::new(3, 1024, 3);
        let mut ss_b = ss_a.clone();
        for (local, heap) in [(1u64, &mut heap_a), (2, &mut heap_b)] {
            heap.update_weighted(&DataInput::U64(local), 5);
            heap.update_weighted(&DataInput::U64(3), 3);
        }
        for (local, ss) in [(1u64, &mut ss_a), (2, &mut ss_b)] {
            ss.update_weighted(&DataInput::U64(local), 5);
            ss.update_weighted(&DataInput::U64(3), 3);
        }
        heap_a.merge(&heap_b);
        ss_a.merge(&ss_b);

        assert_eq!(heap_a.estimate(&DataInput::U64(3)), 6.0);
        assert!(heap_a.heap().find(&DataInput::U64(3)).is_none());
        assert_eq!(ranked(&ss_a.top_k(1)), vec![(3, 6.0)]);
    }

    #[test]
    #[should_panic(expected = "capacity mismatch")]
    fn merge_rejects_a_capacity_mismatch() {
        let mut a = CSSpaceSaving::<Vector2D<i64>, RegularPath>::new(2, 64, 4);
        a.merge(&CSSpaceSaving::new(2, 64, 3));
    }

    #[test]
    #[should_panic(expected = "dimension mismatch")]
    fn merge_rejects_a_dimension_mismatch() {
        let mut a = CSSpaceSaving::<Vector2D<i64>, RegularPath>::new(2, 64, 4);
        a.merge(&CSSpaceSaving::new(2, 32, 4));
    }

    #[test]
    fn a_negative_weight_reaches_the_matrix_alone() {
        let mut sk = CSSpaceSaving::<Vector2D<f64>, RegularPath>::new(3, 1024, 4);
        sk.update_weighted(&DataInput::U64(1), -2.5);
        sk.update_weighted(&DataInput::U64(2), 0.5);
        assert_eq!(sk.estimate(&DataInput::U64(1)), -2.5);
        assert_eq!(sk.space_saving().estimate(&DataInput::U64(1)), 0);
        assert_eq!(sk.space_saving().estimate(&DataInput::U64(2)), 1);
        assert_eq!(ranked(&sk.top_k(4)), vec![(2, 0.5)]);
    }
}
