//! CMSSpaceSaving: a [`CountMin`] sketch beside a [`SpaceSaving`] summary.
//! Every update reaches both; top-k ranks the summary's keys by their Count-Min
//! estimate. The summary's `capacity` is independent of `k`.

use std::cmp::Ordering;

use crate::sketches::space_saving::parts::key_cmp;
use crate::{
    CountMin, DataInput, DefaultXxHasher, HeapItem, MatrixStorage, RegularPath, SketchHasher,
    SpaceSaving, Vector2D, heap_item_to_sketch_input,
};

pub(crate) mod summary_wire;
mod wire;

/// A matrix counter type a Space-Saving summary sits beside.
pub trait SpaceSavingCounter: Copy {
    /// The weight the summary records for an update of `self`: `self` rounded
    /// up and saturating at `u64::MAX`, or `0` (nothing recorded) when `self`
    /// is not positive.
    fn summary_weight(self) -> u64;

    /// The total order top-k ranks estimates by.
    fn order(&self, other: &Self) -> Ordering;
}

impl SpaceSavingCounter for i32 {
    fn summary_weight(self) -> u64 {
        u64::try_from(self).unwrap_or(0)
    }

    fn order(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }
}

impl SpaceSavingCounter for i64 {
    fn summary_weight(self) -> u64 {
        u64::try_from(self).unwrap_or(0)
    }

    fn order(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }
}

impl SpaceSavingCounter for i128 {
    fn summary_weight(self) -> u64 {
        if self <= 0 {
            0
        } else {
            u64::try_from(self).unwrap_or(u64::MAX)
        }
    }

    fn order(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }
}

impl SpaceSavingCounter for f64 {
    fn summary_weight(self) -> u64 {
        if self > 0.0 { self.ceil() as u64 } else { 0 }
    }

    fn order(&self, other: &Self) -> Ordering {
        self.total_cmp(other)
    }
}

/// The summary's monitored keys ranked by `estimate`, highest first, ties by
/// heap key order, cut to `k`.
pub(crate) fn rank_candidates<H: SketchHasher, C: SpaceSavingCounter>(
    summary: &SpaceSaving<H>,
    k: usize,
    estimate: impl Fn(&DataInput) -> C,
) -> Vec<(HeapItem, C)> {
    let mut ranked: Vec<(HeapItem, C)> = summary
        .entries()
        .into_iter()
        .map(|(key, _, _)| {
            let count = estimate(&heap_item_to_sketch_input(&key));
            (key, count)
        })
        .collect();
    ranked.sort_unstable_by(|a, b| b.1.order(&a.1).then_with(|| key_cmp(&a.0, &b.0)));
    ranked.truncate(k);
    ranked
}

/// A Count-Min sketch paired with a Space-Saving candidate summary that hashes
/// with the same `H`.
#[derive(Clone)]
pub struct CMSSpaceSaving<
    S: MatrixStorage = Vector2D<i64>,
    Mode = RegularPath,
    H: SketchHasher = DefaultXxHasher,
> {
    cms: CountMin<S, Mode, H>,
    summary: SpaceSaving<H>,
}

impl<T, M, H: SketchHasher> CMSSpaceSaving<Vector2D<T>, M, H>
where
    T: Copy + Default + std::ops::AddAssign,
{
    /// Creates a `rows x cols` sketch whose summary monitors at most
    /// `capacity` keys.
    pub fn new(rows: usize, cols: usize, capacity: usize) -> Self {
        CMSSpaceSaving {
            cms: CountMin::with_dimensions(rows, cols),
            summary: SpaceSaving::with_capacity(capacity),
        }
    }
}

impl<S: MatrixStorage, M, H: SketchHasher> CMSSpaceSaving<S, M, H> {
    /// Pairs an existing matrix with an existing summary.
    pub fn from_parts(cms: CountMin<S, M, H>, space_saving: SpaceSaving<H>) -> Self {
        CMSSpaceSaving {
            cms,
            summary: space_saving,
        }
    }

    /// The Count-Min matrix.
    pub fn cms(&self) -> &CountMin<S, M, H> {
        &self.cms
    }

    /// The candidate summary.
    pub fn space_saving(&self) -> &SpaceSaving<H> {
        &self.summary
    }

    /// Number of rows in the matrix.
    pub fn rows(&self) -> usize {
        self.cms.rows()
    }

    /// Number of columns in the matrix.
    pub fn cols(&self) -> usize {
        self.cms.cols()
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
            "Space-Saving capacity mismatch while merging CMSSpaceSaving sketches"
        );
        self.cms.merge(&other.cms);
        self.summary.merge(&other.summary);
    }
}

impl<S: MatrixStorage, H: SketchHasher> CMSSpaceSaving<S, RegularPath, H>
where
    S::Counter: Copy + PartialOrd + From<i32> + std::ops::AddAssign + SpaceSavingCounter,
{
    /// Records one occurrence of `key`.
    #[inline]
    pub fn update(&mut self, key: &DataInput) {
        self.cms.update(key);
        self.summary.update(key);
    }

    /// Adds `many` to `key`'s cells and its [`SpaceSavingCounter::summary_weight`]
    /// to the summary.
    #[inline]
    pub fn update_weighted(&mut self, key: &DataInput, weight: S::Counter) {
        self.cms.update_weighted(key, weight);
        self.summary.update_weighted(key, weight.summary_weight());
    }

    /// Records every key in `values`.
    pub fn update_batch(&mut self, values: &[DataInput]) {
        for value in values {
            self.update(value);
        }
    }

    /// The Count-Min estimate for `key`.
    #[inline]
    pub fn estimate(&self, key: &DataInput) -> S::Counter {
        self.cms.estimate(key)
    }

    /// The `k` monitored keys with the largest Count-Min estimates, highest
    /// first, ties broken by heap key order.
    pub fn top_k(&self, k: usize) -> Vec<(HeapItem, S::Counter)> {
        rank_candidates(&self.summary, k, |key| self.cms.estimate(key))
    }
}

impl<S, H: SketchHasher> CMSSpaceSaving<S, crate::FastPath, H>
where
    S: MatrixStorage + crate::FastPathHasher<H>,
    S::Counter: Copy + PartialOrd + From<i32> + std::ops::AddAssign + SpaceSavingCounter,
{
    /// Records one occurrence of `key`.
    #[inline]
    pub fn update(&mut self, key: &DataInput) {
        self.cms.update(key);
        self.summary.update(key);
    }

    /// Adds `many` to `key`'s cells and its [`SpaceSavingCounter::summary_weight`]
    /// to the summary.
    #[inline]
    pub fn update_weighted(&mut self, key: &DataInput, weight: S::Counter) {
        self.cms.update_weighted(key, weight);
        self.summary.update_weighted(key, weight.summary_weight());
    }

    /// Records every key in `values`.
    pub fn update_batch(&mut self, values: &[DataInput]) {
        for value in values {
            self.update(value);
        }
    }

    /// The Count-Min estimate for `key`.
    #[inline]
    pub fn estimate(&self, key: &DataInput) -> S::Counter {
        self.cms.estimate(key)
    }

    /// The `k` monitored keys with the largest Count-Min estimates, highest
    /// first, ties broken by heap key order.
    pub fn top_k(&self, k: usize) -> Vec<(HeapItem, S::Counter)> {
        rank_candidates(&self.summary, k, |key| self.cms.estimate(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CMSHeap, FastPath, heap_item_to_sketch_input};

    fn u64_key(item: &HeapItem) -> u64 {
        match item {
            HeapItem::U64(v) => *v,
            other => panic!("expected a U64 key, got {other:?}"),
        }
    }

    fn ranked(top: &[(HeapItem, i64)]) -> Vec<(u64, i64)> {
        top.iter()
            .map(|(key, count)| (u64_key(key), *count))
            .collect()
    }

    #[test]
    fn every_update_reaches_matrix_and_summary() {
        let mut sk = CMSSpaceSaving::<Vector2D<i64>, RegularPath>::new(3, 1024, 8);
        sk.update(&DataInput::U64(1));
        sk.update_weighted(&DataInput::U64(2), 7);
        assert_eq!(sk.estimate(&DataInput::U64(1)), 1);
        assert_eq!(sk.estimate(&DataInput::U64(2)), 7);
        assert_eq!(sk.space_saving().estimate(&DataInput::U64(1)), 1);
        assert_eq!(sk.space_saving().estimate(&DataInput::U64(2)), 7);
        assert_eq!(sk.space_saving().total(), 8);
        assert_eq!((sk.rows(), sk.cols(), sk.capacity()), (3, 1024, 8));
    }

    /// The summary nominates and the matrix counts: `3` displaces `2` in a
    /// two-counter summary and carries `2`'s count as error, yet `top_k`
    /// reports the matrix's `1` for it and never reports `2`.
    #[test]
    fn top_k_ranks_the_candidates_by_their_matrix_count() {
        let mut sk = CMSSpaceSaving::<Vector2D<i64>, RegularPath>::new(3, 1024, 2);
        sk.update_weighted(&DataInput::U64(1), 5);
        sk.update(&DataInput::U64(2));
        sk.update(&DataInput::U64(3));
        for key in 1..=3u64 {
            let truth = if key == 1 { 5 } else { 1 };
            assert_eq!(sk.estimate(&DataInput::U64(key)), truth);
        }
        assert_eq!(sk.space_saving().estimate(&DataInput::U64(3)), 2);
        assert_eq!(sk.space_saving().estimate(&DataInput::U64(2)), 0);
        assert_eq!(ranked(&sk.top_k(2)), vec![(1, 5), (3, 1)]);
    }

    #[test]
    fn top_k_returns_at_most_capacity_keys_whatever_k() {
        let mut sk = CMSSpaceSaving::<Vector2D<i64>, FastPath>::new(3, 1024, 3);
        for key in 1..=5u64 {
            sk.update_weighted(&DataInput::U64(key), key as i64);
        }
        assert_eq!(sk.top_k(10).len(), 3);
        assert_eq!(ranked(&sk.top_k(1)), vec![(5, 5)]);
        assert!(sk.top_k(0).is_empty());
    }

    #[test]
    fn top_k_breaks_count_ties_by_key_order() {
        let mut sk = CMSSpaceSaving::<Vector2D<i64>, RegularPath>::new(3, 1024, 4);
        for key in [9u64, 4, 7] {
            sk.update_weighted(&DataInput::U64(key), 2);
        }
        assert_eq!(ranked(&sk.top_k(3)), vec![(4, 2), (7, 2), (9, 2)]);
    }

    /// Two sources whose only shared key `3` is the heaviest overall but
    /// second locally: a one-entry heap per source drops it before the merge,
    /// while a three-counter summary keeps it as a candidate.
    #[test]
    fn merge_keeps_a_candidate_no_local_heap_held() {
        let mut heap_a = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 1024, 1);
        let mut heap_b = heap_a.clone();
        let mut ss_a = CMSSpaceSaving::<Vector2D<i64>, RegularPath>::new(3, 1024, 3);
        let mut ss_b = ss_a.clone();
        for (local, heavy) in [(1u64, &mut heap_a), (2, &mut heap_b)] {
            heavy.update_weighted(&DataInput::U64(local), 5);
            heavy.update_weighted(&DataInput::U64(3), 3);
        }
        for (local, ss) in [(1u64, &mut ss_a), (2, &mut ss_b)] {
            ss.update_weighted(&DataInput::U64(local), 5);
            ss.update_weighted(&DataInput::U64(3), 3);
        }
        heap_a.merge(&heap_b);
        ss_a.merge(&ss_b);

        assert_eq!(heap_a.estimate(&DataInput::U64(3)), 6);
        assert!(heap_a.heap().find(&DataInput::U64(3)).is_none());
        assert_eq!(ranked(&ss_a.top_k(1)), vec![(3, 6)]);
        assert_eq!(ss_a.space_saving().total(), 16);
    }

    #[test]
    fn merge_adds_the_matrices() {
        let mut a = CMSSpaceSaving::<Vector2D<i64>, RegularPath>::new(2, 64, 4);
        let mut b = a.clone();
        a.update_weighted(&DataInput::U64(1), 3);
        b.update_weighted(&DataInput::U64(1), 4);
        b.update(&DataInput::U64(2));
        let cells: Vec<i64> = a
            .cms()
            .as_storage()
            .as_slice()
            .iter()
            .zip(b.cms().as_storage().as_slice())
            .map(|(x, y)| x + y)
            .collect();
        a.merge(&b);
        assert_eq!(a.cms().as_storage().as_slice(), cells.as_slice());
        assert_eq!(ranked(&a.top_k(2)), vec![(1, 7), (2, 1)]);
    }

    #[test]
    #[should_panic(expected = "capacity mismatch")]
    fn merge_rejects_a_capacity_mismatch() {
        let mut a = CMSSpaceSaving::<Vector2D<i64>, RegularPath>::new(2, 64, 4);
        a.merge(&CMSSpaceSaving::new(2, 64, 5));
    }

    #[test]
    #[should_panic(expected = "dimension mismatch")]
    fn merge_rejects_a_dimension_mismatch() {
        let mut a = CMSSpaceSaving::<Vector2D<i64>, RegularPath>::new(2, 64, 4);
        a.merge(&CMSSpaceSaving::new(3, 64, 4));
    }

    #[test]
    #[should_panic(expected = "admission rate mismatch")]
    fn merge_rejects_an_admission_rate_mismatch() {
        let mut a = CMSSpaceSaving::<Vector2D<i64>, RegularPath>::new(2, 64, 4);
        let sampled = CMSSpaceSaving::from_parts(
            CountMin::with_dimensions(2, 64).with_admission_rate(0.5),
            SpaceSaving::with_capacity(4),
        );
        a.merge(&sampled);
    }

    #[test]
    fn summary_weight_rounds_up_and_drops_non_positive_weights() {
        assert_eq!(7i32.summary_weight(), 7);
        assert_eq!((-7i32).summary_weight(), 0);
        assert_eq!(0i64.summary_weight(), 0);
        assert_eq!(i64::MAX.summary_weight(), i64::MAX as u64);
        assert_eq!(i128::MAX.summary_weight(), u64::MAX);
        assert_eq!((-1i128).summary_weight(), 0);
        assert_eq!(0.25f64.summary_weight(), 1);
        assert_eq!(2.0f64.summary_weight(), 2);
        assert_eq!((-0.5f64).summary_weight(), 0);
        assert_eq!(f64::NAN.summary_weight(), 0);
        assert_eq!(f64::INFINITY.summary_weight(), u64::MAX);
    }

    #[test]
    fn f64_counters_rank_by_their_fractional_estimate() {
        let mut sk = CMSSpaceSaving::<Vector2D<f64>, RegularPath>::new(3, 1024, 4);
        sk.update_weighted(&DataInput::U64(1), 0.25);
        sk.update_weighted(&DataInput::U64(2), 1.5);
        assert_eq!(sk.space_saving().estimate(&DataInput::U64(1)), 1);
        let top = sk.top_k(2);
        assert_eq!(heap_item_to_sketch_input(&top[0].0), DataInput::U64(2));
        assert_eq!(top[0].1, 1.5);
        assert_eq!(top[1].1, 0.25);
    }

    #[test]
    fn a_non_positive_weight_reaches_the_matrix_alone() {
        let mut sk = CMSSpaceSaving::<Vector2D<i64>, RegularPath>::new(3, 1024, 4);
        sk.update_weighted(&DataInput::U64(1), 0);
        sk.update_weighted(&DataInput::U64(2), -3);
        assert!(sk.space_saving().is_empty());
        assert_eq!(sk.estimate(&DataInput::U64(2)), -3);
    }
}
