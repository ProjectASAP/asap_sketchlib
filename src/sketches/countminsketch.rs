//! Count-Min Sketch implementation.
//!
//! A sub-linear space data structure for estimating frequencies of items in a
//! stream, with one-sided error bounded by the L1 norm of the stream.
//!
//! Reference:
//! - Cormode & Muthukrishnan, "An Improved Data Stream Summary: The Count-Min
//!   Sketch and its Applications," J. Algorithms 55(1), 2005.
//!   <https://www.cs.rutgers.edu/~muthu/cm-jal.pdf>

use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use crate::common::structure_utils::AdmittedRows;
use crate::input_to_owned;
use crate::octo_delta::{CM_PROMASK, CmDelta, KeyedCmDelta, MAX_PROMASK};
use crate::{
    DataInput, DefaultMatrixI32, DefaultMatrixI64, DefaultMatrixI128, DefaultXxHasher, FastPath,
    FastPathHasher, FixedMatrix, MatrixFastHash, MatrixStorage, NitroTarget, QuickMatrixI64,
    QuickMatrixI128, RegularPath, SketchHasher, Vector2D, hash64_seeded, nitro_delta_saturated_i32,
};

mod wire;
pub(crate) use wire::{CmsWireCounter, CmsWireMode};

const DEFAULT_ROW_NUM: usize = 3;
const DEFAULT_COL_NUM: usize = 4096;
/// Recommended row count for quick-start examples.
pub const QUICKSTART_ROW_NUM: usize = 5;
/// Recommended column count for quick-start examples.
pub const QUICKSTART_COL_NUM: usize = 2048;
const LOWER_32_MASK: u64 = (1u64 << 32) - 1;

/// A Count-Min Sketch for estimating item frequencies in a data stream.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(serialize = "S: Serialize", deserialize = "S: Deserialize<'de>"))]
pub struct CountMin<
    S: MatrixStorage = Vector2D<i32>,
    Mode = RegularPath,
    H: SketchHasher = DefaultXxHasher,
> {
    counts: S,
    row: usize,
    col: usize,
    #[serde(skip)]
    _mode: PhantomData<Mode>,
    #[serde(skip)]
    _hasher: PhantomData<H>,
}

// Default CountMin sketch for Vector2D<i32> (RegularPath).
impl Default for CountMin<Vector2D<i32>, RegularPath> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM)
    }
}

// Default CountMin sketch for Vector2D<i32> (FastPath).
impl Default for CountMin<Vector2D<i32>, FastPath> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM)
    }
}

// Default CountMin sketch for Vector2D<i64> (RegularPath).
impl Default for CountMin<Vector2D<i64>, RegularPath> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM)
    }
}

// Default CountMin sketch for Vector2D<i64> (FastPath).
impl Default for CountMin<Vector2D<i64>, FastPath> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM)
    }
}

// Default CountMin sketch for Vector2D<i128> (RegularPath).
impl Default for CountMin<Vector2D<i128>, RegularPath> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM)
    }
}

// Default CountMin sketch for Vector2D<i128> (FastPath).
impl Default for CountMin<Vector2D<i128>, FastPath> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM)
    }
}

// Default CountMin sketch for Vector2D<f64> (RegularPath and FastPath).
impl Default for CountMin<Vector2D<f64>, RegularPath> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM)
    }
}

impl Default for CountMin<Vector2D<f64>, FastPath> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM)
    }
}

// Default CountMin sketch for FixedMatrix (RegularPath).
impl Default for CountMin<FixedMatrix, RegularPath> {
    fn default() -> Self {
        CountMin::from_storage(FixedMatrix::default())
    }
}

// Default CountMin sketch for FixedMatrix (FastPath).
impl Default for CountMin<FixedMatrix, FastPath> {
    fn default() -> Self {
        CountMin::from_storage(FixedMatrix::default())
    }
}

// Default CountMin sketch for DefaultMatrixI32 (RegularPath).
impl Default for CountMin<DefaultMatrixI32, RegularPath> {
    fn default() -> Self {
        CountMin::from_storage(DefaultMatrixI32::default())
    }
}

// Default CountMin sketch for DefaultMatrixI32 (FastPath).
impl Default for CountMin<DefaultMatrixI32, FastPath> {
    fn default() -> Self {
        CountMin::from_storage(DefaultMatrixI32::default())
    }
}

// Default CountMin sketch for QuickMatrixI64 (RegularPath).
impl Default for CountMin<QuickMatrixI64, RegularPath> {
    fn default() -> Self {
        CountMin::from_storage(QuickMatrixI64::default())
    }
}

// Default CountMin sketch for QuickMatrixI64 (FastPath).
impl Default for CountMin<QuickMatrixI64, FastPath> {
    fn default() -> Self {
        CountMin::from_storage(QuickMatrixI64::default())
    }
}

// Default CountMin sketch for QuickMatrixI128 (RegularPath).
impl Default for CountMin<QuickMatrixI128, RegularPath> {
    fn default() -> Self {
        CountMin::from_storage(QuickMatrixI128::default())
    }
}

// Default CountMin sketch for QuickMatrixI128 (FastPath).
impl Default for CountMin<QuickMatrixI128, FastPath> {
    fn default() -> Self {
        CountMin::from_storage(QuickMatrixI128::default())
    }
}

// Default CountMin sketch for DefaultMatrixI64 (RegularPath).
impl Default for CountMin<DefaultMatrixI64, RegularPath> {
    fn default() -> Self {
        CountMin::from_storage(DefaultMatrixI64::default())
    }
}

// Default CountMin sketch for DefaultMatrixI64 (FastPath).
impl Default for CountMin<DefaultMatrixI64, FastPath> {
    fn default() -> Self {
        CountMin::from_storage(DefaultMatrixI64::default())
    }
}

// Default CountMin sketch for DefaultMatrixI128 (RegularPath).
impl Default for CountMin<DefaultMatrixI128, RegularPath> {
    fn default() -> Self {
        CountMin::from_storage(DefaultMatrixI128::default())
    }
}

// Default CountMin sketch for DefaultMatrixI128 (FastPath).
impl Default for CountMin<DefaultMatrixI128, FastPath> {
    fn default() -> Self {
        CountMin::from_storage(DefaultMatrixI128::default())
    }
}

// CountMin constructors for Vector2D-backed storage.
impl<T, M, H: SketchHasher> CountMin<Vector2D<T>, M, H>
where
    T: Copy + Default + std::ops::AddAssign,
{
    /// Creates a sketch with the requested number of rows and columns.
    pub fn with_dimensions(rows: usize, cols: usize) -> Self {
        let mut sk = CountMin {
            counts: Vector2D::init(rows, cols),
            row: rows,
            col: cols,
            _mode: PhantomData,
            _hasher: PhantomData,
        };
        sk.counts.fill(T::default());
        sk
    }
}

// Core CountMin API for any storage.
impl<S: MatrixStorage, Mode, H: SketchHasher> CountMin<S, Mode, H> {
    /// Creates a sketch from an existing matrix storage instance.
    pub fn from_storage(counts: S) -> Self {
        let row = counts.rows();
        let col = counts.cols();
        Self {
            counts,
            row,
            col,
            _mode: PhantomData,
            _hasher: PhantomData,
        }
    }

    /// Number of rows in the sketch.
    #[inline(always)]
    pub fn rows(&self) -> usize {
        self.counts.rows()
    }

    /// Number of columns in the sketch.
    #[inline(always)]
    pub fn cols(&self) -> usize {
        self.counts.cols()
    }

    /// Exposes the backing matrix for inspection/testing.
    pub fn as_storage(&self) -> &S {
        &self.counts
    }

    /// Mutable access used internally for testing scenarios.
    pub fn as_storage_mut(&mut self) -> &mut S {
        &mut self.counts
    }

    /// Merges another sketch while asserting compatible dimensions.
    pub fn merge(&mut self, other: &Self) {
        let self_rows = self.counts.rows();
        let self_cols = self.counts.cols();
        assert_eq!(
            (self_rows, self_cols),
            (other.counts.rows(), other.counts.cols()),
            "dimension mismatch while merging CountMin sketches"
        );

        for i in 0..self_rows {
            for j in 0..self_cols {
                let value = other.counts.query_one_counter(i, j);
                self.counts.increment_by_row(i, j, value);
            }
        }
    }

    /// Merges another sketch by keeping the larger of each counter pair.
    /// Correct only when the two sketches observed disjoint key sets; a shared
    /// key reads back as the larger side rather than the sum.
    pub fn merge_max(&mut self, other: &Self)
    where
        S::Counter: PartialOrd,
    {
        let self_rows = self.counts.rows();
        let self_cols = self.counts.cols();
        assert_eq!(
            (self_rows, self_cols),
            (other.counts.rows(), other.counts.cols()),
            "dimension mismatch while merging CountMin sketches"
        );

        for i in 0..self_rows {
            for j in 0..self_cols {
                let value = other.counts.query_one_counter(i, j);
                self.counts.update_one_counter(
                    i,
                    j,
                    |cell: &mut S::Counter, incoming: S::Counter| {
                        if incoming > *cell {
                            *cell = incoming;
                        }
                    },
                    value,
                );
            }
        }
    }
}

// DataInput adapters for the regular Count-Min update rule.
// Regular-path CountMin operations. Uses PartialOrd to support both integer and f64 counters.
impl<S: MatrixStorage, H: SketchHasher> CountMin<S, RegularPath, H>
where
    S::Counter: Copy + PartialOrd + From<i32> + std::ops::AddAssign,
{
    /// Inserts an observation while using the standard Count-Min minimum row update rule.
    #[inline(always)]
    pub fn insert(&mut self, value: &DataInput) {
        let rows = self.counts.rows();
        let cols = self.counts.cols();
        for r in 0..rows {
            let hashed = H::hash64_seeded(r, value);
            let col = ((hashed & LOWER_32_MASK) as usize) % cols;
            self.counts.increment_by_row(r, col, S::Counter::from(1));
        }
    }

    /// Inserts observations with the given count (supports fractional weights for f64 counters).
    #[inline(always)]
    pub fn insert_many(&mut self, value: &DataInput, many: S::Counter) {
        let rows = self.counts.rows();
        let cols = self.counts.cols();
        for r in 0..rows {
            let hashed = H::hash64_seeded(r, value);
            let col = ((hashed & LOWER_32_MASK) as usize) % cols;
            self.counts.increment_by_row(r, col, many);
        }
    }

    /// Inserts a batch of observations using the regular Count-Min update rule.
    #[inline(always)]
    pub fn bulk_insert(&mut self, values: &[DataInput]) {
        for value in values {
            self.insert(value);
        }
    }

    /// Inserts a batch of observations with per-item counts.
    #[inline(always)]
    pub fn bulk_insert_many(&mut self, values: &[(DataInput, S::Counter)]) {
        for (value, many) in values {
            self.insert_many(value, *many);
        }
    }

    /// Returns the frequency estimate for the provided value.
    #[inline(always)]
    pub fn estimate(&self, value: &DataInput) -> S::Counter {
        let rows = self.counts.rows();
        let cols = self.counts.cols();
        // Seed the running minimum from row 0's probed cell, then fold in the
        // rest. Mirrors the fast path's `fast_query_min`.
        let col0 = ((H::hash64_seeded(0, value) & LOWER_32_MASK) as usize) % cols;
        let mut min = self.counts.query_one_counter(0, col0);
        for r in 1..rows {
            let hashed = H::hash64_seeded(r, value);
            let col = ((hashed & LOWER_32_MASK) as usize) % cols;
            let v = self.counts.query_one_counter(r, col);
            if v < min {
                min = v;
            }
        }
        min
    }
}

/// Count-Min sketch with floating-point counters (no integer rounding).
pub type CountMinF64<H = DefaultXxHasher> = CountMin<Vector2D<f64>, RegularPath, H>;

impl<S: MatrixStorage<Counter = i32>, H: SketchHasher> CountMin<S, RegularPath, H> {
    /// Inserts an observation, emitting a delta at every promotion of the
    /// default threshold `CM_PROMASK`.
    #[inline(always)]
    pub fn insert_emit_delta(&mut self, value: &DataInput, emit: &mut impl FnMut(CmDelta)) {
        self.insert_emit_delta_with_threshold(value, CM_PROMASK, emit);
    }

    /// Inserts an observation, emitting a delta and clearing the counter each
    /// time a row counter reaches `threshold` (OctoSketch Algorithm 1).
    #[inline(always)]
    pub fn insert_emit_delta_with_threshold(
        &mut self,
        value: &DataInput,
        threshold: u32,
        emit: &mut impl FnMut(CmDelta),
    ) {
        let threshold = threshold.clamp(1, MAX_PROMASK) as i32;
        let rows = self.counts.rows();
        let cols = self.counts.cols();
        for r in 0..rows {
            let hashed = hash64_seeded(r, value);
            let col = ((hashed & LOWER_32_MASK) as usize) % cols;
            self.counts.increment_by_row(r, col, 1);
            let current = self.counts.query_one_counter(r, col);
            if current >= threshold {
                emit(CmDelta {
                    row: r as u32,
                    col: col as u32,
                    value: current as u32,
                });
                self.counts.update_one_counter(r, col, |c, _| *c = 0, ());
            }
        }
    }

    /// As `insert_emit_delta_with_threshold`, but every delta carries the flow
    /// key so an aggregator can maintain the heavy-hitter heap that workers
    /// no longer keep.
    pub fn insert_emit_keyed_delta_with_threshold(
        &mut self,
        value: &DataInput,
        threshold: u32,
        emit: &mut impl FnMut(KeyedCmDelta),
    ) {
        self.insert_emit_delta_with_threshold(value, threshold, &mut |delta| {
            emit(KeyedCmDelta {
                key: input_to_owned(value),
                delta,
            })
        });
    }
}

impl<S, H: SketchHasher> CountMin<S, FastPath, H>
where
    S: MatrixStorage<Counter = i32> + FastPathHasher<H>,
{
    /// Inserts an observation via fast-path, emitting a delta at every
    /// promotion of the default threshold `CM_PROMASK`.
    #[inline(always)]
    pub fn insert_emit_delta(&mut self, value: &DataInput, emit: &mut impl FnMut(CmDelta)) {
        self.insert_emit_delta_with_threshold(value, CM_PROMASK, emit);
    }

    /// Fast-path counterpart of the regular-path threshold API.
    #[inline(always)]
    pub fn insert_emit_delta_with_threshold(
        &mut self,
        value: &DataInput,
        threshold: u32,
        emit: &mut impl FnMut(CmDelta),
    ) {
        let threshold = threshold.clamp(1, MAX_PROMASK) as i32;
        let hashed_val = <S as FastPathHasher<H>>::hash_for_matrix(&self.counts, value);
        let rows = self.counts.rows();
        let cols = self.counts.cols();
        for r in 0..rows {
            let col = hashed_val.col_for_row(r, cols);
            self.counts.increment_by_row(r, col, 1);
            let current = self.counts.query_one_counter(r, col);
            if current >= threshold {
                emit(CmDelta {
                    row: r as u32,
                    col: col as u32,
                    value: current as u32,
                });
                self.counts.update_one_counter(r, col, |c, _| *c = 0, ());
            }
        }
    }

    /// Fast-path counterpart of `insert_emit_keyed_delta_with_threshold`.
    pub fn insert_emit_keyed_delta_with_threshold(
        &mut self,
        value: &DataInput,
        threshold: u32,
        emit: &mut impl FnMut(KeyedCmDelta),
    ) {
        self.insert_emit_delta_with_threshold(value, threshold, &mut |delta| {
            emit(KeyedCmDelta {
                key: input_to_owned(value),
                delta,
            })
        });
    }
}

impl<S: MatrixStorage, Mode, H: SketchHasher> CountMin<S, Mode, H>
where
    S::Counter: Copy + std::ops::AddAssign + From<i32>,
{
    /// Applies a delta update to the sketch counters.
    pub fn apply_delta(&mut self, delta: CmDelta) {
        self.counts.increment_by_row(
            delta.row as usize,
            delta.col as usize,
            S::Counter::from(delta.value as i32),
        );
    }
}

// DataInput adapters for the fast-path Count-Min update rule.
// Fast-path CountMin operations using precomputed hashes. Uses PartialOrd for f64 support.
impl<S, H: SketchHasher> CountMin<S, FastPath, H>
where
    S: MatrixStorage + crate::FastPathHasher<H>,
    S::Counter: Copy + PartialOrd + From<i32> + std::ops::AddAssign,
{
    /// Inserts an observation using the combined hash optimization.
    #[inline(always)]
    pub fn insert(&mut self, value: &DataInput) {
        let hashed_val = <S as FastPathHasher<H>>::hash_for_matrix(&self.counts, value);
        self.counts
            .fast_insert(|a, b, _| *a += *b, S::Counter::from(1), &hashed_val);
    }

    /// Inserts observations with the given count using the fast-path hash.
    #[inline(always)]
    pub fn insert_many(&mut self, value: &DataInput, many: S::Counter) {
        let hashed_val = <S as FastPathHasher<H>>::hash_for_matrix(&self.counts, value);
        self.counts
            .fast_insert(|a, b, _| *a += *b, many, &hashed_val);
    }

    /// Inserts a batch of observations using the fast-path hash.
    #[inline(always)]
    pub fn bulk_insert(&mut self, values: &[DataInput]) {
        for value in values {
            self.insert(value);
        }
    }

    /// Inserts a batch of observations with per-item counts using the fast-path hash.
    #[inline(always)]
    pub fn bulk_insert_many(&mut self, values: &[(DataInput, S::Counter)]) {
        for (value, many) in values {
            self.insert_many(value, *many);
        }
    }

    /// Returns the frequency estimate for the provided value.
    #[inline(always)]
    pub fn estimate(&self, value: &DataInput) -> S::Counter {
        let hashed_val = <S as FastPathHasher<H>>::hash_for_matrix(&self.counts, value);
        self.counts.fast_query_min(&hashed_val, |val, _, _| *val)
    }
}

// Core fast-path operations that operate on pre-computed hashes.
impl<S, H: SketchHasher> CountMin<S, FastPath, H>
where
    S: MatrixStorage,
    S::Counter: Copy + PartialOrd + From<i32> + std::ops::AddAssign,
{
    /// Inserts an observation using the combined hash optimization.
    /// Hash value can be reused with other sketches.
    #[inline(always)]
    pub fn fast_insert_with_hash_value(&mut self, hashed_val: &H::HashType) {
        self.counts
            .fast_insert(|a, b, _| *a += *b, S::Counter::from(1), hashed_val);
    }

    #[inline(always)]
    /// Inserts multiple observations using a pre-computed hash value.
    pub fn fast_insert_many_with_hash_value(&mut self, hashed_val: &H::HashType, many: S::Counter) {
        self.counts
            .fast_insert(|a, b, _| *a += *b, many, hashed_val);
    }

    /// Inserts a batch of observations using pre-computed hash values.
    #[inline(always)]
    pub fn bulk_insert_with_hashes(&mut self, hashes: &[H::HashType]) {
        for hashed_val in hashes {
            self.fast_insert_with_hash_value(hashed_val);
        }
    }

    /// Inserts a batch of observations with per-item counts using pre-computed hash values.
    #[inline(always)]
    pub fn bulk_insert_many_with_hashes(&mut self, hashes: &[(H::HashType, S::Counter)]) {
        for (hashed_val, many) in hashes {
            self.fast_insert_many_with_hash_value(hashed_val, *many);
        }
    }

    /// Returns the frequency estimate using a pre-computed hash value.
    #[inline(always)]
    pub fn fast_estimate_with_hash(&self, hashed_val: &H::HashType) -> S::Counter {
        self.counts.fast_query_min(hashed_val, |val, _, _| *val)
    }
}

// Nitro sampling helpers for fast-path CountMin.
impl<H: SketchHasher> CountMin<Vector2D<i32>, FastPath, H> {
    /// Enables Nitro sampling with the provided rate.
    pub fn enable_nitro(&mut self, sampling_rate: f64) {
        self.counts.enable_nitro(sampling_rate);
    }

    /// Enables Nitro sampling with a reproducible, seed-selected schedule.
    ///
    /// See [`crate::Nitro::init_nitro_seeded`]: the unseeded path starts every
    /// sketch at the same point in the skip table, so two sketches at the same
    /// rate admit the same subset and are not independent trials.
    pub fn enable_nitro_with_seed(&mut self, sampling_rate: f64, seed: u64) {
        self.counts.enable_nitro_with_seed(sampling_rate, seed);
    }

    /// Disables Nitro sampling and resets its internal state.
    pub fn disable_nitro(&mut self) {
        self.counts.disable_nitro();
    }

    /// Inserts an observation through Nitro's per-row sampling schedule.
    ///
    /// The cells written are the ones a plain `insert` would write — the hash
    /// is `FastPathHasher::hash_for_matrix` and the column is
    /// `MatrixFastHash::col_for_row`, exactly as `MatrixStorage::fast_insert`
    /// derives them, so `nitro_estimate` reads back what this wrote. The hash
    /// is computed once per observation, and the admitted rows are collected
    /// into an inline buffer, so the hot path does not allocate.
    #[inline(always)]
    pub fn fast_insert_nitro(&mut self, value: &DataInput) {
        let rows = self.counts.rows();
        let mut admitted = AdmittedRows::new();
        self.counts.nitro_mut().admit_rows(rows, &mut admitted);
        if admitted.is_empty() {
            return;
        }
        let cols = self.counts.cols();
        let hashed = <Vector2D<i32> as FastPathHasher<H>>::hash_for_matrix(&self.counts, value);
        for (row, weight) in admitted {
            let col = MatrixFastHash::col_for_row(&hashed, row, cols);
            let delta = nitro_delta_saturated_i32(weight);
            self.counts
                .update_one_counter(row, col, |a: &mut i32, b: i32| *a += b, delta);
        }
    }

    /// Returns the median estimate using a fast-path matrix hash.
    pub fn nitro_estimate(&self, value: &DataInput) -> f64 {
        let hashed_val = <Vector2D<i32> as FastPathHasher<H>>::hash_for_matrix(&self.counts, value);
        self.counts
            .fast_query_median(&hashed_val, |val, _, _| (*val) as f64)
    }
}

/// Thin wrappers to satisfy the NitroTarget trait for CountMin.
// NitroTarget integration for fast-path CountMin.
impl<H: SketchHasher> NitroTarget for CountMin<Vector2D<i32>, FastPath, H> {
    #[inline(always)]
    fn rows(&self) -> usize {
        self.counts.rows()
    }

    #[inline(always)]
    fn update_row(&mut self, row: usize, hashed: u128, delta: u64) {
        self.counts.update_by_row(
            row,
            hashed,
            |a, b| *a += b,
            nitro_delta_saturated_i32(delta),
        );
    }

    #[inline(always)]
    fn update_sample(&mut self, value: &DataInput, delta: u64) {
        // Hash with the SAME derivation the estimator uses so both share one
        // hash domain (raw hash128_seeded bit-slicing diverges in Packed64 mode).
        let hashed = <Vector2D<i32> as FastPathHasher<H>>::hash_for_matrix(&self.counts, value);
        let cols = self.counts.cols();
        for row in 0..self.counts.rows() {
            let col = <H::HashType as MatrixFastHash>::col_for_row(&hashed, row, cols);
            self.counts.update_one_counter(
                row,
                col,
                |a, b| *a += b,
                nitro_delta_saturated_i32(delta),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{all_counter_zero_i32, counter_index, sample_zipf_u64};
    use crate::{DataInput, hash_for_matrix, hash64_seeded};
    use std::collections::{HashMap, HashSet};

    #[test]
    fn countmin_insert_emit_delta_emits_at_threshold_and_resets_period() {
        let mut sketch = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(3, 64);
        let key = DataInput::U64(42);
        let mut deltas: Vec<CmDelta> = Vec::new();

        for _ in 0..(CM_PROMASK - 1) {
            sketch.insert_emit_delta(&key, &mut |d| deltas.push(d));
        }
        assert!(
            deltas.is_empty(),
            "regular CMS worker path should not emit before threshold"
        );

        sketch.insert_emit_delta(&key, &mut |d| deltas.push(d));
        assert_eq!(
            deltas.len(),
            3,
            "should emit one delta per row at threshold"
        );
        assert!(deltas.iter().all(|d| d.value == CM_PROMASK));

        for _ in 0..(CM_PROMASK - 1) {
            sketch.insert_emit_delta(&key, &mut |d| deltas.push(d));
        }
        assert_eq!(deltas.len(), 3, "no second emission before next threshold");
        sketch.insert_emit_delta(&key, &mut |d| deltas.push(d));
        assert_eq!(deltas.len(), 6, "should emit again on next threshold");
    }

    #[test]
    fn countmin_apply_delta_increments_parent_counter() {
        let mut parent = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(3, 64);
        let delta = CmDelta {
            row: 1,
            col: 5,
            value: CM_PROMASK,
        };
        parent.apply_delta(delta);
        assert_eq!(
            parent.as_storage().query_one_counter(1, 5),
            CM_PROMASK as i32
        );
    }

    // test for dimension of CMS after initialization
    #[test]
    fn dimension_test() {
        // test default sketch dimension
        let cm = CountMin::<Vector2D<i32>, RegularPath>::default();
        assert_eq!(cm.rows(), 3);
        assert_eq!(cm.cols(), 4096);
        let storage = cm.as_storage();
        all_counter_zero_i32(storage);

        // test for custom dimension size
        let cm_customize = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(3, 17);
        assert_eq!(cm_customize.rows(), 3);
        assert_eq!(cm_customize.cols(), 17);

        let storage_customize = cm_customize.as_storage();
        all_counter_zero_i32(storage_customize);
    }

    #[test]
    fn fast_insert_same_estimate() {
        let mut slow = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(3, 64);
        let mut fast = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 64);

        let keys = vec![
            DataInput::Str("alpha"),
            DataInput::Str("beta"),
            DataInput::Str("gamma"),
            DataInput::Str("delta"),
            DataInput::Str("epsilon"),
        ];

        for key in &keys {
            slow.insert(key);
            fast.insert(key);
        }

        for key in &keys {
            assert_eq!(
                slow.estimate(key),
                fast.estimate(key),
                "fast path should match standard insert for key {key:?}"
            );
        }
    }

    #[test]
    fn estimate_does_not_clamp_i64_counts_above_i32_max() {
        // Regression: the running minimum used to be seeded with `i32::MAX`,
        // which clamped i64 estimates whenever every probed cell exceeded
        // 2147483647, capping large counts at 2147483647.
        let mut sk = CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(3, 64);
        let key = DataInput::Str("pkt_len");
        let count: i64 = 35_000_000_000; // ~35 billion, well past i32::MAX

        sk.insert_many(&key, count);

        let est = sk.estimate(&key);
        assert!(
            est >= count,
            "estimate {est} should be at least the true count {count}, not clamped"
        );
        assert_ne!(est, i32::MAX as i64, "estimate must not clamp to i32::MAX");
    }

    #[test]
    fn merge_adds_counters_element_wise() {
        let mut left = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(2, 32);
        let mut right = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(2, 32);
        let key = DataInput::Str("delta");

        left.insert(&key);
        right.insert(&key);
        right.insert(&key);

        let left_indices: Vec<_> = (0..left.rows())
            .map(|row| counter_index(row, &key, left.cols()))
            .collect();

        left.merge(&right);

        for (row, idx) in left_indices.into_iter().enumerate() {
            assert_eq!(left.as_storage().query_one_counter(row, idx), 3);
        }
    }

    #[test]
    #[should_panic(expected = "dimension mismatch while merging CountMin sketches")]
    fn merge_requires_matching_dimensions() {
        let mut left = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(2, 32);
        let right = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(3, 32);
        left.merge(&right);
    }

    #[test]
    fn cm_regular_path_correctness() {
        let mut sk = CountMin::<Vector2D<i32>, RegularPath>::default();
        // Insert values 0..9 once using the regular path.
        for i in 0..10 {
            sk.insert(&DataInput::I32(i));
        }

        // Build the expected counter array by mirroring the regular-path hashing logic.
        let storage = sk.as_storage();
        let rows = storage.rows();
        let cols = storage.cols();
        let mut expected_once = vec![0_i32; rows * cols];
        for i in 0..10 {
            let value = DataInput::I32(i);
            for r in 0..rows {
                let hashed = hash64_seeded(r, &value);
                let col = ((hashed & LOWER_32_MASK) as usize) % cols;
                let idx = r * cols + col;
                expected_once[idx] += 1;
            }
        }
        // All counters should match the expected single-pass values.
        assert_eq!(storage.as_slice(), expected_once.as_slice());

        // Insert the same values again; counters should double.
        for i in 0..10 {
            sk.insert(&DataInput::I32(i));
        }
        let expected_twice: Vec<i32> = expected_once.iter().map(|v| v * 2).collect();
        assert_eq!(sk.as_storage().as_slice(), expected_twice.as_slice());

        // Estimates for inserted keys should be exactly 2.
        for i in 0..10 {
            assert_eq!(
                sk.estimate(&DataInput::I32(i)),
                2,
                "estimate for {i} should be 2, but get {}",
                sk.estimate(&DataInput::I32(i))
            )
        }
    }

    #[test]
    fn cm_fast_path_correctness() {
        let mut sk = CountMin::<Vector2D<i32>, FastPath>::default();
        for i in 0..10 {
            sk.insert(&DataInput::I32(i));
        }

        let storage = sk.as_storage();
        let rows = storage.rows();
        let cols = storage.cols();
        let mask_bits = storage.get_mask_bits();
        let mask = (1u64 << mask_bits) - 1;
        let mut expected_once = vec![0_i32; rows * cols];

        for i in 0..10 {
            let value = DataInput::I32(i);
            let hash = hash64_seeded(0, &value);
            for row in 0..rows {
                let hashed = (hash >> (mask_bits as usize * row)) & mask;
                let col = (hashed as usize) % cols;
                let idx = row * cols + col;
                expected_once[idx] += 1;
            }
        }

        assert_eq!(storage.as_slice(), expected_once.as_slice());
    }

    // ----------------------------------------------------- instance coverage
    //
    // Structural properties of the storage matrix and of the insert/estimate
    // entry points. None of these need a statistical bound: they are exact
    // equalities that hold for every stream, so they live here rather than in
    // the E2E frequency suites.

    /// Counter values reach the assertions as `f64`; the concrete counter type
    /// is known at each macro expansion, so this stays a plain conversion.
    trait CounterAsF64: Copy {
        fn as_f64(self) -> f64;
    }
    impl CounterAsF64 for i32 {
        fn as_f64(self) -> f64 {
            self as f64
        }
    }
    impl CounterAsF64 for i64 {
        fn as_f64(self) -> f64 {
            self as f64
        }
    }
    impl CounterAsF64 for i128 {
        fn as_f64(self) -> f64 {
            self as f64
        }
    }
    impl CounterAsF64 for f64 {
        fn as_f64(self) -> f64 {
            self
        }
    }

    /// Eight well-separated keys in a 2048- or 4096-column grid: the chance
    /// that any pair collides in every row is negligible, and with fixed seeds
    /// the outcome is deterministic, so a failure here means a storage backend
    /// is mis-indexing rather than that the stream was unlucky.
    const COLLISION_FREE_KEYS: [u64; 8] = [
        1,
        7,
        4_242,
        90_210,
        1_000_003,
        2_147_483_647,
        4_294_967_311,
        9_007_199_254_740_993,
    ];

    const NON_POWER_OF_TWO_WIDTHS: [usize; 4] = [3, 100, 1_000, 4_095];

    /// On a workload small enough to be collision-free, *both* paths must
    /// return exact counts — every probed cell belongs to the queried key
    /// alone.
    ///
    /// This, and not estimate-for-estimate equality, is the cross-path
    /// contract. `RegularPath` makes one hash call per row with `seed_list[r]`;
    /// `FastPath` makes a single call with `seed_list[0]` and slices row bits
    /// out of it. They are different hash functions, so they place keys in
    /// different columns and legitimately disagree wherever either one
    /// collides.
    macro_rules! assert_both_paths_exact_without_collisions {
        ($storage:ty) => {{
            let mut regular = CountMin::<$storage, RegularPath>::default();
            let mut fast = CountMin::<$storage, FastPath>::default();
            let mut truth = HashMap::<u64, i64>::new();
            for (i, k) in COLLISION_FREE_KEYS.iter().enumerate() {
                for _ in 0..(i + 1) * 10 {
                    let d = DataInput::U64(*k);
                    regular.insert(&d);
                    fast.insert(&d);
                    *truth.entry(*k).or_insert(0) += 1;
                }
            }
            let label = concat!("CountMin<", stringify!($storage), ">");
            for (k, c) in &truth {
                let r = regular.estimate(&DataInput::U64(*k)).as_f64();
                let f = fast.estimate(&DataInput::U64(*k)).as_f64();
                assert_eq!(r, *c as f64, "{label} regular path, key {k}");
                assert_eq!(f, *c as f64, "{label} fast path, key {k}");
            }
        }};
    }

    #[test]
    fn both_paths_are_exact_on_a_collision_free_workload() {
        assert_both_paths_exact_without_collisions!(Vector2D<i32>);
        assert_both_paths_exact_without_collisions!(Vector2D<i64>);
        assert_both_paths_exact_without_collisions!(Vector2D<i128>);
        assert_both_paths_exact_without_collisions!(Vector2D<f64>);
        assert_both_paths_exact_without_collisions!(FixedMatrix);
        assert_both_paths_exact_without_collisions!(DefaultMatrixI32);
        assert_both_paths_exact_without_collisions!(QuickMatrixI64);
        assert_both_paths_exact_without_collisions!(QuickMatrixI128);
        assert_both_paths_exact_without_collisions!(DefaultMatrixI64);
        assert_both_paths_exact_without_collisions!(DefaultMatrixI128);
    }

    /// A width that is not a power of two leaves the fast path's mask wider
    /// than `cols`, so the fold has to bring the index back in range. Summing
    /// every cell pins that it lands inside the matrix exactly once per row:
    /// an out-of-range index would panic, a dropped one would lower the total.
    #[test]
    fn a_non_power_of_two_width_keeps_every_column_index_inside_the_matrix() {
        const N: u64 = 20_000;
        for &cols in &NON_POWER_OF_TWO_WIDTHS {
            assert!(
                !cols.is_power_of_two(),
                "the width axis must stay off the power-of-two grid, got {cols}"
            );
            let mut fast = CountMin::<Vector2D<i64>, FastPath>::with_dimensions(3, cols);
            for k in 0..N {
                fast.insert(&DataInput::U64(k.wrapping_mul(0x9E37_79B9_7F4A_7C15)));
            }
            assert_eq!(fast.cols(), cols, "reported width");
            let total: i64 = (0..fast.rows())
                .flat_map(|row| (0..cols).map(move |col| (row, col)))
                .map(|(row, col)| fast.as_storage().query_one_counter(row, col))
                .sum();
            assert_eq!(
                total,
                N as i64 * fast.rows() as i64,
                "w={cols}: every insert must land in exactly one counter per row"
            );
        }
    }

    /// The only thing the three counter widths actually promise differently is
    /// how much mass a single cell can hold. Each width must carry a count the
    /// next smaller one cannot, and the estimate must come back intact rather
    /// than wrapped.
    #[test]
    fn counter_widths_carry_the_mass_their_type_allows() {
        let key = DataInput::U64(0xFEED_FACE);

        // i32: just under the signed 32-bit ceiling.
        let mut cm32 = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(3, 64);
        let near_i32 = i32::MAX - 1;
        cm32.insert_many(&key, near_i32);
        assert_eq!(
            cm32.estimate(&key),
            near_i32,
            "i32 counters must hold {near_i32} without wrapping"
        );

        // i64: a count an i32 cell could not represent at all.
        let mut cm64 = CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(3, 64);
        let beyond_i32 = i32::MAX as i64 * 4;
        cm64.insert_many(&key, beyond_i32);
        assert_eq!(
            cm64.estimate(&key),
            beyond_i32,
            "i64 counters must hold {beyond_i32}, which overflows i32"
        );

        // i128: a count an i64 cell could not represent.
        let mut cm128 = CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(3, 64);
        let beyond_i64 = i64::MAX as i128 * 4;
        cm128.insert_many(&key, beyond_i64);
        assert_eq!(
            cm128.estimate(&key),
            beyond_i64,
            "i128 counters must hold {beyond_i64}, which overflows i64"
        );

        // Merging two sketches each holding half the i64 range must not wrap
        // the i128 target either.
        let mut a = CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(3, 64);
        let mut b = CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(3, 64);
        a.insert_many(&key, i64::MAX as i128);
        b.insert_many(&key, i64::MAX as i128);
        a.merge(&b);
        assert_eq!(
            a.estimate(&key),
            i64::MAX as i128 * 2,
            "merging two i64::MAX counts into i128 storage must not wrap"
        );
    }

    #[test]
    fn merge_max_dominates_both_sides_on_disjoint_key_sets() {
        let mut left = CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(4, 2_048);
        let mut right = CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(4, 2_048);
        let mut left_truth = HashMap::<u64, i64>::new();
        let mut right_truth = HashMap::<u64, i64>::new();
        for k in sample_zipf_u64(4_096, 1.1, 20_000, 0x10BE_C700) {
            if k % 2 == 0 {
                left.insert(&DataInput::U64(k));
                *left_truth.entry(k).or_insert(0) += 1;
            } else {
                right.insert(&DataInput::U64(k));
                *right_truth.entry(k).or_insert(0) += 1;
            }
        }

        let estimates = |sketch: &CountMin<Vector2D<i64>, RegularPath>,
                         truth: &HashMap<u64, i64>| {
            truth
                .keys()
                .map(|key| (*key, sketch.estimate(&DataInput::U64(*key))))
                .collect::<Vec<_>>()
        };
        let left_before = estimates(&left, &left_truth);
        let right_before = estimates(&right, &right_truth);

        let mut merged = left.clone();
        merged.merge_max(&right);

        for (truth, before) in [(&left_truth, &left_before), (&right_truth, &right_before)] {
            for (key, was) in before {
                let after = merged.estimate(&DataInput::U64(*key));
                assert!(
                    after >= *was,
                    "key {key}: merge_max lowered the estimate from {was} to {after}"
                );
                assert!(
                    after >= truth[key],
                    "key {key}: merge_max underestimated the true count {}",
                    truth[key]
                );
            }
        }

        // Elementwise max can never exceed elementwise sum on non-negative
        // counters, so the max-merged sketch is the tighter of the two.
        let summed = {
            let mut s = left.clone();
            s.merge(&right);
            s
        };
        for key in left_truth.keys() {
            let by_max = merged.estimate(&DataInput::U64(*key));
            let by_sum = summed.estimate(&DataInput::U64(*key));
            assert!(
                by_max <= by_sum,
                "key {key}: elementwise max {by_max} exceeded elementwise sum {by_sum}"
            );
        }
    }

    #[test]
    fn merge_max_is_idempotent_and_absorbs_an_empty_sketch() {
        let mut sketch = CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(4, 2_048);
        let stream = sample_zipf_u64(4_096, 1.1, 20_000, 0x10BE_C701);
        for k in &stream {
            sketch.insert(&DataInput::U64(*k));
        }
        let keys: HashSet<u64> = stream.iter().copied().collect();
        let baseline: Vec<(u64, i64)> = keys
            .iter()
            .map(|key| (*key, sketch.estimate(&DataInput::U64(*key))))
            .collect();

        let empty = CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(4, 2_048);
        let mut with_empty = sketch.clone();
        with_empty.merge_max(&empty);
        let mut with_self = sketch.clone();
        let twin = sketch.clone();
        with_self.merge_max(&twin);

        for (key, before) in baseline {
            assert_eq!(
                with_empty.estimate(&DataInput::U64(key)),
                before,
                "key {key}: merging an empty sketch by max moved the estimate"
            );
            assert_eq!(
                with_self.estimate(&DataInput::U64(key)),
                before,
                "key {key}: merge_max with an identical sketch is not idempotent"
            );
        }
    }

    /// The precomputed-hash entry points are a bypass around `insert`, so they
    /// have to reach the same cells it does — exactly, on every key.
    #[test]
    fn precomputed_hash_entry_points_match_the_value_entry_points() {
        const ROWS: usize = 4;
        const COLS: usize = 2_048;
        let stream = sample_zipf_u64(512, 1.1, 4_000, 0x10BE_C702);

        let mut by_value = CountMin::<Vector2D<i64>, FastPath>::with_dimensions(ROWS, COLS);
        let mut by_hash = CountMin::<Vector2D<i64>, FastPath>::with_dimensions(ROWS, COLS);
        let mut by_bulk_hash = CountMin::<Vector2D<i64>, FastPath>::with_dimensions(ROWS, COLS);
        let hashes: Vec<_> = stream
            .iter()
            .map(|k| hash_for_matrix(ROWS, COLS, &DataInput::U64(*k)))
            .collect();
        for k in &stream {
            by_value.insert(&DataInput::U64(*k));
        }
        for h in &hashes {
            by_hash.fast_insert_with_hash_value(h);
        }
        by_bulk_hash.bulk_insert_with_hashes(&hashes);

        for key in stream.iter().collect::<HashSet<_>>() {
            let probe = DataInput::U64(*key);
            let hashed = hash_for_matrix(ROWS, COLS, &probe);
            let expected = by_value.estimate(&probe);
            assert_eq!(
                by_hash.estimate(&probe),
                expected,
                "key {key}: fast_insert_with_hash_value diverged from insert"
            );
            assert_eq!(
                by_bulk_hash.estimate(&probe),
                expected,
                "key {key}: bulk_insert_with_hashes diverged from insert"
            );
            assert_eq!(
                by_value.fast_estimate_with_hash(&hashed),
                expected,
                "key {key}: fast_estimate_with_hash diverged from estimate"
            );
        }
    }

    #[test]
    fn weighted_batch_entry_points_match_a_loop_of_single_inserts() {
        const ROWS: usize = 4;
        const COLS: usize = 2_048;
        let stream = sample_zipf_u64(512, 1.1, 4_000, 0x10BE_C703);

        let mut by_loop = CountMin::<Vector2D<i64>, FastPath>::with_dimensions(ROWS, COLS);
        let mut by_bulk_many = CountMin::<Vector2D<i64>, FastPath>::with_dimensions(ROWS, COLS);
        let mut by_hashed_many = CountMin::<Vector2D<i64>, FastPath>::with_dimensions(ROWS, COLS);

        let weighted: Vec<(DataInput, i64)> = stream
            .iter()
            .enumerate()
            .map(|(i, k)| (DataInput::U64(*k), (i % 5) as i64 + 1))
            .collect();
        let hashed_weighted: Vec<_> = stream
            .iter()
            .enumerate()
            .map(|(i, k)| {
                (
                    hash_for_matrix(ROWS, COLS, &DataInput::U64(*k)),
                    (i % 5) as i64 + 1,
                )
            })
            .collect();

        for (value, many) in &weighted {
            by_loop.insert_many(value, *many);
        }
        by_bulk_many.bulk_insert_many(&weighted);
        by_hashed_many.bulk_insert_many_with_hashes(&hashed_weighted);

        for key in stream.iter().collect::<HashSet<_>>() {
            let probe = DataInput::U64(*key);
            let expected = by_loop.estimate(&probe);
            assert_eq!(
                by_bulk_many.estimate(&probe),
                expected,
                "key {key}: bulk_insert_many diverged from a loop of insert_many"
            );
            assert_eq!(
                by_hashed_many.estimate(&probe),
                expected,
                "key {key}: bulk_insert_many_with_hashes diverged from a loop of insert_many"
            );
        }

        let mut single = CountMin::<Vector2D<i64>, FastPath>::with_dimensions(ROWS, COLS);
        let hashed = hash_for_matrix(ROWS, COLS, &DataInput::U64(7));
        single.fast_insert_many_with_hash_value(&hashed, 9);
        assert_eq!(
            single.estimate(&DataInput::U64(7)),
            9,
            "fast_insert_many_with_hash_value must apply the whole weight"
        );
    }
}
