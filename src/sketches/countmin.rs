//! Count-Min Sketch implementation.
//!
//! A sub-linear space data structure for estimating frequencies of items in a
//! stream, with one-sided error bounded by the L1 norm of the stream.
//!
//! Reference:
//! - Cormode & Muthukrishnan, "An Improved Data Stream Summary: The Count-Min
//!   Sketch and its Applications," J. Algorithms 55(1), 2005.
//!   <https://www.cs.rutgers.edu/~muthu/cm-jal.pdf>

use rmp_serde::{
    decode::Error as RmpDecodeError, encode::Error as RmpEncodeError, from_slice, to_vec_named,
};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use crate::octo_delta::{CM_PROMASK, CmDelta};
use crate::{
    DataInput, DefaultMatrixI32, DefaultMatrixI64, DefaultMatrixI128, DefaultXxHasher, FastPath,
    FastPathHasher, FixedMatrix, MatrixFastHash, MatrixStorage, NitroTarget, QuickMatrixI64,
    QuickMatrixI128, RegularPath, SketchHasher, Vector2D, hash64_seeded,
};

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
}

// Serialization helpers for CountMin.
impl<S: MatrixStorage + Serialize, Mode, H: SketchHasher> CountMin<S, Mode, H> {
    /// Serializes the sketch into MessagePack bytes.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        to_vec_named(self)
    }
}

impl<S: MatrixStorage + for<'de> Deserialize<'de>, Mode, H: SketchHasher> CountMin<S, Mode, H> {
    /// Deserializes a sketch from MessagePack bytes.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        from_slice(bytes)
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
        let mut min = S::Counter::from(i32::MAX);
        for r in 0..rows {
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
    /// Inserts an observation and emits a delta when the counter crosses a threshold.
    #[inline(always)]
    pub fn insert_emit_delta(&mut self, value: &DataInput, emit: &mut impl FnMut(CmDelta)) {
        let rows = self.counts.rows();
        let cols = self.counts.cols();
        for r in 0..rows {
            let hashed = hash64_seeded(r, value);
            let col = ((hashed & LOWER_32_MASK) as usize) % cols;
            self.counts.increment_by_row(r, col, 1);
            let current = self.counts.query_one_counter(r, col);
            if current % CM_PROMASK as i32 == 0 {
                emit(CmDelta {
                    row: r as u16,
                    col: col as u16,
                    value: CM_PROMASK,
                });
            }
        }
    }
}

impl<S, H: SketchHasher> CountMin<S, FastPath, H>
where
    S: MatrixStorage<Counter = i32> + FastPathHasher<H>,
{
    /// Inserts an observation via fast-path and emits a delta at threshold crossings.
    #[inline(always)]
    pub fn insert_emit_delta(&mut self, value: &DataInput, emit: &mut impl FnMut(CmDelta)) {
        let hashed_val = <S as FastPathHasher<H>>::hash_for_matrix(&self.counts, value);
        let rows = self.counts.rows();
        let cols = self.counts.cols();
        for r in 0..rows {
            let col = hashed_val.col_for_row(r, cols);
            self.counts.increment_by_row(r, col, 1);
            let current = self.counts.query_one_counter(r, col);
            if current % CM_PROMASK as i32 == 0 {
                emit(CmDelta {
                    row: r as u16,
                    col: col as u16,
                    value: CM_PROMASK,
                });
            }
        }
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

    /// Disables Nitro sampling and resets its internal state.
    pub fn disable_nitro(&mut self) {
        self.counts.disable_nitro();
    }

    /// Inserts an observation using Nitro-aware sampling logic.
    #[inline(always)]
    pub fn fast_insert_nitro(&mut self, value: &DataInput) {
        let rows = self.counts.rows();
        let delta = self.counts.nitro().delta as i32;
        if self.counts.nitro().to_skip >= rows {
            self.counts.reduce_nitro_skip(rows);
        } else {
            let hashed = H::hash128_seeded(0, value);
            let r = self.counts.nitro().to_skip;
            self.counts.update_by_row(r, hashed, |a, b| *a += b, delta);
            self.counts.nitro_mut().draw_geometric();
            let temp = self.counts.get_nitro_skip();
            self.counts.update_nitro_skip((r + temp + 1) - rows);
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
        self.counts
            .update_by_row(row, hashed, |a, b| *a += b, delta as i32);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        all_counter_zero_i32, counter_index, sample_uniform_f64, sample_zipf_u64,
    };
    use crate::{DataInput, hash64_seeded};
    use core::f64;
    use std::collections::HashMap;

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

    fn run_zipf_stream(
        rows: usize,
        cols: usize,
        domain: usize,
        exponent: f64,
        samples: usize,
        seed: u64,
    ) -> (CountMin<Vector2D<i32>, RegularPath>, HashMap<u64, i32>) {
        let mut truth = HashMap::<u64, i32>::new();
        let mut sketch = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(rows, cols);

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
        domain: usize,
        exponent: f64,
        samples: usize,
        seed: u64,
    ) -> (CountMin<Vector2D<i32>, FastPath>, HashMap<u64, i32>) {
        let mut truth = HashMap::<u64, i32>::new();
        let mut sketch = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(rows, cols);

        for value in sample_zipf_u64(domain, exponent, samples, seed) {
            let key = DataInput::U64(value);
            sketch.insert(&key);
            *truth.entry(value).or_insert(0) += 1;
        }

        (sketch, truth)
    }

    fn run_uniform_stream(
        rows: usize,
        cols: usize,
        min: f64,
        max: f64,
        samples: usize,
        seed: u64,
    ) -> (CountMin<Vector2D<i32>, RegularPath>, HashMap<u64, i32>) {
        let mut truth = HashMap::<u64, i32>::new();
        let mut sketch = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(rows, cols);

        for value in sample_uniform_f64(min, max, samples, seed) {
            let key = DataInput::F64(value);
            sketch.insert(&key);
            *truth.entry(value.to_bits()).or_insert(0) += 1;
        }

        (sketch, truth)
    }

    fn run_uniform_stream_fast(
        rows: usize,
        cols: usize,
        min: f64,
        max: f64,
        samples: usize,
        seed: u64,
    ) -> (CountMin<Vector2D<i32>, FastPath>, HashMap<u64, i32>) {
        let mut truth = HashMap::<u64, i32>::new();
        let mut sketch = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(rows, cols);

        for value in sample_uniform_f64(min, max, samples, seed) {
            let key = DataInput::F64(value);
            sketch.insert(&key);
            *truth.entry(value.to_bits()).or_insert(0) += 1;
        }

        (sketch, truth)
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

    // test for zipf distribution for domain 8192 and exponent 1.1 with 200_000 items
    // verify: (1-delta)*(query_size) is within bound (epsilon*input_size)
    #[test]
    fn cm_error_bound_zipf() {
        // regular path
        let (sk, truth) = run_zipf_stream(
            DEFAULT_ROW_NUM,
            DEFAULT_COL_NUM,
            8192,
            1.1,
            200_000,
            0x5eed_c0de,
        );
        let epsilon = std::f64::consts::E / DEFAULT_COL_NUM as f64;
        let delta = 1.0 / std::f64::consts::E.powi(DEFAULT_ROW_NUM as i32);
        let error_bound = epsilon * 200_000_f64;
        let keys = truth.keys();
        let correct_lower_bound = keys.len() as f64 * (1.0 - delta);
        let mut within_count = 0;
        for key in keys {
            let est = sk.estimate(&DataInput::U64(*key));
            if (est.abs_diff(*truth.get(key).unwrap()) as f64) < error_bound {
                within_count += 1;
            }
        }
        assert!(
            within_count as f64 > correct_lower_bound,
            "in-bound items number {within_count} not greater than expected amount {correct_lower_bound}"
        );
        // fast path
        let (sk, truth) = run_zipf_stream_fast(
            DEFAULT_ROW_NUM,
            DEFAULT_COL_NUM,
            8192,
            1.1,
            200_000,
            0x5eed_c0de,
        );
        let epsilon = std::f64::consts::E / DEFAULT_COL_NUM as f64;
        let delta = 1.0 / std::f64::consts::E.powi(DEFAULT_ROW_NUM as i32);
        let error_bound = epsilon * 200_000_f64;
        let keys = truth.keys();
        let correct_lower_bound = keys.len() as f64 * (1.0 - delta);
        let mut within_count = 0;
        for key in keys {
            let est = sk.estimate(&DataInput::U64(*key));
            if (est.abs_diff(*truth.get(key).unwrap()) as f64) < error_bound {
                within_count += 1;
            }
        }
        assert!(
            within_count as f64 > correct_lower_bound,
            "in-bound items number {within_count} not greater than expected amount {correct_lower_bound}"
        );
    }

    // test for uniform distribution from 100.0 to 1000.0 with 200_000 items
    // verify: (1-delta)*(query_size) is within bound (epsilon*input_size)
    #[test]
    fn cm_error_bound_uniform() {
        // regular path
        let (sk, truth) = run_uniform_stream(
            DEFAULT_ROW_NUM,
            DEFAULT_COL_NUM,
            100.0,
            1000.0,
            200_000,
            0x5eed_c0de,
        );
        let epsilon = std::f64::consts::E / DEFAULT_COL_NUM as f64;
        let delta = 1.0 / std::f64::consts::E.powi(DEFAULT_ROW_NUM as i32);
        let error_bound = epsilon * 200_000_f64;
        let keys = truth.keys();
        let correct_lower_bound = keys.len() as f64 * (1.0 - delta);
        let mut within_count = 0;
        for key in keys {
            let est = sk.estimate(&DataInput::U64(*key));
            if (est.abs_diff(*truth.get(key).unwrap()) as f64) < error_bound {
                within_count += 1;
            }
        }
        assert!(
            within_count as f64 > correct_lower_bound,
            "in-bound items number {within_count} not greater than expected amount {correct_lower_bound}"
        );
        // fast path
        let (sk, truth) = run_uniform_stream_fast(
            DEFAULT_ROW_NUM,
            DEFAULT_COL_NUM,
            100.0,
            1000.0,
            200_000,
            0x5eed_c0de,
        );
        let epsilon = std::f64::consts::E / DEFAULT_COL_NUM as f64;
        let delta = 1.0 / std::f64::consts::E.powi(DEFAULT_ROW_NUM as i32);
        let error_bound = epsilon * 200_000_f64;
        let keys = truth.keys();
        let correct_lower_bound = keys.len() as f64 * (1.0 - delta);
        let mut within_count = 0;
        for key in keys {
            let est = sk.estimate(&DataInput::U64(*key));
            if (est.abs_diff(*truth.get(key).unwrap()) as f64) < error_bound {
                within_count += 1;
            }
        }
        assert!(
            within_count as f64 > correct_lower_bound,
            "in-bound items number {within_count} not greater than expected amount {correct_lower_bound}"
        );
    }

    #[test]
    fn count_min_round_trip_serialization() {
        let mut sketch = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(3, 8);
        sketch.insert(&DataInput::U64(42));
        sketch.insert(&DataInput::U64(7));

        let encoded = sketch.serialize_to_bytes().expect("serialize CountMin");
        assert!(!encoded.is_empty());
        let data_copied = encoded.clone();

        let decoded = CountMin::<Vector2D<i32>, RegularPath>::deserialize_from_bytes(&data_copied)
            .expect("deserialize CountMin");

        assert_eq!(sketch.rows(), decoded.rows());
        assert_eq!(sketch.cols(), decoded.cols());
        assert_eq!(
            sketch.as_storage().as_slice(),
            decoded.as_storage().as_slice()
        );
    }
}

// =====================================================================
// asap_sketchlib wire-format-aligned variant.
//
// `CountMinSketch` and `CountMinDelta` below are the public-field,
// proto-decode-friendly types consumed by the ASAP query engine
// accumulators, backed by `asap_sketchlib`'s in-tree CountMin. The
// high-throughput in-process variant above (`CountMin`) keeps its
// original design.
// =====================================================================

// (de-duplicated) use serde::{Deserialize, Serialize};

// ----- asap_sketchlib-backed Count-Min helpers -----
// Used below by `CountMinSketch`. Lives in this file so the wire-format
// type and its backend share a single home.

/// Concrete Count-Min type backing the wire-format `CountMinSketch`.
/// Uses f64 counters (`Vector2D<f64>`) for weighted updates without integer rounding.
pub type SketchlibCms = CountMin<Vector2D<f64>, RegularPath>;

/// Creates a fresh sketchlib Count-Min sketch with the given dimensions.
pub fn new_sketchlib_cms(row_num: usize, col_num: usize) -> SketchlibCms {
    SketchlibCms::with_dimensions(row_num, col_num)
}

/// Builds a sketchlib Count-Min sketch from an existing `sketch` matrix.
pub fn sketchlib_cms_from_matrix(
    row_num: usize,
    col_num: usize,
    sketch: &[Vec<f64>],
) -> SketchlibCms {
    let matrix = Vector2D::from_fn(row_num, col_num, |r, c| {
        sketch
            .get(r)
            .and_then(|row| row.get(c))
            .copied()
            .unwrap_or(0.0)
    });
    SketchlibCms::from_storage(matrix)
}

/// Converts a sketchlib Count-Min sketch into a `Vec<Vec<f64>>` matrix.
pub fn matrix_from_sketchlib_cms(inner: &SketchlibCms) -> Vec<Vec<f64>> {
    let storage: &Vector2D<f64> = inner.as_storage();
    let rows = storage.rows();
    let cols = storage.cols();
    let mut sketch = vec![vec![0.0; cols]; rows];

    for (r, row) in sketch.iter_mut().enumerate().take(rows) {
        for (c, cell) in row.iter_mut().enumerate().take(cols) {
            if let Some(v) = storage.get(r, c) {
                *cell = *v;
            }
        }
    }

    sketch
}

/// Helper to update a sketchlib Count-Min with a weighted key.
pub fn sketchlib_cms_update(inner: &mut SketchlibCms, key: &str, value: f64) {
    if value <= 0.0 {
        return;
    }
    let input = DataInput::String(key.to_owned());
    inner.insert_many(&input, value);
}

/// Helper to query a sketchlib Count-Min for a key, returning f64.
pub fn sketchlib_cms_query(inner: &SketchlibCms, key: &str) -> f64 {
    let input = DataInput::String(key.to_owned());
    inner.estimate(&input)
}

#[derive(Serialize, Deserialize)]
struct WireFormat {
    sketch: Vec<Vec<f64>>,
    #[serde(rename = "row_num")]
    rows: usize,
    #[serde(rename = "col_num")]
    cols: usize,
}

/// Sparse delta between two consecutive CountMinSketch snapshots —
/// the input shape for [`CountMinSketch::apply_delta`]. Mirrors the
/// `CountMinSketchDelta` proto in
/// `sketchlib-go/proto/countminsketch/countminsketch.proto` (packed
/// encoding only).
///
/// Cells apply additively: `matrix[row][col] += d_count`. Per-row
/// L1 and L2 norm deltas are carried for downstream error-accounting
/// but are not consumed by `apply_delta` itself.
#[derive(Debug, Clone, Default)]
pub struct CountMinSketchDelta {
    pub rows: u32,
    pub cols: u32,
    pub cells: Vec<(u32, u32, i64)>,
    pub l1: Vec<f64>,
    pub l2: Vec<f64>,
}

/// Provides approximate frequency counts with error bounds.
/// The msgpack wire format is the contract between sketch producers and
/// the query engine consumer.
#[derive(Debug, Clone)]
pub struct CountMinSketch {
    pub rows: usize,
    pub cols: usize,
    pub(crate) backend: SketchlibCms,
}

impl CountMinSketch {
    pub fn new(rows: usize, cols: usize) -> Self {
        Self {
            rows,
            cols,
            backend: new_sketchlib_cms(rows, cols),
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

    /// Returns the sketch matrix (for wire format, serialization, tests).
    pub fn sketch(&self) -> Vec<Vec<f64>> {
        matrix_from_sketchlib_cms(&self.backend)
    }

    /// Construct from a `Vec<Vec<f64>>` matrix (used by deserialization and query engine).
    pub fn from_legacy_matrix(sketch: Vec<Vec<f64>>, rows: usize, cols: usize) -> Self {
        Self {
            rows,
            cols,
            backend: sketchlib_cms_from_matrix(rows, cols, &sketch),
        }
    }

    pub fn update(&mut self, key: &str, value: f64) {
        sketchlib_cms_update(&mut self.backend, key, value);
    }

    /// Estimate the frequency of `key` (CountMin point query).
    pub fn estimate(&self, key: &str) -> f64 {
        sketchlib_cms_query(&self.backend, key)
    }

    /// Merge another CountMinSketch into self in place. Both operands
    /// must have identical dimensions.
    pub fn merge(
        &mut self,
        other: &CountMinSketch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.rows != other.rows || self.cols != other.cols {
            return Err(format!(
                "CountMinSketch dimension mismatch: self={}x{}, other={}x{}",
                self.rows, self.cols, other.rows, other.cols
            )
            .into());
        }
        self.backend.merge(&other.backend);
        Ok(())
    }

    /// Merge from references, allocating only the output — no input clones.
    pub fn merge_refs(
        accumulators: &[&Self],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        let rows = accumulators[0].rows;
        let cols = accumulators[0].cols;

        for acc in accumulators {
            if acc.rows != rows || acc.cols != cols {
                return Err(
                    "Cannot merge CountMinSketch accumulators with different dimensions".into(),
                );
            }
        }

        let mut merged = CountMinSketch::new(rows, cols);
        for acc in accumulators {
            merged.backend.merge(&acc.backend);
        }
        Ok(merged)
    }

    /// Apply a sparse delta in place. Matches the `ApplyDelta`
    /// semantics in `sketchlib-go/sketches/CountMinSketch/delta.go`:
    /// `matrix[row][col] += d_count` for each cell in the delta.
    ///
    /// The FFI handle is opaque, so we snapshot the matrix, apply
    /// cell updates, and rebuild the backend. The rebuild is
    /// O(rows × cols) per delta and is acceptable for ingest-side
    /// reconstitution — no delta should fire more than once per
    /// window (10s–300s in the paper's B3 / B4 configs).
    pub fn apply_delta(
        &mut self,
        delta: &CountMinSketchDelta,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (row, col, _) in &delta.cells {
            let r = *row as usize;
            let c = *col as usize;
            if r >= self.rows || c >= self.cols {
                return Err(format!(
                    "CountMinSketchDelta cell ({r},{c}) out of range (matrix={}x{})",
                    self.rows, self.cols
                )
                .into());
            }
        }
        let mut matrix = self.sketch();
        for (row, col, d_count) in &delta.cells {
            matrix[*row as usize][*col as usize] += *d_count as f64;
        }
        self.backend = sketchlib_cms_from_matrix(self.rows, self.cols, &matrix);
        Ok(())
    }

    /// Serialize to MessagePack — matches the wire format exactly.
    pub fn serialize_msgpack(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        let sketch = self.sketch();
        let wire = WireFormat {
            sketch,
            rows: self.rows,
            cols: self.cols,
        };

        let mut buf = Vec::new();
        wire.serialize(&mut rmp_serde::Serializer::new(&mut buf))?;
        Ok(buf)
    }

    /// Deserialize from MessagePack.
    pub fn deserialize_msgpack(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let wire: WireFormat = rmp_serde::from_slice(buffer).map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("Failed to deserialize CountMinSketch from MessagePack: {e}").into()
            },
        )?;

        let backend = sketchlib_cms_from_matrix(wire.rows, wire.cols, &wire.sketch);

        Ok(Self {
            rows: wire.rows,
            cols: wire.cols,
            backend,
        })
    }

    /// One-shot aggregation: build a sketch from parallel key/value slices
    /// and return the msgpack bytes.
    pub fn aggregate_count(
        depth: usize,
        width: usize,
        keys: &[&str],
        values: &[f64],
    ) -> Option<Vec<u8>> {
        if keys.is_empty() {
            return None;
        }
        let mut sketch = Self::new(depth, width);
        for (key, &value) in keys.iter().zip(values.iter()) {
            sketch.update(key, value);
        }
        sketch.serialize_msgpack().ok()
    }

    /// Same as aggregate_count — CMS accumulates sums by construction.
    pub fn aggregate_sum(
        depth: usize,
        width: usize,
        keys: &[&str],
        values: &[f64],
    ) -> Option<Vec<u8>> {
        Self::aggregate_count(depth, width, keys, values)
    }
}

#[cfg(test)]
mod tests_wire_countmin {
    use super::*;

    #[test]
    fn test_count_min_sketch_creation() {
        let cms = CountMinSketch::new(4, 1000);
        assert_eq!(cms.rows, 4);
        assert_eq!(cms.cols, 1000);
        let sketch = cms.sketch();
        assert_eq!(sketch.len(), 4);
        assert_eq!(sketch[0].len(), 1000);

        // Check all values are initialized to 0
        for row in &sketch {
            for &value in row {
                assert_eq!(value, 0.0);
            }
        }
    }

    #[test]
    fn test_count_min_sketch_update() {
        let mut cms = CountMinSketch::new(2, 10);
        cms.update("key1", 1.0);
        // Query should return at least the updated value
        let result = cms.estimate("key1");
        assert!(result >= 1.0);
    }

    #[test]
    fn test_count_min_sketch_query_empty() {
        let cms = CountMinSketch::new(2, 10);
        assert_eq!(cms.estimate("anything"), 0.0);
    }

    #[test]
    fn test_count_min_sketch_merge() {
        // Use from_legacy_matrix so the test works regardless of sketchlib/legacy config
        let mut sketch1 = vec![vec![0.0; 3]; 2];
        sketch1[0][0] = 5.0;
        sketch1[1][2] = 10.0;
        let mut cms1 = CountMinSketch::from_legacy_matrix(sketch1, 2, 3);

        let mut sketch2 = vec![vec![0.0; 3]; 2];
        sketch2[0][0] = 3.0;
        sketch2[0][1] = 7.0;
        let cms2 = CountMinSketch::from_legacy_matrix(sketch2, 2, 3);

        cms1.merge(&cms2).unwrap();
        let merged_sketch = cms1.sketch();

        assert_eq!(merged_sketch[0][0], 8.0); // 5 + 3
        assert_eq!(merged_sketch[0][1], 7.0); // 0 + 7
        assert_eq!(merged_sketch[1][2], 10.0); // 10 + 0
    }

    #[test]
    fn test_count_min_sketch_merge_dimension_mismatch() {
        let mut cms1 = CountMinSketch::new(2, 3);
        let cms2 = CountMinSketch::new(3, 3);
        assert!(cms1.merge(&cms2).is_err());
    }

    #[test]
    fn test_count_min_sketch_msgpack_round_trip() {
        let mut cms = CountMinSketch::new(4, 256);
        cms.update("apple", 5.0);
        cms.update("banana", 3.0);
        cms.update("apple", 2.0); // total "apple" = 7

        let bytes = cms.serialize_msgpack().unwrap();
        let deserialized = CountMinSketch::deserialize_msgpack(&bytes).unwrap();

        assert_eq!(deserialized.rows, 4);
        assert_eq!(deserialized.cols, 256);
        assert!(deserialized.estimate("apple") >= 7.0);
        assert!(deserialized.estimate("banana") >= 3.0);
    }

    #[test]
    fn test_aggregate_count() {
        let keys = ["a", "b", "a"];
        let values = [1.0, 2.0, 3.0];
        let bytes = CountMinSketch::aggregate_count(4, 100, &keys, &values).unwrap();
        let cms = CountMinSketch::deserialize_msgpack(&bytes).unwrap();
        // "a" was updated twice (1.0 + 3.0 = 4.0), "b" once (2.0)
        assert!(cms.estimate("a") >= 4.0);
        assert!(cms.estimate("b") >= 2.0);
    }

    #[test]
    fn test_aggregate_count_empty() {
        assert!(CountMinSketch::aggregate_count(4, 100, &[], &[]).is_none());
    }

    #[test]
    fn test_apply_delta_additive() {
        let mut cms = CountMinSketch::from_legacy_matrix(
            vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]],
            2,
            3,
        );
        let delta = CountMinSketchDelta {
            rows: 2,
            cols: 3,
            cells: vec![(0, 0, 10), (1, 2, 100)],
            l1: vec![],
            l2: vec![],
        };
        cms.apply_delta(&delta).unwrap();
        assert_eq!(
            cms.sketch(),
            vec![vec![11.0, 2.0, 3.0], vec![4.0, 5.0, 106.0]]
        );
    }

    #[test]
    fn test_apply_delta_matches_full_merge() {
        let base = CountMinSketch::from_legacy_matrix(vec![vec![1.0, 2.0], vec![3.0, 4.0]], 2, 2);
        let addition =
            CountMinSketch::from_legacy_matrix(vec![vec![10.0, 0.0], vec![0.0, 20.0]], 2, 2);
        let mut via_merge = base.clone();
        via_merge.merge(&addition).unwrap();

        let delta = CountMinSketchDelta {
            rows: 2,
            cols: 2,
            cells: vec![(0, 0, 10), (1, 1, 20)],
            l1: vec![],
            l2: vec![],
        };
        let mut via_delta = base;
        via_delta.apply_delta(&delta).unwrap();
        assert_eq!(via_delta.sketch(), via_merge.sketch());
    }

    #[test]
    fn test_apply_delta_out_of_range() {
        let mut cms = CountMinSketch::new(2, 3);
        let delta = CountMinSketchDelta {
            rows: 2,
            cols: 3,
            cells: vec![(5, 0, 1)],
            l1: vec![],
            l2: vec![],
        };
        assert!(cms.apply_delta(&delta).is_err());
    }
}
