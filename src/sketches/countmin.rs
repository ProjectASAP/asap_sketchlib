use rmp_serde::{
    decode::Error as RmpDecodeError, encode::Error as RmpEncodeError, from_slice, to_vec_named,
};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use crate::FastPathHasher;
use crate::{
    DefaultMatrixI32, DefaultMatrixI64, DefaultMatrixI128, DefaultXxHasher, FastPath, FixedMatrix,
    MatrixHashType, MatrixStorage, NitroTarget, QuickMatrixI64, QuickMatrixI128, RegularPath,
    SketchHasher, SketchInput, Vector2D, hash64_seeded,
};

const DEFAULT_ROW_NUM: usize = 3;
const DEFAULT_COL_NUM: usize = 4096;
pub const QUICKSTART_ROW_NUM: usize = 5;
pub const QUICKSTART_COL_NUM: usize = 2048;
const LOWER_32_MASK: u64 = (1u64 << 32) - 1;

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

// SketchInput adapters for the regular Count-Min update rule.
// Regular-path CountMin operations. Uses PartialOrd to support both integer and f64 counters.
impl<S: MatrixStorage, H: SketchHasher> CountMin<S, RegularPath, H>
where
    S::Counter: Copy + PartialOrd + From<i32> + std::ops::AddAssign,
{
    /// Inserts an observation while using the standard Count-Min minimum row update rule.
    #[inline(always)]
    pub fn insert(&mut self, value: &SketchInput) {
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
    pub fn insert_many(&mut self, value: &SketchInput, many: S::Counter) {
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
    pub fn bulk_insert(&mut self, values: &[SketchInput]) {
        for value in values {
            self.insert(value);
        }
    }

    /// Inserts a batch of observations with per-item counts.
    #[inline(always)]
    pub fn bulk_insert_many(&mut self, values: &[(SketchInput, S::Counter)]) {
        for (value, many) in values {
            self.insert_many(value, *many);
        }
    }

    /// Returns the frequency estimate for the provided value.
    #[inline(always)]
    pub fn estimate(&self, value: &SketchInput) -> S::Counter {
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

// Fast-path hashing adapter for Vector2D.
impl<T> FastPathHasher for Vector2D<T>
where
    T: Copy + std::ops::AddAssign,
{
    #[inline(always)]
    fn hash_for_matrix(&self, value: &SketchInput) -> MatrixHashType {
        Vector2D::hash_for_matrix(self, value)
    }
}

// Fast-path hashing adapter for u64-backed storage.
impl<S> FastPathHasher for S
where
    S: MatrixStorage<HashValueType = u64>,
{
    #[inline(always)]
    fn hash_for_matrix(&self, value: &SketchInput) -> u64 {
        hash64_seeded(0, value)
    }
}

// SketchInput adapters for the fast-path Count-Min update rule.
// Fast-path CountMin operations using precomputed hashes. Uses PartialOrd for f64 support.
impl<S, H: SketchHasher> CountMin<S, FastPath, H>
where
    S: MatrixStorage + FastPathHasher,
    S::Counter: Copy + PartialOrd + From<i32> + std::ops::AddAssign,
{
    /// Inserts an observation using the combined hash optimization.
    #[inline(always)]
    pub fn insert(&mut self, value: &SketchInput) {
        let hashed_val = self.counts.hash_for_matrix(value);
        self.counts
            .fast_insert(|a, b, _| *a += *b, S::Counter::from(1), &hashed_val);
    }

    #[inline(always)]
    pub fn insert_many(&mut self, value: &SketchInput, many: S::Counter) {
        let hashed_val = self.counts.hash_for_matrix(value);
        self.counts
            .fast_insert(|a, b, _| *a += *b, many, &hashed_val);
    }

    /// Inserts a batch of observations using the fast-path hash.
    #[inline(always)]
    pub fn bulk_insert(&mut self, values: &[SketchInput]) {
        for value in values {
            self.insert(value);
        }
    }

    /// Inserts a batch of observations with per-item counts using the fast-path hash.
    #[inline(always)]
    pub fn bulk_insert_many(&mut self, values: &[(SketchInput, S::Counter)]) {
        for (value, many) in values {
            self.insert_many(value, *many);
        }
    }

    /// Returns the frequency estimate for the provided value.
    #[inline(always)]
    pub fn estimate(&self, value: &SketchInput) -> S::Counter {
        let hashed_val = self.counts.hash_for_matrix(value);
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
    pub fn fast_insert_with_hash_value(&mut self, hashed_val: &S::HashValueType) {
        self.counts
            .fast_insert(|a, b, _| *a += *b, S::Counter::from(1), hashed_val);
    }

    #[inline(always)]
    /// Inserts multiple observations using a pre-computed hash value.
    pub fn fast_insert_many_with_hash_value(
        &mut self,
        hashed_val: &S::HashValueType,
        many: S::Counter,
    ) {
        self.counts
            .fast_insert(|a, b, _| *a += *b, many, hashed_val);
    }

    /// Inserts a batch of observations using pre-computed hash values.
    #[inline(always)]
    pub fn bulk_insert_with_hashes(&mut self, hashes: &[S::HashValueType]) {
        for hashed_val in hashes {
            self.fast_insert_with_hash_value(hashed_val);
        }
    }

    /// Inserts a batch of observations with per-item counts using pre-computed hash values.
    #[inline(always)]
    pub fn bulk_insert_many_with_hashes(&mut self, hashes: &[(S::HashValueType, S::Counter)]) {
        for (hashed_val, many) in hashes {
            self.fast_insert_many_with_hash_value(hashed_val, *many);
        }
    }

    /// Returns the frequency estimate using a pre-computed hash value.
    #[inline(always)]
    pub fn fast_estimate_with_hash(&self, hashed_val: &S::HashValueType) -> S::Counter {
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
    pub fn fast_insert_nitro(&mut self, value: &SketchInput) {
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
    pub fn nitro_estimate(&self, value: &SketchInput) -> f64 {
        let hashed_val = self.counts.hash_for_matrix(value);
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
    use crate::SketchInput;
    use crate::test_utils::{
        all_counter_zero_i32, counter_index, sample_uniform_f64, sample_zipf_u64,
    };
    use core::f64;
    use std::collections::HashMap;

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
            let key = SketchInput::U64(value);
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
            let key = SketchInput::U64(value);
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
            let key = SketchInput::F64(value);
            sketch.insert(&key);
            *truth.entry(value.to_bits() as u64).or_insert(0) += 1;
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
            let key = SketchInput::F64(value);
            sketch.insert(&key);
            *truth.entry(value.to_bits() as u64).or_insert(0) += 1;
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
            SketchInput::Str("alpha"),
            SketchInput::Str("beta"),
            SketchInput::Str("gamma"),
            SketchInput::Str("delta"),
            SketchInput::Str("epsilon"),
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
        let key = SketchInput::Str("delta");

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
            sk.insert(&SketchInput::I32(i));
        }

        // Build the expected counter array by mirroring the regular-path hashing logic.
        let storage = sk.as_storage();
        let rows = storage.rows();
        let cols = storage.cols();
        let mut expected_once = vec![0_i32; rows * cols];
        for i in 0..10 {
            let value = SketchInput::I32(i);
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
            sk.insert(&SketchInput::I32(i));
        }
        let expected_twice: Vec<i32> = expected_once.iter().map(|v| v * 2).collect();
        assert_eq!(sk.as_storage().as_slice(), expected_twice.as_slice());

        // Estimates for inserted keys should be exactly 2.
        for i in 0..10 {
            assert_eq!(
                sk.estimate(&SketchInput::I32(i)),
                2,
                "estimate for {i} should be 2, but get {}",
                sk.estimate(&SketchInput::I32(i))
            )
        }
    }

    #[test]
    fn cm_fast_path_correctness() {
        let mut sk = CountMin::<Vector2D<i32>, FastPath>::default();
        for i in 0..10 {
            sk.insert(&SketchInput::I32(i));
        }

        let storage = sk.as_storage();
        let rows = storage.rows();
        let cols = storage.cols();
        let mask_bits = storage.get_mask_bits();
        let mask = (1u64 << mask_bits) - 1;
        let mut expected_once = vec![0_i32; rows * cols];

        for i in 0..10 {
            let value = SketchInput::I32(i);
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
        let error_bound = epsilon * 200_000 as f64;
        let keys = truth.keys();
        let correct_lower_bound = keys.len() as f64 * (1.0 - delta);
        let mut within_count = 0;
        for key in keys {
            let est = sk.estimate(&SketchInput::U64(*key));
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
        let error_bound = epsilon * 200_000 as f64;
        let keys = truth.keys();
        let correct_lower_bound = keys.len() as f64 * (1.0 - delta);
        let mut within_count = 0;
        for key in keys {
            let est = sk.estimate(&SketchInput::U64(*key));
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
        let error_bound = epsilon * 200_000 as f64;
        let keys = truth.keys();
        let correct_lower_bound = keys.len() as f64 * (1.0 - delta);
        let mut within_count = 0;
        for key in keys {
            let est = sk.estimate(&SketchInput::U64(*key));
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
        let error_bound = epsilon * 200_000 as f64;
        let keys = truth.keys();
        let correct_lower_bound = keys.len() as f64 * (1.0 - delta);
        let mut within_count = 0;
        for key in keys {
            let est = sk.estimate(&SketchInput::U64(*key));
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
        sketch.insert(&SketchInput::U64(42));
        sketch.insert(&SketchInput::U64(7));

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
