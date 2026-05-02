//! Count Sketch implementation.
//!
//! A frequency-estimation sketch that uses random sign projections to estimate
//! item counts with bounded error in the L2 norm.
//!
//! Reference:
//! - Charikar, Chen & Farach-Colton, "Finding Frequent Items in Data Streams,"
//!   ICALP 2002. <https://www.cs.rutgers.edu/~farach/pubs/FrequentStream.pdf>

use crate::{
    DataInput, DefaultMatrixI32, DefaultMatrixI64, DefaultMatrixI128, DefaultXxHasher, FastPath,
    FastPathHasher, FixedMatrix, MatrixFastHash, MatrixStorage, NitroTarget, QuickMatrixI64,
    QuickMatrixI128, RegularPath, SketchHasher, Vector1D, Vector2D, compute_median_inline_f64,
    hash64_seeded,
};
use rmp_serde::{
    decode::Error as RmpDecodeError, encode::Error as RmpEncodeError, from_slice, to_vec_named,
};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::ops::Neg;

const DEFAULT_ROW_NUM: usize = 3;
const DEFAULT_COL_NUM: usize = 4096;
const LOWER_32_MASK: u64 = (1u64 << 32) - 1;

/// A frequency-estimation sketch using random sign projections (Count Sketch).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(serialize = "S: Serialize", deserialize = "S: Deserialize<'de>"))]
pub struct Count<
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

/// Counter trait required by Count Sketch backends.
pub trait CountSketchCounter: Copy + std::ops::AddAssign + Neg<Output = Self> + From<i32> {
    /// Converts the counter into `f64`.
    fn to_f64(self) -> f64;
}

// Implements CountSketchCounter for i32.
impl CountSketchCounter for i32 {
    fn to_f64(self) -> f64 {
        self as f64
    }
}

// Implements CountSketchCounter for i64.
impl CountSketchCounter for i64 {
    fn to_f64(self) -> f64 {
        self as f64
    }
}

// Implements CountSketchCounter for i128.
impl CountSketchCounter for i128 {
    fn to_f64(self) -> f64 {
        self as f64
    }
}

// Default Count sketch for Vector2D<i32> (RegularPath).
impl Default for Count<Vector2D<i32>, RegularPath> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM)
    }
}

// Default Count sketch for Vector2D<i32> (FastPath).
impl Default for Count<Vector2D<i32>, FastPath> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM)
    }
}

// Default Count sketch for Vector2D<i64> (RegularPath).
impl Default for Count<Vector2D<i64>, RegularPath> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM)
    }
}

// Default Count sketch for Vector2D<i64> (FastPath).
impl Default for Count<Vector2D<i64>, FastPath> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM)
    }
}

// Default Count sketch for Vector2D<i128> (RegularPath).
impl Default for Count<Vector2D<i128>, RegularPath> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM)
    }
}

// Default Count sketch for Vector2D<i128> (FastPath).
impl Default for Count<Vector2D<i128>, FastPath> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM)
    }
}

// Default Count sketch for FixedMatrix (RegularPath).
impl Default for Count<FixedMatrix, RegularPath> {
    fn default() -> Self {
        Count::from_storage(FixedMatrix::default())
    }
}

// Default Count sketch for FixedMatrix (FastPath).
impl Default for Count<FixedMatrix, FastPath> {
    fn default() -> Self {
        Count::from_storage(FixedMatrix::default())
    }
}

// Default Count sketch for DefaultMatrixI32 (RegularPath).
impl Default for Count<DefaultMatrixI32, RegularPath> {
    fn default() -> Self {
        Count::from_storage(DefaultMatrixI32::default())
    }
}

// Default Count sketch for DefaultMatrixI32 (FastPath).
impl Default for Count<DefaultMatrixI32, FastPath> {
    fn default() -> Self {
        Count::from_storage(DefaultMatrixI32::default())
    }
}

// Default Count sketch for DefaultMatrixI64 (RegularPath).
impl Default for Count<DefaultMatrixI64, RegularPath> {
    fn default() -> Self {
        Count::from_storage(DefaultMatrixI64::default())
    }
}

// Default Count sketch for DefaultMatrixI64 (FastPath).
impl Default for Count<DefaultMatrixI64, FastPath> {
    fn default() -> Self {
        Count::from_storage(DefaultMatrixI64::default())
    }
}

// Default Count sketch for DefaultMatrixI128 (RegularPath).
impl Default for Count<DefaultMatrixI128, RegularPath> {
    fn default() -> Self {
        Count::from_storage(DefaultMatrixI128::default())
    }
}

// Default Count sketch for DefaultMatrixI128 (FastPath).
impl Default for Count<DefaultMatrixI128, FastPath> {
    fn default() -> Self {
        Count::from_storage(DefaultMatrixI128::default())
    }
}

// Default Count sketch for QuickMatrixI64 (RegularPath).
impl Default for Count<QuickMatrixI64, RegularPath> {
    fn default() -> Self {
        Count::from_storage(QuickMatrixI64::default())
    }
}

// Default Count sketch for QuickMatrixI64 (FastPath).
impl Default for Count<QuickMatrixI64, FastPath> {
    fn default() -> Self {
        Count::from_storage(QuickMatrixI64::default())
    }
}

// Default Count sketch for QuickMatrixI128 (RegularPath).
impl Default for Count<QuickMatrixI128, RegularPath> {
    fn default() -> Self {
        Count::from_storage(QuickMatrixI128::default())
    }
}

// Default Count sketch for QuickMatrixI128 (FastPath).
impl Default for Count<QuickMatrixI128, FastPath> {
    fn default() -> Self {
        Count::from_storage(QuickMatrixI128::default())
    }
}

// Count constructors for Vector2D-backed storage.
impl<T, M, H: SketchHasher> Count<Vector2D<T>, M, H>
where
    T: CountSketchCounter,
{
    /// Creates a sketch with the requested number of rows and columns.
    pub fn with_dimensions(rows: usize, cols: usize) -> Self {
        let mut sk = Count {
            counts: Vector2D::init(rows, cols),
            row: rows,
            col: cols,
            _mode: PhantomData,
            _hasher: PhantomData,
        };
        sk.counts.fill(T::from(0));
        sk
    }
}

// Core Count API for any storage/counter.
impl<S, C, Mode, H: SketchHasher> Count<S, Mode, H>
where
    S: MatrixStorage<Counter = C>,
    C: CountSketchCounter,
{
    /// Wraps an existing matrix storage as a Count Sketch.
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
    pub fn rows(&self) -> usize {
        self.counts.rows()
    }

    /// Number of columns in the sketch.
    pub fn cols(&self) -> usize {
        self.counts.cols()
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
                self.counts.update_one_counter(
                    i,
                    j,
                    |a, b| *a += b,
                    other.counts.query_one_counter(i, j),
                );
            }
        }
    }

    /// Exposes the backing matrix for inspection/testing.
    pub fn as_storage(&self) -> &S {
        &self.counts
    }

    /// Mutable access used internally for testing scenarios.
    pub fn as_storage_mut(&mut self) -> &mut S {
        &mut self.counts
    }
}

// Serialization helpers for Count.
impl<S, C, Mode, H: SketchHasher> Count<S, Mode, H>
where
    S: MatrixStorage<Counter = C> + Serialize,
    C: CountSketchCounter,
{
    /// Serializes the sketch into MessagePack bytes.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        to_vec_named(self)
    }
}

// Deserialization helpers for Count.
impl<S, C, Mode, H: SketchHasher> Count<S, Mode, H>
where
    S: MatrixStorage<Counter = C> + for<'de> Deserialize<'de>,
    C: CountSketchCounter,
{
    /// Deserializes a sketch from MessagePack bytes.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        from_slice(bytes)
    }
}

// Regular-path Count operations.
impl<S, C, H: SketchHasher> Count<S, RegularPath, H>
where
    S: MatrixStorage<Counter = C>,
    C: CountSketchCounter,
{
    /// Inserts an observation with standard Count Sketch updating algorithm.
    pub fn insert(&mut self, value: &DataInput) {
        let rows = self.counts.rows();
        let cols = self.counts.cols();
        for r in 0..rows {
            let hashed = H::hash64_seeded(r, value);
            let col = ((hashed & LOWER_32_MASK) as usize) % cols;
            let bit = ((hashed >> 63) & 1) as i32;
            let sign_bit = if bit == 1 { 1 } else { -1 };
            let delta = if sign_bit > 0 {
                C::from(1)
            } else {
                -C::from(1)
            };
            self.counts
                .update_one_counter(r, col, |a, b| *a += b, delta);
        }
    }

    /// Inserts an observation with the given count (weight).
    pub fn insert_many(&mut self, value: &DataInput, many: C) {
        let rows = self.counts.rows();
        let cols = self.counts.cols();
        for r in 0..rows {
            let hashed = H::hash64_seeded(r, value);
            let col = ((hashed & LOWER_32_MASK) as usize) % cols;
            let bit = ((hashed >> 63) & 1) as i32;
            let sign_bit = if bit == 1 { 1 } else { -1 };
            let delta = if sign_bit > 0 { many } else { -many };
            self.counts
                .update_one_counter(r, col, |a, b| *a += b, delta);
        }
    }

    /// Returns the frequency estimate for the provided value.
    pub fn estimate(&self, value: &DataInput) -> f64 {
        let rows = self.counts.rows();
        let cols = self.counts.cols();
        let mut estimates = Vec::with_capacity(rows);
        for r in 0..rows {
            let hashed = H::hash64_seeded(r, value);
            let col = ((hashed & LOWER_32_MASK) as usize) % cols;
            let bit = ((hashed >> 63) & 1) as i32;
            let sign_bit = if bit == 1 { 1 } else { -1 };
            let counter = self.counts.query_one_counter(r, col);
            if sign_bit > 0 {
                estimates.push(counter.to_f64());
            } else {
                estimates.push(-counter.to_f64());
            }
        }
        if estimates.is_empty() {
            return 0.0;
        }
        estimates.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
        let mid = estimates.len() / 2;
        if estimates.len() % 2 == 1 {
            estimates[mid]
        } else {
            (estimates[mid - 1] + estimates[mid]) / 2.0
        }
    }
}

// Fast-path Count operations using precomputed hashes.
impl<S, H: SketchHasher> Count<S, FastPath, H>
where
    S: MatrixStorage + crate::FastPathHasher<H>,
    S::Counter: CountSketchCounter,
{
    /// Inserts an observation using the combined hash optimization.
    #[inline(always)]
    pub fn insert(&mut self, value: &DataInput) {
        let hashed_val = <S as FastPathHasher<H>>::hash_for_matrix(&self.counts, value);
        self.counts.fast_insert(
            |counter, value, row| {
                let sign = hashed_val.sign_for_row(row);
                let delta = if sign > 0 { *value } else { -*value };
                *counter += delta;
            },
            S::Counter::from(1),
            &hashed_val,
        );
    }

    /// Inserts an observation with the given count using the combined hash optimization.
    #[inline(always)]
    pub fn insert_many(&mut self, value: &DataInput, many: S::Counter) {
        let hashed_val = <S as FastPathHasher<H>>::hash_for_matrix(&self.counts, value);
        self.counts.fast_insert(
            |counter, value, row| {
                let sign = hashed_val.sign_for_row(row);
                let delta = if sign > 0 { *value } else { -*value };
                *counter += delta;
            },
            many,
            &hashed_val,
        );
    }

    /// Returns the frequency estimate for the provided value.
    #[inline(always)]
    pub fn estimate(&self, value: &DataInput) -> f64 {
        let hashed_val = <S as FastPathHasher<H>>::hash_for_matrix(&self.counts, value);
        self.counts
            .fast_query_median(&hashed_val, |val, row, hash| {
                let sign = hash.sign_for_row(row);
                if sign > 0 {
                    (*val).to_f64()
                } else {
                    -(*val).to_f64()
                }
            })
    }

    /// Inserts an observation using a pre-computed hash value.
    /// Hash value can be reused with other sketches.
    #[inline(always)]
    pub fn fast_insert_with_hash_value(&mut self, hashed_val: &H::HashType) {
        self.counts.fast_insert(
            |counter, value, row| {
                let sign = hashed_val.sign_for_row(row);
                let delta = if sign > 0 { *value } else { -*value };
                *counter += delta;
            },
            S::Counter::from(1),
            hashed_val,
        );
    }

    /// Inserts an observation with the given count using a pre-computed hash value.
    #[inline(always)]
    pub fn fast_insert_many_with_hash_value(&mut self, hashed_val: &H::HashType, many: S::Counter) {
        self.counts.fast_insert(
            |counter, value, row| {
                let sign = hashed_val.sign_for_row(row);
                let delta = if sign > 0 { *value } else { -*value };
                *counter += delta;
            },
            many,
            hashed_val,
        );
    }

    /// Returns the frequency estimate using a pre-computed hash value.
    #[inline(always)]
    pub fn fast_estimate_with_hash(&self, hashed_val: &H::HashType) -> f64 {
        self.counts.fast_query_median(hashed_val, |val, row, hash| {
            let sign = hash.sign_for_row(row);
            if sign > 0 {
                (*val).to_f64()
            } else {
                -(*val).to_f64()
            }
        })
    }
}

// Debug helpers for i32 Vector2D Count.
impl<M, H: SketchHasher> Count<Vector2D<i32>, M, H> {
    /// Human-friendly helper used by the serializer demo binaries.
    pub fn debug(&self) {
        for row in 0..self.counts.rows() {
            println!("row {}: {:?}", row, &self.counts.row_slice(row));
        }
    }
}

// Nitro sampling helpers for fast-path Count.
impl<H: SketchHasher> Count<Vector2D<i32>, FastPath, H> {
    /// Enables Nitro sampling with the provided rate.
    pub fn enable_nitro(&mut self, sampling_rate: f64) {
        self.counts.enable_nitro(sampling_rate);
    }

    /// Inserts an observation using Nitro geometric-sampling acceleration.
    #[inline(always)]
    pub fn fast_insert_nitro(&mut self, value: &DataInput) {
        let rows = self.counts.rows();
        let delta = self.counts.nitro().delta;
        if self.counts.nitro().to_skip >= rows {
            self.counts.reduce_nitro_skip(rows);
        } else {
            let hashed = H::hash128_seeded(0, value);
            let mut r = self.counts.nitro().to_skip;
            loop {
                let bit = (hashed >> (127 - r)) & 1;
                let sign = (bit << 1) as i32 - 1;
                self.counts
                    .update_by_row(r, hashed, |a, b| *a += b, sign * (delta as i32));
                self.counts.nitro_mut().draw_geometric();
                if r + self.counts.nitro_mut().to_skip + 1 >= rows {
                    break;
                }
                r += self.counts.nitro_mut().to_skip + 1;
            }
            let temp = self.counts.get_nitro_skip();
            self.counts.update_nitro_skip((r + temp + 1) - rows);
        }
    }
}

// NitroTarget integration for fast-path Count.
impl<H: SketchHasher> NitroTarget for Count<Vector2D<i32>, FastPath, H> {
    #[inline(always)]
    fn rows(&self) -> usize {
        self.counts.rows()
    }

    #[inline(always)]
    fn update_row(&mut self, row: usize, hashed: u128, delta: u64) {
        let bit = (hashed >> (127 - row)) & 1;
        let sign = (bit << 1) as i32 - 1;
        self.counts
            .update_by_row(row, hashed, |a, b| *a += b, sign * (delta as i32));
    }
}

/// Count Sketch augmented with per-row L2 norm tracking for heavy-hitter detection.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(bound = "")]
pub struct CountL2HH<H: SketchHasher = DefaultXxHasher> {
    counts: Vector2D<i64>,
    l2: Vector1D<i64>,
    row: usize,
    col: usize,
    seed_idx: usize,
    #[serde(skip)]
    _hasher: PhantomData<H>,
}

// Default CountL2HH configuration.
impl Default for CountL2HH {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM)
    }
}

// CountL2HH constructors and operations.
impl<H: SketchHasher> CountL2HH<H> {
    /// Creates a sketch with the requested number of rows and columns.
    pub fn with_dimensions(rows: usize, cols: usize) -> Self {
        Self::with_dimensions_and_seed(rows, cols, 0)
    }

    /// Creates a sketch with the requested dimensions and a custom seed offset.
    pub fn with_dimensions_and_seed(rows: usize, cols: usize, seed_idx: usize) -> Self {
        let mut sk = CountL2HH {
            counts: Vector2D::init(rows, cols),
            l2: Vector1D::filled(rows, 0),
            row: rows,
            col: cols,
            seed_idx,
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

        for i in 0..self.row {
            for j in 0..self.col {
                self.counts[i][j] += other.counts[i][j];
            }
            self.l2[i] = other.l2[i];
        }
    }

    /// Resets all counters and L2 accumulators to zero without reallocating.
    pub fn clear(&mut self) {
        self.counts.fill(0);
        self.l2.fill(0);
    }

    /// Inserts with hash optimization - computes hash once and reuses it.
    /// due to the limitation of seeds, use fast_insert only
    pub fn fast_insert_with_count(&mut self, val: &DataInput, c: i64) {
        let hashed_val = H::hash128_seeded(self.seed_idx, val);
        self.fast_insert_with_count_and_hash(hashed_val, c);
    }

    /// Inserts with hash optimization using precomputed hash value.
    pub fn fast_insert_with_count_and_hash(&mut self, hashed_val: u128, c: i64) {
        let mask_bits = self.counts.get_mask_bits() as usize;
        let mask = (1u128 << mask_bits) - 1;
        let mut shift_amount = 0;
        let mut sign_bit_pos = 127;

        for i in 0..self.row {
            let hashed = (hashed_val >> shift_amount) & mask;
            let idx = (hashed as usize) % self.col;
            let bit = ((hashed_val >> sign_bit_pos) & 1) as i64;
            let sign_bit = -(1 - 2 * bit);

            let old_value = self.counts.query_one_counter(i, idx);
            let new_value = old_value + sign_bit * c;
            self.counts[i][idx] = new_value;

            let old_l2 = self.l2.as_slice()[i];
            let new_l2 = old_l2 + new_value * new_value - old_value * old_value;
            self.l2[i] = new_l2;

            shift_amount += mask_bits;
            sign_bit_pos -= 1;
        }
    }

    // /// Inserts without L2 update using hash optimization.
    // /// due to the limitation of seeds, use fast_insert only
    // pub fn fast_insert_with_count_without_l2(&mut self, val: &DataInput, c: i64) {
    //     let hashed_val = hash128_seeded(self.seed_idx, val);
    //     self.fast_insert_with_count_without_l2_and_hash(hashed_val, c);
    // }

    /// Inserts without L2 update using precomputed hash value.
    pub fn fast_insert_with_count_without_l2_and_hash(&mut self, hashed_val: u128, c: i64) {
        let mask_bits = self.counts.get_mask_bits() as usize;
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
    pub fn fast_update_and_est(&mut self, val: &DataInput, c: i64) -> f64 {
        let hashed_val = H::hash128_seeded(self.seed_idx, val);
        self.fast_insert_with_count_and_hash(hashed_val, c);
        self.fast_get_est_with_hash(hashed_val)
    }

    /// Update and estimate without L2 with hash optimization.
    /// due to the limitation of seeds, use fast_insert only
    pub fn fast_update_and_est_without_l2(&mut self, val: &DataInput, c: i64) -> f64 {
        let hashed_val = H::hash128_seeded(self.seed_idx, val);
        self.fast_insert_with_count_without_l2_and_hash(hashed_val, c);
        self.fast_get_est_with_hash(hashed_val)
    }

    /// Returns the median estimate of the squared L2 norm across rows.
    pub fn get_l2_sqr(&self) -> f64 {
        let mut values: Vec<f64> = self.l2.as_slice()[..self.row]
            .iter()
            .map(|&v| v as f64)
            .collect();
        compute_median_inline_f64(&mut values)
    }

    /// Returns the estimated L2 norm (square root of `get_l2_sqr`).
    pub fn get_l2(&self) -> f64 {
        let l2 = self.get_l2_sqr();
        l2.sqrt()
    }

    /// Returns the frequency estimate with hash optimization.
    /// due to the limitation of seeds, use fast_insert only
    pub fn fast_get_est(&self, val: &DataInput) -> f64 {
        let hashed_val = H::hash128_seeded(self.seed_idx, val);
        self.fast_get_est_with_hash(hashed_val)
    }

    /// Returns the frequency estimate using precomputed hash value.
    /// due to the limitation of seeds, use fast_insert only
    pub fn fast_get_est_with_hash(&self, hashed_val: u128) -> f64 {
        let mask_bits = self.counts.get_mask_bits() as usize;
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

    /// Serializes the CountL2HH sketch into MessagePack bytes.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        to_vec_named(self)
    }

    /// Deserializes a CountL2HH sketch from MessagePack bytes.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        from_slice(bytes)
    }
}

use crate::octo_delta::{COUNT_PROMASK, CountDelta};

impl<S: MatrixStorage<Counter = i32>, H: SketchHasher> Count<S, RegularPath, H> {
    /// Inserts a value and emits a delta when any counter exceeds the promotion threshold.
    #[inline(always)]
    pub fn insert_emit_delta(&mut self, value: &DataInput, emit: &mut impl FnMut(CountDelta)) {
        let rows = self.counts.rows();
        let cols = self.counts.cols();
        for r in 0..rows {
            let hashed = hash64_seeded(r, value);
            let col = ((hashed & LOWER_32_MASK) as usize) % cols;
            let sign: i32 = if ((hashed >> 63) & 1) == 1 { 1 } else { -1 };
            self.counts.increment_by_row(r, col, sign);
            let current = self.counts.query_one_counter(r, col);
            if current.unsigned_abs() >= COUNT_PROMASK as u32 {
                emit(CountDelta {
                    row: r as u16,
                    col: col as u16,
                    value: current as i8,
                });
                self.counts.update_one_counter(r, col, |c, _| *c = 0, ());
            }
        }
    }
}

impl<S, H: SketchHasher> Count<S, FastPath, H>
where
    S: MatrixStorage<Counter = i32> + FastPathHasher<H>,
{
    /// Inserts a value using the fast path and emits a delta on counter overflow.
    #[inline(always)]
    pub fn insert_emit_delta(&mut self, value: &DataInput, emit: &mut impl FnMut(CountDelta)) {
        let hashed_val = <S as FastPathHasher<H>>::hash_for_matrix(&self.counts, value);
        let rows = self.counts.rows();
        let cols = self.counts.cols();
        for r in 0..rows {
            let col = hashed_val.col_for_row(r, cols);
            let sign = hashed_val.sign_for_row(r);
            self.counts.increment_by_row(r, col, sign);
            let current = self.counts.query_one_counter(r, col);
            if current.unsigned_abs() >= COUNT_PROMASK as u32 {
                emit(CountDelta {
                    row: r as u16,
                    col: col as u16,
                    value: current as i8,
                });
                self.counts.update_one_counter(r, col, |c, _| *c = 0, ());
            }
        }
    }
}

impl<S: MatrixStorage, Mode, H: SketchHasher> Count<S, Mode, H>
where
    S::Counter: Copy + std::ops::AddAssign + From<i32>,
{
    /// Applies a previously emitted delta to this sketch.
    pub fn apply_delta(&mut self, delta: CountDelta) {
        self.counts.increment_by_row(
            delta.row as usize,
            delta.col as usize,
            S::Counter::from(delta.value as i32),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        all_counter_zero_i32, counter_index, sample_uniform_f64, sample_zipf_u64,
    };
    use crate::{DataInput, hash64_seeded};
    use std::collections::HashMap;

    #[test]
    fn count_child_insert_emits_at_threshold() {
        let mut child = Count::<Vector2D<i32>, RegularPath>::with_dimensions(3, 64);
        let key = DataInput::U64(99);
        let mut deltas: Vec<CountDelta> = Vec::new();

        for _ in 0..200 {
            child.insert_emit_delta(&key, &mut |d| deltas.push(d));
        }
        assert!(
            deltas.len() >= 3,
            "expected at least one promoted delta per row"
        );
    }

    fn counter_sign(row: usize, key: &DataInput) -> i32 {
        let hash = hash64_seeded(row, key);
        if (hash >> 63) & 1 == 1 { 1 } else { -1 }
    }

    fn run_zipf_stream(
        rows: usize,
        cols: usize,
        domain: usize,
        exponent: f64,
        samples: usize,
        seed: u64,
    ) -> (Count, HashMap<u64, i32>) {
        let mut truth = HashMap::<u64, i32>::new();
        let mut sketch = Count::<Vector2D<i32>, RegularPath>::with_dimensions(rows, cols);

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
    ) -> (Count<Vector2D<i32>, FastPath>, HashMap<u64, u64>) {
        let mut truth = HashMap::<u64, u64>::new();
        let mut sketch = Count::<Vector2D<i32>, FastPath>::with_dimensions(rows, cols);

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
    ) -> (Count, HashMap<u64, u64>) {
        let mut truth = HashMap::<u64, u64>::new();
        let mut sketch = Count::<Vector2D<i32>, RegularPath>::with_dimensions(rows, cols);

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
    ) -> (Count<Vector2D<i32>, FastPath>, HashMap<u64, u64>) {
        let mut truth = HashMap::<u64, u64>::new();
        let mut sketch = Count::<Vector2D<i32>, FastPath>::with_dimensions(rows, cols);

        for value in sample_uniform_f64(min, max, samples, seed) {
            let key = DataInput::F64(value);
            sketch.insert(&key);
            *truth.entry(value.to_bits()).or_insert(0) += 1;
        }

        (sketch, truth)
    }

    #[test]
    fn default_initializes_expected_dimensions() {
        let cs = Count::<Vector2D<i32>, RegularPath>::default();
        assert_eq!(cs.rows(), 3);
        assert_eq!(cs.cols(), 4096);
        all_counter_zero_i32(cs.as_storage());
    }

    #[test]
    fn with_dimensions_uses_custom_sizes() {
        let cs = Count::<Vector2D<i32>, RegularPath>::with_dimensions(3, 17);
        assert_eq!(cs.rows(), 3);
        assert_eq!(cs.cols(), 17);

        let storage = cs.as_storage();
        for row in 0..cs.rows() {
            assert!(
                storage.row_slice(row).iter().all(|&value| value == 0),
                "expected row {} to be zero-initialized, got {:?}",
                row,
                storage.row_slice(row)
            );
        }
    }

    #[test]
    fn insert_updates_signed_counters_per_row() {
        let mut sketch = Count::<Vector2D<i32>, RegularPath>::with_dimensions(3, 64);
        let key = DataInput::Str("alpha");

        sketch.insert(&key);

        for row in 0..sketch.rows() {
            let idx = counter_index(row, &key, sketch.cols());
            let expected = counter_sign(row, &key);
            assert_eq!(
                sketch.counts.query_one_counter(row, idx),
                expected,
                "row {row} counter mismatch"
            );
        }
    }

    #[test]
    fn fast_insert_produces_consistent_estimates() {
        let mut fast = Count::<Vector2D<i32>, FastPath>::with_dimensions(4, 128);

        let keys = vec![
            DataInput::Str("alpha"),
            DataInput::Str("beta"),
            DataInput::Str("gamma"),
            DataInput::Str("delta"),
            DataInput::Str("epsilon"),
        ];

        for key in &keys {
            fast.insert(key);
        }

        for key in &keys {
            let estimate = fast.estimate(key);
            assert!(
                (estimate - 1.0).abs() < f64::EPSILON,
                "fast estimate for key {key:?} should be 1.0, got {estimate}"
            );
        }
    }

    #[test]
    fn insert_produces_consistent_estimates() {
        let mut sketch = Count::<Vector2D<i32>, RegularPath>::with_dimensions(3, 64);

        let keys = vec![
            DataInput::Str("alpha"),
            DataInput::Str("beta"),
            DataInput::Str("gamma"),
            DataInput::Str("delta"),
            DataInput::Str("epsilon"),
        ];

        for key in &keys {
            sketch.insert(key);
        }

        for key in &keys {
            let estimate = sketch.estimate(key);
            assert!(
                (estimate - 1.0).abs() < f64::EPSILON,
                "estimate for key {key:?} should be 1.0, got {estimate}"
            );
        }
    }

    #[test]
    fn estimate_recovers_frequency_for_repeated_key() {
        let mut sketch = Count::<Vector2D<i32>, RegularPath>::with_dimensions(3, 64);
        let key = DataInput::Str("theta");

        let repeats = 37;
        for _ in 0..repeats {
            sketch.insert(&key);
        }

        let estimate = sketch.estimate(&key);
        assert!(
            (estimate - repeats as f64).abs() < f64::EPSILON,
            "expected estimate {repeats}, got {estimate}"
        );
    }

    #[test]
    fn fast_path_recovers_repeated_insertions() {
        let mut sketch = Count::<Vector2D<i32>, FastPath>::with_dimensions(4, 256);
        let keys = vec![
            DataInput::Str("alpha"),
            DataInput::Str("beta"),
            DataInput::Str("gamma"),
            DataInput::Str("delta"),
            DataInput::Str("epsilon"),
        ];

        for _ in 0..5 {
            for key in &keys {
                sketch.insert(key);
            }
        }

        for key in &keys {
            let estimate = sketch.estimate(key);
            assert!(
                (estimate - 5.0).abs() < f64::EPSILON,
                "fast estimate for key {key:?} should be 5.0, got {estimate}"
            );
        }
    }

    #[test]
    fn merge_adds_counters_element_wise() {
        let mut left = Count::<Vector2D<i32>, RegularPath>::with_dimensions(2, 32);
        let mut right = Count::<Vector2D<i32>, RegularPath>::with_dimensions(2, 32);
        let key = DataInput::Str("delta");

        left.insert(&key);
        right.insert(&key);
        right.insert(&key);

        let left_indices: Vec<_> = (0..left.rows())
            .map(|row| counter_index(row, &key, left.cols()))
            .collect();

        left.merge(&right);

        for (row, idx) in left_indices.into_iter().enumerate() {
            let expected = counter_sign(row, &key) * 3;
            assert_eq!(left.as_storage().query_one_counter(row, idx), expected);
        }
    }

    #[test]
    #[should_panic(expected = "dimension mismatch while merging CountMin sketches")]
    fn merge_requires_matching_dimensions() {
        let mut left = Count::<Vector2D<i32>, RegularPath>::with_dimensions(2, 32);
        let right = Count::<Vector2D<i32>, RegularPath>::with_dimensions(3, 32);
        left.merge(&right);
    }

    #[test]
    fn zipf_stream_stays_within_twenty_percent_for_most_keys() {
        let (sketch, truth) = run_zipf_stream(5, 8192, 8192, 1.1, 200_000, 0x5eed_c0de);
        let mut within_tolerance = 0usize;
        for (&value, &count) in &truth {
            let estimate = sketch.estimate(&DataInput::U64(value));
            let rel_error = ((estimate - count as f64).abs()) / (count as f64);
            if rel_error < 0.20 {
                within_tolerance += 1;
            }
        }

        let total = truth.len();
        let accuracy = within_tolerance as f64 / total as f64;
        assert!(
            accuracy >= 0.70,
            "Only {:.2}% of keys within tolerance ({} of {}); expected at least 70%",
            accuracy * 100.0,
            within_tolerance,
            total
        );
    }

    #[test]
    fn cs_regular_path_correctness() {
        let mut sk = Count::<Vector2D<i32>, RegularPath>::default();
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
                let bit = ((hashed >> 63) & 1) as i32;
                let sign_bit = -(1 - 2 * bit);
                let idx = r * cols + col;
                expected_once[idx] += sign_bit;
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
            let estimate = sk.estimate(&DataInput::I32(i));
            assert!(
                (estimate - 2.0).abs() < f64::EPSILON,
                "estimate for {i} should be 2.0, but get {estimate}"
            );
        }
    }

    #[test]
    fn cs_fast_path_correctness() {
        let mut sk = Count::<Vector2D<i32>, FastPath>::default();
        // Insert values 0..9 once using the fast path.
        for i in 0..10 {
            sk.insert(&DataInput::I32(i));
        }

        // Build the expected counter array by mirroring the fast-path hashing logic.
        let storage = sk.as_storage();
        let rows = storage.rows();
        let cols = storage.cols();
        let mask_bits = storage.get_mask_bits();
        let mask = (1u128 << mask_bits) - 1;
        let mut expected_once = vec![0_i32; rows * cols];

        for i in 0..10 {
            let value = DataInput::I32(i);
            let hash = <Vector2D<i32> as FastPathHasher<DefaultXxHasher>>::hash_for_matrix(
                storage, &value,
            );
            for row in 0..rows {
                let hashed = hash.row_hash(row, mask_bits, mask);
                let col = (hashed % cols as u128) as usize;
                let idx = row * cols + col;
                expected_once[idx] += hash.sign_for_row(row);
            }
        }

        assert_eq!(storage.as_slice(), expected_once.as_slice());
    }

    // test for zipf distribution for domain 8192 and exponent 1.1 with 200_000 items
    // verify: (1-delta)*(query_size) is within bound (epsilon*L2Norm)
    #[test]
    fn cs_error_bound_zipf() {
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
            if (est - (*truth.get(key).unwrap() as f64)).abs() < error_bound {
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
            if (est - (*truth.get(key).unwrap() as f64)).abs() < error_bound {
                within_count += 1;
            }
        }
        assert!(
            within_count as f64 > correct_lower_bound,
            "in-bound items number {within_count} not greater than expected amount {correct_lower_bound}"
        );
    }

    // test for uniform distribution from 100.0 to 1000.0 with 200_000 items
    // verify: (1-delta)*(query_size) is within bound (epsilon*L2Norm)
    #[test]
    fn cs_error_bound_uniform() {
        // regular path
        let (sk, truth) = run_uniform_stream(
            DEFAULT_ROW_NUM,
            DEFAULT_COL_NUM,
            100.0,
            1000.0,
            200_000,
            0x5eed_c0de,
        );
        let epsilon = (std::f64::consts::E / DEFAULT_COL_NUM as f64).sqrt();
        let l2_norm = truth
            .values()
            .map(|&c| (c as f64).powi(2))
            .sum::<f64>()
            .sqrt();
        let error_bound = epsilon * l2_norm;
        let delta = 1.0 / std::f64::consts::E.powi(DEFAULT_ROW_NUM as i32);
        let keys = truth.keys();
        let correct_lower_bound = keys.len() as f64 * (1.0 - delta);
        let mut within_count = 0;
        for key in keys {
            let est = sk.estimate(&DataInput::U64(*key));
            if (est - (*truth.get(key).unwrap() as f64)).abs() < error_bound {
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
            if (est - (*truth.get(key).unwrap() as f64)).abs() < error_bound {
                within_count += 1;
            }
        }
        assert!(
            within_count as f64 > correct_lower_bound,
            "in-bound items number {within_count} not greater than expected amount {correct_lower_bound}"
        );
    }

    #[test]
    fn count_sketch_round_trip_serialization() {
        let mut sketch = Count::<Vector2D<i32>, RegularPath>::with_dimensions(3, 8);
        sketch.insert(&DataInput::U64(42));
        sketch.insert(&DataInput::U64(7));

        let encoded = sketch.serialize_to_bytes().expect("serialize Count");
        assert!(!encoded.is_empty());
        let data_copied = encoded.clone();

        let decoded = Count::<Vector2D<i32>, RegularPath>::deserialize_from_bytes(&data_copied)
            .expect("deserialize Count");

        assert_eq!(sketch.rows(), decoded.rows());
        assert_eq!(sketch.cols(), decoded.cols());
        assert_eq!(
            sketch.as_storage().as_slice(),
            decoded.as_storage().as_slice()
        );
    }

    #[test]
    fn countl2hh_estimates_and_l2_are_consistent() {
        let mut sketch: CountL2HH = CountL2HH::with_dimensions(3, 32);
        let key = DataInput::Str("gamma");

        let est_after_first = sketch.fast_update_and_est(&key, 5);
        assert_eq!(est_after_first, 5.0);

        let est_after_second = sketch.fast_update_and_est(&key, -2);
        assert_eq!(est_after_second, 3.0);

        let l2 = sketch.get_l2();
        assert!(l2 >= 3.0, "expected non-trivial l2, got {l2}");
    }

    #[test]
    fn countl2hh_merge_combines_frequency_vectors() {
        let mut left: CountL2HH = CountL2HH::with_dimensions(3, 32);
        let mut right: CountL2HH = CountL2HH::with_dimensions(3, 32);
        let key = DataInput::U32(42);

        left.fast_insert_with_count(&key, 4);
        assert_eq!(left.fast_get_est(&key), 4.0);
        right.fast_insert_with_count(&key, 9);
        assert_eq!(right.fast_get_est(&key), 9.0);

        left.merge(&right);
        assert_eq!(left.fast_get_est(&key), 13.0);
    }

    #[test]
    fn countl2hh_round_trip_serialization() {
        let mut sketch: CountL2HH = CountL2HH::with_dimensions_and_seed(3, 32, 7);
        let key = DataInput::Str("serialize");

        sketch.fast_insert_with_count(&key, 11);
        sketch.fast_insert_with_count(&key, -3);
        let base_est = sketch.fast_get_est(&key);
        let base_l2 = sketch.get_l2();

        let encoded = sketch
            .serialize_to_bytes()
            .expect("serialize CountL2HH into MessagePack");
        assert!(!encoded.is_empty(), "serialized bytes should not be empty");
        let data = encoded.clone();

        let decoded: CountL2HH = CountL2HH::deserialize_from_bytes(&data)
            .expect("deserialize CountL2HH from MessagePack");

        assert_eq!(sketch.rows(), decoded.rows());
        assert_eq!(sketch.cols(), decoded.cols());
        assert!(
            (decoded.fast_get_est(&key) - base_est).abs() < f64::EPSILON,
            "estimate changed after round trip"
        );
        assert!(
            (decoded.get_l2() - base_l2).abs() < f64::EPSILON,
            "L2 changed after round trip"
        );
    }
}

// =====================================================================
// ASAP runtime wire-format-aligned variant .
//
// `CountSketch` and `CountSketchDelta` below are the public-field,
// proto-decode-friendly types consumed by the ASAP query engine
// accumulators. The high-throughput in-process variant above
// (`Count`) keeps its original design.
// =====================================================================

// Count Sketch (a.k.a. Count-Min-style signed-counter sketch) —
// element-wise mergeable frequency estimator.
//
// Parallel to `count_min::CountMinSketch` but with **signed** counters,
// matching the `asap_sketchlib::proto::sketchlib::CountSketchState` wire
// format that DataCollector's `countsketchprocessor` emits via the
// modified OTLP `Metric.data = CountSketch{…}` variant.
//
// This is the minimal surface needed for PR C-CountSketch in the
// modified-OTLP hot path: construct from a decoded proto state, merge
// element-wise with another sketch, emit the matrix for queries and
// serialization. The richer query semantics of Count Sketch (median-
// of-estimators heavy-hitter tracking, `TopKState` integration, etc.)
// are intentionally deferred to a follow-up — the wire format already
// carries the matrix losslessly, so the merge/store round-trip works
// with just a matrix today.

// (de-duplicated) use serde::{Deserialize, Serialize};

/// Sparse delta between two consecutive CountSketch snapshots —
/// the input shape for [`CountSketch::apply_delta`]. Mirrors the
/// `CountSketchDelta` proto in
/// `sketchlib-go/proto/countsketch/countsketch.proto` (packed
/// encoding only — the deprecated `cells_legacy` path + the
/// non-delta `topk` / `hh_keys` top-K carrier aren't modeled here).
///
/// Cells apply additively: `matrix[row][col] += d_count` for each
/// `(row, col, d_count)` triple. Top-K on the delta path is a
/// separate follow-up (CS top-K is non-linear; merging deltas
/// would require re-querying the merged matrix).
#[derive(Debug, Clone, Default)]
pub struct CountSketchDelta {
    pub rows: u32,
    pub cols: u32,
    /// `(row, col, d_count)` cell updates, additive on the CS matrix.
    pub cells: Vec<(u32, u32, i64)>,
    /// Per-row L2 norm deltas. Additive, one scalar per row of the
    /// base sketch. Kept on the delta surface for downstream
    /// error-accounting; `apply_delta` itself ignores L2.
    pub l2: Vec<f64>,
}

/// Minimal Count Sketch state — a flat `rows × cols` matrix of signed
/// counts. Element-wise mergeable (sum over aligned cells).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountSketch {
    #[serde(rename = "row_num")]
    pub rows: usize,
    #[serde(rename = "col_num")]
    pub cols: usize,
    /// Row-major matrix of signed counts. `matrix[r][c]` is the value of
    /// hash row `r`, column `c`.
    pub matrix: Vec<Vec<f64>>,
}

impl CountSketch {
    /// Construct an all-zero sketch with the given dimensions.
    pub fn new(rows: usize, cols: usize) -> Self {
        Self {
            rows,
            cols,
            matrix: vec![vec![0.0; cols]; rows],
        }
    }

    /// Construct from a pre-built matrix (used by the modified-OTLP
    /// proto-decode path).
    pub fn from_legacy_matrix(matrix: Vec<Vec<f64>>, rows: usize, cols: usize) -> Self {
        debug_assert_eq!(matrix.len(), rows, "row count mismatch");
        debug_assert!(
            matrix.iter().all(|r| r.len() == cols),
            "column count mismatch in at least one row"
        );
        Self { rows, cols, matrix }
    }

    /// Borrow the inner matrix.
    pub fn sketch(&self) -> &Vec<Vec<f64>> {
        &self.matrix
    }

    /// Insert a single weighted observation. Each row uses an independent
    /// hash seed and a sign bit to update the matrix in place — the
    /// standard CountSketch update primitive. The wire format here uses
    /// xxh64 with per-row seeding; this matches sketchlib-go's
    /// `DeriveIndex`/`DeriveSign` decomposition for matrix-backed
    /// sketches and is intended for in-process tests / ground-truth
    /// builds, not cross-language replay.
    pub fn update(&mut self, key: &str, value: f64) {
        if self.rows == 0 || self.cols == 0 {
            return;
        }
        let key_bytes = key.as_bytes();
        for r in 0..self.rows {
            let h = twox_hash::XxHash64::oneshot(r as u64, key_bytes);
            let col = (h as usize) % self.cols;
            // Sign derived from the high bit, matching the in-process
            // Count Sketch implementation above.
            let sign = if (h >> 63) & 1 == 1 { 1.0 } else { -1.0 };
            self.matrix[r][col] += sign * value;
        }
    }

    /// Estimate the frequency of `key` via the standard median-of-rows
    /// CountSketch query. Returns 0 for an empty sketch.
    pub fn estimate(&self, key: &str) -> f64 {
        if self.rows == 0 || self.cols == 0 {
            return 0.0;
        }
        let key_bytes = key.as_bytes();
        let mut estimates: Vec<f64> = Vec::with_capacity(self.rows);
        for r in 0..self.rows {
            let h = twox_hash::XxHash64::oneshot(r as u64, key_bytes);
            let col = (h as usize) % self.cols;
            let sign = if (h >> 63) & 1 == 1 { 1.0 } else { -1.0 };
            estimates.push(sign * self.matrix[r][col]);
        }
        estimates.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mid = estimates.len() / 2;
        if estimates.len() % 2 == 1 {
            estimates[mid]
        } else {
            (estimates[mid - 1] + estimates[mid]) / 2.0
        }
    }

    /// Merge one other sketch into self via element-wise addition. Both
    /// operands must have identical dimensions.
    pub fn merge(
        &mut self,
        other: &CountSketch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.rows != other.rows || self.cols != other.cols {
            return Err(format!(
                "CountSketch dimension mismatch: self={}x{}, other={}x{}",
                self.rows, self.cols, other.rows, other.cols
            )
            .into());
        }
        for r in 0..self.rows {
            for c in 0..self.cols {
                self.matrix[r][c] += other.matrix[r][c];
            }
        }
        Ok(())
    }

    /// Apply a sparse delta in place. Matches the `ApplyDelta`
    /// semantics in `sketchlib-go/sketches/CountSketch/delta.go`:
    /// `matrix[row][col] += d_count` for each cell in the delta.
    /// Returns `Err` if any `(row, col)` is out of range — indicating
    /// a dimension mismatch between the snapshot this sketch was
    /// built from and the delta sender.
    pub fn apply_delta(
        &mut self,
        delta: &CountSketchDelta,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (row, col, d_count) in &delta.cells {
            let r = *row as usize;
            let c = *col as usize;
            if r >= self.rows || c >= self.cols {
                return Err(format!(
                    "CountSketchDelta cell ({r},{c}) out of range (matrix={}x{})",
                    self.rows, self.cols
                )
                .into());
            }
            // `d_count` is signed on the wire; CS counts are signed
            // too (can go negative under adversarial keys).
            self.matrix[r][c] += *d_count as f64;
        }
        Ok(())
    }

    /// Merge a slice of references into a single new sketch. All inputs
    /// must share the same dimensions; returns `Err` on mismatch or an
    /// empty input.
    pub fn merge_refs(
        inputs: &[&CountSketch],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let first = inputs
            .first()
            .ok_or("CountSketch::merge_refs called with empty input")?;
        let mut merged = CountSketch::new(first.rows, first.cols);
        for cs in inputs {
            merged.merge(cs)?;
        }
        Ok(merged)
    }

    /// Serialize to MessagePack bytes (used by the legacy wire path
    /// and by PR I's `_ENCODING_MSGPACK` variant when that lands).
    pub fn serialize_msgpack(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::to_vec(self)
    }

    /// Deserialize from MessagePack bytes.
    pub fn deserialize_msgpack(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(rmp_serde::from_slice(buffer)?)
    }
}

#[cfg(test)]
mod tests_wire_count {
    use super::*;

    #[test]
    fn test_new_empty() {
        let cs = CountSketch::new(2, 3);
        assert_eq!(cs.rows, 2);
        assert_eq!(cs.cols, 3);
        assert_eq!(cs.sketch(), &vec![vec![0.0, 0.0, 0.0], vec![0.0, 0.0, 0.0]]);
    }

    #[test]
    fn test_from_legacy_matrix() {
        let m = vec![vec![1.0, -2.0, 3.0], vec![-4.0, 5.0, -6.0]];
        let cs = CountSketch::from_legacy_matrix(m.clone(), 2, 3);
        assert_eq!(cs.sketch(), &m);
    }

    #[test]
    fn test_merge_element_wise() {
        let mut a = CountSketch::from_legacy_matrix(vec![vec![1.0, 2.0], vec![3.0, 4.0]], 2, 2);
        let b = CountSketch::from_legacy_matrix(vec![vec![-1.0, -2.0], vec![-3.0, -4.0]], 2, 2);
        a.merge(&b).unwrap();
        assert_eq!(a.sketch(), &vec![vec![0.0, 0.0], vec![0.0, 0.0]]);
    }

    #[test]
    fn test_merge_dimension_mismatch() {
        let mut a = CountSketch::new(2, 3);
        let b = CountSketch::new(3, 3);
        assert!(a.merge(&b).is_err());
    }

    #[test]
    fn test_merge_refs() {
        let a = CountSketch::from_legacy_matrix(vec![vec![1.0, 2.0]], 1, 2);
        let b = CountSketch::from_legacy_matrix(vec![vec![3.0, 4.0]], 1, 2);
        let c = CountSketch::from_legacy_matrix(vec![vec![5.0, 6.0]], 1, 2);
        let merged = CountSketch::merge_refs(&[&a, &b, &c]).unwrap();
        assert_eq!(merged.sketch(), &vec![vec![9.0, 12.0]]);
    }

    #[test]
    fn test_apply_delta_additive() {
        let mut cs = CountSketch::from_legacy_matrix(
            vec![vec![1.0, -2.0, 3.0], vec![-4.0, 5.0, -6.0]],
            2,
            3,
        );
        let delta = CountSketchDelta {
            rows: 2,
            cols: 3,
            cells: vec![
                (0, 0, 10),  // 1 + 10 = 11
                (0, 2, -3),  // 3 - 3 = 0
                (1, 1, -15), // 5 - 15 = -10
            ],
            l2: vec![],
        };
        cs.apply_delta(&delta).unwrap();
        assert_eq!(
            cs.sketch(),
            &vec![vec![11.0, -2.0, 0.0], vec![-4.0, -10.0, -6.0]]
        );
    }

    #[test]
    fn test_apply_delta_matches_full_merge() {
        let base = CountSketch::from_legacy_matrix(vec![vec![1.0, 2.0], vec![3.0, 4.0]], 2, 2);
        let addition =
            CountSketch::from_legacy_matrix(vec![vec![10.0, 0.0], vec![0.0, 20.0]], 2, 2);
        let mut via_merge = base.clone();
        via_merge.merge(&addition).unwrap();

        let delta = CountSketchDelta {
            rows: 2,
            cols: 2,
            cells: vec![(0, 0, 10), (1, 1, 20)],
            l2: vec![],
        };
        let mut via_delta = base;
        via_delta.apply_delta(&delta).unwrap();
        assert_eq!(via_delta.sketch(), via_merge.sketch());
    }

    #[test]
    fn test_apply_delta_out_of_range() {
        let mut cs = CountSketch::new(2, 3);
        let delta = CountSketchDelta {
            rows: 2,
            cols: 3,
            cells: vec![(2, 0, 1)], // row 2 out of range for 2-row matrix
            l2: vec![],
        };
        assert!(cs.apply_delta(&delta).is_err());
    }

    #[test]
    fn test_msgpack_round_trip() {
        let original =
            CountSketch::from_legacy_matrix(vec![vec![1.5, -2.5], vec![3.5, -4.5]], 2, 2);
        let bytes = original.serialize_msgpack().unwrap();
        let decoded = CountSketch::deserialize_msgpack(&bytes).unwrap();
        assert_eq!(decoded.sketch(), original.sketch());
        assert_eq!(decoded.rows, original.rows);
        assert_eq!(decoded.cols, original.cols);
    }
}
