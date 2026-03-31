use crate::{
    DefaultMatrixI32, DefaultMatrixI64, DefaultMatrixI128, DefaultXxHasher, FastPath,
    FastPathHasher, FixedMatrix, MatrixFastHash, MatrixStorage, NitroTarget, QuickMatrixI64,
    QuickMatrixI128, RegularPath, SketchHasher, SketchInput, Vector1D, Vector2D,
    compute_median_inline_f64, hash64_seeded,
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

/// Count Sketch based on Common structure
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

pub trait CountSketchCounter: Copy + std::ops::AddAssign + Neg<Output = Self> + From<i32> {
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
    pub fn insert(&mut self, value: &SketchInput) {
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

    pub fn insert_many(&mut self, value: &SketchInput, many: C) {
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
    pub fn estimate(&self, value: &SketchInput) -> f64 {
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
            estimates[mid] as f64
        } else {
            (estimates[mid - 1] as f64 + estimates[mid] as f64) / 2.0
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
    pub fn insert(&mut self, value: &SketchInput) {
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

    #[inline(always)]
    pub fn insert_many(&mut self, value: &SketchInput, many: S::Counter) {
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
    pub fn estimate(&self, value: &SketchInput) -> f64 {
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

    #[inline(always)]
    pub fn fast_insert_nitro(&mut self, value: &SketchInput) {
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
    pub fn with_dimensions(rows: usize, cols: usize) -> Self {
        Self::with_dimensions_and_seed(rows, cols, 0)
    }

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
    pub fn fast_insert_with_count(&mut self, val: &SketchInput, c: i64) {
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
    // pub fn fast_insert_with_count_without_l2(&mut self, val: &SketchInput, c: i64) {
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
    pub fn fast_update_and_est(&mut self, val: &SketchInput, c: i64) -> f64 {
        let hashed_val = H::hash128_seeded(self.seed_idx, val);
        self.fast_insert_with_count_and_hash(hashed_val, c);
        self.fast_get_est_with_hash(hashed_val)
    }

    /// Update and estimate without L2 with hash optimization.
    /// due to the limitation of seeds, use fast_insert only
    pub fn fast_update_and_est_without_l2(&mut self, val: &SketchInput, c: i64) -> f64 {
        let hashed_val = H::hash128_seeded(self.seed_idx, val);
        self.fast_insert_with_count_without_l2_and_hash(hashed_val, c);
        self.fast_get_est_with_hash(hashed_val)
    }

    pub fn get_l2_sqr(&self) -> f64 {
        let mut values: Vec<f64> = self.l2.as_slice()[..self.row]
            .iter()
            .map(|&v| v as f64)
            .collect();
        compute_median_inline_f64(&mut values)
    }

    pub fn get_l2(&self) -> f64 {
        let l2 = self.get_l2_sqr();
        l2.sqrt()
    }

    /// Returns the frequency estimate with hash optimization.
    /// due to the limitation of seeds, use fast_insert only
    pub fn fast_get_est(&self, val: &SketchInput) -> f64 {
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
    #[inline(always)]
    pub fn insert_emit_delta(&mut self, value: &SketchInput, emit: &mut impl FnMut(CountDelta)) {
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
    #[inline(always)]
    pub fn insert_emit_delta(&mut self, value: &SketchInput, emit: &mut impl FnMut(CountDelta)) {
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
    use crate::{SketchInput, hash64_seeded};
    use std::collections::HashMap;

    #[test]
    fn count_child_insert_emits_at_threshold() {
        let mut child = Count::<Vector2D<i32>, RegularPath>::with_dimensions(3, 64);
        let key = SketchInput::U64(99);
        let mut deltas: Vec<CountDelta> = Vec::new();

        for _ in 0..200 {
            child.insert_emit_delta(&key, &mut |d| deltas.push(d));
        }
        assert!(
            deltas.len() >= 3,
            "expected at least one promoted delta per row"
        );
    }

    fn counter_sign(row: usize, key: &SketchInput) -> i32 {
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
    ) -> (Count<Vector2D<i32>, FastPath>, HashMap<u64, u64>) {
        let mut truth = HashMap::<u64, u64>::new();
        let mut sketch = Count::<Vector2D<i32>, FastPath>::with_dimensions(rows, cols);

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
    ) -> (Count, HashMap<u64, u64>) {
        let mut truth = HashMap::<u64, u64>::new();
        let mut sketch = Count::<Vector2D<i32>, RegularPath>::with_dimensions(rows, cols);

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
    ) -> (Count<Vector2D<i32>, FastPath>, HashMap<u64, u64>) {
        let mut truth = HashMap::<u64, u64>::new();
        let mut sketch = Count::<Vector2D<i32>, FastPath>::with_dimensions(rows, cols);

        for value in sample_uniform_f64(min, max, samples, seed) {
            let key = SketchInput::F64(value);
            sketch.insert(&key);
            *truth.entry(value.to_bits() as u64).or_insert(0) += 1;
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
        let key = SketchInput::Str("alpha");

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
            SketchInput::Str("alpha"),
            SketchInput::Str("beta"),
            SketchInput::Str("gamma"),
            SketchInput::Str("delta"),
            SketchInput::Str("epsilon"),
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
            SketchInput::Str("alpha"),
            SketchInput::Str("beta"),
            SketchInput::Str("gamma"),
            SketchInput::Str("delta"),
            SketchInput::Str("epsilon"),
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
        let key = SketchInput::Str("theta");

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
            SketchInput::Str("alpha"),
            SketchInput::Str("beta"),
            SketchInput::Str("gamma"),
            SketchInput::Str("delta"),
            SketchInput::Str("epsilon"),
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
        let key = SketchInput::Str("delta");

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
            let estimate = sketch.estimate(&SketchInput::U64(value));
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
            sk.insert(&SketchInput::I32(i));
        }
        let expected_twice: Vec<i32> = expected_once.iter().map(|v| v * 2).collect();
        assert_eq!(sk.as_storage().as_slice(), expected_twice.as_slice());

        // Estimates for inserted keys should be exactly 2.
        for i in 0..10 {
            let estimate = sk.estimate(&SketchInput::I32(i));
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
            sk.insert(&SketchInput::I32(i));
        }

        // Build the expected counter array by mirroring the fast-path hashing logic.
        let storage = sk.as_storage();
        let rows = storage.rows();
        let cols = storage.cols();
        let mask_bits = storage.get_mask_bits();
        let mask = (1u128 << mask_bits) - 1;
        let mut expected_once = vec![0_i32; rows * cols];

        for i in 0..10 {
            let value = SketchInput::I32(i);
            let hash = <Vector2D<i32> as FastPathHasher<DefaultXxHasher>>::hash_for_matrix(
                storage, &value,
            );
            for row in 0..rows {
                let hashed = hash.row_hash(row, mask_bits, mask);
                let col = (hashed % cols as u128) as usize;
                let idx = row * cols + col;
                expected_once[idx] += hash.sign_for_row(row) as i32;
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
        let error_bound = epsilon * 200_000 as f64;
        let keys = truth.keys();
        let correct_lower_bound = keys.len() as f64 * (1.0 - delta);
        let mut within_count = 0;
        for key in keys {
            let est = sk.estimate(&SketchInput::U64(*key));
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
        let error_bound = epsilon * 200_000 as f64;
        let keys = truth.keys();
        let correct_lower_bound = keys.len() as f64 * (1.0 - delta);
        let mut within_count = 0;
        for key in keys {
            let est = sk.estimate(&SketchInput::U64(*key));
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
            let est = sk.estimate(&SketchInput::U64(*key));
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
        let error_bound = epsilon * 200_000 as f64;
        let keys = truth.keys();
        let correct_lower_bound = keys.len() as f64 * (1.0 - delta);
        let mut within_count = 0;
        for key in keys {
            let est = sk.estimate(&SketchInput::U64(*key));
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
        sketch.insert(&SketchInput::U64(42));
        sketch.insert(&SketchInput::U64(7));

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
        let key = SketchInput::Str("gamma");

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
        let key = SketchInput::U32(42);

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
        let key = SketchInput::Str("serialize");

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
