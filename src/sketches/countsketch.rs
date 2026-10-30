//! Count Sketch implementation.
//!
//! A frequency-estimation sketch that uses random sign projections to estimate
//! item counts with bounded error in the L2 norm.
//!
//! Reference:
//! - Charikar, Chen & Farach-Colton, "Finding Frequent Items in Data Streams,"
//!   ICALP 2002. <https://www.cs.rutgers.edu/~farach/pubs/FrequentStream.pdf>

use crate::asapv1::admission_rate::admit_all;
use crate::common::hash::check_matrix_shape;
use crate::common::structure_utils::AdmittedRows;
use crate::common::structures::matrix_storage::RowEstimates;
use crate::{
    DataInput, DefaultMatrixI32, DefaultMatrixI64, DefaultMatrixI128, DefaultXxHasher, FastPath,
    FastPathHasher, FixedMatrix, MATRIX_MAX_ROWS, MatrixFastHash, MatrixStorage, NitroTarget,
    QuickMatrixI64, QuickMatrixI128, RegularPath, SketchHasher, Vector2D, hash64_seeded,
    nitro_delta_saturated_i32,
};
use std::marker::PhantomData;
use std::ops::Neg;

mod delta;
mod wire;
pub use delta::CountSketchDelta;
pub(crate) use wire::{CsWireCounter, CsWireMode};

use crate::common::structures::fixed_structure::{DEFAULT_COLS, DEFAULT_ROWS};
const LOWER_32_MASK: u64 = (1u64 << 32) - 1;

/// A frequency-estimation sketch using random sign projections (Count Sketch).
#[derive(Clone, Debug)]
pub struct CountSketch<
    S: MatrixStorage = Vector2D<i32>,
    Mode = RegularPath,
    H: SketchHasher = DefaultXxHasher,
> {
    counts: S,
    _mode: PhantomData<Mode>,
    _hasher: PhantomData<H>,
    admission_rate: f64,
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

// Implements CountSketchCounter for f64.
impl CountSketchCounter for f64 {
    fn to_f64(self) -> f64 {
        self
    }
}

// Default Count sketch for Vector2D<i32> (RegularPath).
impl Default for CountSketch<Vector2D<i32>, RegularPath> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROWS, DEFAULT_COLS)
    }
}

// Default Count sketch for Vector2D<i32> (FastPath).
impl Default for CountSketch<Vector2D<i32>, FastPath> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROWS, DEFAULT_COLS)
    }
}

// Default Count sketch for Vector2D<i64> (RegularPath).
impl Default for CountSketch<Vector2D<i64>, RegularPath> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROWS, DEFAULT_COLS)
    }
}

// Default Count sketch for Vector2D<i64> (FastPath).
impl Default for CountSketch<Vector2D<i64>, FastPath> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROWS, DEFAULT_COLS)
    }
}

// Default Count sketch for Vector2D<i128> (RegularPath).
impl Default for CountSketch<Vector2D<i128>, RegularPath> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROWS, DEFAULT_COLS)
    }
}

// Default Count sketch for Vector2D<i128> (FastPath).
impl Default for CountSketch<Vector2D<i128>, FastPath> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROWS, DEFAULT_COLS)
    }
}

// Default Count sketch for FixedMatrix (RegularPath).
impl Default for CountSketch<FixedMatrix, RegularPath> {
    fn default() -> Self {
        CountSketch::from_storage(FixedMatrix::default())
    }
}

// Default Count sketch for FixedMatrix (FastPath).
impl Default for CountSketch<FixedMatrix, FastPath> {
    fn default() -> Self {
        CountSketch::from_storage(FixedMatrix::default())
    }
}

// Default Count sketch for DefaultMatrixI32 (RegularPath).
impl Default for CountSketch<DefaultMatrixI32, RegularPath> {
    fn default() -> Self {
        CountSketch::from_storage(DefaultMatrixI32::default())
    }
}

// Default Count sketch for DefaultMatrixI32 (FastPath).
impl Default for CountSketch<DefaultMatrixI32, FastPath> {
    fn default() -> Self {
        CountSketch::from_storage(DefaultMatrixI32::default())
    }
}

// Default Count sketch for DefaultMatrixI64 (RegularPath).
impl Default for CountSketch<DefaultMatrixI64, RegularPath> {
    fn default() -> Self {
        CountSketch::from_storage(DefaultMatrixI64::default())
    }
}

// Default Count sketch for DefaultMatrixI64 (FastPath).
impl Default for CountSketch<DefaultMatrixI64, FastPath> {
    fn default() -> Self {
        CountSketch::from_storage(DefaultMatrixI64::default())
    }
}

// Default Count sketch for DefaultMatrixI128 (RegularPath).
impl Default for CountSketch<DefaultMatrixI128, RegularPath> {
    fn default() -> Self {
        CountSketch::from_storage(DefaultMatrixI128::default())
    }
}

// Default Count sketch for DefaultMatrixI128 (FastPath).
impl Default for CountSketch<DefaultMatrixI128, FastPath> {
    fn default() -> Self {
        CountSketch::from_storage(DefaultMatrixI128::default())
    }
}

// Default Count sketch for QuickMatrixI64 (RegularPath).
impl Default for CountSketch<QuickMatrixI64, RegularPath> {
    fn default() -> Self {
        CountSketch::from_storage(QuickMatrixI64::default())
    }
}

// Default Count sketch for QuickMatrixI64 (FastPath).
impl Default for CountSketch<QuickMatrixI64, FastPath> {
    fn default() -> Self {
        CountSketch::from_storage(QuickMatrixI64::default())
    }
}

// Default Count sketch for QuickMatrixI128 (RegularPath).
impl Default for CountSketch<QuickMatrixI128, RegularPath> {
    fn default() -> Self {
        CountSketch::from_storage(QuickMatrixI128::default())
    }
}

// Default Count sketch for QuickMatrixI128 (FastPath).
impl Default for CountSketch<QuickMatrixI128, FastPath> {
    fn default() -> Self {
        CountSketch::from_storage(QuickMatrixI128::default())
    }
}

// Count constructors for Vector2D-backed storage.
impl<T, M, H: SketchHasher> CountSketch<Vector2D<T>, M, H>
where
    T: CountSketchCounter,
{
    /// Creates a sketch with the requested number of rows and columns.
    ///
    /// Panics when [`CountSketch::check_shape`] refuses the shape.
    pub fn with_dimensions(rows: usize, cols: usize) -> Self {
        if let Err(problem) = check_matrix_shape("Count Sketch", rows, cols) {
            panic!("{problem}");
        }
        CountSketch {
            counts: Vector2D::filled(rows, cols, T::from(0)),
            _mode: PhantomData,
            _hasher: PhantomData,
            admission_rate: admit_all(),
        }
    }
}

// Shape check; the rule is the same for every storage, path and hasher.
impl CountSketch {
    /// The shapes this sketch builds: both dimensions non-zero and
    /// `rows * cols` within `usize`. The constructors panic on any other
    /// shape; `to_asapv1` also needs `rows <= MATRIX_MAX_ROWS` and
    /// `cols <= u32::MAX`.
    ///
    /// ```
    /// use asap_sketchlib::CountSketch;
    /// assert!(CountSketch::check_shape(3, 64).is_ok());
    /// assert!(CountSketch::check_shape(0, 64).is_err());
    /// ```
    pub fn check_shape(rows: usize, cols: usize) -> Result<(), String> {
        check_matrix_shape("Count Sketch", rows, cols)
    }
}

// Memory estimate; the size is the same for every hasher.
impl<S: MatrixStorage, Mode> CountSketch<S, Mode> {
    /// Bytes a sketch of `rows * cols` counters holds: [`Self::memory_bytes`]
    /// of one built with those dimensions. Defined on the default hasher, so
    /// only the storage and path are named.
    pub fn theoretical_memory_bytes(rows: usize, cols: usize) -> usize {
        size_of::<Self>().saturating_add(
            rows.saturating_mul(cols)
                .saturating_mul(size_of::<S::Counter>()),
        )
    }
}

// Core Count API for any storage/counter.
impl<S, C, Mode, H: SketchHasher> CountSketch<S, Mode, H>
where
    S: MatrixStorage<Counter = C>,
    C: CountSketchCounter,
{
    /// Wraps an existing matrix storage as a Count Sketch.
    ///
    /// Panics when [`Self::check_shape`] refuses the storage's shape.
    pub fn from_storage(counts: S) -> Self {
        if let Err(problem) = CountSketch::check_shape(counts.rows(), counts.cols()) {
            panic!("{problem}");
        }
        Self {
            counts,
            _mode: PhantomData,
            _hasher: PhantomData,
            admission_rate: admit_all(),
        }
    }

    /// Sets the probability `p` with which each update was admitted. Estimates
    /// count admitted updates alone; a consumer divides a key's frequency by `p`.
    pub fn with_admission_rate(mut self, p: f64) -> Self {
        self.admission_rate = p;
        self
    }

    /// The probability with which each update was admitted; `1` unless set.
    pub fn admission_rate(&self) -> f64 {
        self.admission_rate
    }

    /// Number of rows in the sketch.
    pub fn rows(&self) -> usize {
        self.counts.rows()
    }

    /// Number of columns in the sketch.
    pub fn cols(&self) -> usize {
        self.counts.cols()
    }

    /// Merges another sketch while asserting compatible dimensions and equal
    /// admission rates.
    pub fn merge(&mut self, other: &Self) {
        let self_rows = self.counts.rows();
        let self_cols = self.counts.cols();
        assert_eq!(
            (self_rows, self_cols),
            (other.counts.rows(), other.counts.cols()),
            "dimension mismatch while merging Count Sketches"
        );
        assert_eq!(
            self.admission_rate, other.admission_rate,
            "admission rate mismatch while merging Count Sketches"
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

    /// Bytes this sketch holds: the struct and its `rows * cols` counters.
    /// Exact for `Vector2D` storage.
    pub fn memory_bytes(&self) -> usize {
        size_of::<Self>() + self.rows() * self.cols() * size_of::<S::Counter>()
    }

    /// Exposes the backing matrix for inspection/testing.
    pub fn as_storage(&self) -> &S {
        &self.counts
    }

    /// Mutable access used internally for testing scenarios.
    pub fn as_storage_mut(&mut self) -> &mut S {
        &mut self.counts
    }

    /// Zeroes every counter and returns the Nitro sampler to its start,
    /// keeping the shape, the allocation and the admission rate.
    pub fn clear(&mut self) {
        self.counts.reset_sampler();
        let zero = C::from(0);
        for r in 0..self.counts.rows() {
            for c in 0..self.counts.cols() {
                self.counts
                    .update_one_counter(r, c, |cell: &mut C, z| *cell = z, zero);
            }
        }
    }
}

// Regular-path Count operations.
impl<S, C, H: SketchHasher> CountSketch<S, RegularPath, H>
where
    S: MatrixStorage<Counter = C>,
    C: CountSketchCounter,
{
    /// Inserts an observation with standard Count Sketch updating algorithm.
    pub fn update(&mut self, value: &DataInput) {
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
    pub fn update_weighted(&mut self, value: &DataInput, weight: C) {
        let rows = self.counts.rows();
        let cols = self.counts.cols();
        for r in 0..rows {
            let hashed = H::hash64_seeded(r, value);
            let col = ((hashed & LOWER_32_MASK) as usize) % cols;
            let bit = ((hashed >> 63) & 1) as i32;
            let sign_bit = if bit == 1 { 1 } else { -1 };
            let delta = if sign_bit > 0 { weight } else { -weight };
            self.counts
                .update_one_counter(r, col, |a, b| *a += b, delta);
        }
    }

    /// Returns the frequency estimate for the provided value.
    pub fn estimate(&self, value: &DataInput) -> f64 {
        let rows = self.counts.rows();
        let cols = self.counts.cols();
        let mut estimates = RowEstimates::with_capacity(rows);
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
        crate::analytics::ops::add(rows);
        crate::analytics::ops::add_sort(rows);
        estimates.sort_unstable_by(f64::total_cmp);
        let mid = estimates.len() / 2;
        if estimates.len() % 2 == 1 {
            estimates[mid]
        } else {
            (estimates[mid - 1] + estimates[mid]) / 2.0
        }
    }
}

// Checked updates for f64 counters.
impl<H: SketchHasher> CountSketch<Vector2D<f64>, RegularPath, H> {
    /// Checks a weight [`Self::try_update_weighted`] accepts: finite. A
    /// negative weight is a signed update.
    pub fn check_weight(weight: f64) -> Result<(), String> {
        if weight.is_finite() {
            Ok(())
        } else {
            Err(format!("Count Sketch weight {weight} must be finite"))
        }
    }

    /// [`Self::update_weighted`] that rejects a weight [`Self::check_weight`]
    /// rejects, and an update that would carry a probed counter past
    /// `f64::MAX` in either direction. On an error the sketch is unchanged.
    pub fn try_update_weighted(&mut self, value: &DataInput, weight: f64) -> Result<(), String> {
        Self::check_weight(weight)?;
        let cols = self.counts.cols();
        let probed = (0..self.counts.rows())
            .map(|r| {
                let hashed = H::hash64_seeded(r, value);
                let col = ((hashed & LOWER_32_MASK) as usize) % cols;
                let delta = if (hashed >> 63) & 1 == 1 {
                    weight
                } else {
                    -weight
                };
                (col, delta)
            })
            .collect::<smallvec::SmallVec<[(usize, f64); MATRIX_MAX_ROWS]>>();
        for (r, &(col, delta)) in probed.iter().enumerate() {
            if !(self.counts.query_one_counter(r, col) + delta).is_finite() {
                return Err(format!(
                    "Count Sketch counter overflow adding weight {weight}"
                ));
            }
        }
        for (r, (col, delta)) in probed.into_iter().enumerate() {
            self.counts
                .update_one_counter(r, col, |a, b| *a += b, delta);
        }
        Ok(())
    }
}

// Fast-path Count operations using precomputed hashes.
impl<S, H: SketchHasher> CountSketch<S, FastPath, H>
where
    S: MatrixStorage + crate::FastPathHasher<H>,
    S::Counter: CountSketchCounter,
{
    /// Inserts an observation using the combined hash optimization.
    #[inline(always)]
    pub fn update(&mut self, value: &DataInput) {
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
    pub fn update_weighted(&mut self, value: &DataInput, weight: S::Counter) {
        let hashed_val = <S as FastPathHasher<H>>::hash_for_matrix(&self.counts, value);
        self.counts.fast_insert(
            |counter, value, row| {
                let sign = hashed_val.sign_for_row(row);
                let delta = if sign > 0 { *value } else { -*value };
                *counter += delta;
            },
            weight,
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
    pub fn update_with_hash(&mut self, hashed_val: &H::HashType) {
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
    pub fn update_weighted_with_hash(&mut self, hashed_val: &H::HashType, weight: S::Counter) {
        self.counts.fast_insert(
            |counter, value, row| {
                let sign = hashed_val.sign_for_row(row);
                let delta = if sign > 0 { *value } else { -*value };
                *counter += delta;
            },
            weight,
            hashed_val,
        );
    }

    /// Returns the frequency estimate using a pre-computed hash value.
    #[inline(always)]
    pub fn estimate_with_hash(&self, hashed_val: &H::HashType) -> f64 {
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
impl<M, H: SketchHasher> CountSketch<Vector2D<i32>, M, H> {
    /// Human-friendly helper used by the serializer demo binaries.
    pub fn debug(&self) {
        for row in 0..self.counts.rows() {
            println!("row {row}: {:?}", self.counts.row_slice(row));
        }
    }
}

// Nitro sampling helpers for fast-path Count.
impl<H: SketchHasher> CountSketch<Vector2D<i32>, FastPath, H> {
    /// Enables Nitro sampling with the provided rate.
    pub fn enable_nitro(&mut self, sampling_rate: f64) {
        self.counts.enable_nitro(sampling_rate);
    }

    /// Enables Nitro sampling with a reproducible, seed-selected schedule.
    ///
    /// See [`crate::Nitro::with_seed`]: the unseeded path starts every
    /// sketch at the same point in the skip table, so two sketches at the same
    /// rate admit the same subset and are not independent trials.
    pub fn enable_nitro_with_seed(&mut self, sampling_rate: f64, seed: u64) {
        self.counts.enable_nitro_with_seed(sampling_rate, seed);
    }

    /// Inserts an observation through Nitro's per-row sampling schedule.
    ///
    /// Cells and signs are derived exactly as a plain `update` derives them —
    /// `FastPathHasher::hash_for_matrix`, then `col_for_row` and
    /// `sign_for_row` — so `estimate` reads back what this wrote. The hash is
    /// computed once per observation, and the admitted rows are collected into
    /// an inline buffer, so the hot path does not allocate.
    #[inline(always)]
    pub fn nitro_update(&mut self, value: &DataInput) {
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
            let signed = if hashed.sign_for_row(row) > 0 {
                nitro_delta_saturated_i32(weight)
            } else {
                -nitro_delta_saturated_i32(weight)
            };
            self.counts
                .update_one_counter(row, col, |a: &mut i32, b: i32| *a += b, signed);
        }
    }
}

// NitroTarget integration for fast-path Count.
impl<H: SketchHasher> NitroTarget for CountSketch<Vector2D<i32>, FastPath, H> {
    #[inline(always)]
    fn rows(&self) -> usize {
        self.counts.rows()
    }

    #[inline(always)]
    fn update_row(&mut self, row: usize, hashed: u128, delta: u64) {
        let bit = (hashed >> (127 - row)) & 1;
        let sign = (bit << 1) as i32 - 1;
        self.counts.update_by_row(
            row,
            hashed,
            |a, b| *a += b,
            sign * nitro_delta_saturated_i32(delta),
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
            let sign = <H::HashType as MatrixFastHash>::sign_for_row(&hashed, row);
            self.counts.update_one_counter(
                row,
                col,
                |a, b| *a += b,
                sign * nitro_delta_saturated_i32(delta),
            );
        }
    }
}

use crate::octo_delta::{
    CountSketchCellDelta, KeyedCountSketchCellDelta, OCTO_COUNT_SKETCH_DEFAULT_THRESHOLD,
    OCTO_MAX_PROMOTION_THRESHOLD,
};

impl<S: MatrixStorage<Counter = i32>, H: SketchHasher> CountSketch<S, RegularPath, H> {
    /// Inserts a value, emitting a delta at every promotion of the default
    /// threshold `OCTO_COUNT_SKETCH_DEFAULT_THRESHOLD`.
    #[inline(always)]
    pub fn update_emit_delta(
        &mut self,
        value: &DataInput,
        emit: &mut impl FnMut(CountSketchCellDelta),
    ) {
        self.update_emit_delta_with_threshold(value, OCTO_COUNT_SKETCH_DEFAULT_THRESHOLD, emit);
    }

    /// Inserts a value, emitting a delta and clearing the counter each time
    /// `|counter|` reaches `threshold`. Count sketch counters are signed, so
    /// the magnitude is what the threshold is applied to (§4.4).
    #[inline(always)]
    pub fn update_emit_delta_with_threshold(
        &mut self,
        value: &DataInput,
        threshold: u32,
        emit: &mut impl FnMut(CountSketchCellDelta),
    ) {
        let threshold = threshold.clamp(1, OCTO_MAX_PROMOTION_THRESHOLD);
        let rows = self.counts.rows();
        let cols = self.counts.cols();
        for r in 0..rows {
            let hashed = hash64_seeded(r, value);
            let col = ((hashed & LOWER_32_MASK) as usize) % cols;
            let sign: i32 = if ((hashed >> 63) & 1) == 1 { 1 } else { -1 };
            self.counts.increment_by_row(r, col, sign);
            let current = self.counts.query_one_counter(r, col);
            if current.unsigned_abs() >= threshold {
                emit(CountSketchCellDelta {
                    row: r as u32,
                    col: col as u32,
                    count: current,
                });
                self.counts.update_one_counter(r, col, |c, _| *c = 0, ());
            }
        }
    }

    /// As `update_emit_delta_with_threshold`, but every delta carries the flow
    /// key so an aggregator can maintain a heavy-hitter heap.
    pub fn update_emit_keyed_delta_with_threshold(
        &mut self,
        value: &DataInput,
        threshold: u32,
        emit: &mut impl FnMut(KeyedCountSketchCellDelta),
    ) {
        self.update_emit_delta_with_threshold(value, threshold, &mut |delta| {
            emit(KeyedCountSketchCellDelta {
                key: value.to_owned_input(),
                delta,
            })
        });
    }
}

impl<S, H: SketchHasher> CountSketch<S, FastPath, H>
where
    S: MatrixStorage<Counter = i32> + FastPathHasher<H>,
{
    /// Inserts a value using the fast path, emitting a delta at every
    /// promotion of the default threshold `OCTO_COUNT_SKETCH_DEFAULT_THRESHOLD`.
    #[inline(always)]
    pub fn update_emit_delta(
        &mut self,
        value: &DataInput,
        emit: &mut impl FnMut(CountSketchCellDelta),
    ) {
        self.update_emit_delta_with_threshold(value, OCTO_COUNT_SKETCH_DEFAULT_THRESHOLD, emit);
    }

    /// Fast-path counterpart of the regular-path threshold API.
    #[inline(always)]
    pub fn update_emit_delta_with_threshold(
        &mut self,
        value: &DataInput,
        threshold: u32,
        emit: &mut impl FnMut(CountSketchCellDelta),
    ) {
        let threshold = threshold.clamp(1, OCTO_MAX_PROMOTION_THRESHOLD);
        let hashed_val = <S as FastPathHasher<H>>::hash_for_matrix(&self.counts, value);
        let rows = self.counts.rows();
        let cols = self.counts.cols();
        for r in 0..rows {
            let col = hashed_val.col_for_row(r, cols);
            let sign = hashed_val.sign_for_row(r);
            self.counts.increment_by_row(r, col, sign);
            let current = self.counts.query_one_counter(r, col);
            if current.unsigned_abs() >= threshold {
                emit(CountSketchCellDelta {
                    row: r as u32,
                    col: col as u32,
                    count: current,
                });
                self.counts.update_one_counter(r, col, |c, _| *c = 0, ());
            }
        }
    }

    /// Fast-path counterpart of `update_emit_keyed_delta_with_threshold`.
    pub fn update_emit_keyed_delta_with_threshold(
        &mut self,
        value: &DataInput,
        threshold: u32,
        emit: &mut impl FnMut(KeyedCountSketchCellDelta),
    ) {
        self.update_emit_delta_with_threshold(value, threshold, &mut |delta| {
            emit(KeyedCountSketchCellDelta {
                key: value.to_owned_input(),
                delta,
            })
        });
    }
}

impl<S: MatrixStorage, Mode, H: SketchHasher> CountSketch<S, Mode, H>
where
    S::Counter: Copy + std::ops::AddAssign + From<i32>,
{
    /// Applies a previously emitted delta to this sketch.
    pub fn apply_delta(&mut self, delta: CountSketchCellDelta) {
        self.counts.increment_by_row(
            delta.row as usize,
            delta.col as usize,
            S::Counter::from(delta.count),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{all_counter_zero_i32, counter_index, sample_zipf_u64};
    use crate::{DataInput, MATRIX_MAX_ROWS, hash64_seeded};
    use std::collections::HashMap;
    use std::panic::catch_unwind;

    /// `with_dimensions` and `from_storage` panic on both paths exactly when
    /// `check_shape` refuses the shape: a zero dimension or a counter count
    /// past `usize`. Any row count builds.
    #[test]
    fn constructors_panic_exactly_when_check_shape_refuses() {
        type Regular = CountSketch<Vector2D<i64>, RegularPath>;
        type Fast = CountSketch<Vector2D<i64>, FastPath>;
        for (rows, cols) in [
            (0, 8),
            (1, 0),
            (1, 1),
            (3, 64),
            (MATRIX_MAX_ROWS, 7),
            (MATRIX_MAX_ROWS + 1, 7),
            (2 * MATRIX_MAX_ROWS, 64),
        ] {
            let accepted = CountSketch::check_shape(rows, cols).is_ok();
            assert_eq!(accepted, rows >= 1 && cols >= 1);
            let shape = format!("{rows}x{cols}");
            assert_eq!(
                catch_unwind(|| Regular::with_dimensions(rows, cols)).is_ok(),
                accepted,
                "{shape}"
            );
            assert_eq!(
                catch_unwind(|| Fast::with_dimensions(rows, cols)).is_ok(),
                accepted,
                "{shape}"
            );
            let storage = || Vector2D::filled(rows, cols, 0i64);
            assert_eq!(
                catch_unwind(|| Regular::from_storage(storage())).is_ok(),
                accepted,
                "{shape}"
            );
            assert_eq!(
                catch_unwind(|| Fast::from_storage(storage())).is_ok(),
                accepted,
                "{shape}"
            );
        }
        let problem = CountSketch::check_shape(2, usize::MAX).expect_err("overflow");
        assert!(problem.contains("overflow usize"), "got {problem}");
        assert!(catch_unwind(|| Regular::with_dimensions(2, usize::MAX)).is_err());
    }

    /// `clear` returns the Nitro sampler to its start: a cleared sketch fed
    /// more updates equals a fresh one fed only those.
    #[test]
    fn clear_restarts_the_nitro_sampler() {
        for (rate, seed) in [(0.3, None), (0.25, Some(11u64))] {
            let build = || {
                let mut sketch = CountSketch::<Vector2D<i32>, FastPath>::with_dimensions(5, 128);
                match seed {
                    Some(seed) => sketch.enable_nitro_with_seed(rate, seed),
                    None => sketch.enable_nitro(rate),
                }
                sketch
            };
            let mut cleared = build();
            for i in 0..3_000u64 {
                cleared.nitro_update(&DataInput::U64(i % 97));
            }
            cleared.clear();
            let mut fresh = build();
            for i in 0..3_000u64 {
                let key = DataInput::U64(i.wrapping_mul(31) % 211);
                cleared.nitro_update(&key);
                fresh.nitro_update(&key);
            }
            assert_eq!(
                cleared.as_storage().as_slice(),
                fresh.as_storage().as_slice(),
                "rate {rate}, seed {seed:?}"
            );
        }
    }

    /// `clear` zeroes every counter and keeps the shape; a cleared sketch then
    /// counts like a new one.
    #[test]
    fn clear_zeroes_the_counters_and_keeps_the_shape() {
        let mut sketch = CountSketch::<Vector2D<f64>, RegularPath>::with_dimensions(5, 32);
        for i in 0..1_000u64 {
            sketch.update_weighted(&DataInput::U64(i % 41), 1.5);
        }
        sketch.clear();
        assert_eq!((sketch.rows(), sketch.cols()), (5, 32));
        assert!(sketch.as_storage().as_slice().iter().all(|&c| c == 0.0));
        sketch.update(&DataInput::U64(9));
        let mut fresh = CountSketch::<Vector2D<f64>, RegularPath>::with_dimensions(5, 32);
        fresh.update(&DataInput::U64(9));
        assert_eq!(
            sketch.as_storage().as_slice(),
            fresh.as_storage().as_slice()
        );
    }

    #[test]
    fn count_sketch_child_update_emits_at_threshold() {
        let mut child = CountSketch::<Vector2D<i32>, RegularPath>::with_dimensions(3, 64);
        let key = DataInput::U64(99);
        let mut deltas: Vec<CountSketchCellDelta> = Vec::new();

        for _ in 0..200 {
            child.update_emit_delta(&key, &mut |d| deltas.push(d));
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
    ) -> (CountSketch, HashMap<u64, i32>) {
        let mut truth = HashMap::<u64, i32>::new();
        let mut sketch = CountSketch::<Vector2D<i32>, RegularPath>::with_dimensions(rows, cols);

        for value in sample_zipf_u64(domain, exponent, samples, seed) {
            let key = DataInput::U64(value);
            sketch.update(&key);
            *truth.entry(value).or_insert(0) += 1;
        }

        (sketch, truth)
    }

    #[test]
    fn default_initializes_expected_dimensions() {
        let cs = CountSketch::<Vector2D<i32>, RegularPath>::default();
        assert_eq!(cs.rows(), 3);
        assert_eq!(cs.cols(), 4096);
        all_counter_zero_i32(cs.as_storage());
    }

    #[test]
    fn with_dimensions_uses_custom_sizes() {
        let cs = CountSketch::<Vector2D<i32>, RegularPath>::with_dimensions(3, 17);
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
    fn update_writes_signed_counters_per_row() {
        let mut sketch = CountSketch::<Vector2D<i32>, RegularPath>::with_dimensions(3, 64);
        let key = DataInput::Str("alpha");

        sketch.update(&key);

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
    fn fast_path_update_produces_consistent_estimates() {
        let mut fast = CountSketch::<Vector2D<i32>, FastPath>::with_dimensions(4, 128);

        let keys = vec![
            DataInput::Str("alpha"),
            DataInput::Str("beta"),
            DataInput::Str("gamma"),
            DataInput::Str("delta"),
            DataInput::Str("epsilon"),
        ];

        for key in &keys {
            fast.update(key);
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
    fn update_produces_consistent_estimates() {
        let mut sketch = CountSketch::<Vector2D<i32>, RegularPath>::with_dimensions(3, 64);

        let keys = vec![
            DataInput::Str("alpha"),
            DataInput::Str("beta"),
            DataInput::Str("gamma"),
            DataInput::Str("delta"),
            DataInput::Str("epsilon"),
        ];

        for key in &keys {
            sketch.update(key);
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
        let mut sketch = CountSketch::<Vector2D<i32>, RegularPath>::with_dimensions(3, 64);
        let key = DataInput::Str("theta");

        let repeats = 37;
        for _ in 0..repeats {
            sketch.update(&key);
        }

        let estimate = sketch.estimate(&key);
        assert!(
            (estimate - repeats as f64).abs() < f64::EPSILON,
            "expected estimate {repeats}, got {estimate}"
        );
    }

    #[test]
    fn fast_path_recovers_repeated_insertions() {
        let mut sketch = CountSketch::<Vector2D<i32>, FastPath>::with_dimensions(4, 256);
        let keys = vec![
            DataInput::Str("alpha"),
            DataInput::Str("beta"),
            DataInput::Str("gamma"),
            DataInput::Str("delta"),
            DataInput::Str("epsilon"),
        ];

        for _ in 0..5 {
            for key in &keys {
                sketch.update(key);
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
        let mut left = CountSketch::<Vector2D<i32>, RegularPath>::with_dimensions(2, 32);
        let mut right = CountSketch::<Vector2D<i32>, RegularPath>::with_dimensions(2, 32);
        let key = DataInput::Str("delta");

        left.update(&key);
        right.update(&key);
        right.update(&key);

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
    fn f64_estimate_is_the_unrounded_median_and_takes_nan() {
        let key = DataInput::Str("alpha");
        let mut sketch = CountSketch::<Vector2D<f64>, RegularPath>::with_dimensions(3, 64);
        sketch.update_weighted(&key, 1.5);
        assert_eq!(sketch.estimate(&key), 1.5);

        let nan = CountSketch::<Vector2D<f64>, RegularPath>::from_storage(Vector2D::from_fn(
            3,
            4,
            |_, _| f64::NAN,
        ));
        assert!(nan.estimate(&key).is_nan());
    }

    #[test]
    #[should_panic(expected = "dimension mismatch while merging Count Sketches")]
    fn merge_requires_matching_dimensions() {
        let mut left = CountSketch::<Vector2D<i32>, RegularPath>::with_dimensions(2, 32);
        let right = CountSketch::<Vector2D<i32>, RegularPath>::with_dimensions(3, 32);
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
        let mut sk = CountSketch::<Vector2D<i32>, RegularPath>::default();
        // Insert values 0..9 once using the regular path.
        for i in 0..10 {
            sk.update(&DataInput::I32(i));
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
            sk.update(&DataInput::I32(i));
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
        let mut sk = CountSketch::<Vector2D<i32>, FastPath>::default();
        // Insert values 0..9 once using the fast path.
        for i in 0..10 {
            sk.update(&DataInput::I32(i));
        }

        // Build the expected counter array by mirroring the fast-path hashing logic.
        let storage = sk.as_storage();
        let rows = storage.rows();
        let cols = storage.cols();
        let mask_bits = storage.mask_bits();
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

    #[test]
    fn countsketch_error_stays_rank_independent_across_frequency_deciles() {
        const ROWS: usize = 5;
        const COLS: usize = 4096;
        const DOMAIN: usize = 8192;
        const SAMPLES: usize = 200_000;
        const STREAM_SEED: u64 = 1007;
        const DECILES: usize = 10;

        let mut truth = HashMap::<u64, i64>::new();
        let mut cs = CountSketch::<Vector2D<i64>, RegularPath>::with_dimensions(ROWS, COLS);
        for value in sample_zipf_u64(DOMAIN, 1.1, SAMPLES, STREAM_SEED) {
            cs.update(&DataInput::U64(value));
            *truth.entry(value).or_insert(0) += 1;
        }

        let mut pairs: Vec<(u64, i64)> = truth.into_iter().collect();
        pairs.sort_by_key(|(_, c)| *c);
        let per = pairs.len() / DECILES;
        let means: Vec<f64> = (0..DECILES)
            .map(|d| {
                let slice = &pairs[d * per..(d + 1) * per];
                slice
                    .iter()
                    .map(|(k, c)| (cs.estimate(&DataInput::U64(*k)) - *c as f64).abs())
                    .sum::<f64>()
                    / slice.len() as f64
            })
            .collect();
        let lo = means.iter().cloned().fold(f64::INFINITY, f64::min);
        let hi = means.iter().cloned().fold(0.0f64, f64::max);
        assert!(
            hi <= lo * 3.0,
            "Count Sketch mean |error| per frequency decile spans {lo:.1}..{hi:.1} ({:.2}x), \
             beyond the documented 3x empirical band — the error has started tracking key \
             frequency, which Count Sketch's L2 guarantee says it must not. \
             stream_seed={STREAM_SEED} rows={ROWS} cols={COLS} deciles={means:?}",
            hi / lo
        );
    }
}
