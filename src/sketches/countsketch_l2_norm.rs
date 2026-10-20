//! Count Sketch whose rows also keep a running sum of squared counters.
//!
//! [`CountSketchWithL2Norm`] is the per-layer sketch of `UnivMon` and
//! `UnivMonTwoTier`; its L2 norm sets their heavy-hitter threshold.

use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use crate::common::structures::matrix_storage::cols_mask_bits;
use crate::octo_delta::CountSketchCellDelta;
use crate::{
    DataInput, DefaultXxHasher, SketchHasher, Vector1D, Vector2D, compute_median_inline_f64,
};

pub(crate) mod wire;

use crate::common::structures::fixed_structure::{DEFAULT_COLS, DEFAULT_ROWS};

/// Count Sketch augmented with per-row L2 norm tracking for heavy-hitter detection.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(bound = "")]
pub struct CountSketchWithL2Norm<H: SketchHasher = DefaultXxHasher> {
    counts: Vector2D<i64>,
    row_l2_squared: Vector1D<i64>,
    rows: usize,
    cols: usize,
    seed_index: usize,
    #[serde(skip)]
    _hasher: PhantomData<H>,
}

impl CountSketchWithL2Norm {
    /// Checks that a `rows` x `cols` `CountSketchWithL2Norm` fits its one 128-bit key hash.
    ///
    /// Row `r` reads its column from bits `[r * mask_bits, (r + 1) * mask_bits)`
    /// and its sign from bit `127 - r`, where `mask_bits = cols_mask_bits(cols)`.
    /// Each row therefore claims `mask_bits + 1` bits, and the fields are in range
    /// and pairwise disjoint (so every column and sign is an independent hash
    /// draw) exactly while `rows * (mask_bits + 1) <= 128`. Past that, deep rows'
    /// columns reuse shallow rows' sign bits, and from `rows * mask_bits >= 128`
    /// the shift runs off the hash.
    pub(crate) fn check_hash_bits(rows: usize, cols: usize) -> Result<(), String> {
        let mask_bits = cols_mask_bits(cols.max(1)) as usize;
        let bits_required = mask_bits.saturating_add(1).saturating_mul(rows);
        if bits_required > 128 {
            return Err(format!(
                "CountSketchWithL2Norm rows={rows}, cols={cols} needs {bits_required} hash bits \
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
    pub fn cell_for_row(hashed_val: u128, row: usize, cols: usize, mask_bits: u32) -> (usize, i64) {
        let mask = (1u128 << mask_bits) - 1;
        let hashed = (hashed_val >> (row * mask_bits as usize)) & mask;
        let col = (hashed as usize) % cols;
        let bit = ((hashed_val >> (127 - row)) & 1) as i64;
        (col, -(1 - 2 * bit))
    }
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
        if let Err(problem) = CountSketchWithL2Norm::check_hash_bits(rows, cols) {
            panic!("{problem}");
        }
        CountSketchWithL2Norm {
            counts: Vector2D::filled(rows, cols, 0),
            row_l2_squared: Vector1D::filled(rows, 0),
            rows,
            cols,
            seed_index,
            _hasher: PhantomData,
        }
    }

    /// Number of rows in the sketch.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Number of columns in the sketch.
    pub fn cols(&self) -> usize {
        self.cols
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
            (self.rows, self.cols),
            (other.rows, other.cols),
            "dimension mismatch while merging CountSketchWithL2Norm sketches"
        );
        assert_eq!(
            self.seed_index, other.seed_index,
            "seed mismatch while merging CountSketchWithL2Norm sketches"
        );

        for i in 0..self.rows {
            let mut row_l2 = 0_i128;
            for j in 0..self.cols {
                self.counts[i][j] += other.counts[i][j];
                let counter = self.counts[i][j] as i128;
                row_l2 = row_l2.saturating_add(counter.saturating_mul(counter));
            }
            self.row_l2_squared[i] = row_l2.min(i64::MAX as i128) as i64;
        }
    }

    /// Resets all counters and L2 accumulators to zero without reallocating.
    pub fn clear(&mut self) {
        self.counts.fill(0);
        self.row_l2_squared.fill(0);
    }

    /// Inserts with hash optimization - computes hash once and reuses it.
    /// due to the limitation of seeds, use fast_insert only
    pub fn update_weighted(&mut self, val: &DataInput, c: i64) {
        let hashed_val = H::hash128_seeded(self.seed_index, val);
        self.update_weighted_with_hash(hashed_val, c);
    }

    /// Inserts with hash optimization using precomputed hash value.
    pub fn update_weighted_with_hash(&mut self, hashed_val: u128, weight: i64) {
        let mask_bits = self.counts.mask_bits();
        for i in 0..self.rows {
            let (idx, sign_bit) =
                CountSketchWithL2Norm::cell_for_row(hashed_val, i, self.cols, mask_bits);
            let old_value = self.counts.query_one_counter(i, idx);
            self.write_counter(i, idx, old_value + sign_bit * weight);
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
        let old_l2 = self.row_l2_squared.as_slice()[row] as i128;
        let new_l2 = old_l2 + (new_value as i128) * (new_value as i128)
            - (old_value as i128) * (old_value as i128);
        self.row_l2_squared[row] = new_l2.clamp(0, i64::MAX as i128) as i64;
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
        let new_value = self.counts.query_one_counter(row, col) + delta.count as i64;
        self.write_counter(row, col, new_value);
    }

    /// Inserts without L2 update using precomputed hash value.
    pub fn update_weighted_with_hash_without_l2(&mut self, hashed_val: u128, weight: i64) {
        let mask_bits = self.counts.mask_bits() as usize;
        let mask = (1u128 << mask_bits) - 1;
        let mut shift_amount = 0;
        let mut sign_bit_pos = 127;

        for i in 0..self.rows {
            let hashed = (hashed_val >> shift_amount) & mask;
            let idx = (hashed as usize) % self.cols;
            let bit = ((hashed_val >> sign_bit_pos) & 1) as i64;
            let sign_bit = -(1 - 2 * bit);

            self.counts[i][idx] += sign_bit * weight;

            shift_amount += mask_bits;
            sign_bit_pos -= 1;
        }
    }

    /// Update and estimate with hash optimization.
    /// due to the limitation of seeds, use fast_insert only
    pub fn update_weighted_and_estimate(&mut self, val: &DataInput, weight: i64) -> f64 {
        let hashed_val = H::hash128_seeded(self.seed_index, val);
        self.update_weighted_with_hash(hashed_val, weight);
        self.estimate_with_hash(hashed_val)
    }

    /// Returns the median estimate of the squared L2 norm across rows.
    pub fn l2_squared(&self) -> f64 {
        let mut values: Vec<f64> = self.row_l2_squared.as_slice()[..self.rows]
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
        let mut lst = Vec::with_capacity(self.rows);
        let mut shift_amount = 0;
        let mut sign_bit_pos = 127;

        for i in 0..self.rows {
            let hashed = (hashed_val >> shift_amount) & mask;
            let idx = (hashed as usize) % self.cols;
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
            .expect("serialize CountSketchWithL2Norm into MessagePack");
        assert!(!encoded.is_empty(), "serialized bytes should not be empty");
        let data = encoded.clone();

        let decoded: CountSketchWithL2Norm = CountSketchWithL2Norm::from_asapv1(&data)
            .expect("deserialize CountSketchWithL2Norm from MessagePack");

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
