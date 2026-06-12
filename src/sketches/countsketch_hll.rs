//! Count Sketch + HyperLogLog hybrid (`CountHll`).
//!
//! A frequency-style hashing layout (the Count Sketch row/column grid) where
//! **every `(row, col)` bucket is a small HyperLogLog** instead of a single
//! signed counter. Each item is routed to one column per row (exactly like a
//! Count Sketch) and recorded into that bucket's HLL registers. This answers
//! *distinct-count* questions rather than frequency questions:
//!
//! - [`CountHll::estimate`] — the number of **distinct** items sharing a key's
//!   buckets (median across rows, to suppress collision noise).
//! - [`CountHll::estimate_total_cardinality`] — total stream cardinality,
//!   exploiting the fact that, within a row, items are partitioned across
//!   columns, so the per-bucket distinct counts sum to the total.
//!
//! Storage is a [`Vector3D<u8>`] of shape `rows x cols x (2^precision)`: the
//! third dimension is the HLL register array for each bucket.
//!
//! The HyperLogLog register/rank math mirrors [`crate::sketches::hll`] (classic
//! estimator with small/large-range corrections).
//!
//! References:
//! - Charikar, Chen & Farach-Colton, "Finding Frequent Items in Data Streams,"
//!   ICALP 2002.
//! - Flajolet, Fusy, Gandouet & Meunier, "HyperLogLog: the analysis of a
//!   near-optimal cardinality estimation algorithm," 2007.

use crate::{DataInput, DefaultXxHasher, SketchHasher, Vector3D};
use rmp_serde::{
    decode::Error as RmpDecodeError, encode::Error as RmpEncodeError, from_slice, to_vec_named,
};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

const DEFAULT_ROW_NUM: usize = 4;
const DEFAULT_COL_NUM: usize = 64;
const DEFAULT_PRECISION: u32 = 8;
const LOWER_32_MASK: u64 = (1u64 << 32) - 1;

/// A Count Sketch grid whose cells are per-bucket HyperLogLog sketches.
///
/// `rows` independent hash rows each route an item to one of `cols` columns; the
/// selected `(row, col)` bucket holds a `2^precision`-register HyperLogLog that
/// records the item. See the [module docs](crate::sketches::countsketch_hll) for
/// the supported queries.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct CountHll<H: SketchHasher = DefaultXxHasher> {
    buckets: Vector3D<u8>,
    rows: usize,
    cols: usize,
    precision: u32,
    #[serde(skip)]
    _hasher: PhantomData<H>,
}

impl Default for CountHll<DefaultXxHasher> {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM, DEFAULT_PRECISION)
    }
}

impl<H: SketchHasher> CountHll<H> {
    /// Creates a sketch with the requested grid size and per-bucket HLL precision.
    ///
    /// `precision` is the HyperLogLog precision `p`; each bucket holds `2^p`
    /// registers. Panics if `precision` is not in `1..=18` (the range for which
    /// the register layout and estimator are well-defined here).
    pub fn with_dimensions(rows: usize, cols: usize, precision: u32) -> Self {
        assert!(
            (1..=18).contains(&precision),
            "precision must be in 1..=18, got {precision}"
        );
        assert!(rows > 0 && cols > 0, "rows and cols must be non-zero");
        let depth = 1usize << precision;
        let mut buckets = Vector3D::init(rows, cols, depth);
        buckets.fill(0);
        Self {
            buckets,
            rows,
            cols,
            precision,
            _hasher: PhantomData,
        }
    }

    /// Number of hash rows.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Number of columns per row.
    pub fn cols(&self) -> usize {
        self.cols
    }

    /// HyperLogLog precision `p` (each bucket has `2^p` registers).
    pub fn precision(&self) -> u32 {
        self.precision
    }

    /// Number of HLL registers per `(row, col)` bucket.
    pub fn registers_per_bucket(&self) -> usize {
        self.buckets.depth()
    }

    /// Exposes the backing storage for inspection/testing.
    pub fn as_storage(&self) -> &Vector3D<u8> {
        &self.buckets
    }

    /// Mutable access used internally for testing scenarios.
    pub fn as_storage_mut(&mut self) -> &mut Vector3D<u8> {
        &mut self.buckets
    }

    /// Seed used for the per-bucket HLL register hash.
    ///
    /// Distinct from the per-row column-selection seeds (`0..rows`), so the
    /// register hash is independent of column placement.
    #[inline(always)]
    fn hll_seed(&self) -> usize {
        self.rows
    }

    /// Computes the `(register index, rank)` pair for a value, shared by every
    /// bucket the value lands in.
    #[inline(always)]
    fn register_and_rank(&self, value: &DataInput) -> (usize, u8) {
        let p = self.precision;
        let register_bits = 64 - p;
        let p_mask = (1u64 << p) - 1;
        let hll_hash = H::hash64_seeded(self.hll_seed(), value);
        let index = ((hll_hash >> register_bits) & p_mask) as usize;
        let rank = ((hll_hash << p) + p_mask).leading_zeros() as u8 + 1;
        (index, rank)
    }

    /// Inserts one observation: route to one column per row and record the value
    /// in that bucket's HyperLogLog.
    pub fn insert(&mut self, value: &DataInput) {
        let cols = self.cols;
        let (index, rank) = self.register_and_rank(value);
        for r in 0..self.rows {
            let col_hash = H::hash64_seeded(r, value);
            let col = ((col_hash & LOWER_32_MASK) as usize) % cols;
            let bucket = self.buckets.bucket_slice_mut(r, col);
            if rank > bucket[index] {
                bucket[index] = rank;
            }
        }
    }

    /// Inserts each value in the slice.
    pub fn insert_many(&mut self, values: &[DataInput]) {
        for value in values {
            self.insert(value);
        }
    }

    /// Estimates the number of distinct items sharing `value`'s buckets.
    ///
    /// Each of the `rows` buckets the value maps to estimates the distinct count
    /// of all items routed there (the value plus collisions); the median across
    /// rows suppresses collision over-counting.
    pub fn estimate(&self, value: &DataInput) -> f64 {
        let cols = self.cols;
        let mut estimates = Vec::with_capacity(self.rows);
        for r in 0..self.rows {
            let col_hash = H::hash64_seeded(r, value);
            let col = ((col_hash & LOWER_32_MASK) as usize) % cols;
            estimates.push(estimate_bucket(self.buckets.bucket_slice(r, col)));
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

    /// Estimates the total number of distinct items in the stream.
    ///
    /// Within a single row, every item is routed to exactly one column, so the
    /// columns partition the stream and the per-bucket distinct counts sum to the
    /// total cardinality. The median of the per-row sums is returned for
    /// stability across rows.
    pub fn estimate_total_cardinality(&self) -> f64 {
        let mut per_row = Vec::with_capacity(self.rows);
        for r in 0..self.rows {
            let mut row_sum = 0.0;
            for c in 0..self.cols {
                row_sum += estimate_bucket(self.buckets.bucket_slice(r, c));
            }
            per_row.push(row_sum);
        }
        if per_row.is_empty() {
            return 0.0;
        }
        per_row.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
        let mid = per_row.len() / 2;
        if per_row.len() % 2 == 1 {
            per_row[mid]
        } else {
            (per_row[mid - 1] + per_row[mid]) / 2.0
        }
    }

    /// Merges another sketch by taking the element-wise register maximum.
    ///
    /// Both sketches must share the same grid dimensions and precision.
    pub fn merge(&mut self, other: &Self) {
        assert_eq!(
            (self.rows, self.cols, self.precision),
            (other.rows, other.cols, other.precision),
            "dimension/precision mismatch while merging CountHll sketches"
        );
        for (reg, other_reg) in self
            .buckets
            .as_mut_slice()
            .iter_mut()
            .zip(other.buckets.as_slice().iter().copied())
        {
            if other_reg > *reg {
                *reg = other_reg;
            }
        }
    }

    /// Serializes the sketch into MessagePack bytes.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        to_vec_named(self)
    }

    /// Deserializes a sketch from MessagePack bytes.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        from_slice(bytes)
    }
}

/// Classic HyperLogLog cardinality estimate over a single register slice.
///
/// Mirrors [`crate::sketches::hll`]'s classic estimator, including the
/// small-range linear-counting and large-range corrections.
fn estimate_bucket(registers: &[u8]) -> f64 {
    let m = registers.len() as f64;
    let alpha_m = 0.7213 / (1.0 + 1.079 / m);
    let mut z = 0.0;
    for &reg_val in registers {
        z += 2f64.powi(-(reg_val as i32));
    }
    let mut est = alpha_m * m * m / z;
    if est <= m * 5.0 / 2.0 {
        let zero_count = registers.iter().filter(|&&reg| reg == 0).count();
        if zero_count != 0 {
            est = m * (m / zero_count as f64).ln();
        }
    } else if est > 143_165_576.533 {
        let correction_aux = i32::MAX as f64;
        est = -correction_aux * (1.0 - est / correction_aux).ln();
    }
    est
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DataInput;

    fn distinct_keys(count: u64) -> Vec<DataInput<'static>> {
        (0..count).map(DataInput::U64).collect()
    }

    #[test]
    fn default_initializes_expected_dimensions() {
        let sk = CountHll::default();
        assert_eq!(sk.rows(), DEFAULT_ROW_NUM);
        assert_eq!(sk.cols(), DEFAULT_COL_NUM);
        assert_eq!(sk.precision(), DEFAULT_PRECISION);
        assert_eq!(sk.registers_per_bucket(), 1 << DEFAULT_PRECISION);
        // Every register starts at zero.
        assert!(sk.as_storage().as_slice().iter().all(|&r| r == 0));
    }

    #[test]
    fn with_dimensions_uses_custom_sizes() {
        let sk = CountHll::<DefaultXxHasher>::with_dimensions(3, 17, 6);
        assert_eq!(sk.rows(), 3);
        assert_eq!(sk.cols(), 17);
        assert_eq!(sk.precision(), 6);
        assert_eq!(sk.registers_per_bucket(), 64);
        assert_eq!(sk.as_storage().len(), 3 * 17 * 64);
    }

    #[test]
    fn repeated_key_counts_as_one_distinct() {
        let mut sk = CountHll::<DefaultXxHasher>::default();
        let key = DataInput::Str("alpha");
        for _ in 0..500 {
            sk.insert(&key);
        }
        // Only one distinct item touched these buckets, so the distinct estimate
        // should sit close to 1.
        let est = sk.estimate(&key);
        assert!(est < 3.0, "expected near-1 distinct estimate, got {est}");
    }

    #[test]
    fn estimate_total_cardinality_tracks_distinct_count() {
        let mut sk = CountHll::<DefaultXxHasher>::default();
        let n = 4000u64;
        for key in &distinct_keys(n) {
            sk.insert(key);
        }
        let est = sk.estimate_total_cardinality();
        let truth = n as f64;
        let rel_err = (est - truth).abs() / truth;
        assert!(
            rel_err < 0.25,
            "total cardinality estimate {est} too far from {truth} (rel_err {rel_err})"
        );
    }

    #[test]
    fn merge_takes_register_max_and_unions_cardinality() {
        let mut a = CountHll::<DefaultXxHasher>::default();
        let mut b = CountHll::<DefaultXxHasher>::default();
        for key in (0..2000u64).map(DataInput::U64) {
            a.insert(&key);
        }
        for key in (2000..4000u64).map(DataInput::U64) {
            b.insert(&key);
        }
        let a_card = a.estimate_total_cardinality();

        a.merge(&b);
        let merged = a.estimate_total_cardinality();

        assert!(
            merged > a_card,
            "merged cardinality {merged} should exceed single-set {a_card}"
        );
        let rel_err = (merged - 4000.0).abs() / 4000.0;
        assert!(
            rel_err < 0.25,
            "merged cardinality {merged} too far from 4000 (rel_err {rel_err})"
        );
    }

    #[test]
    fn serialize_round_trip_preserves_estimates() {
        let mut sk = CountHll::<DefaultXxHasher>::with_dimensions(4, 32, 8);
        for key in &distinct_keys(1500) {
            sk.insert(key);
        }
        let bytes = sk.serialize_to_bytes().expect("serialize");
        let restored = CountHll::<DefaultXxHasher>::deserialize_from_bytes(&bytes).expect("decode");

        assert_eq!(sk.rows(), restored.rows());
        assert_eq!(sk.cols(), restored.cols());
        assert_eq!(sk.precision(), restored.precision());
        assert_eq!(sk.as_storage().as_slice(), restored.as_storage().as_slice());
        assert_eq!(
            sk.estimate_total_cardinality(),
            restored.estimate_total_cardinality()
        );
    }
}
