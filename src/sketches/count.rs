use crate::{SketchInput, Vector1D, Vector2D, hash_it_to_128};
use rmp_serde::{
    decode::Error as RmpDecodeError, encode::Error as RmpEncodeError, from_slice, to_vec_named,
};
use serde::{Deserialize, Serialize};

const DEFAULT_ROW_NUM: usize = 3;
const DEFAULT_COL_NUM: usize = 4096;
const LOWER_32_MASK: u64 = (1u64 << 32) - 1;

/// Count Sketch based on Common structure
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Count {
    counts: Vector2D<i64>,
    row: usize,
    col: usize,
}

impl Default for Count {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM)
    }
}

impl Count {
    /// Creates a sketch with the requested number of rows and columns.
    pub fn with_dimensions(rows: usize, cols: usize) -> Self {
        let mut sk = Count {
            counts: Vector2D::init(rows, cols),
            row: rows,
            col: cols,
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

    /// Inserts an observation with standard Count Sketch updating algorithm.
    pub fn insert(&mut self, value: &SketchInput) {
        for r in 0..self.row {
            let hashed = hash_it_to_128(r, value);
            let col = ((hashed as u64 & LOWER_32_MASK) as usize) % self.col;
            let bit = ((hashed >> (127)) & 1) as i64;
            let sign_bit = -(1 - 2 * bit);
            self.counts
                .update_one_counter(r, col, |a, b| *a += sign_bit * b, 1_i64);
        }
    }

    /// Inserts an observation with hash optimization of Count Sketch updating algorithm.
    /// On some architecture, this optimization may not have effect for small sketch
    /// Inferred reason is the u128 is expensive
    pub fn fast_insert(&mut self, value: &SketchInput) {
        let hashed_val = hash_it_to_128(0, value);
        self.fast_insert_with_hash_value(hashed_val);
    }

    /// Inserts an observation with hash optimization of Count Sketch updating algorithm.
    /// The hash may be reused with other sketches
    pub fn fast_insert_with_hash_value(&mut self, hashed_val: u128) {
        let mask_bits = self.counts.get_mask_bits() as usize;
        let mask = (1u128 << mask_bits) - 1;
        let mut shift_amount = 0;
        let mut sign_bit_pos = 127;
        for _r in 0..self.row {
            let hashed = (hashed_val >> shift_amount) & mask;
            let col = (hashed as usize) % self.col;
            let bit = ((hashed_val >> sign_bit_pos) & 1) as i64;
            let sign_bit = -(1 - 2 * bit);
            self.counts
                .update_one_counter(_r, col, |a, b| *a += sign_bit * b, 1_i64);
            shift_amount += mask_bits;
            sign_bit_pos -= 1;
        }
    }

    /// Returns the frequency estimate for the provided value.
    pub fn estimate(&self, value: &SketchInput) -> f64 {
        let mut estimates = Vec::with_capacity(self.row);
        for r in 0..self.row {
            let hashed = hash_it_to_128(r, value);
            let col = ((hashed as u64 & LOWER_32_MASK) as usize) % self.col;
            let bit = ((hashed >> (127)) & 1) as i64;
            let sign_bit = -(1 - 2 * bit);
            let counter = self.counts.query_one_counter(r, col);
            estimates.push(sign_bit * counter);
        }
        if estimates.is_empty() {
            return 0.0;
        }
        estimates.sort_unstable();
        let mid = estimates.len() / 2;
        if estimates.len() % 2 == 1 {
            estimates[mid] as f64
        } else {
            (estimates[mid - 1] as f64 + estimates[mid] as f64) / 2.0
        }
    }

    /// Returns the frequency estimate for the provided value, with hash optimization.
    /// On some architecture, this optimization may not have effect for small sketch
    /// Inferred reason is the u128 is expensive
    pub fn fast_estimate(&self, value: &SketchInput) -> f64 {
        let hashed_val = hash_it_to_128(0, value);
        let mask_bits = self.counts.get_mask_bits() as usize;
        let mask = (1u128 << mask_bits) - 1;
        let mut estimates = Vec::with_capacity(self.row);
        let mut shift_amount = 0;
        let mut sign_bit_pos = 127;
        for r in 0..self.row {
            let hashed = (hashed_val >> shift_amount) & mask;
            let col = (hashed as usize) % self.col;
            let bit = ((hashed_val >> sign_bit_pos) & 1) as i64;
            let sign_bit = -(1 - 2 * bit);
            let counter = self.counts.query_one_counter(r, col);
            estimates.push(sign_bit * counter);
            shift_amount += mask_bits;
            sign_bit_pos -= 1;
        }
        if estimates.is_empty() {
            return 0.0;
        }
        estimates.sort_unstable();
        let mid = estimates.len() / 2;
        if estimates.len() % 2 == 1 {
            estimates[mid] as f64
        } else {
            (estimates[mid - 1] as f64 + estimates[mid] as f64) / 2.0
        }
    }

    /// Merges another sketch while asserting compatible dimensions.
    pub fn merge(&mut self, other: &Self) {
        assert_eq!(
            (self.row, self.col),
            (other.row, other.col),
            "dimension mismatch while merging CountMin sketches"
        );

        for i in 0..self.row {
            for j in 0..self.col {
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
    pub fn as_storage(&self) -> &Vector2D<i64> {
        &self.counts
    }

    /// Mutable access used internally for testing scenarios.
    pub fn as_storage_mut(&mut self) -> &mut Vector2D<i64> {
        &mut self.counts
    }

    /// Human-friendly helper used by the serializer demo binaries.
    pub fn debug(&self) {
        for row in 0..self.row {
            println!("row {}: {:?}", row, &self.counts.row_slice(row));
        }
    }

    /// Serializes the sketch into MessagePack bytes.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        to_vec_named(self)
    }

    /// Convenience alias matching the previous API.
    pub fn serialize(&self) -> Result<Vec<u8>, RmpEncodeError> {
        self.serialize_to_bytes()
    }

    /// Deserializes a sketch from MessagePack bytes.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        from_slice(bytes)
    }

    /// Convenience alias matching the previous API.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        Self::deserialize_from_bytes(bytes)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CountL2HH {
    counts: Vector2D<i64>,
    l2: Vector1D<i64>,
    row: usize,
    col: usize,
}


impl Default for CountL2HH {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM)
    }
}

impl CountL2HH {
    pub fn with_dimensions(rows: usize, cols: usize) -> Self {
        let mut sk = CountL2HH {
            counts: Vector2D::init(rows, cols),
            l2: Vector1D::init(rows),
            row: rows,
            col: cols,
        };
        sk.counts.fill(0);
        sk.l2.fill(0);
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

    // pub fn insert_once<T: Hash+?Sized>(&mut self, val: &T) {
    //     self.insert_with_count(val, 1);
    // }
    pub fn insert_once(&mut self, val: &SketchInput) {
        self.insert_with_count(val, 1);
    }

    pub fn insert_with_count(&mut self, val: &SketchInput, c: i64) {
        for i in 0..self.row {
            let hashed = hash_it_to_128(i, val);
            let idx = ((hashed as u64 & LOWER_32_MASK) as usize) % self.col;
            let bit = ((hashed >> 127) & 1) as i64;
            let sign_bit = -(1 - 2 * bit);

            let old_value = self.counts[i][idx];
            let new_value = old_value + sign_bit * c;
            self.counts[i][idx] = new_value;

            let old_l2 = self.l2[i];
            let new_l2 = old_l2 + new_value * new_value - old_value * old_value;
            self.l2[i] = new_l2;
        }
    }

    /// Inserts with hash optimization - computes hash once and reuses it.
    pub fn fast_insert_with_count(&mut self, val: &SketchInput, c: i64) {
        let hashed_val = hash_it_to_128(0, val);
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

    pub fn insert_with_count_without_l2(&mut self, val: &SketchInput, c: i64) {
        for i in 0..self.row {
            let hashed = hash_it_to_128(i, val);
            let idx = ((hashed as u64 & LOWER_32_MASK) as usize) % self.col;
            let bit = ((hashed >> 127) & 1) as i64;
            let sign_bit = -(1 - 2 * bit);

            self.counts[i][idx] += sign_bit * c;
        }
    }

    /// Inserts without L2 update using hash optimization.
    pub fn fast_insert_with_count_without_l2(&mut self, val: &SketchInput, c: i64) {
        let hashed_val = hash_it_to_128(0, val);
        self.fast_insert_with_count_without_l2_and_hash(hashed_val, c);
    }

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

            self.counts[i][idx] += sign_bit*c;

            shift_amount += mask_bits;
            sign_bit_pos -= 1;
        }
    }

    pub fn update_and_est(&mut self, val: &SketchInput, c: i64) -> f64 {
        self.insert_with_count(val, c);
        self.get_est(val)
    }

    pub fn update_and_est_without_l2(&mut self, val: &SketchInput, c: i64) -> f64 {
        self.insert_with_count_without_l2(val, c);
        self.get_est(val)
    }

    /// Update and estimate with hash optimization.
    pub fn fast_update_and_est(&mut self, val: &SketchInput, c: i64) -> f64 {
        let hashed_val = hash_it_to_128(0, val);
        self.fast_insert_with_count_and_hash(hashed_val, c);
        self.fast_get_est_with_hash(hashed_val)
    }

    /// Update and estimate without L2 with hash optimization.
    pub fn fast_update_and_est_without_l2(&mut self, val: &SketchInput, c: i64) -> f64 {
        let hashed_val = hash_it_to_128(0, val);
        self.fast_insert_with_count_without_l2_and_hash(hashed_val, c);
        self.fast_get_est_with_hash(hashed_val)
    }

    pub fn get_l2_sqr(&self) -> f64 {
        let mut lst = Vec::new();
        for i in 0..self.row {
            lst.push(self.l2.as_slice()[i]);
        }
        lst.sort();
        // get median
        if self.row == 1 {
            return lst[0] as f64;
        } else if self.row == 2 {
            return (lst[0] + lst[1]) as f64 / 2.0;
        } else if self.row == 3 {
            return lst[1] as f64;
        } else if self.row % 2 == 0 {
            return (lst[self.row / 2] + lst[(self.row / 2) - 1]) as f64 / 2.0;
        } else {
            return lst[self.row / 2] as f64;
        }
    }

    pub fn get_l2(&self) -> f64 {
        let l2 = self.get_l2_sqr();
        return l2.sqrt();
    }

    pub fn get_est(&self, val: &SketchInput) -> f64 {
        let mut lst = Vec::new();
        for i in 0..self.row {
            let hashed = hash_it_to_128(i, val);
            let idx = ((hashed as u64 & LOWER_32_MASK) as usize) % self.col;
            let bit = ((hashed >> 127) & 1) as i64;
            let sign_bit = -(1 - 2 * bit);
            let counter = self.counts.query_one_counter(i, idx);
            lst.push(sign_bit * counter);
        }
        lst.sort();
        // get median
        if self.row == 1 {
            return lst[0] as f64;
        } else if self.row == 2 {
            return (lst[0] + lst[1]) as f64 / 2.0;
        } else if self.row == 3 {
            return lst[1] as f64;
        } else if self.row % 2 == 0 {
            return (lst[self.row / 2] + lst[(self.row / 2) - 1]) as f64 / 2.0;
        } else {
            return lst[self.row / 2] as f64;
        }
    }

    /// Returns the frequency estimate with hash optimization.
    pub fn fast_get_est(&self, val: &SketchInput) -> f64 {
        let hashed_val = hash_it_to_128(0, val);
        self.fast_get_est_with_hash(hashed_val)
    }

    /// Returns the frequency estimate using precomputed hash value.
    pub fn fast_get_est_with_hash(&self, hashed_val: u128) -> f64 {
        let mask_bits = self.counts.get_mask_bits() as usize;
        let mask = (1u128 << mask_bits) - 1;
        let mut lst = Vec::new();
        let mut shift_amount = 0;
        let mut sign_bit_pos = 127;

        for i in 0..self.row {
            let hashed = (hashed_val >> shift_amount) & mask;
            let idx = (hashed as usize) % self.col;
            let bit = ((hashed_val >> sign_bit_pos) & 1) as i64;
            let sign_bit = -(1 - 2 * bit);
            let counter = self.counts.query_one_counter(i, idx);
            lst.push(sign_bit * counter);

            shift_amount += mask_bits;
            sign_bit_pos -= 1;
        }
        lst.sort();
        // get median
        if self.row == 1 {
            return lst[0] as f64;
        } else if self.row == 2 {
            return (lst[0] + lst[1]) as f64 / 2.0;
        } else if self.row == 3 {
            return lst[1] as f64;
        } else if self.row % 2 == 0 {
            return (lst[self.row / 2] + lst[(self.row / 2) - 1]) as f64 / 2.0;
        } else {
            return lst[self.row / 2] as f64;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::sample_zipf_u64;
    use crate::{SketchInput, hash_it_to_128};
    use std::collections::HashMap;
    const LARGE_ROW_NUM: usize = 5;
    const LARGE_COL_NUM: usize = 32768;

    fn counter_index(row: usize, key: &SketchInput, columns: usize) -> usize {
        let hash = hash_it_to_128(row, key);
        ((hash & ((0x1 << 32) - 1)) as usize) % columns
    }

    fn counter_sign(row: usize, key: &SketchInput) -> i64 {
        let hash = hash_it_to_128(row, key);
        if (hash >> 127) & 1 == 1 { 1 } else { -1 }
    }

    fn run_zipf_stream(
        rows: usize,
        cols: usize,
        domain: usize,
        exponent: f64,
        samples: usize,
        seed: u64,
    ) -> (Count, HashMap<u64, i64>) {
        let mut truth = HashMap::<u64, i64>::new();
        let mut sketch = Count::with_dimensions(rows, cols);

        for value in sample_zipf_u64(domain, exponent, samples, seed) {
            let key = SketchInput::U64(value);
            sketch.insert(&key);
            *truth.entry(value).or_insert(0) += 1;
        }

        (sketch, truth)
    }

    #[test]
    fn default_initializes_expected_dimensions() {
        let cs = Count::default();
        assert_eq!(cs.rows(), 3);
        assert_eq!(cs.cols(), 4096);

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
    fn with_dimensions_uses_custom_sizes() {
        let cs = Count::with_dimensions(3, 17);
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
        let mut sketch = Count::with_dimensions(3, 64);
        let key = SketchInput::Str("alpha");

        sketch.insert(&key);

        for row in 0..sketch.rows() {
            let idx = counter_index(row, &key, sketch.cols());
            let expected = counter_sign(row, &key);
            assert_eq!(
                sketch.counts.query_one_counter(row, idx),
                expected,
                "row {} counter mismatch",
                row
            );
        }
    }

    #[test]
    fn fast_insert_produces_consistent_estimates() {
        let mut fast = Count::with_dimensions(4, 128);

        let keys = vec![
            SketchInput::Str("alpha"),
            SketchInput::Str("beta"),
            SketchInput::Str("gamma"),
            SketchInput::Str("delta"),
            SketchInput::Str("epsilon"),
        ];

        for key in &keys {
            fast.fast_insert(key);
        }

        for key in &keys {
            let estimate = fast.fast_estimate(key);
            assert!(
                (estimate - 1.0).abs() < f64::EPSILON,
                "fast estimate for key {:?} should be 1.0, got {}",
                key,
                estimate
            );
        }
    }

    #[test]
    fn insert_produces_consistent_estimates() {
        let mut sketch = Count::with_dimensions(3, 64);

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
                "estimate for key {:?} should be 1.0, got {}",
                key,
                estimate
            );
        }
    }

    #[test]
    fn estimate_recovers_frequency_for_repeated_key() {
        let mut sketch = Count::with_dimensions(3, 64);
        let key = SketchInput::Str("theta");

        let repeats = 37;
        for _ in 0..repeats {
            sketch.insert(&key);
        }

        let estimate = sketch.estimate(&key);
        assert!(
            (estimate - repeats as f64).abs() < f64::EPSILON,
            "expected estimate {}, got {}",
            repeats,
            estimate
        );
    }

    #[test]
    fn fast_path_recovers_repeated_insertions() {
        let mut sketch = Count::with_dimensions(4, 256);
        let keys = vec![
            SketchInput::Str("alpha"),
            SketchInput::Str("beta"),
            SketchInput::Str("gamma"),
            SketchInput::Str("delta"),
            SketchInput::Str("epsilon"),
        ];

        for _ in 0..5 {
            for key in &keys {
                sketch.fast_insert(key);
            }
        }

        for key in &keys {
            let estimate = sketch.fast_estimate(key);
            assert!(
                (estimate - 5.0).abs() < f64::EPSILON,
                "fast estimate for key {:?} should be 5.0, got {}",
                key,
                estimate
            );
        }
    }

    #[test]
    fn merge_adds_counters_element_wise() {
        let mut left = Count::with_dimensions(2, 32);
        let mut right = Count::with_dimensions(2, 32);
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
        let mut left = Count::with_dimensions(2, 32);
        let right = Count::with_dimensions(3, 32);
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
    fn zipf_stream_large_sketch() {
        let (sketch, truth) = run_zipf_stream(
            LARGE_ROW_NUM,
            LARGE_COL_NUM,
            8192,
            1.1,
            200_000,
            0x5eed_c0de,
        );
        let mut within_tolerance = 0usize;
        for (&value, &count) in &truth {
            let estimate = sketch.estimate(&SketchInput::U64(value));
            let rel_error = ((estimate - count as f64).abs()) / (count as f64);
            if rel_error < 0.10 {
                within_tolerance += 1;
            }
        }

        let total = truth.len();
        let accuracy = within_tolerance as f64 / total as f64;
        assert!(
            accuracy >= 0.90,
            "Only {:.2}% of keys within tolerance ({} of {}); expected at least 70%",
            accuracy * 100.0,
            within_tolerance,
            total
        );
    }

    #[test]
    fn zipf_stream_estimates_heavy_hitters_within_twenty_percent() {
        let (sketch, truth) = run_zipf_stream(5, 4096, 8192, 1.1, 200_000, 0x5eed_c0de);
        let mut counts: Vec<(u64, i64)> = truth.iter().map(|(&k, &v)| (k, v)).collect();
        counts.sort_unstable_by(|a, b| b.1.cmp(&a.1));

        let top_k = counts.len().min(25);
        assert!(top_k > 0, "expected at least one heavy hitter");

        for (key, count) in counts.into_iter().take(top_k) {
            let estimate = sketch.estimate(&SketchInput::U64(key));
            let rel_error = ((estimate - count as f64).abs()) / (count as f64);
            assert!(
                rel_error < 0.20,
                "Heavy hitter key {key} truth {count} estimate {estimate} rel error {rel_error:.4}"
            );
        }
    }

    #[test]
    fn count_sketch_round_trip_serialization() {
        let mut sketch = Count::with_dimensions(3, 8);
        sketch.insert(&SketchInput::U64(42));
        sketch.insert(&SketchInput::U64(7));

        let encoded = sketch.serialize_to_bytes().expect("serialize Count");
        assert!(!encoded.is_empty());

        let decoded = Count::deserialize_from_bytes(&encoded).expect("deserialize Count");

        assert_eq!(sketch.rows(), decoded.rows());
        assert_eq!(sketch.cols(), decoded.cols());
        assert_eq!(
            sketch.as_storage().as_slice(),
            decoded.as_storage().as_slice()
        );
    }

    #[test]
    fn countl2hh_estimates_and_l2_are_consistent() {
        let mut sketch = CountL2HH::with_dimensions(3, 32);
        let key = SketchInput::Str("gamma");

        let est_after_first = sketch.update_and_est(&key, 5);
        assert_eq!(est_after_first, 5.0);

        let est_after_second = sketch.update_and_est(&key, -2);
        assert_eq!(est_after_second, 3.0);

        let l2 = sketch.get_l2();
        assert!(l2 >= 3.0, "expected non-trivial l2, got {}", l2);
    }

    #[test]
    fn countl2hh_merge_combines_frequency_vectors() {
        let mut left = CountL2HH::with_dimensions(3, 32);
        let mut right = CountL2HH::with_dimensions(3, 32);
        let key = SketchInput::U32(42);

        left.insert_with_count(&key, 4);
        right.insert_with_count(&key, 9);

        left.merge(&right);
        assert_eq!(left.get_est(&key), 13.0);
    }
}

