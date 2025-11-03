use rmp_serde::{
    decode::Error as RmpDecodeError, encode::Error as RmpEncodeError, from_slice, to_vec_named,
};
use serde::{Deserialize, Serialize};

use crate::Vector2D;
use crate::{SketchInput, hash_it_to_128};

const DEFAULT_ROW_NUM: usize = 3;
const DEFAULT_COL_NUM: usize = 4096;
const LOWER_32_MASK: u64 = (1u64 << 32) - 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CountMin {
    counts: Vector2D<u64>,
    row: usize,
    col: usize,
}

impl Default for CountMin {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM)
    }
}

impl CountMin {
    /// Creates a sketch with the requested number of rows and columns.
    pub fn with_dimensions(rows: usize, cols: usize) -> Self {
        let mut sk = CountMin {
            counts: Vector2D::init(rows, cols),
            row: rows,
            col: cols,
        };
        sk.counts.fill(0);
        sk
    }

    /// Legacy constructor retaining the historic naming scheme.
    pub fn init_cm_with_row_col(rows: usize, cols: usize) -> Self {
        Self::with_dimensions(rows, cols)
    }

    /// Legacy constructor retaining the historic naming scheme.
    pub fn init_count_min() -> Self {
        Self::default()
    }

    /// Number of rows in the sketch.
    pub fn rows(&self) -> usize {
        self.row
    }

    /// Number of columns in the sketch.
    pub fn cols(&self) -> usize {
        self.col
    }

    /// Inserts an observation while using the standard Count-Min minimum row update rule.
    pub fn insert(&mut self, value: &SketchInput) {
        for r in 0..self.row {
            let hashed = hash_it_to_128(r, value);
            let col = ((hashed as u64 & LOWER_32_MASK) as usize) % self.col;
            self.counts[r][col] += 1;
        }
    }

    /// Inserts an observation using the combined hash optimization.
    pub fn fast_insert(&mut self, value: &SketchInput) {
        let hashed_val = hash_it_to_128(0, value);
        self.fast_insert_with_hash_value(hashed_val);
    }

    /// Inserts an observation using the combined hash optimization.
    /// Hash value can be reused with other sketches.
    pub fn fast_insert_with_hash_value(&mut self, hashed_val: u128) {
        self.counts
            .fast_insert(|a, b, _| *a += b, 1_u64, hashed_val);
    }

    /// Returns the frequency estimate for the provided value.
    pub fn estimate(&self, value: &SketchInput) -> u64 {
        let mut min = u64::MAX;
        for r in 0..self.row {
            let hashed = hash_it_to_128(r, value);
            let col = ((hashed as u64 & LOWER_32_MASK) as usize) % self.col;
            // let idx = row * cols + col;
            // min = min.min(self.counts.query_one_counter(r, col));
            min = min.min(self.counts[r][col]);
        }
        min
    }

    /// Returns the frequency estimate for the provided value, with hash optimization.
    pub fn fast_estimate(&self, value: &SketchInput) -> u64 {
        let hashed_val = hash_it_to_128(0, value);
        self.counts.fast_query_min(hashed_val, |val, _, _| *val)
    }

    /// Returns the frequency estimate using a pre-computed hash value.
    pub fn fast_estimate_with_hash(&self, hashed_val: u128) -> u64 {
        self.counts.fast_query_min(hashed_val, |val, _, _| *val)
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
                self.counts[i][j] += other.counts[i][j];
            }
        }
    }

    /// Exposes the backing matrix for inspection/testing.
    pub fn as_storage(&self) -> &Vector2D<u64> {
        &self.counts
    }

    /// Mutable access used internally for testing scenarios.
    pub fn as_storage_mut(&mut self) -> &mut Vector2D<u64> {
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

    /// Legacy helper retaining the historic naming scheme.
    pub fn insert_cm(&mut self, value: &SketchInput) {
        self.insert(value);
    }

    /// Legacy helper retaining the historic naming scheme.
    pub fn get_est(&self, value: &SketchInput) -> u64 {
        self.estimate(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SketchInput;
    use crate::test_utils::sample_zipf_u64;
    use std::collections::HashMap;

    fn counter_index(row: usize, key: &SketchInput, columns: usize) -> usize {
        let hash = hash_it_to_128(row, key);
        ((hash & ((0x1 << 32) - 1)) as usize) % columns
    }

    fn run_zipf_stream(
        rows: usize,
        cols: usize,
        domain: usize,
        exponent: f64,
        samples: usize,
        seed: u64,
    ) -> (CountMin, HashMap<u64, u64>) {
        let mut truth = HashMap::<u64, u64>::new();
        let mut sketch = CountMin::with_dimensions(rows, cols);

        for value in sample_zipf_u64(domain, exponent, samples, seed) {
            let key = SketchInput::U64(value);
            sketch.insert(&key);
            *truth.entry(value).or_insert(0) += 1;
        }

        (sketch, truth)
    }
    #[test]
    fn default_initializes_expected_dimensions() {
        let cm = CountMin::default();
        assert_eq!(cm.rows(), 3);
        assert_eq!(cm.cols(), 4096);

        let storage = cm.as_storage();
        for row in 0..cm.rows() {
            assert!(
                storage.row_slice(row).iter().all(|&value| value == 0),
                "expected row {} to be zero-initialized, got {:?}",
                row,
                storage.row_slice(row)
            );
        }
    }

    #[test]
    fn init_cm_with_row_col_uses_custom_sizes() {
        let cm = CountMin::with_dimensions(3, 17);
        assert_eq!(cm.rows(), 3);
        assert_eq!(cm.cols(), 17);

        let storage = cm.as_storage();
        for row in 0..cm.rows() {
            assert!(
                storage.row_slice(row).iter().all(|&value| value == 0),
                "expected row {} to be zero-initialized, got {:?}",
                row,
                storage.row_slice(row)
            );
        }
    }

    #[test]
    fn required_bits_match_expected_thresholds() {
        let default_dims = CountMin::with_dimensions(3, 4096);
        assert_eq!(default_dims.as_storage().get_required_bits(), 64);

        let smaller_cols = CountMin::with_dimensions(3, 64);
        assert_eq!(smaller_cols.as_storage().get_required_bits(), 32);

        let larger_shape = CountMin::with_dimensions(5, 1_048_576);
        assert_eq!(larger_shape.as_storage().get_required_bits(), 128);
    }

    #[test]
    fn insert_cm_updates_all_minimal_rows() {
        let mut cm = CountMin::with_dimensions(4, 64);
        let key = SketchInput::Str("alpha");

        cm.insert(&key);

        for row in 0..cm.rows() {
            let idx = counter_index(row, &key, cm.cols());
            assert_eq!(
                cm.counts.query_one_counter(row, idx),
                1,
                "row {row} counter should be 1"
            );
        }
    }

    #[test]
    fn fast_insert_matches_standard_estimate() {
        let mut slow = CountMin::with_dimensions(3, 64);
        let mut fast = CountMin::with_dimensions(3, 64);

        let keys = vec![
            SketchInput::Str("alpha"),
            SketchInput::Str("beta"),
            SketchInput::Str("gamma"),
            SketchInput::Str("delta"),
            SketchInput::Str("epsilon"),
        ];

        for key in &keys {
            slow.insert(key);
            fast.fast_insert(key);
        }

        for key in &keys {
            assert_eq!(
                slow.estimate(key),
                fast.fast_estimate(key),
                "fast path should match standard insert for key {key:?}"
            );
        }
    }

    // #[test]
    // fn get_est_returns_smallest_counter_for_key() {
    //     let mut cm = CountMin::with_dimensions(3, 32);
    //     let key = SketchInput::Str("alpha");

    //     for row in 0..cm.rows() {
    //         let idx = counter_index(row, &key, cm.cols());
    //         let value = (row as u64 + 4) * 2;
    //         cm.as_storage_mut()
    //             .update_one_counter(row, idx, |_, new| new, value);
    //     }

    //     assert_eq!(cm.estimate(&key), 8);
    // }

    #[test]
    fn merge_adds_counters_element_wise() {
        let mut left = CountMin::with_dimensions(2, 32);
        let mut right = CountMin::with_dimensions(2, 32);
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
        let mut left = CountMin::with_dimensions(2, 32);
        let right = CountMin::with_dimensions(3, 32);
        left.merge(&right);
    }

    #[test]
    fn zipf_stream_stays_within_five_percent_for_most_keys() {
        let (sketch, truth) = run_zipf_stream(5, 8192, 8192, 1.1, 200_000, 0x5eed_c0de);
        let mut within_tolerance = 0usize;
        for (&value, &count) in &truth {
            let estimate = sketch.estimate(&SketchInput::U64(value));
            let rel_error = (estimate.abs_diff(count) as f64) / (count as f64);
            if rel_error < 0.05 {
                within_tolerance += 1;
            }
        }

        let total = truth.len();
        let accuracy = within_tolerance as f64 / total as f64;
        assert!(
            accuracy >= 0.90,
            "Only {:.2}% of keys within tolerance ({} of {}); expected at least 90%",
            accuracy * 100.0,
            within_tolerance,
            total
        );
    }

    #[test]
    fn zipf_stream_estimates_heavy_hitters_within_six_percent() {
        let (sketch, truth) = run_zipf_stream(3, 2048, 8192, 1.1, 200_000, 0x5eed_c0de);
        let mut counts: Vec<(u64, u64)> = truth.iter().map(|(&k, &v)| (k, v)).collect();
        counts.sort_unstable_by(|a, b| b.1.cmp(&a.1));

        let top_k = counts.len().min(25);
        assert!(top_k > 0, "expected at least one heavy hitter");

        for (key, count) in counts.into_iter().take(top_k) {
            let estimate = sketch.estimate(&SketchInput::U64(key));
            let rel_error = (estimate.abs_diff(count) as f64) / (count as f64);
            assert!(
                rel_error < 0.06,
                "Heavy hitter key {key} truth {count} estimate {estimate} rel error {rel_error:.4}"
            );
        }
    }

    #[test]
    fn count_min_round_trip_serialization() {
        let mut sketch = CountMin::with_dimensions(3, 8);
        sketch.insert(&SketchInput::U64(42));
        sketch.insert(&SketchInput::U64(7));

        let encoded = sketch.serialize_to_bytes().expect("serialize CountMin");
        assert!(!encoded.is_empty());

        let decoded = CountMin::deserialize_from_bytes(&encoded).expect("deserialize CountMin");

        assert_eq!(sketch.rows(), decoded.rows());
        assert_eq!(sketch.cols(), decoded.cols());
        assert_eq!(
            sketch.as_storage().as_slice(),
            decoded.as_storage().as_slice()
        );
    }
}
