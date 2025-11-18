use rand::rngs::ThreadRng;
use rand::{Rng, rng};
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

/// Count-Min sketch variant that applies geometric sampling following the
/// DPDK member sketch implementation. Reference:
/// <https://github.com/DPDK/dpdk/blob/main/lib/member/rte_member_sketch.c>.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CountMinGS {
    counts: Vector2D<u64>,
    row: usize,
    col: usize,
    sample_rate: f64,
    #[serde(default)]
    until_next: usize,
    // #[serde(default)]
    // packet_until_next: usize,
    #[serde(skip)]
    #[serde(default = "rng")]
    generator: ThreadRng,
    delta: u64,
}

impl Default for CountMinGS {
    fn default() -> Self {
        Self::with_dimensions_and_sample_rate(DEFAULT_ROW_NUM, DEFAULT_COL_NUM, 1.0)
    }
}

impl CountMinGS {
    pub fn with_dimensions_and_sample_rate(rows: usize, cols: usize, sample_rate: f64) -> Self {
        assert!(rows > 0, "CountMinGS requires at least one row");
        assert!(cols > 0, "CountMinGS requires at least one column");
        assert!(
            !sample_rate.is_nan() && sample_rate > 0.0 && sample_rate <= 1.0,
            "sample_rate must be within (0.0, 1.0]"
        );

        let mut counts = Vector2D::init(rows, cols);
        counts.fill(0);

        let mut cm = CountMinGS {
            counts,
            row: rows,
            col: cols,
            sample_rate,
            until_next: 0,
            // packet_until_next: 0,
            generator: rng(),
            delta: 0
        };
        cm.delta = cm.scaled_increment(1);
        cm
    }

    pub fn with_sample_rate(sample_rate: f64) -> Self {
        Self::with_dimensions_and_sample_rate(DEFAULT_ROW_NUM, DEFAULT_COL_NUM, sample_rate)
    }

    pub fn rows(&self) -> usize {
        self.row
    }

    pub fn cols(&self) -> usize {
        self.col
    }

    pub fn sample_rate(&self) -> f64 {
        self.sample_rate
    }

    fn draw_geometric(&mut self, sample_rate: f64) -> usize {
        let k = loop {
            let r = self.generator.random::<f64>();
            if r != 0.0_f64 && r != 1.0_f64 {
                break r;
            }
        };
        ((1.0 - k).ln() / (1.0 - sample_rate).ln()).ceil() as usize
    }

    pub fn nitro_insert(&mut self, value: &SketchInput) {
        if self.until_next >= self.row {
            self.until_next -= self.row;
            return;
        }
        let mut cur_row = self.until_next;
        // let delta = self.scaled_increment(1);
        let delta = self.delta;

        loop {
            let hashed = hash_it_to_128(cur_row, value);
            let col = ((hashed as u64 & LOWER_32_MASK) as usize) % self.col;
            self.counts[cur_row][col] += delta;

            self.until_next = self.draw_geometric(self.sample_rate);
            if cur_row + self.until_next >= self.row {
                break;
            }
            cur_row += self.until_next;
        }

        // Adjust remaining state for next insert (DPDK behavior)
        self.until_next -= self.row - cur_row;
    }

    pub fn packet_nitro_insert(&mut self, value: &SketchInput) {
        if self.until_next > 0 {
            self.until_next -= 1;
            return;
        }

        // let delta = self.scaled_increment(1);
        let delta = self.delta;
        for row in 0..self.row {
            let hashed = hash_it_to_128(row, value);
            let col = ((hashed as u64 & LOWER_32_MASK) as usize) % self.col;
            self.counts[row][col] += delta;
        }

        self.until_next = self.draw_geometric(self.sample_rate);
    }

    pub fn packet_nitro_fast_insert(&mut self, value: &SketchInput) {
        if self.until_next > 0 {
            self.until_next -= 1;
            return;
        }

        // let delta = self.scaled_increment(1);
        let delta = self.delta;
        let hashed_val = hash_it_to_128(0, value);
        self.counts
            .fast_insert(|a, b, _| *a += b, delta, hashed_val);

        self.until_next = self.draw_geometric(self.sample_rate);
    }

    pub fn nitro_estimate(&self, value: &SketchInput) -> f64 {
        let mut estimates = Vec::with_capacity(self.row);
        for r in 0..self.row {
            let hashed = hash_it_to_128(r, value);
            let col = ((hashed as u64 & LOWER_32_MASK) as usize) % self.col;
            let counter = self.counts.query_one_counter(r, col);
            estimates.push(counter);
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

    // pub fn insert(&mut self, value: &SketchInput) {
    //     self.fast_insert(value);
    // }

    // pub fn fast_insert(&mut self, value: &SketchInput) {
    //     let hashed_val = hash_it_to_128(0, value);
    //     self.fast_insert_with_hash_value(hashed_val);
    // }

    // pub fn fast_insert_with_hash_value(&mut self, hashed_val: u128) {
    //     self.apply_sampled_update(hashed_val, 1);
    // }

    // pub fn estimate(&self, value: &SketchInput) -> u64 {
    //     let mut min = u64::MAX;
    //     for r in 0..self.row {
    //         let hashed = hash_it_to_128(r, value);
    //         let col = ((hashed as u64 & LOWER_32_MASK) as usize) % self.col;
    //         min = min.min(self.counts[r][col]);
    //     }
    //     min
    // }

    pub fn fast_estimate(&self, value: &SketchInput) -> u64 {
        let hashed_val = hash_it_to_128(0, value);
        self.fast_estimate_with_hash(hashed_val)
    }

    pub fn fast_estimate_with_hash(&self, hashed_val: u128) -> u64 {
        self.counts.fast_query_min(hashed_val, |val, _, _| *val)
    }

    pub fn merge(&mut self, other: &Self) {
        assert_eq!(
            (self.row, self.col),
            (other.row, other.col),
            "dimension mismatch while merging CountMinGS sketches"
        );
        assert!(
            (self.sample_rate - other.sample_rate).abs() <= f64::EPSILON,
            "sample_rate mismatch while merging CountMinGS sketches"
        );

        for i in 0..self.row {
            for j in 0..self.col {
                self.counts[i][j] += other.counts[i][j];
            }
        }
        // Reset sampling state after merge to avoid biasing follow-up inserts.
        self.until_next = 0;
        // self.packet_until_next = 0;
    }

    pub fn as_storage(&self) -> &Vector2D<u64> {
        &self.counts
    }

    pub fn as_storage_mut(&mut self) -> &mut Vector2D<u64> {
        &mut self.counts
    }

    pub fn debug(&self) {
        for row in 0..self.row {
            println!("row {}: {:?}", row, &self.counts.row_slice(row));
        }
    }

    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        to_vec_named(self)
    }

    pub fn serialize(&self) -> Result<Vec<u8>, RmpEncodeError> {
        self.serialize_to_bytes()
    }

    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        from_slice(bytes)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        Self::deserialize_from_bytes(bytes)
    }

    #[inline]
    fn scaled_increment(&self, weight: u64) -> u64 {
        if self.is_full_sampling() {
            weight
        } else {
            ((weight as f64) / self.sample_rate).ceil() as u64
        }
    }

    #[inline]
    fn is_full_sampling(&self) -> bool {
        (self.sample_rate - 1.0).abs() <= f64::EPSILON
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::sample_zipf_u64;
    use crate::{HHHeap, SketchInput};
    use std::collections::HashMap;

    fn counter_index(row: usize, key: &SketchInput, columns: usize) -> usize {
        let hash = hash_it_to_128(row, key);
        ((hash & ((0x1 << 32) - 1)) as usize) % columns
    }

    fn generate_unique_keys(rows: usize, cols: usize, count: usize) -> Vec<u64> {
        let mut keys = Vec::with_capacity(count);
        let mut positions: Vec<Vec<usize>> = Vec::with_capacity(count);
        let mut candidate = 0u64;

        while keys.len() < count {
            let key = SketchInput::U64(candidate);
            let candidate_positions: Vec<usize> = (0..rows)
                .map(|row| counter_index(row, &key, cols))
                .collect();

            let collision_free = positions.iter().all(|existing| {
                existing
                    .iter()
                    .zip(candidate_positions.iter())
                    .all(|(a, b)| a != b)
            });

            if collision_free {
                keys.push(candidate);
                positions.push(candidate_positions);
            }

            candidate = candidate.saturating_add(1);
        }

        keys
    }

    fn snapshot_topk(heap: &HHHeap) -> Vec<(String, i64)> {
        let mut items: Vec<(String, i64)> = heap
            .heap()
            .iter()
            .map(|item| (item.key.clone(), item.count))
            .collect();
        items.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        items
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

    fn run_zipf_stream_gs(
        rows: usize,
        cols: usize,
        domain: usize,
        exponent: f64,
        samples: usize,
        seed: u64,
        sample_rate: f64,
    ) -> (CountMinGS, HashMap<u64, u64>) {
        let mut truth = HashMap::<u64, u64>::new();
        let mut sketch = CountMinGS::with_dimensions_and_sample_rate(rows, cols, sample_rate);

        for value in sample_zipf_u64(domain, exponent, samples, seed) {
            let key = SketchInput::U64(value);
            sketch.nitro_insert(&key);
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
    fn packet_nitro_insert_matches_regular_insert_with_full_sampling() {
        let rows = 3;
        let cols = 64;
        let mut gs = CountMinGS::with_dimensions_and_sample_rate(rows, cols, 1.0);
        let mut cm = CountMin::with_dimensions(rows, cols);
        let key = SketchInput::Str("packet");

        gs.packet_nitro_insert(&key);
        cm.insert(&key);

        assert_eq!(gs.as_storage().as_slice(), cm.as_storage().as_slice());
    }

    #[test]
    fn packet_nitro_insert_respects_skip_state() {
        let mut sketch = CountMinGS::with_dimensions_and_sample_rate(3, 32, 0.5);
        // sketch.packet_until_next = 2;
        sketch.until_next = 2;
        let key = SketchInput::U64(7);

        sketch.packet_nitro_insert(&key);

        assert!(sketch
            .as_storage()
            .as_slice()
            .iter()
            .all(|&counter| counter == 0));
        // assert_eq!(sketch.packet_until_next, 1);
        assert_eq!(sketch.until_next, 1);
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

    #[test]
    fn countmingst_estimate_accuracy_with_sampling() {
        const SAMPLE_RATE: f64 = 0.1;
        let (sketch, truth) =
            run_zipf_stream_gs(5, 8192, 8192, 1.1, 200_000, 0x5eed_c0de, SAMPLE_RATE);

        // Focus on high-frequency items where sampling variance is lower
        let mut high_freq_items: Vec<_> = truth.iter().filter(|(_, count)| **count >= 50).collect();
        high_freq_items.sort_by(|a, b| b.1.cmp(a.1));

        let mut within_tolerance = 0usize;
        for (value, count) in &high_freq_items {
            let estimate = sketch.nitro_estimate(&SketchInput::U64(**value));
            let rel_error = ((estimate - **count as f64).abs()) / (**count as f64);
            if rel_error < 0.20 {
                // 20% tolerance for sampled estimates
                within_tolerance += 1;
            }
        }

        let total = high_freq_items.len();
        assert!(
            total > 0,
            "Expected at least some high-frequency items in the stream"
        );
        let accuracy = within_tolerance as f64 / total as f64;
        assert!(
            accuracy >= 0.70,
            "Only {:.2}% of high-frequency keys within 20% tolerance ({} of {}); expected at least 70%",
            accuracy * 100.0,
            within_tolerance,
            total
        );
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

    #[test]
    fn countmingst_topk_heavy_hitters_with_sampling() {
        const ROWS: usize = 5;
        const COLS: usize = 4096;
        const TOP_K: usize = 3;
        const NOISE_KEYS: usize = 12;
        const NOISE_COUNT: u64 = 40;
        const HEAVY_COUNTS: [u64; TOP_K] = [1000, 700, 500];
        const SAMPLE_RATE: f64 = 0.1;

        let mut sketch = CountMinGS::with_dimensions_and_sample_rate(ROWS, COLS, SAMPLE_RATE);
        let keys = generate_unique_keys(ROWS, COLS, TOP_K + NOISE_KEYS);
        let heavy_keys = &keys[..TOP_K];
        let noise_keys = &keys[TOP_K..];
        let mut truth = HashMap::<u64, u64>::new();

        for (value, &count) in heavy_keys.iter().zip(HEAVY_COUNTS.iter()) {
            let key = SketchInput::U64(*value);
            for _ in 0..count {
                sketch.nitro_insert(&key);
                *truth.entry(*value).or_insert(0) += 1;
            }
        }

        for &value in noise_keys {
            let key = SketchInput::U64(value);
            for _ in 0..NOISE_COUNT {
                sketch.nitro_insert(&key);
                *truth.entry(value).or_insert(0) += 1;
            }
        }

        let mut truth_heap = HHHeap::new(TOP_K);
        for (&value, &count) in &truth {
            truth_heap.update(&value.to_string(), count as i64);
        }

        let mut estimated_heap = HHHeap::new(TOP_K);
        for (&value, _) in &truth {
            let estimate = sketch.nitro_estimate(&SketchInput::U64(value));
            estimated_heap.update(&value.to_string(), estimate as i64);
        }

        let truth_keys: Vec<String> = snapshot_topk(&truth_heap)
            .iter()
            .map(|(key, _)| key.clone())
            .collect();
        let estimated_keys: Vec<String> = snapshot_topk(&estimated_heap)
            .iter()
            .map(|(key, _)| key.clone())
            .collect();

        assert_eq!(
            truth_keys, estimated_keys,
            "CountMinGS with sampling should identify the correct top-k heavy hitter keys"
        );
    }
}
