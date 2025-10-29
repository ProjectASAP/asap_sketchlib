// use crate::common::{LASTSTATE, SketchInput, SketchMatrix, Vector2D, hash_it};

// const LOWER_32_MASK: u64 = (1u64 << 32) - 1;
// const FAST_ROW_WIDTH: u64 = (1u64 << 13) - 1;
// const FAST_ROW_SHIFT: usize = 12;

// fn median(values: &mut Vec<i64>) -> f64 {
//     values.sort();
//     let len = values.len();
//     if len == 0 {
//         return 0.0;
//     }

//     if len % 2 == 0 {
//         (values[len / 2 - 1] + values[len / 2]) as f64 / 2.0
//     } else {
//         values[len / 2] as f64
//     }
// }

// fn median_f64(values: &mut Vec<f64>) -> f64 {
//     values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
//     let len = values.len();
//     if len == 0 {
//         return 0.0;
//     }

//     if len % 2 == 0 {
//         (values[len / 2 - 1] + values[len / 2]) / 2.0
//     } else {
//         values[len / 2]
//     }
// }

// fn signed_delta(count: i64, value: &SketchInput) -> i64 {
//     let sign_hash = hash_it(LASTSTATE, value);
//     if sign_hash % 2 == 1 { count } else { -count }
// }

// /// Count sketch backed by the shared `SketchMatrix` abstraction.
// #[derive(Clone, Debug)]
// pub struct Count {
//     counts: SketchMatrix<i64>,
// }

// impl Default for Count {
//     fn default() -> Self {
//         Self::with_dimensions(3, 4096)
//     }
// }

// impl Count {
//     /// Creates a sketch with the requested number of rows and columns.
//     pub fn with_dimensions(rows: usize, cols: usize) -> Self {
//         Self {
//             counts: SketchMatrix::filled(rows, cols, 0),
//         }
//     }

//     /// Number of rows in the sketch.
//     pub fn rows(&self) -> usize {
//         self.counts.rows()
//     }

//     /// Number of columns in the sketch.
//     pub fn cols(&self) -> usize {
//         self.counts.cols()
//     }

//     /// Inserts a single observation using the signed update rule.
//     pub fn insert(&mut self, value: &SketchInput) {
//         self.insert_with_count(value, 1);
//     }

//     /// Inserts an observation with an explicit weight.
//     pub fn insert_with_count(&mut self, value: &SketchInput, count: i64) {
//         let delta = signed_delta(count, value);
//         for row in 0..self.rows() {
//             let hashed = hash_it(row, value);
//             let col = ((hashed & LOWER_32_MASK) as usize) % self.cols();
//             if let Some(cell) = self.counts.get_mut(row, col) {
//                 *cell += delta;
//             }
//         }
//     }

//     /// Returns the frequency estimate for the provided value.
//     pub fn estimate(&self, value: &SketchInput) -> f64 {
//         let mut samples = Vec::with_capacity(self.rows());
//         let sign = signed_delta(1, value);
//         for row in 0..self.rows() {
//             let hashed = hash_it(row, value);
//             let col = ((hashed & LOWER_32_MASK) as usize) % self.cols();
//             let raw = self.counts[row][col];
//             samples.push(raw * sign);
//         }
//         median(&mut samples)
//     }

//     /// Merges another sketch while asserting compatible dimensions.
//     pub fn merge(&mut self, other: &Self) {
//         assert_eq!(
//             (self.rows(), self.cols()),
//             (other.rows(), other.cols()),
//             "dimension mismatch while merging Count sketches"
//         );

//         for row in 0..self.rows() {
//             for col in 0..self.cols() {
//                 let src = other.counts[row][col];
//                 if let Some(cell) = self.counts.get_mut(row, col) {
//                     *cell += src;
//                 }
//             }
//         }
//     }

//     /// Exposes the backing matrix for inspection/testing.
//     pub fn as_matrix(&self) -> &SketchMatrix<i64> {
//         &self.counts
//     }
// }

// /// Count sketch backed by the `Vector2D` abstraction.
// #[derive(Clone, Debug)]
// pub struct VectorCount {
//     counts: Vector2D<i64>,
//     rows: usize,
//     cols: usize,
// }

// impl Default for VectorCount {
//     fn default() -> Self {
//         Self::with_dimensions(4, 32)
//     }
// }

// impl VectorCount {
//     /// Creates a sketch with the requested number of rows and columns.
//     pub fn with_dimensions(rows: usize, cols: usize) -> Self {
//         let mut counts = Vector2D::init(rows, cols);
//         counts.fill(0);
//         Self { counts, rows, cols }
//     }

//     /// Number of rows in the sketch.
//     pub fn rows(&self) -> usize {
//         self.rows
//     }

//     /// Number of columns in the sketch.
//     pub fn cols(&self) -> usize {
//         self.cols
//     }

//     /// Inserts a single observation using the signed update rule.
//     pub fn insert(&mut self, value: &SketchInput) {
//         self.insert_with_count(value, 1);
//     }

//     /// Inserts an observation with an explicit weight.
//     pub fn insert_with_count(&mut self, value: &SketchInput, count: i64) {
//         let delta = signed_delta(count, value);
//         for row in 0..self.rows {
//             let hashed = hash_it(row, value);
//             let col = ((hashed & LOWER_32_MASK) as usize) % self.cols;
//             self.counts
//                 .update_one_counter(row, col, std::ops::Add::add, delta);
//         }
//     }

//     /// Inserts an observation using the combined hash optimization.
//     pub fn fast_insert(&mut self, value: &SketchInput) {
//         self.fast_insert_with_count(value, 1);
//     }

//     /// Inserts an observation with an explicit weight using the fast path.
//     pub fn fast_insert_with_count(&mut self, value: &SketchInput, count: i64) {
//         let delta = signed_delta(count, value);
//         self.counts
//             .fast_insert(std::ops::Add::add, delta, hash_it(0, value));
//     }

//     /// Returns the frequency estimate for the provided value.
//     pub fn estimate(&self, value: &SketchInput) -> f64 {
//         let mut samples = Vec::with_capacity(self.rows);
//         let sign = signed_delta(1, value);
//         for row in 0..self.rows {
//             let hashed = hash_it(row, value);
//             let col = ((hashed & LOWER_32_MASK) as usize) % self.cols;
//             let raw = self.counts.query_one_counter(row, col);
//             samples.push(raw * sign);
//         }
//         median(&mut samples)
//     }

//     /// Returns the frequency estimate using the combined hash optimization.
//     pub fn fast_estimate(&self, value: &SketchInput) -> f64 {
//         let mut samples = Vec::with_capacity(self.rows);
//         let sign = signed_delta(1, value);
//         let hashed_vals = hash_it(0, value);
//         for row in 0..self.rows {
//             let hashed = (hashed_vals >> (FAST_ROW_SHIFT * row)) & FAST_ROW_WIDTH;
//             let col = ((hashed & LOWER_32_MASK) as usize) % self.cols;
//             let raw = self.counts.query_one_counter(row, col);
//             samples.push(raw * sign);
//         }
//         median(&mut samples)
//     }

//     /// Approximates the L2 norm using the median-of-rows energy estimator.
//     pub fn l2(&self) -> f64 {
//         let mut row_energy = Vec::with_capacity(self.rows);
//         for row in 0..self.rows {
//             let mut sum = 0.0;
//             for col in 0..self.cols {
//                 let value = self.counts.query_one_counter(row, col) as f64;
//                 sum += value * value;
//             }
//             row_energy.push(sum);
//         }
//         median_f64(&mut row_energy).sqrt()
//     }

//     /// Merges another sketch while asserting compatible dimensions.
//     pub fn merge(&mut self, other: &Self) {
//         assert_eq!(
//             (self.rows, self.cols),
//             (other.rows, other.cols),
//             "dimension mismatch while merging VectorCount sketches"
//         );

//         for row in 0..self.rows {
//             for col in 0..self.cols {
//                 let src = other.counts.query_one_counter(row, col);
//                 self.counts
//                     .update_one_counter(row, col, std::ops::Add::add, src);
//             }
//         }
//     }

//     /// Exposes the backing storage for inspection/testing.
//     pub fn as_storage(&self) -> &Vector2D<i64> {
//         &self.counts
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::common::SketchInput;

//     #[test]
//     fn count_estimate_matches_inserts() {
//         let mut sketch = Count::with_dimensions(4, 64);
//         let key = SketchInput::Str("alpha");
//         let other = SketchInput::Str("beta");

//         for _ in 0..5 {
//             sketch.insert(&key);
//         }

//         assert_eq!(sketch.estimate(&key), 5.0);
//         assert_eq!(sketch.estimate(&other), 0.0);
//     }

//     #[test]
//     fn count_merge_combines_counts() {
//         let mut left = Count::with_dimensions(3, 32);
//         let mut right = Count::with_dimensions(3, 32);
//         let key = SketchInput::Str("gamma");

//         for _ in 0..4 {
//             left.insert(&key);
//         }
//         for _ in 0..3 {
//             right.insert(&key);
//         }

//         left.merge(&right);

//         assert_eq!(left.estimate(&key), 7.0);
//         assert_eq!(right.estimate(&key), 3.0);
//     }

//     #[test]
//     fn vector_count_fast_paths_match_regular() {
//         let mut regular = VectorCount::with_dimensions(4, 64);
//         let mut fast = VectorCount::with_dimensions(4, 64);
//         let key = SketchInput::Str("delta");

//         for _ in 0..6 {
//             regular.insert(&key);
//             fast.fast_insert(&key);
//         }

//         let regular_est = regular.estimate(&key);
//         let fast_est = fast.fast_estimate(&key);

//         assert_eq!(regular_est, 6.0);
//         assert_eq!(fast_est, 6.0);
//         assert!(
//             (regular_est - fast_est).abs() < f64::EPSILON,
//             "fast path estimate should align with regular path"
//         );
//     }

//     #[test]
//     fn vector_count_merge_adds_counters() {
//         let mut left = VectorCount::with_dimensions(3, 32);
//         let mut right = VectorCount::with_dimensions(3, 32);
//         let key = SketchInput::Str("epsilon");

//         left.insert_with_count(&key, 2);
//         left.insert_with_count(&key, 3);
//         right.insert_with_count(&key, 4);

//         left.merge(&right);

//         assert_eq!(left.estimate(&key), 9.0);
//         assert_eq!(right.estimate(&key), 4.0);
//     }
// }
