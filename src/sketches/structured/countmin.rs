use std::arch::aarch64::vld1_dup_p16;

use crate::{
    common::{SketchInput, SketchMatrix, Vector2D, hash_it},
    // input::hash_for_all_rows,
};

/// Count-Min sketch built on top of the shared `SketchMatrix` abstraction.
#[derive(Clone, Debug)]
pub struct CountMin {
    counts: SketchMatrix<u64>,
}

impl Default for CountMin {
    fn default() -> Self {
        Self::with_dimensions(3, 4096)
    }
}

impl CountMin {
    /// Creates a sketch with the requested number of rows and columns.
    pub fn with_dimensions(rows: usize, cols: usize) -> Self {
        Self {
            counts: SketchMatrix::filled(rows, cols, 0),
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

    /// Inserts an observation while using the standard Count-Min minimum row update rule.
    pub fn insert(&mut self, value: &SketchInput) {
        let mut min_weight = u64::MAX;
        let mut targets: Vec<(usize, usize)> = Vec::with_capacity(self.rows());

        for row in 0..self.rows() {
            let hashed = hash_it(row, value);
            let col = ((hashed & ((1u64 << 32) - 1)) as usize) % self.cols();
            let weight = self.counts[row][col];
            if weight < min_weight {
                targets.clear();
                targets.push((row, col));
                min_weight = weight;
            } else if weight == min_weight {
                targets.push((row, col));
            }
        }

        for (row, col) in targets {
            if let Some(cell) = self.counts.get_mut(row, col) {
                *cell += 1;
            }
        }
    }

    /// Inserts an observation
    /// Shares the same logic with regular insert, but has hash optimization
    pub fn fast_insert(&mut self, value: &SketchInput) {
        let mut min_weight = u64::MAX;
        let mut targets: Vec<(usize, usize)> = Vec::with_capacity(self.rows());
        let hashed_vals = hash_it(0, value);

        for row in 0..self.rows() {
            let hashed = (hashed_vals >> (12 * row)) & ((0x1 << 13) - 1);
            let col = ((hashed & ((1u64 << 32) - 1)) as usize) % self.cols();
            let weight = self.counts[row][col];
            if weight < min_weight {
                targets.clear();
                targets.push((row, col));
                min_weight = weight;
            } else if weight == min_weight {
                targets.push((row, col));
            }
        }

        for (row, col) in targets {
            if let Some(cell) = self.counts.get_mut(row, col) {
                *cell += 1;
            }
        }
    }

    /// Returns the frequency estimate for the provided value.
    pub fn estimate(&self, value: &SketchInput) -> u64 {
        let mut min = u64::MAX;
        for row in 0..self.rows() {
            let hashed = hash_it(row, value);
            let col = ((hashed & ((1u64 << 32) - 1)) as usize) % self.cols();
            min = min.min(self.counts[row][col]);
        }
        min
    }

    /// Returns the frequency estimate for the provided value, with hash optimization
    pub fn fast_estimate(&self, value: &SketchInput) -> u64 {
        let mut min = u64::MAX;
        let hashed_vals = hash_it(0, value);
        for row in 0..self.rows() {
            let hashed = (hashed_vals >> (12 * row)) & ((0x1 << 13) - 1);
            let col = ((hashed & ((1u64 << 32) - 1)) as usize) % self.cols();
            min = min.min(self.counts[row][col]);
        }
        min
    }

    /// Merges another sketch while asserting compatible dimensions.
    pub fn merge(&mut self, other: &Self) {
        assert_eq!(
            (self.rows(), self.cols()),
            (other.rows(), other.cols()),
            "dimension mismatch while merging CountMin sketches"
        );

        for row in 0..self.rows() {
            for col in 0..self.cols() {
                let dest = self.counts.get_mut(row, col).expect("row bound checked");
                let src = other.counts[row][col];
                *dest += src;
            }
        }
    }

    /// Exposes the backing matrix for inspection/testing.
    pub fn as_matrix(&self) -> &SketchMatrix<u64> {
        &self.counts
    }
}

/// Count-Min sketch backed by the `Vector2D` abstraction.
#[derive(Clone, Debug)]
pub struct VectorCountMin {
    counts: Vector2D<u64>,
    row: usize,
    col: usize,
}

impl Default for VectorCountMin {
    fn default() -> Self {
        Self::with_dimensions(3, 4096)
    }
}

impl VectorCountMin {
    /// Creates a sketch with the requested number of rows and columns.
    pub fn with_dimensions(rows: usize, cols: usize) -> Self {
        let mut sk = VectorCountMin {
            counts: Vector2D::init(rows, cols),
            row: rows,
            col: cols,
        };
        sk.counts.fill(0);
        sk
    }

    // /// Number of rows in the sketch.
    // pub fn rows(&self) -> usize {
    //     self.counts.get_row()
    // }

    // /// Number of columns in the sketch.
    // pub fn cols(&self) -> usize {
    //     self.counts.get_col()
    // }

    /// Inserts an observation while using the standard Count-Min minimum row update rule.
    pub fn insert(&mut self, value: &SketchInput) {
        for r in 0..self.row {
            let hashed = hash_it(r, value);
            let col = ((hashed & ((1u64 << 32) - 1)) as usize) % self.col;
            self.counts
                .update_one_counter(r, col, std::ops::Add::add, 1_u64);
        }
    }

    /// Inserts an observation using the combined hash optimization.
    pub fn fast_insert(&mut self, value: &SketchInput) {
        self.counts
            .fast_insert(std::ops::Add::add, 1_u64, hash_it(0, value));
    }

    /// Returns the frequency estimate for the provided value.
    pub fn estimate(&self, value: &SketchInput) -> u64 {
        let mut min = u64::MAX;
        for r in 0..self.row {
            let hashed = hash_it(r, value);
            let col = ((hashed & ((1u64 << 32) - 1)) as usize) % self.col;
            // let idx = row * cols + col;
            min = min.min(self.counts.query_one_counter(r, col));
        }
        min
    }

    /// Returns the frequency estimate for the provided value, with hash optimization.
    pub fn fast_estimate(&self, value: &SketchInput) -> u64 {
        self.counts.fast_query(hash_it(0, value))
    }

    /// Merges another sketch while asserting compatible dimensions.
    pub fn merge(&mut self, other: &Self) {
        assert_eq!(
            (self.row, self.col),
            (other.row, other.col),
            "dimension mismatch while merging VectorCountMin sketches"
        );

        for i in 0..self.row {
            for j in 0..self.col {
                self.counts.update_one_counter(
                    i,
                    j,
                    std::ops::Add::add,
                    other.counts.query_one_counter(i, j),
                );
            }
        }
    }

    /// Exposes the backing matrix for inspection/testing.
    pub fn as_storage(&self) -> &Vector2D<u64> {
        &self.counts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::SketchInput;

    #[test]
    fn countmin_insert_and_estimate_roundtrip() {
        let mut sketch = CountMin::with_dimensions(3, 64);
        let key = SketchInput::Str("alpha");

        for _ in 0..4 {
            sketch.insert(&key);
        }

        assert_eq!(sketch.estimate(&key), 4);
    }

    #[test]
    fn countmin_fast_insert_roundtrip() {
        let mut sketch = CountMin::with_dimensions(3, 64);
        let key = SketchInput::Str("alpha-fast");

        for _ in 0..4 {
            sketch.fast_insert(&key);
        }

        assert_eq!(sketch.fast_estimate(&key), 4);
    }

    #[test]
    fn countmin_merge_combines_sketches() {
        let mut left = CountMin::with_dimensions(3, 64);
        let mut right = CountMin::with_dimensions(3, 64);
        let key = SketchInput::Str("beta");

        for _ in 0..3 {
            left.insert(&key);
        }
        for _ in 0..2 {
            right.insert(&key);
        }

        left.merge(&right);

        assert_eq!(left.estimate(&key), 5);
        assert_eq!(right.estimate(&key), 2);
    }

    #[test]
    fn vector_countmin_fast_paths_match_regular() {
        let mut slow = VectorCountMin::with_dimensions(3, 64);
        let mut fast = VectorCountMin::with_dimensions(3, 64);
        let key = SketchInput::Str("gamma");

        for _ in 0..5 {
            slow.insert(&key);
            fast.fast_insert(&key);
        }

        let slow_est = slow.estimate(&key);
        let fast_est = fast.fast_estimate(&key);

        assert_eq!(slow_est, 5);
        assert_eq!(fast_est, 5);
        assert_eq!(slow_est, fast_est);
    }

    #[test]
    fn vector_countmin_merge_adds_counters() {
        let mut left = VectorCountMin::with_dimensions(2, 32);
        let mut right = VectorCountMin::with_dimensions(2, 32);
        let key = SketchInput::Str("delta");

        left.insert(&key);
        left.insert(&key);
        right.insert(&key);

        left.merge(&right);

        assert_eq!(left.estimate(&key), 3);
        assert_eq!(right.estimate(&key), 1);
    }
}
