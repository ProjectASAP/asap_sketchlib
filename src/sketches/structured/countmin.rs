use crate::common::{SketchInput, SketchMatrix, hash_it};

/// Count-Min sketch built on top of the shared `SketchMatrix` abstraction.
#[derive(Clone, Debug)]
pub struct CountMin {
    counts: SketchMatrix<u64>,
}

impl Default for CountMin {
    fn default() -> Self {
        Self::with_dimensions(4, 32)
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
