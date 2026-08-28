//! Packed bit storage for membership sketches.
//!
//! [`BitMatrix`] is a `rows x cols` grid of single bits behind the same
//! [`MatrixStorage`] interface the counter matrices use, so a sketch written
//! against that trait runs over bits without a second code path. One bit per
//! cell rather than one byte is the whole point: a Bloom filter sized for a
//! target false-positive rate is a memory structure first.

use super::matrix_storage::{
    MatrixFastHash, MatrixStorage, cols_mask, cols_mask_bits, fold_to_col,
};
use serde::{Deserialize, Serialize};

const BITS_PER_WORD: usize = u64::BITS as usize;

/// A `rows x cols` bit grid packed into `u64` words, one row after another.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BitMatrix {
    words: Vec<u64>,
    rows: usize,
    cols: usize,
    words_per_row: usize,
    mask_bits: u32,
    mask: u128,
}

impl BitMatrix {
    /// Creates a `rows x cols` grid with every bit clear.
    pub fn new(rows: usize, cols: usize) -> Self {
        assert!(rows > 0 && cols > 0, "a bit matrix needs both dimensions");
        let words_per_row = cols.div_ceil(BITS_PER_WORD);
        Self {
            words: vec![0; rows * words_per_row],
            rows,
            cols,
            words_per_row,
            mask_bits: cols_mask_bits(cols),
            mask: cols_mask(cols),
        }
    }

    /// Number of rows.
    #[inline(always)]
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Number of columns.
    #[inline(always)]
    pub fn cols(&self) -> usize {
        self.cols
    }

    /// Bytes of packed storage the grid occupies.
    pub fn size_in_bytes(&self) -> usize {
        self.words.len() * std::mem::size_of::<u64>()
    }

    /// Number of bits currently set.
    pub fn count_ones(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    /// Fraction of bits currently set.
    pub fn fill_ratio(&self) -> f64 {
        self.count_ones() as f64 / (self.rows * self.cols) as f64
    }

    /// Clears every bit.
    pub fn clear(&mut self) {
        self.words.iter_mut().for_each(|w| *w = 0);
    }

    /// Sets every bit in `self` that is set in `other`.
    ///
    /// Dimensions must match; a union across different geometries is not
    /// defined.
    pub fn union_from(&mut self, other: &Self) {
        assert_eq!(
            (self.rows, self.cols),
            (other.rows, other.cols),
            "bit matrices must have the same dimensions to be unioned"
        );
        for (dst, src) in self.words.iter_mut().zip(other.words.iter()) {
            *dst |= *src;
        }
    }

    #[inline(always)]
    fn locate(&self, row: usize, col: usize) -> (usize, u64) {
        let idx = row * self.words_per_row + col / BITS_PER_WORD;
        (idx, 1u64 << (col % BITS_PER_WORD))
    }

    /// Reads the bit at `(row, col)`.
    #[inline(always)]
    pub fn get(&self, row: usize, col: usize) -> bool {
        let (idx, bit) = self.locate(row, col);
        self.words[idx] & bit != 0
    }

    /// Sets the bit at `(row, col)`.
    #[inline(always)]
    pub fn set(&mut self, row: usize, col: usize) {
        let (idx, bit) = self.locate(row, col);
        self.words[idx] |= bit;
    }

    /// Writes the bit at `(row, col)`.
    #[inline(always)]
    pub fn put(&mut self, row: usize, col: usize, value: bool) {
        let (idx, bit) = self.locate(row, col);
        if value {
            self.words[idx] |= bit;
        } else {
            self.words[idx] &= !bit;
        }
    }

    #[inline(always)]
    fn col_for_row<Hash: MatrixFastHash>(&self, hashed_val: &Hash, row: usize) -> usize {
        let raw = hashed_val.row_hash(row, self.mask_bits, self.mask);
        fold_to_col(raw, self.cols)
    }
}

impl MatrixStorage for BitMatrix {
    type Counter = bool;

    #[inline(always)]
    fn rows(&self) -> usize {
        self.rows
    }

    #[inline(always)]
    fn cols(&self) -> usize {
        self.cols
    }

    #[inline(always)]
    fn update_one_counter<F, V>(&mut self, row: usize, col: usize, op: F, value: V)
    where
        F: Fn(&mut Self::Counter, V),
    {
        let mut cell = self.get(row, col);
        op(&mut cell, value);
        self.put(row, col, cell);
    }

    #[inline(always)]
    fn increment_by_row(&mut self, row: usize, col: usize, value: Self::Counter) {
        if value {
            self.set(row, col);
        }
    }

    #[inline(always)]
    fn fast_insert<Hash, F, V>(&mut self, op: F, value: V, hashed_val: &Hash)
    where
        Hash: MatrixFastHash,
        F: Fn(&mut Self::Counter, &V, usize),
        V: Clone,
    {
        for row in 0..self.rows {
            let col = self.col_for_row(hashed_val, row);
            let mut cell = self.get(row, col);
            op(&mut cell, &value, row);
            self.put(row, col, cell);
        }
    }

    #[inline(always)]
    fn fast_query_min<Hash, F, R>(&self, hashed_val: &Hash, op: F) -> R
    where
        Hash: MatrixFastHash,
        F: Fn(&Self::Counter, usize, &Hash) -> R,
        R: PartialOrd,
    {
        let c0 = self.col_for_row(hashed_val, 0);
        let mut min = op(&self.get(0, c0), 0, hashed_val);
        for row in 1..self.rows {
            let col = self.col_for_row(hashed_val, row);
            let candidate = op(&self.get(row, col), row, hashed_val);
            if candidate < min {
                min = candidate;
            }
        }
        min
    }

    #[inline(always)]
    fn fast_query_median<Hash, F>(&self, hashed_val: &Hash, op: F) -> f64
    where
        Hash: MatrixFastHash,
        F: Fn(&Self::Counter, usize, &Hash) -> f64,
    {
        let mut values: Vec<f64> = (0..self.rows)
            .map(|row| {
                let col = self.col_for_row(hashed_val, row);
                op(&self.get(row, col), row, hashed_val)
            })
            .collect();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mid = values.len() / 2;
        if values.len() % 2 == 0 {
            (values[mid - 1] + values[mid]) / 2.0
        } else {
            values[mid]
        }
    }

    #[inline(always)]
    fn query_one_counter(&self, row: usize, col: usize) -> Self::Counter {
        self.get(row, col)
    }
}

impl<H> crate::FastPathHasher<H> for BitMatrix
where
    H: crate::SketchHasher,
{
    #[inline(always)]
    fn hash_for_matrix(&self, value: &crate::DataInput) -> H::HashType {
        <H::HashType as MatrixFastHash>::assert_compatible(self.rows, self.cols);
        H::hash_for_matrix_seeded(0, self.rows, self.cols, value)
    }
}
