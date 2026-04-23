//! Trait bound for matrix-backed sketches.

use smallvec::SmallVec;

use crate::DataInput;

/// Fast-path hash container for matrix-backed sketches.
#[derive(Clone, Debug)]
pub enum MatrixHashType {
    /// Packed per-row hashes stored in one `u64`.
    Packed64(u64),
    /// Packed per-row hashes stored in one `u128`.
    Packed128(u128),
    /// One hash value per row.
    Rows(SmallVec<[u64; 8]>),
}

impl MatrixHashType {
    #[inline(always)]
    /// Extracts the row-local hash bits for one row.
    pub fn row_hash(&self, row: usize, mask_bits: u32, mask: u128) -> u128 {
        match self {
            MatrixHashType::Packed64(value) => {
                let shifted = (*value >> (mask_bits as usize * row)) as u128;
                shifted & mask
            }
            MatrixHashType::Packed128(value) => (value >> (mask_bits as usize * row)) & mask,
            MatrixHashType::Rows(values) => {
                debug_assert!(row < values.len(), "row index out of bounds for hash rows");
                (values[row] as u128) & mask
            }
        }
    }

    #[inline(always)]
    /// Returns the Count-Sketch sign for one row.
    pub fn sign_for_row(&self, row: usize) -> i32 {
        let bit = match self {
            MatrixHashType::Packed64(value) => (value >> (63 - row)) & 1,
            MatrixHashType::Packed128(value) => ((value >> (127 - row)) & 1) as u64,
            MatrixHashType::Rows(values) => {
                debug_assert!(row < values.len(), "row index out of bounds for hash rows");
                (values[row] >> 63) & 1
            }
        };
        (bit as i32 * 2) - 1
    }

    #[inline(always)]
    /// Returns the lower 64 bits of the stored hash.
    pub fn lower_64(&self) -> u64 {
        match self {
            MatrixHashType::Packed64(value) => *value,
            MatrixHashType::Packed128(value) => *value as u64,
            MatrixHashType::Rows(values) => values.first().copied().unwrap_or(0),
        }
    }
}

/// Trait for hash values that support fast row/column decoding.
pub trait MatrixFastHash: Clone {
    /// Verifies that the hash type can encode the given dimensions.
    fn assert_compatible(rows: usize, cols: usize);
    /// Returns the column index for one row.
    fn col_for_row(&self, row: usize, cols: usize) -> usize;
    /// Returns the Count-Sketch sign for one row.
    fn sign_for_row(&self, row: usize) -> i32;
}

impl MatrixFastHash for MatrixHashType {
    #[inline(always)]
    fn assert_compatible(_rows: usize, _cols: usize) {}

    #[inline(always)]
    fn col_for_row(&self, row: usize, cols: usize) -> usize {
        let mask_bits = if cols.is_power_of_two() {
            cols.ilog2()
        } else {
            cols.ilog2() + 1
        };
        let mask = (1u128 << mask_bits) - 1;
        self.row_hash(row, mask_bits, mask) as usize % cols
    }

    #[inline(always)]
    fn sign_for_row(&self, row: usize) -> i32 {
        MatrixHashType::sign_for_row(self, row)
    }
}

impl MatrixFastHash for u64 {
    #[inline(always)]
    fn assert_compatible(rows: usize, cols: usize) {
        let mask_bits = if cols.is_power_of_two() {
            cols.ilog2() as usize
        } else {
            cols.ilog2() as usize + 1
        };
        let bits_per_row = mask_bits + 1;
        let bits_required = bits_per_row.saturating_mul(rows);
        assert!(
            bits_required <= 64,
            "SketchHasher hash type u64 cannot represent fast-path hash for rows={rows}, cols={cols}; use u128 or MatrixHashType"
        );
    }

    #[inline(always)]
    fn col_for_row(&self, row: usize, cols: usize) -> usize {
        let mask_bits = if cols.is_power_of_two() {
            cols.ilog2() as usize
        } else {
            cols.ilog2() as usize + 1
        };
        let mask = (1u64 << mask_bits) - 1;
        ((*self >> (mask_bits * row)) & mask) as usize % cols
    }

    #[inline(always)]
    fn sign_for_row(&self, row: usize) -> i32 {
        let bit = (self >> (63 - row)) & 1;
        (bit as i32 * 2) - 1
    }
}

impl MatrixFastHash for u128 {
    #[inline(always)]
    fn assert_compatible(rows: usize, cols: usize) {
        let mask_bits = if cols.is_power_of_two() {
            cols.ilog2() as usize
        } else {
            cols.ilog2() as usize + 1
        };
        let bits_per_row = mask_bits + 1;
        let bits_required = bits_per_row.saturating_mul(rows);
        assert!(
            bits_required <= 128,
            "SketchHasher hash type u128 cannot represent fast-path hash for rows={rows}, cols={cols}; use MatrixHashType"
        );
    }

    #[inline(always)]
    fn col_for_row(&self, row: usize, cols: usize) -> usize {
        let mask_bits = if cols.is_power_of_two() {
            cols.ilog2() as usize
        } else {
            cols.ilog2() as usize + 1
        };
        let mask = (1u128 << mask_bits) - 1;
        ((*self >> (mask_bits * row)) & mask) as usize % cols
    }

    #[inline(always)]
    fn sign_for_row(&self, row: usize) -> i32 {
        let bit = (self >> (127 - row)) & 1;
        (bit as i32 * 2) - 1
    }
}

/// Storage interface implemented by matrix-backed sketch backends.
pub trait MatrixStorage {
    /// Counter type stored in each cell.
    type Counter: Clone;
    /// Returns the number of rows.
    fn rows(&self) -> usize;
    /// Returns the number of columns.
    fn cols(&self) -> usize;

    /// Updates a single counter at `(row, col)`.
    fn update_one_counter<F, V>(&mut self, row: usize, col: usize, op: F, value: V)
    where
        F: Fn(&mut Self::Counter, V);

    /// Increments one counter by a typed value.
    fn increment_by_row(&mut self, row: usize, col: usize, value: Self::Counter);

    /// Inserts one value into all rows using a precomputed hash.
    fn fast_insert<Hash, F, V>(&mut self, op: F, value: V, hashed_val: &Hash)
    where
        Hash: MatrixFastHash,
        F: Fn(&mut Self::Counter, &V, usize),
        V: Clone;

    /// Queries the minimum across rows using a precomputed hash.
    fn fast_query_min<Hash, F, R>(&self, hashed_val: &Hash, op: F) -> R
    where
        Hash: MatrixFastHash,
        F: Fn(&Self::Counter, usize, &Hash) -> R,
        R: PartialOrd;

    /// Queries the median across rows using a precomputed hash.
    fn fast_query_median<Hash, F>(&self, hashed_val: &Hash, op: F) -> f64
    where
        Hash: MatrixFastHash,
        F: Fn(&Self::Counter, usize, &Hash) -> f64;

    /// Reads one counter at `(row, col)`.
    fn query_one_counter(&self, row: usize, col: usize) -> Self::Counter;
}

/// Trait for storages that can derive fast-path hashes for their dimensions.
pub trait FastPathHasher<H>: MatrixStorage
where
    H: crate::SketchHasher,
{
    /// Computes a compatible fast-path hash for `value`.
    fn hash_for_matrix(&self, value: &DataInput) -> H::HashType;
}
