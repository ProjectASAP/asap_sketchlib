//! Trait bound for matrix-backed sketches.

use smallvec::SmallVec;

use crate::DataInput;

/// Fast-path hash container for matrix-backed sketches.
#[derive(Clone, Debug)]
pub enum MatrixHashType {
    Packed64(u64),
    Packed128(u128),
    Rows(SmallVec<[u64; 8]>),
}

impl MatrixHashType {
    #[inline(always)]
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
    pub fn lower_64(&self) -> u64 {
        match self {
            MatrixHashType::Packed64(value) => *value,
            MatrixHashType::Packed128(value) => *value as u64,
            MatrixHashType::Rows(values) => values.first().copied().unwrap_or(0),
        }
    }
}

pub trait MatrixFastHash: Clone {
    fn assert_compatible(rows: usize, cols: usize);
    fn col_for_row(&self, row: usize, cols: usize) -> usize;
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

pub trait MatrixStorage {
    type Counter: Clone;
    fn rows(&self) -> usize;
    fn cols(&self) -> usize;

    fn update_one_counter<F, V>(&mut self, row: usize, col: usize, op: F, value: V)
    where
        F: Fn(&mut Self::Counter, V);

    fn increment_by_row(&mut self, row: usize, col: usize, value: Self::Counter);

    fn fast_insert<Hash, F, V>(&mut self, op: F, value: V, hashed_val: &Hash)
    where
        Hash: MatrixFastHash,
        F: Fn(&mut Self::Counter, &V, usize),
        V: Clone;

    fn fast_query_min<Hash, F, R>(&self, hashed_val: &Hash, op: F) -> R
    where
        Hash: MatrixFastHash,
        F: Fn(&Self::Counter, usize, &Hash) -> R,
        R: PartialOrd;

    fn fast_query_median<Hash, F>(&self, hashed_val: &Hash, op: F) -> f64
    where
        Hash: MatrixFastHash,
        F: Fn(&Self::Counter, usize, &Hash) -> f64;

    fn query_one_counter(&self, row: usize, col: usize) -> Self::Counter;
}

pub trait FastPathHasher<H>: MatrixStorage
where
    H: crate::SketchHasher,
{
    fn hash_for_matrix(&self, value: &DataInput) -> H::HashType;
}
