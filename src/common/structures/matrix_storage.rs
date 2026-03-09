//! Trait bound for matrix-backed sketches.

use smallvec::SmallVec;

use crate::SketchInput;

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

pub trait MatrixStorage {
    type Counter: Clone;
    type HashValueType;
    fn rows(&self) -> usize;
    fn cols(&self) -> usize;

    fn update_one_counter<F, V>(&mut self, row: usize, col: usize, op: F, value: V)
    where
        F: Fn(&mut Self::Counter, V);

    fn increment_by_row(&mut self, row: usize, col: usize, value: Self::Counter);

    fn fast_insert<F, V>(&mut self, op: F, value: V, hashed_val: &Self::HashValueType)
    where
        F: Fn(&mut Self::Counter, &V, usize),
        V: Clone;

    fn fast_query_min<F, R>(&self, hashed_val: &Self::HashValueType, op: F) -> R
    where
        F: Fn(&Self::Counter, usize, &Self::HashValueType) -> R,
        R: PartialOrd;

    fn fast_query_median<F>(&self, hashed_val: &Self::HashValueType, op: F) -> f64
    where
        F: Fn(&Self::Counter, usize, &Self::HashValueType) -> f64;

    fn query_one_counter(&self, row: usize, col: usize) -> Self::Counter;
}

pub trait FastPathHasher: MatrixStorage {
    fn hash_for_matrix(&self, value: &SketchInput) -> Self::HashValueType;
}
