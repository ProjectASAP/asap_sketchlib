//! A fixed integer matrix.
//! Size fixed at compile time and heap-backed via Box.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::ops::{Index, IndexMut, Range};

use crate::{MatrixStorage, compute_median_inline_f64};

pub const QUICKSTART_ROW_NUM: usize = 5;
pub const QUICKSTART_COL_NUM: usize = 2048;
pub const QUICKSTART_SIZE: usize = QUICKSTART_ROW_NUM * QUICKSTART_COL_NUM;
pub const DEFAULT_ROW_NUM: usize = 3;
pub const DEFAULT_COL_NUM: usize = 4096;

/// The greater is P, the smaller the error.
const HLL_P: usize = 14_usize;
const NUM_REGISTERS: usize = 1_usize << HLL_P;

const fn mask_bits(cols: usize) -> usize {
    let mut bits = 0;
    let mut value = 1;
    while value < cols {
        value <<= 1;
        bits += 1;
    }
    bits
}

#[derive(Clone, Debug)]
pub struct HllBucketList {
    pub registers: Box<[u8; NUM_REGISTERS]>,
}

impl Default for HllBucketList {
    fn default() -> Self {
        Self {
            registers: Box::new([0_u8; NUM_REGISTERS]),
        }
    }
}

impl Serialize for HllBucketList {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serde_big_array::BigArray::serialize(&*self.registers, serializer)
    }
}

impl<'de> Deserialize<'de> for HllBucketList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data: [u8; NUM_REGISTERS] = serde_big_array::BigArray::deserialize(deserializer)?;
        Ok(Self {
            registers: Box::new(data),
        })
    }
}

impl Index<usize> for HllBucketList {
    type Output = u8;

    fn index(&self, index: usize) -> &Self::Output {
        debug_assert!(index < NUM_REGISTERS, "index out of bounds");
        &self.registers[index]
    }
}

impl IndexMut<usize> for HllBucketList {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        debug_assert!(index < NUM_REGISTERS, "index out of bounds");
        &mut self.registers[index]
    }
}

impl Index<Range<usize>> for HllBucketList {
    type Output = [u8];

    fn index(&self, range: Range<usize>) -> &Self::Output {
        debug_assert!(range.end <= NUM_REGISTERS, "range end out of bounds");
        &self.registers[range]
    }
}

impl IndexMut<Range<usize>> for HllBucketList {
    fn index_mut(&mut self, range: Range<usize>) -> &mut Self::Output {
        debug_assert!(range.end <= NUM_REGISTERS, "range end out of bounds");
        &mut self.registers[range]
    }
}

impl<'a> IntoIterator for &'a HllBucketList {
    type Item = &'a u8;
    type IntoIter = std::slice::Iter<'a, u8>;

    fn into_iter(self) -> Self::IntoIter {
        self.registers.iter()
    }
}

impl HllBucketList {
    pub fn len(&self) -> usize {
        NUM_REGISTERS as usize
    }
}

macro_rules! impl_fixed_matrix {
    ($name:ident, $counter:ty, $rows:literal, $cols:literal, $hash_ty:ty) => {
        #[derive(Clone, Debug)]
        pub struct $name {
            pub data: Box<[$counter; $rows * $cols]>,
        }

        impl $name {
            const MASK_BITS: usize = mask_bits($cols);
            const MASK: $hash_ty = ((1 as $hash_ty) << Self::MASK_BITS) - 1;
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    data: Box::new([0 as $counter; $rows * $cols]),
                }
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serde_big_array::BigArray::serialize(&*self.data, serializer)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let data: [$counter; $rows * $cols] =
                    serde_big_array::BigArray::deserialize(deserializer)?;
                Ok(Self {
                    data: Box::new(data),
                })
            }
        }

        impl MatrixStorage for $name {
            type Counter = $counter;
            type HashValueType = $hash_ty;

            #[inline(always)]
            fn rows(&self) -> usize {
                $rows
            }

            #[inline(always)]
            fn cols(&self) -> usize {
                $cols
            }

            #[inline(always)]
            fn update_one_counter<F, V>(&mut self, row: usize, col: usize, op: F, value: V)
            where
                F: Fn(&mut Self::Counter, V),
            {
                let idx = row * $cols + col;
                op(&mut self.data[idx], value);
            }

            #[inline(always)]
            fn increment_by_row(&mut self, row: usize, col: usize, value: Self::Counter) {
                let idx = row * $cols + col;
                self.data[idx] += value;
            }

            #[inline(always)]
            fn fast_insert<F, V>(&mut self, op: F, value: V, hashed_val: &$hash_ty)
            where
                F: Fn(&mut Self::Counter, &V, usize),
                V: Clone,
            {
                let hashed_val = *hashed_val;
                for row in 0..$rows {
                    let hashed = (hashed_val >> (Self::MASK_BITS * row)) & Self::MASK;
                    let col = (hashed as usize) % $cols;
                    let idx = row * $cols + col;
                    op(&mut self.data[idx], &value, row);
                }
            }

            #[inline(always)]
            fn fast_query_min<F, R>(&self, hashed_val: &$hash_ty, op: F) -> R
            where
                F: Fn(&Self::Counter, usize, &$hash_ty) -> R,
                R: PartialOrd,
            {
                let hashed_val = *hashed_val;
                let hashed = hashed_val & Self::MASK;
                let col = (hashed as usize) % $cols;
                let mut min = op(&self.data[col], 0, &hashed_val);
                for row in 1..$rows {
                    let hashed = (hashed_val >> (Self::MASK_BITS * row)) & Self::MASK;
                    let col = (hashed as usize) % $cols;
                    let idx = row * $cols + col;
                    let candidate = op(&self.data[idx], row, &hashed_val);
                    if candidate < min {
                        min = candidate;
                    }
                }
                min
            }

            #[inline(always)]
            fn fast_query_median<F>(&self, hashed_val: &$hash_ty, op: F) -> f64
            where
                F: Fn(&Self::Counter, usize, &$hash_ty) -> f64,
            {
                let hashed_val = *hashed_val;
                let mut estimates = Vec::with_capacity($rows);
                for row in 0..$rows {
                    let hashed = (hashed_val >> (Self::MASK_BITS * row)) & Self::MASK;
                    let col = (hashed as usize) % $cols;
                    let idx = row * $cols + col;
                    estimates.push(op(&self.data[idx], row, &hashed_val));
                }
                compute_median_inline_f64(&mut estimates)
            }

            #[inline(always)]
            fn query_one_counter(&self, row: usize, col: usize) -> Self::Counter {
                self.data[row * $cols + col]
            }
        }
    };
}

impl_fixed_matrix!(QuickMatrixI32, i32, 5, 2048, u64);
impl_fixed_matrix!(QuickMatrixI64, i64, 5, 2048, u64);
impl_fixed_matrix!(QuickMatrixI128, i128, 5, 2048, u64);

impl_fixed_matrix!(DefaultMatrixI32, i32, 3, 4096, u64);
impl_fixed_matrix!(DefaultMatrixI64, i64, 3, 4096, u64);
impl_fixed_matrix!(DefaultMatrixI128, i128, 3, 4096, u64);

/// Backward compatibility: FixedMatrix = QuickMatrixI32.
pub type FixedMatrix = QuickMatrixI32;
