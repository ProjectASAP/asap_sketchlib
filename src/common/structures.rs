use std::ops::{Index, IndexMut};

use serde::{Deserialize, Serialize};

/// Helper trait for converting sketch counter types to f64 for median calculation.
pub trait ToF64 {
    fn to_f64(self) -> f64;
}

impl ToF64 for u64 {
    fn to_f64(self) -> f64 {
        self as f64
    }
}

impl ToF64 for i64 {
    fn to_f64(self) -> f64 {
        self as f64
    }
}

impl ToF64 for u32 {
    fn to_f64(self) -> f64 {
        self as f64
    }
}

impl ToF64 for i32 {
    fn to_f64(self) -> f64 {
        self as f64
    }
}

/// Shared thin wrapper over `Vec<T>` tailored for sketches.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Vector1D<T: Clone> {
    data: Vec<T>,
    length: usize,
}

impl<T: Clone> Vector1D<T> {
    /// Creates an empty vector with reserved capacity.
    pub fn init(len: usize) -> Self {
        Self {
            data: Vec::with_capacity(len),
            length: len,
        }
    }

    /// Creates a vector by cloning `value` `len` times.
    pub fn filled(len: usize, value: T) -> Self {
        Self {
            data: vec![value; len],
            length: len,
        }
    }

    /// Replaces the contents with `len` clones of `value`.
    pub fn fill(&mut self, value: T) {
        self.data.clear();
        self.data.resize(self.length, value);
        self.length = self.data.len();
    }

    /// Builds a vector from supplied storage.
    pub fn from_vec(vec: Vec<T>) -> Self {
        let length = vec.len();
        Self { data: vec, length }
    }

    /// Returns the number of stored elements.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Indicates whether the vector is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Provides immutable access to the underlying slice.
    pub fn as_slice(&self) -> &[T] {
        &self.data
    }

    /// Provides mutable access to the underlying slice.
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.data
    }

    /// Returns a reference by index when it exists.
    pub fn get(&self, index: usize) -> Option<&T> {
        self.data.get(index)
    }

    /// Returns a mutable reference by index when it exists.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.data.get_mut(index)
    }

    /// Returns an iterator over immutable references.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.data.iter()
    }

    /// Returns an iterator over mutable references.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.data.iter_mut()
    }

    /// Consumes the wrapper and returns the underlying vector.
    pub fn into_vec(self) -> Vec<T> {
        self.data
    }

    /// Update value at ```pos``` if ```val``` is greater
    pub fn update_if_greater(&mut self, pos: usize, val: T)
    where
        T: Copy + Ord,
    {
        self.data[pos] = self.data[pos].max(val);
    }

    /// Update value at ```pos``` if ```val``` is greater
    pub fn update_if_smaller(&mut self, pos: usize, val: T)
    where
        T: Copy + Ord,
    {
        self.data[pos] = self.data[pos].min(val);
    }

    /// Applies an update to a single cell via the supplied operator.
    pub fn update_one_counter<F>(&mut self, pos: usize, op: F, value: T)
    where
        F: Fn(T, T) -> T,
        T: Clone,
    {
        self.data[pos] = op(self.data[pos].clone(), value);
    }
}

/// Shared thin wrapper over `Vec<T>` tailored for sketches.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Vector2D<T> {
    data: Vec<T>,
    rows: usize,
    cols: usize,
}

impl<T> Vector2D<T> {
    /// Creates an empty matrix with reserved capacity.
    pub fn init(rows: usize, cols: usize) -> Self {
        Self {
            data: Vec::with_capacity(rows * cols),
            rows,
            cols,
        }
    }

    /// Builds a matrix using a generator that receives `(row, col)`.
    pub fn from_fn<F>(rows: usize, cols: usize, mut f: F) -> Self
    where
        F: FnMut(usize, usize) -> T,
    {
        let mut data = Vec::with_capacity(rows * cols);
        for r in 0..rows {
            for c in 0..cols {
                data.push(f(r, c));
            }
        }
        Self { data, rows, cols }
    }

    /// Replaces the contents with clones of `value`.
    pub fn fill(&mut self, value: T)
    where
        T: Clone,
    {
        self.data.clear();
        self.data.resize(self.rows * self.cols, value);
    }

    /// Returns the number of rows.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Returns the number of columns.
    pub fn cols(&self) -> usize {
        self.cols
    }

    /// Returns the total number of elements.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Provides immutable access to the flattened storage.
    pub fn as_slice(&self) -> &[T] {
        &self.data
    }

    /// Provides mutable access to the flattened storage.
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.data
    }

    /// Returns a reference to a cell when it exists.
    pub fn get(&self, row: usize, col: usize) -> Option<&T> {
        if row < self.rows && col < self.cols {
            Some(&self.data[row * self.cols + col])
        } else {
            None
        }
    }

    /// Returns a mutable reference to a cell when it exists.
    pub fn get_mut(&mut self, row: usize, col: usize) -> Option<&mut T> {
        if row < self.rows && col < self.cols {
            Some(&mut self.data[row * self.cols + col])
        } else {
            None
        }
    }

    /// Applies an update to a single cell via the supplied operator.
    pub fn update_one_counter<F, V>(&mut self, row: usize, col: usize, op: F, value: V)
    where
        F: Fn(&mut T, V),
        T: Clone,
    {
        let idx = row * self.cols + col;
        op(&mut self.data[idx], value);
    }

    /// get the number of bits required to cover the col size
    #[inline(always)]
    pub fn get_mask_bits(&self) -> u32 {
        if self.cols.is_power_of_two() {
            self.cols.ilog2()
        } else {
            self.cols.ilog2() + 1
        }
    }

    /// get the number of bits required for hashed value
    /// only three case possible: 32, 64, 128
    #[inline]
    pub fn get_required_bits(&self) -> usize {
        let mut bits_required = self.get_mask_bits() as usize;
        bits_required = bits_required * self.rows;
        bits_required = 32 << ((bits_required > 32) as u32 + (bits_required > 64) as u32);
        bits_required = bits_required.min(128);
        bits_required
    }

    /// Inserts a value along every row using a hashed column selection.
    #[inline(always)]
    pub fn fast_insert<F, V>(&mut self, op: F, value: V, hashed_val: u128)
    where
        F: Fn(&mut T, V),
        V: Clone,
    {
        let mask_bits = self.get_mask_bits();
        let mask = (1u128 << mask_bits) - 1;
        let cols = self.cols;
        for row in 0..self.rows {
            let hashed = (hashed_val >> (mask_bits as usize * row)) & mask;
            let col = (hashed as usize) % cols;
            let idx = row * cols + col;
            op(&mut self.data[idx], value.clone());
        }
    }

    /// Reads a single counter by `(row, col)`.
    pub fn query_one_counter(&self, row: usize, col: usize) -> T
    where
        T: Clone,
    {
        self.data[row * self.cols + col].clone()
    }

    /// Queries all rows using precomputed hashed values to find the minimum.
    #[inline(always)]
    pub fn fast_query_min(&self, hashed_val: u128) -> T
    where
        T: Clone + Ord,
    {
        let mask_bits = self.get_mask_bits();
        let mask = (1u128 << mask_bits) - 1;
        let cols = self.cols;
        let hashed = (hashed_val) & mask;
        let c0 = (hashed as usize) % cols;
        let mut min = self.data[c0].clone();
        for row in 1..self.rows {
            let hashed = (hashed_val >> (mask_bits as usize * row)) & mask;
            let col = (hashed as usize) % cols;
            let idx = row * cols + col;
            let candidate = self.data[idx].clone();
            if candidate < min {
                min = candidate;
            }
        }
        min
    }

    /// Queries all rows using precomputed hashed values to find the median.
    ///
    /// Note: This method requires T to be convertible to f64 for median calculation.
    #[inline(always)]
    pub fn fast_query_median(&self, hashed_val: u128) -> f64
    where
        T: Clone + Ord + Copy + ToF64,
    {
        let mask_bits = self.get_mask_bits();
        let mask = (1u128 << mask_bits) - 1;
        let mut estimates = Vec::with_capacity(self.rows);
        for r in 0..self.rows {
            let hashed = (hashed_val >> (mask_bits as usize * r)) & mask;
            let col = (hashed as usize) % self.cols;
            let idx = r * self.cols + col;
            estimates.push(self.data[idx]);
        }
        if estimates.is_empty() {
            return 0.0;
        }
        estimates.sort_unstable();
        let mid = estimates.len() / 2;
        if estimates.len() % 2 == 1 {
            estimates[mid].to_f64()
        } else {
            let left = estimates[mid - 1].to_f64();
            let right = estimates[mid].to_f64();
            (left + right) / 2.0
        }
    }

    /// Queries all rows using precomputed hashed values to find the maximum.
    #[inline(always)]
    pub fn fast_query_max(&self, hashed_val: u128) -> T
    where
        T: Clone + Ord,
    {
        let mask_bits = self.get_mask_bits();
        let mask = (1u128 << mask_bits) - 1;
        let cols = self.cols;
        let hashed = (hashed_val) & mask;
        let c0 = (hashed as usize) % cols;
        let mut max = self.data[c0].clone();
        for row in 1..self.rows {
            let hashed = (hashed_val >> (mask_bits as usize * row)) & mask;
            let col = (hashed as usize) % cols;
            let idx = row * cols + col;
            let candidate = self.data[idx].clone();
            if candidate > max {
                max = candidate;
            }
        }
        max
    }

    /// Returns an immutable slice corresponding to a full row.
    pub fn row_slice(&self, row: usize) -> &[T] {
        debug_assert!(row < self.rows, "row index out of bounds");
        let start = row * self.cols;
        let end = start + self.cols;
        &self.data[start..end]
    }

    /// Returns a mutable slice corresponding to a full row.
    pub fn row_slice_mut(&mut self, row: usize) -> &mut [T] {
        debug_assert!(row < self.rows, "row index out of bounds");
        let start = row * self.cols;
        let end = start + self.cols;
        &mut self.data[start..end]
    }

    /// Returns the number of rows (legacy helper).
    pub fn get_row(&self) -> usize {
        self.rows
    }

    /// Returns the number of columns (legacy helper).
    pub fn get_col(&self) -> usize {
        self.cols
    }
}

impl<T> Index<usize> for Vector2D<T> {
    type Output = [T];

    fn index(&self, index: usize) -> &Self::Output {
        self.row_slice(index)
    }
}

impl<T> IndexMut<usize> for Vector2D<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.row_slice_mut(index)
    }
}

/// Shared thin wrapper over `Vec<T>` tailored for sketches.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Vector3D<T> {
    data: Vec<T>,
    layer: usize,
    row: usize,
    col: usize,
}

impl<T> Vector3D<T> {
    pub fn init(layer: usize, row: usize, col: usize) -> Self {
        Self {
            data: Vec::with_capacity(layer * row * col),
            layer,
            row,
            col,
        }
    }
}
