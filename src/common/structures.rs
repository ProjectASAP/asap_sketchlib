use core::hash;
use std::ops::{Index, IndexMut};

use serde::{Deserialize, Serialize};

/// Lightweight wrapper for a row-major matrix that enforces rectangular shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SketchMatrix<T> {
    rows: usize,
    cols: usize,
    data: Vec<Vec<T>>,
}

impl<T> SketchMatrix<T> {
    /// Builds a matrix by cloning `value` into every cell.
    pub fn filled(rows: usize, cols: usize, value: T) -> Self
    where
        T: Clone,
    {
        let data = vec![vec![value; cols]; rows];
        Self { rows, cols, data }
    }

    /// Builds a matrix using a generator that receives the row/col indices.
    pub fn from_fn<F>(rows: usize, cols: usize, mut f: F) -> Self
    where
        F: FnMut(usize, usize) -> T,
    {
        let mut data = Vec::with_capacity(rows);
        for r in 0..rows {
            let mut row = Vec::with_capacity(cols);
            for c in 0..cols {
                row.push(f(r, c));
            }
            data.push(row);
        }
        Self { rows, cols, data }
    }

    /// Returns the number of rows.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Returns the number of columns.
    pub fn cols(&self) -> usize {
        self.cols
    }

    /// Provides immutable access to the underlying storage.
    pub fn as_slice(&self) -> &[Vec<T>] {
        &self.data
    }

    /// Provides mutable access to the underlying storage.
    pub fn as_mut_slice(&mut self) -> &mut [Vec<T>] {
        &mut self.data
    }

    /// Returns a reference to an element when it exists.
    pub fn get(&self, row: usize, col: usize) -> Option<&T> {
        self.data.get(row).and_then(|r| r.get(col))
    }

    /// Returns a mutable reference to an element when it exists.
    pub fn get_mut(&mut self, row: usize, col: usize) -> Option<&mut T> {
        self.data.get_mut(row).and_then(|r| r.get_mut(col))
    }

    /// Applies a visitor closure to every cell in row-major order.
    pub fn for_each_mut<F>(&mut self, mut f: F)
    where
        F: FnMut(usize, usize, &mut T),
    {
        for (r, row) in self.data.iter_mut().enumerate() {
            for (c, value) in row.iter_mut().enumerate() {
                f(r, c, value);
            }
        }
    }
}

impl<T> Index<usize> for SketchMatrix<T> {
    type Output = [T];

    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

impl<T> IndexMut<usize> for SketchMatrix<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.data[index]
    }
}

/// Shared thin wrapper over `Vec<T>` tailored for sketches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SketchList<T> {
    data: Vec<T>,
}

impl<T> SketchList<T> {
    /// Creates an empty list.
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    /// Builds a list by cloning `value` `len` times.
    pub fn filled(len: usize, value: T) -> Self
    where
        T: Clone,
    {
        Self {
            data: vec![value; len],
        }
    }

    /// Creates a list from supplied storage after validating length.
    pub fn from_vec(vec: Vec<T>) -> Self {
        Self { data: vec }
    }

    /// Number of elements currently stored.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Indicates whether the list is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Pushes a new element to the end.
    pub fn push(&mut self, value: T) {
        self.data.push(value);
    }

    /// Returns an iterator over immutable references.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.data.iter()
    }

    /// Returns an iterator over mutable references.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.data.iter_mut()
    }

    /// Returns a reference by index.
    pub fn get(&self, index: usize) -> Option<&T> {
        self.data.get(index)
    }

    /// Returns a mutable reference by index.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.data.get_mut(index)
    }

    /// Provides access to the underlying slice.
    pub fn as_slice(&self) -> &[T] {
        &self.data
    }

    /// Provides mutable access to the underlying slice.
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.data
    }

    /// Consumes the wrapper and returns the backing vector.
    pub fn into_vec(self) -> Vec<T> {
        self.data
    }
}

impl<T> Default for SketchList<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> From<Vec<T>> for SketchList<T> {
    fn from(value: Vec<T>) -> Self {
        Self::from_vec(value)
    }
}

impl<T> IntoIterator for SketchList<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a SketchList<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut SketchList<T> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.iter_mut()
    }
}

impl<T> Index<usize> for SketchList<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

impl<T> IndexMut<usize> for SketchList<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.data[index]
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
    pub fn update_one_counter<F>(&mut self, row: usize, col: usize, op: F, value: T)
    where
        F: Fn(T, T) -> T,
        T: Clone,
    {
        let idx = row * self.cols + col;
        self.data[idx] = op(self.data[idx].clone(), value);
    }

    /// Inserts a value along every row using a hashed column selection.
    pub fn fast_insert<F>(&mut self, op: F, value: T, hashed_val: u64)
    where
        F: Fn(T, T) -> T,
        T: Clone,
    {
        let mask = (1u64 << 13) - 1;
        for row in 0..self.rows {
            let hashed = (hashed_val >> (12 * row)) & mask;
            let col = ((hashed & ((1u64 << 32) - 1)) as usize) % self.cols;
            let idx = row * self.cols + col;
            self.data[idx] = op(self.data[idx].clone(), value.clone());
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
    pub fn fast_query(&self, hashed_val: u64) -> T
    where
        T: Clone + Ord,
    {
        // let mask = (1u64 << 13) - 1;
        // let mut min: Option<T> = None;
        // for row in 0..self.rows {
        //     let hashed = (hashed_val >> (12 * row)) & mask;
        //     let col = ((hashed & ((1u64 << 32) - 1)) as usize) % self.cols;
        //     let value = self.data[row * self.cols + col].clone();
        //     if let Some(current) = &mut min {
        //         if value < *current {
        //             *current = value;
        //         }
        //     } else {
        //         min = Some(value);
        //     }
        // }
        // min.expect("fast_query called on empty matrix")
        let mask = (1u64 << 13) - 1;
        let mut min = self.data[(hashed_val as usize & ((1usize << 13) - 1)) % self.cols].clone();
        for row in 1..self.rows {
            let hashed = (hashed_val >> (12 * row)) & mask;
            let col = ((hashed & ((1u64 << 32) - 1)) as usize) % self.cols;
            if min >  self.data[row * self.cols + col] {
                min = self.data[row*self.cols+col].clone();
            }
        }
        min
        //     let value = self.data[row * self.cols + col].clone();
        //     if let Some(current) = &mut min {
        //         if value < *current {
        //             *current = value;
        //         }
        //     } else {
        //         min = Some(value);
        //     }
        // }
        // min.expect("fast_query called on empty matrix")
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
