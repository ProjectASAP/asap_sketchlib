use std::ops::{Index, IndexMut};

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
