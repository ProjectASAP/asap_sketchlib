//! Common data structure that is served as basic building block
//! Vector1D:
//! Vector2D:
//! Vector3D:
//! CommonHeap:

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
pub struct Vector1D<T> {
    data: Vec<T>,
    length: usize,
}

impl<T> Vector1D<T> {
    /// Creates an empty vector with reserved capacity.
    pub fn init(len: usize) -> Self {
        Self {
            data: Vec::with_capacity(len),
            length: len,
        }
    }

    /// Creates a vector by cloning `value` `len` times.
    pub fn filled(len: usize, value: T) -> Self
    where
        T: Clone,
    {
        Self {
            data: vec![value; len],
            length: len,
        }
    }

    /// Replaces the contents with `len` clones of `value`.
    pub fn fill(&mut self, value: T)
    where
        T: Clone,
    {
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
    pub fn update_one_counter<F, V>(&mut self, pos: usize, op: F, value: V)
    where
        F: Fn(&mut T, V),
        T: Clone,
    {
        op(&mut self.data[pos], value);
    }

    /// Appends an element to the back of the vector.
    pub fn push(&mut self, value: T) {
        self.data.push(value);
        self.length = self.data.len();
    }

    /// Moves all elements from `other` into `self`, leaving `other` empty.
    pub fn append(&mut self, other: &mut Vec<T>) {
        self.data.append(other);
        self.length = self.data.len();
    }

    /// Clones and appends all elements in a slice to the vector.
    pub fn extend_from_slice(&mut self, other: &[T])
    where
        T: Clone,
    {
        self.data.extend_from_slice(other);
        self.length = self.data.len();
    }

    /// Swaps two elements in the vector.
    pub fn swap(&mut self, a: usize, b: usize) {
        self.data.swap(a, b);
    }

    /// Sorts the vector with a comparator function.
    pub fn sort_by<F>(&mut self, compare: F)
    where
        F: FnMut(&T, &T) -> std::cmp::Ordering,
    {
        self.data.sort_by(compare);
    }

    /// Clears the vector, removing all values.
    pub fn clear(&mut self) {
        self.data.clear();
        self.length = 0;
    }
}

impl<T> Index<usize> for Vector1D<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        debug_assert!(index < self.length, "index out of bounds");
        &self.data[index]
    }
}

impl<T> IndexMut<usize> for Vector1D<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        debug_assert!(index < self.length, "index out of bounds");
        &mut self.data[index]
    }
}

impl<'a, T> IntoIterator for &'a Vector1D<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.iter()
    }
}

/// Shared thin wrapper over `Vec<T>` tailored for sketches.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Vector2D<T> {
    data: Vec<T>,
    rows: usize,
    cols: usize,
    mask_bits: u32,
    mask: u128,
}

impl<T> Vector2D<T> {
    /// Creates an empty matrix with reserved capacity.
    pub fn init(rows: usize, cols: usize) -> Self {
        let mask_bits = if cols.is_power_of_two() {
            cols.ilog2()
        } else {
            cols.ilog2() + 1
        };
        let mask = (1u128 << mask_bits) - 1;
        Self {
            data: Vec::with_capacity(rows * cols),
            rows,
            cols,
            mask_bits,
            mask,
        }
    }

    /// Builds a matrix using a generator that receives `(row, col)`.
    pub fn from_fn<F>(rows: usize, cols: usize, mut f: F) -> Self
    where
        F: FnMut(usize, usize) -> T,
    {
        let mask_bits = if cols.is_power_of_two() {
            cols.ilog2()
        } else {
            cols.ilog2() + 1
        };
        let mask = (1u128 << mask_bits) - 1;
        let mut data = Vec::with_capacity(rows * cols);
        for r in 0..rows {
            for c in 0..cols {
                data.push(f(r, c));
            }
        }
        Self {
            data,
            rows,
            cols,
            mask_bits,
            mask,
        }
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

    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
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
        bits_required *= self.rows;
        bits_required = 32 << ((bits_required > 32) as u32 + (bits_required > 64) as u32);
        bits_required = bits_required.min(128);
        bits_required
    }

    /// Inserts a value along every row using a hashed column selection.
    ///
    /// The closure receives three parameters: mutable counter reference, the value,
    /// and the current row index. For simple operations that don't need the row index,
    /// use `_` to ignore it (zero performance cost due to compiler optimization).
    ///
    /// # Examples
    ///
    /// Simple increment (row-independent):
    /// ```ignore
    /// sketch.fast_insert(|counter, value, _| *counter += value, 1, hash);
    /// ```
    ///
    /// Row-dependent operation (e.g., Count sketch with sign bits):
    /// ```ignore
    /// sketch.fast_insert(|counter, value, row| {
    ///     let sign = compute_sign(hash, row);
    ///     *counter += sign * value;
    /// }, 1, hash);
    /// ```
    #[inline(always)]
    pub fn fast_insert<F, V>(&mut self, op: F, value: V, hashed_val: u128)
    where
        F: Fn(&mut T, V, usize),
        V: Clone,
    {
        let mask_bits = self.mask_bits;
        let mask = self.mask;
        let cols = self.cols;
        for row in 0..self.rows {
            let hashed = (hashed_val >> (mask_bits as usize * row)) & mask;
            let col = (hashed as usize) % cols;
            let idx = row * cols + col;
            op(&mut self.data[idx], value.clone(), row);
        }
    }

    /// Reads a single counter by `(row, col)`.
    /// seems to be faster than [][] operation
    pub fn query_one_counter(&self, row: usize, col: usize) -> T
    where
        T: Clone,
    {
        self.data[row * self.cols + col].clone()
    }

    /// Queries all rows using precomputed hashed values to find the minimum.
    ///
    /// The closure receives: counter reference, row index, and hash value.
    /// Use `_` to ignore unused parameters (zero performance cost).
    ///
    /// # Examples
    ///
    /// Simple min (row-independent):
    /// ```ignore
    /// let min = sketch.fast_query_min(hash, |val, _, _| *val);
    /// ```
    ///
    /// Row-dependent with transformation:
    /// ```ignore
    /// let min = sketch.fast_query_min(hash, |val, row, _| *val as f64 * weight(row));
    /// ```
    #[inline(always)]
    pub fn fast_query_min<F, R>(&self, hashed_val: u128, op: F) -> R
    where
        F: Fn(&T, usize, u128) -> R,
        R: Ord,
    {
        let mask_bits = self.mask_bits;
        let mask = self.mask;
        let cols = self.cols;
        let hashed = hashed_val & mask;
        let c0 = (hashed as usize) % cols;
        let mut min = op(&self.data[c0], 0, hashed_val);
        for row in 1..self.rows {
            let hashed = (hashed_val >> (mask_bits as usize * row)) & mask;
            let col = (hashed as usize) % cols;
            let idx = row * cols + col;
            let candidate = op(&self.data[idx], row, hashed_val);
            if candidate < min {
                min = candidate;
            }
        }
        min
    }

    /// Queries all rows using precomputed hashed values to find the median.
    ///
    /// The closure receives: counter reference, row index, and hash value.
    /// Returns f64 values which are collected and sorted to compute median.
    /// Use `_` to ignore unused parameters (zero performance cost).
    ///
    /// # Examples
    ///
    /// Simple median (row-independent):
    /// ```ignore
    /// let median = sketch.fast_query_median(hash, |val, _, _| *val as f64);
    /// ```
    ///
    /// Row-dependent (e.g., Count sketch with sign bits):
    /// ```ignore
    /// let median = sketch.fast_query_median(hash, |val, row, hash| {
    ///     let sign_bit = (hash >> (127 - row)) & 1;
    ///     let sign = -(1 - 2 * sign_bit as i64) as f64;
    ///     *val as f64 * sign
    /// });
    /// ```
    #[inline(always)]
    pub fn fast_query_median<F>(&self, hashed_val: u128, op: F) -> f64
    where
        F: Fn(&T, usize, u128) -> i64,
    {
        let mask_bits = self.mask_bits;
        let mask = self.mask;
        let mut estimates = Vec::with_capacity(self.rows);
        for row in 0..self.rows {
            let hashed = (hashed_val >> (mask_bits as usize * row)) & mask;
            let col = (hashed as usize) % self.cols;
            let idx = row * self.cols + col;
            estimates.push(op(&self.data[idx], row, hashed_val));
        }

        // Inline median computation
        self.compute_median_inline_f64(&mut estimates)
    }

    /// Queries all rows using precomputed hashed values to find the maximum.
    ///
    /// The closure receives: counter reference, row index, and hash value.
    /// Use `_` to ignore unused parameters (zero performance cost).
    ///
    /// # Examples
    ///
    /// Simple max (row-independent):
    /// ```ignore
    /// let max = sketch.fast_query_max(hash, |val, _, _| *val);
    /// ```
    ///
    /// Row-dependent with transformation:
    /// ```ignore
    /// let max = sketch.fast_query_max(hash, |val, row, _| *val as f64 / (row + 1) as f64);
    /// ```
    #[inline(always)]
    pub fn fast_query_max<F, R>(&self, hashed_val: u128, op: F) -> R
    where
        F: Fn(&T, usize, u128) -> R,
        R: Ord,
    {
        let mask_bits = self.mask_bits;
        let mask = self.mask;
        let cols = self.cols;
        let hashed = hashed_val & mask;
        let c0 = (hashed as usize) % cols;
        let mut max = op(&self.data[c0], 0, hashed_val);
        for row in 1..self.rows {
            let hashed = (hashed_val >> (mask_bits as usize * row)) & mask;
            let col = (hashed as usize) % cols;
            let idx = row * cols + col;
            let candidate = op(&self.data[idx], row, hashed_val);
            if candidate > max {
                max = candidate;
            }
        }
        max
    }

    /// Queries all rows to find the minimum with a query key.
    ///
    /// The closure receives: counter reference, query key, row index, and hash value.
    /// Use `_` to ignore unused parameters (zero performance cost).
    ///
    /// # Examples
    ///
    /// With complex counter type:
    /// ```ignore
    /// let min = sketch.fast_query_min_with_key(hash, &query_key,
    ///     |counter, key, _, _| counter.estimate(key));
    /// ```
    #[inline(always)]
    pub fn fast_query_min_with_key<F, Q, R>(&self, hashed_val: u128, query_key: &Q, op: F) -> R
    where
        F: Fn(&T, &Q, usize, u128) -> R,
        R: Ord,
    {
        let mask_bits = self.mask_bits;
        let mask = self.mask;
        let cols = self.cols;
        let hashed = hashed_val & mask;
        let c0 = (hashed as usize) % cols;
        let mut min = op(&self.data[c0], query_key, 0, hashed_val);
        for row in 1..self.rows {
            let hashed = (hashed_val >> (mask_bits as usize * row)) & mask;
            let col = (hashed as usize) % cols;
            let idx = row * cols + col;
            let candidate = op(&self.data[idx], query_key, row, hashed_val);
            if candidate < min {
                min = candidate;
            }
        }
        min
    }

    /// Queries all rows to find the maximum with a query key.
    ///
    /// The closure receives: counter reference, query key, row index, and hash value.
    /// Use `_` to ignore unused parameters (zero performance cost).
    ///
    /// # Examples
    ///
    /// With complex counter type:
    /// ```ignore
    /// let max = sketch.fast_query_max_with_key(hash, &query_key,
    ///     |counter, key, _, _| counter.estimate(key));
    /// ```
    #[inline(always)]
    pub fn fast_query_max_with_key<F, Q, R>(&self, hashed_val: u128, query_key: &Q, op: F) -> R
    where
        F: Fn(&T, &Q, usize, u128) -> R,
        R: Ord,
    {
        let mask_bits = self.mask_bits;
        let mask = self.mask;
        let cols = self.cols;
        let hashed = hashed_val & mask;
        let c0 = (hashed as usize) % cols;
        let mut max = op(&self.data[c0], query_key, 0, hashed_val);
        for row in 1..self.rows {
            let hashed = (hashed_val >> (mask_bits as usize * row)) & mask;
            let col = (hashed as usize) % cols;
            let idx = row * cols + col;
            let candidate = op(&self.data[idx], query_key, row, hashed_val);
            if candidate > max {
                max = candidate;
            }
        }
        max
    }

    /// Queries all rows to find the median with a query key.
    ///
    /// The closure receives: counter reference, query key, row index, and hash value.
    /// Returns f64 values which are collected and sorted to compute median.
    /// Use `_` to ignore unused parameters (zero performance cost).
    ///
    /// # Examples
    ///
    /// With complex counter type:
    /// ```ignore
    /// let median = sketch.fast_query_median_with_key(hash, &query_key,
    ///     |counter, key, _, _| counter.estimate(key));
    /// ```
    #[inline(always)]
    pub fn fast_query_median_with_key<F, Q>(&self, hashed_val: u128, query_key: &Q, op: F) -> f64
    where
        F: Fn(&T, &Q, usize, u128) -> i64,
    {
        let mask_bits = self.mask_bits;
        let mask = self.mask;
        let mut estimates = Vec::with_capacity(self.rows);
        for row in 0..self.rows {
            let hashed = (hashed_val >> (mask_bits as usize * row)) & mask;
            let col = (hashed as usize) % self.cols;
            let idx = row * self.cols + col;
            estimates.push(op(&self.data[idx], query_key, row, hashed_val));
        }

        self.compute_median_inline_f64(&mut estimates)
    }

    /// Compute median from a mutable slice of f64 values (inline helper)
    /// This is used by query_median_with_custom_hash for HydraCounter queries
    #[inline(always)]
    fn compute_median_inline_f64(&self, values: &mut [i64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        values.sort_unstable();
        let mid = values.len() / 2;
        if values.len() % 2 == 1 {
            values[mid] as f64
        } else {
            (values[mid - 1] + values[mid]) as f64 / 2.0
        }
    }

    /// Queries all rows with custom aggregation logic (fold/reduce pattern).
    ///
    /// This is the most flexible query method, allowing custom aggregation beyond
    /// min/max/median. Uses a fold pattern where the closure receives an accumulator
    /// and updates it for each row.
    ///
    /// The closure receives: accumulator, counter reference, query key, row index, and hash value.
    /// Use `_` to ignore unused parameters (zero performance cost).
    ///
    /// # Examples
    ///
    /// Custom sum with row-dependent weights:
    /// ```ignore
    /// let sum = sketch.fast_query_aggregate(hash, &(), 0.0,
    ///     |acc, val, _, row, _| acc + (*val as f64 * weight(row)));
    /// ```
    ///
    /// Count sketch estimation (sign-based sum then median):
    /// ```ignore
    /// let mut estimates = Vec::new();
    /// sketch.fast_query_aggregate(hash, &(), &mut estimates,
    ///     |acc, val, _, row, hash| {
    ///         let sign_bit = (hash >> (127 - row)) & 1;
    ///         let sign = -(1 - 2 * sign_bit as i64) as f64;
    ///         acc.push(*val as f64 * sign);
    ///         acc
    ///     });
    /// ```
    #[inline(always)]
    pub fn fast_query_aggregate<F, Q, R>(
        &self,
        hashed_val: u128,
        query_key: &Q,
        init: R,
        fold_fn: F,
    ) -> R
    where
        F: Fn(R, &T, &Q, usize, u128) -> R,
    {
        let mask_bits = self.mask_bits;
        let mask = self.mask;
        let mut acc = init;
        for row in 0..self.rows {
            let hashed = (hashed_val >> (mask_bits as usize * row)) & mask;
            let col = (hashed as usize) % self.cols;
            let idx = row * self.cols + col;
            acc = fold_fn(acc, &self.data[idx], query_key, row, hashed_val);
        }
        acc
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

/// Trait defining heap ordering behavior.
/// Implement this trait to define custom heap orderings.
pub trait CommonHeapOrder<T> {
    /// Returns true if parent and child should be swapped.
    /// This determines whether the heap is min or max, and what property to compare.
    fn should_swap(&self, parent: &T, child: &T) -> bool;

    /// Returns true if the new value should replace the root when heap is at capacity.
    /// For min-heap: new value should be larger than root
    /// For max-heap: new value should be smaller than root
    fn should_replace_root(&self, root: &T, new_value: &T) -> bool;
}

/// Min-heap ordering: smaller values have higher priority (bubble up).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommonMinHeap;

impl<T: Ord> CommonHeapOrder<T> for CommonMinHeap {
    #[inline(always)]
    fn should_swap(&self, parent: &T, child: &T) -> bool {
        child < parent
    }

    #[inline(always)]
    fn should_replace_root(&self, root: &T, new_value: &T) -> bool {
        // For min-heap: replace root (minimum) if new value is larger
        new_value > root
    }
}

/// Max-heap ordering: larger values have higher priority (bubble up).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommonMaxHeap;

impl<T: Ord> CommonHeapOrder<T> for CommonMaxHeap {
    #[inline(always)]
    fn should_swap(&self, parent: &T, child: &T) -> bool {
        child > parent
    }

    #[inline(always)]
    fn should_replace_root(&self, root: &T, new_value: &T) -> bool {
        // For max-heap: replace root (maximum) if new value is smaller
        new_value < root
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommonHeap<T, O: CommonHeapOrder<T>> {
    data: Vec<T>,
    size: usize,
    order: O,
}

impl<T, O: CommonHeapOrder<T>> CommonHeap<T, O> {
    /// Creates a new heap with the specified capacity and ordering.
    pub fn with_capacity(capacity: usize, order: O) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            size: capacity,
            order,
        }
    }

    /// Returns the number of elements currently in the heap.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true if the heap is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the maximum capacity of the heap.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.size
    }

    /// Returns true if the heap is at full capacity.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.data.len() >= self.size
    }

    /// Clears all elements from the heap.
    #[inline]
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Returns a reference to the root element (min or max depending on order) without removing it.
    #[inline]
    pub fn peek(&self) -> Option<&T> {
        self.data.first()
    }

    /// Returns a mutable reference to the root element without removing it.
    #[inline]
    pub fn peek_mut(&mut self) -> Option<&mut T> {
        self.data.first_mut()
    }

    /// Inserts an element into the heap.
    /// If the heap is at capacity, the root element is replaced if appropriate.
    pub fn push(&mut self, value: T) {
        if self.data.len() < self.size {
            self.data.push(value);
            self.bubble_up(self.data.len() - 1);
        } else if !self.data.is_empty() && self.order.should_replace_root(&self.data[0], &value) {
            // For bounded heap: replace root if new value should replace it
            self.data[0] = value;
            self.bubble_down(0);
        }
    }

    /// Removes and returns the root element (min or max depending on order).
    pub fn pop(&mut self) -> Option<T> {
        if self.data.is_empty() {
            return None;
        }
        if self.data.len() == 1 {
            return self.data.pop();
        }
        let root = self.data.swap_remove(0);
        self.bubble_down(0);
        Some(root)
    }

    /// Updates an element at the given index and maintains heap property.
    /// Returns true if the element was moved.
    #[inline]
    pub fn update_at(&mut self, index: usize) -> bool {
        if index >= self.data.len() {
            return false;
        }
        if !self.bubble_down(index) {
            self.bubble_up(index);
            true
        } else {
            true
        }
    }

    /// Provides immutable access to the underlying data slice.
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        &self.data
    }

    /// Returns an iterator over heap elements (not in sorted order).
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.data.iter()
    }

    /// Returns a mutable iterator over heap elements.
    /// Warning: Modifying elements may break heap invariants.
    #[inline]
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.data.iter_mut()
    }

    /// Gets the index of the left child.
    #[inline(always)]
    fn left_child(i: usize) -> usize {
        2 * i + 1
    }

    /// Gets the index of the right child.
    #[inline(always)]
    fn right_child(i: usize) -> usize {
        2 * i + 2
    }

    /// Gets the index of the parent.
    #[inline(always)]
    fn parent(i: usize) -> usize {
        (i.saturating_sub(1)) / 2
    }

    /// Bubbles an element down to maintain heap property.
    /// Returns true if the element was moved.
    fn bubble_down(&mut self, mut idx: usize) -> bool {
        let start_idx = idx;
        let len = self.data.len();

        while idx < len {
            let left = Self::left_child(idx);
            let right = Self::right_child(idx);
            let mut target = idx;

            // Find which child (if any) should be swapped with parent
            if left < len && self.order.should_swap(&self.data[target], &self.data[left]) {
                target = left;
            }
            if right < len
                && self
                    .order
                    .should_swap(&self.data[target], &self.data[right])
            {
                target = right;
            }

            if target == idx {
                break;
            }

            self.data.swap(idx, target);
            idx = target;
        }

        idx != start_idx
    }

    /// Bubbles an element up to maintain heap property.
    fn bubble_up(&mut self, mut idx: usize) {
        while idx > 0 {
            let parent_idx = Self::parent(idx);
            if self
                .order
                .should_swap(&self.data[parent_idx], &self.data[idx])
            {
                self.data.swap(parent_idx, idx);
                idx = parent_idx;
            } else {
                break;
            }
        }
    }
}

// Convenience constructors for common heap types
impl<T: Ord> CommonHeap<T, CommonMinHeap> {
    /// Creates a new min-heap with the specified capacity.
    #[inline]
    pub fn new_min(capacity: usize) -> Self {
        Self::with_capacity(capacity, CommonMinHeap)
    }
}

impl<T: Ord> CommonHeap<T, CommonMaxHeap> {
    /// Creates a new max-heap with the specified capacity.
    #[inline]
    pub fn new_max(capacity: usize) -> Self {
        Self::with_capacity(capacity, CommonMaxHeap)
    }
}

impl<T, O: CommonHeapOrder<T>> Index<usize> for CommonHeap<T, O> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

impl<T, O: CommonHeapOrder<T>> IndexMut<usize> for CommonHeap<T, O> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.data[index]
    }
}

#[cfg(test)]
mod heap_tests {
    use crate::{CommonHeap, CommonHeapOrder, CommonMaxHeap, CommonMinHeap, common::input::HHItem};

    #[test]
    fn test_min_heap_basic() {
        let mut heap = CommonHeap::<i32, CommonMinHeap>::new_min(5);
        heap.push(5);
        heap.push(3);
        heap.push(7);
        heap.push(1);

        assert_eq!(heap.peek(), Some(&1));
        assert_eq!(heap.pop(), Some(1));
        assert_eq!(heap.pop(), Some(3));
        assert_eq!(heap.pop(), Some(5));
        assert_eq!(heap.pop(), Some(7));
        assert_eq!(heap.pop(), None);
    }

    #[test]
    fn test_max_heap_basic() {
        let mut heap = CommonHeap::<i32, CommonMaxHeap>::new_max(5);
        heap.push(5);
        heap.push(3);
        heap.push(7);
        heap.push(1);

        assert_eq!(heap.peek(), Some(&7));
        assert_eq!(heap.pop(), Some(7));
        assert_eq!(heap.pop(), Some(5));
        assert_eq!(heap.pop(), Some(3));
        assert_eq!(heap.pop(), Some(1));
        assert_eq!(heap.pop(), None);
    }

    #[test]
    fn test_bounded_heap_capacity() {
        let mut heap = CommonHeap::<i32, CommonMinHeap>::new_min(3);

        heap.push(5);
        heap.push(3);
        heap.push(7);
        assert_eq!(heap.len(), 3);

        // Should not grow beyond capacity
        heap.push(1);
        assert_eq!(heap.len(), 3);

        // Smallest should be replaced by larger value since it's a min heap
        heap.push(10);
        assert_eq!(heap.len(), 3);

        // Should contain 5, 7, 10 (1 and 3 were kicked out)
        let mut vals: Vec<i32> = vec![];
        while let Some(v) = heap.pop() {
            vals.push(v);
        }
        vals.sort();
        assert_eq!(vals, vec![5, 7, 10]);
    }

    #[test]
    fn test_update_at() {
        let mut heap = CommonHeap::<i32, CommonMinHeap>::new_min(5);
        heap.push(10);
        heap.push(20);
        heap.push(5);

        // Modify element and update heap
        heap[1] = 3;
        heap.update_at(1);

        assert_eq!(heap.peek(), Some(&3));
    }

    #[test]
    fn test_custom_struct_with_ord() {
        let mut heap = CommonHeap::<HHItem, CommonMinHeap>::new_min(3);
        heap.push(HHItem::new("five".to_string(), 5));
        heap.push(HHItem::new("three".to_string(), 3));
        heap.push(HHItem::new("seven".to_string(), 7));

        assert_eq!(heap.peek().map(|item| item.count), Some(3));
    }

    #[test]
    fn test_topk_use_case() {
        // Simulates TopKHeap use case: maintain top-K items by count
        // Use min-heap so smallest is at root and can be evicted

        // Create a min-heap with capacity 3 to keep top-3 items
        let mut heap = CommonHeap::<HHItem, CommonMinHeap>::new_min(3);

        // Insert items (simulating TopKHeap behavior)
        for i in 1..=5 {
            heap.push(HHItem::new(format!("key-{i}"), i));
        }

        // Should keep top 3: counts 3, 4, 5
        assert_eq!(heap.len(), 3);
        let mut counts: Vec<i64> = heap.iter().map(|item| item.count).collect();
        counts.sort_unstable();
        assert_eq!(counts, vec![3, 4, 5]);

        // Test finding an item (linear search like TopKHeap::find)
        let found = heap.iter().find(|item| item.key == "key-4");
        assert!(found.is_some());
        assert_eq!(found.unwrap().count, 4);
    }

    #[test]
    fn test_heap_size() {
        // Verify that MinHeap/MaxHeap add zero overhead
        use std::mem::size_of;

        let vec_size = size_of::<Vec<u64>>();
        let heap_min_size = size_of::<CommonHeap<u64, CommonMinHeap>>();
        let heap_max_size = size_of::<CommonHeap<u64, CommonMaxHeap>>();

        println!("Vec<u64> size: {vec_size}");
        println!("Heap<u64, MinHeap> size: {heap_min_size}");
        println!("Heap<u64, MaxHeap> size: {heap_max_size}");

        // Vec is (ptr, capacity, len) = 24 bytes on 64-bit
        // Our heap is (Vec, usize, O) where O is zero-sized
        // So it should be 24 + 8 = 32 bytes
        assert_eq!(heap_min_size, vec_size + size_of::<usize>());
        assert_eq!(heap_max_size, vec_size + size_of::<usize>());
    }

    #[test]
    fn test_topk_with_custom_comparator() {
        // Example of custom heap ordering (though Item already has Ord by count)
        // This demonstrates how to create custom orderings
        #[derive(Clone)]
        struct CompareByCount;

        impl CommonHeapOrder<HHItem> for CompareByCount {
            fn should_swap(&self, parent: &HHItem, child: &HHItem) -> bool {
                child.count < parent.count
            }

            fn should_replace_root(&self, root: &HHItem, new_value: &HHItem) -> bool {
                new_value.count > root.count
            }
        }

        let mut heap = CommonHeap::<HHItem, CompareByCount>::with_capacity(3, CompareByCount);

        heap.push(HHItem::new("a".to_string(), 5));
        heap.push(HHItem::new("b".to_string(), 3));
        heap.push(HHItem::new("c".to_string(), 7));
        heap.push(HHItem::new("d".to_string(), 1)); // Won't be added
        heap.push(HHItem::new("e".to_string(), 10)); // Will replace min

        assert_eq!(heap.len(), 3);
        let min_count = heap.peek().map(|item| item.count);
        assert_eq!(min_count, Some(5)); // 5 is now the minimum in the heap
    }

    #[test]
    fn test_exact_topk_heap_replacement() {
        // This test demonstrates EXACT TopKHeap behavior using generic Heap

        // TopKHeap::init_heap(3) equivalent:
        let mut heap = CommonHeap::<HHItem, CommonMinHeap>::new_min(3);

        // TopKHeap::update("key-1", 1) equivalent:
        let find_and_update =
            |heap: &mut CommonHeap<HHItem, CommonMinHeap>, key: &str, count: i64| {
                // TopKHeap::find() equivalent:
                let idx_opt = heap.iter().position(|item| item.key == key);

                if let Some(idx) = idx_opt {
                    // Found: update count
                    heap[idx].count = count;
                    heap.update_at(idx);
                } else {
                    // Not found: insert (TopKHeap::insert equivalent)
                    heap.push(HHItem::new(key.to_string(), count));
                }
            };

        // Replicate the exact test from TopKHeap
        for i in 1..=5 {
            let key = format!("key-{i}");
            find_and_update(&mut heap, &key, i);
        }

        // Should match TopKHeap behavior exactly
        assert_eq!(heap.len(), 3);
        let mut counts: Vec<i64> = heap.iter().map(|item| item.count).collect();
        counts.sort_unstable();
        assert_eq!(counts, vec![3, 4, 5]); // Same as TopKHeap test!

        // TopKHeap::find() equivalent:
        let found = heap.iter().find(|item| item.key == "key-4");
        assert!(found.is_some());
        assert_eq!(found.unwrap().count, 4);

        // TopKHeap::clean() equivalent:
        heap.clear();
        assert!(heap.is_empty());
    }
}
