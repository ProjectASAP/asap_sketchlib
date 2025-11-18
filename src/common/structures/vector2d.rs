use serde::{Deserialize, Serialize};
use std::ops::{Index, IndexMut};
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
