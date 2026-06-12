use serde::{Deserialize, Serialize};
use std::ops::{Index, IndexMut};

use crate::{MatrixFastHash, MatrixHashType, Nitro, compute_median_inline_f64};

/// Shared thin wrapper over `Vec<T>` tailored for layered / per-bucket sketches.
///
/// `Vector3D` is the three-dimensional sibling of [`crate::Vector2D`]. It models
/// a `rows * cols` grid where **every `(row, col)` cell is itself a contiguous
/// run of `depth` elements** — a "bucket". This is the natural storage for
/// sketches that keep a small vector (e.g. a HyperLogLog register array) at each
/// matrix position.
///
/// Storage is a single flat `Vec<T>` in row-major / bucket-major order; the
/// element at `(row, col, d)` lives at `(row * cols + col) * depth + d`.
///
/// The row/column addressing (mask bits, `col_for_row`, hashing) mirrors
/// [`crate::Vector2D`] exactly, so the same `MatrixFastHash` machinery selects a
/// column per row; the third dimension is addressed within the selected bucket.
#[derive(Clone, Debug, Serialize)]
pub struct Vector3D<T> {
    data: Vec<T>,
    rows: usize,
    cols: usize,
    depth: usize,
    mask_bits: u32,
    mask: u128,
    nitro: Nitro,
}

// Helper type for deserialization: we only read stored fields and recompute
// derived ones (mask_bits, mask) from cols, mirroring `Vector2D`.
#[derive(Deserialize)]
struct Vector3DDeserialize<T> {
    data: Vec<T>,
    rows: usize,
    cols: usize,
    depth: usize,
    #[serde(default)]
    nitro: Nitro,
}

#[inline]
fn mask_bits_for_cols(cols: usize) -> u32 {
    if cols.is_power_of_two() {
        cols.ilog2()
    } else {
        cols.ilog2() + 1
    }
}

impl<'de, T> Deserialize<'de> for Vector3D<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let input = Vector3DDeserialize::deserialize(deserializer)?;
        let mask_bits = mask_bits_for_cols(input.cols);
        let mask = (1u128 << mask_bits) - 1;
        Ok(Self {
            data: input.data,
            rows: input.rows,
            cols: input.cols,
            depth: input.depth,
            mask_bits,
            mask,
            nitro: input.nitro,
        })
    }
}

impl<T> Vector3D<T> {
    /// Creates an empty container with reserved capacity for `rows * cols * depth`
    /// elements. The underlying storage is left uninitialized until `fill` or
    /// similar methods are called, allowing callers to decide when and how cells
    /// are populated.
    pub fn init(rows: usize, cols: usize, depth: usize) -> Self {
        let mask_bits = mask_bits_for_cols(cols);
        let mask = (1u128 << mask_bits) - 1;
        Self {
            data: Vec::with_capacity(rows * cols * depth),
            rows,
            cols,
            depth,
            mask_bits,
            mask,
            nitro: Nitro::default(),
        }
    }

    /// Builds a container by invoking a generator for every `(row, col, d)`
    /// position. Useful for types that require per-cell construction logic
    /// instead of cloning a single value across all cells.
    pub fn from_fn<F>(rows: usize, cols: usize, depth: usize, mut f: F) -> Self
    where
        F: FnMut(usize, usize, usize) -> T,
    {
        let mask_bits = mask_bits_for_cols(cols);
        let mask = (1u128 << mask_bits) - 1;
        let mut data = Vec::with_capacity(rows * cols * depth);
        for r in 0..rows {
            for c in 0..cols {
                for d in 0..depth {
                    data.push(f(r, c, d));
                }
            }
        }
        Self {
            data,
            rows,
            cols,
            depth,
            mask_bits,
            mask,
            nitro: Nitro::default(),
        }
    }

    /// Enables Nitro sampling with the provided rate.
    pub fn enable_nitro(&mut self, sampling_rate: f64) {
        self.nitro = Nitro::init_nitro(sampling_rate);
    }

    /// Disables Nitro sampling and resets the internal state.
    pub fn disable_nitro(&mut self) {
        self.nitro = Nitro::default();
    }

    #[inline(always)]
    /// Decrements the Nitro skip counter by one.
    pub fn reduce_to_skip(&mut self) {
        self.nitro.reduce_to_skip();
    }

    /// Returns the Nitro configuration.
    #[inline(always)]
    pub fn nitro(&self) -> &Nitro {
        &self.nitro
    }

    #[inline(always)]
    /// Returns the current Nitro delta weight.
    pub fn get_delta(&self) -> u64 {
        self.nitro.delta
    }

    /// Returns a mutable Nitro configuration reference.
    #[inline(always)]
    pub fn nitro_mut(&mut self) -> &mut Nitro {
        &mut self.nitro
    }

    /// Replaces the entire container with `rows * cols * depth` clones of `value`,
    /// reusing the existing allocation. This is the most efficient way to reset
    /// cells to a baseline without reallocating.
    pub fn fill(&mut self, value: T)
    where
        T: Clone,
    {
        self.data.clear();
        self.data.resize(self.rows * self.cols * self.depth, value);
    }

    #[inline(always)]
    fn col_for_row<Hash: MatrixFastHash>(&self, hashed_val: &Hash, row: usize) -> usize {
        hashed_val.col_for_row(row, self.cols)
    }

    #[inline(always)]
    fn bucket_start(&self, row: usize, col: usize) -> usize {
        (row * self.cols + col) * self.depth
    }

    /// Returns the number of rows.
    #[inline(always)]
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Returns the number of columns.
    #[inline(always)]
    pub fn cols(&self) -> usize {
        self.cols
    }

    /// Returns the per-bucket depth (length of each `(row, col)` cell).
    #[inline(always)]
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Allocates one extra row initialized with `value`.
    pub fn allocate_extra_row(&mut self, value: T)
    where
        T: Clone,
    {
        self.rows += 1;
        self.data.resize(self.rows * self.cols * self.depth, value);
    }

    /// Returns the total number of elements.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline(always)]
    /// Returns `true` when the container stores no elements.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Provides immutable access to the flattened storage.
    #[inline(always)]
    pub fn as_slice(&self) -> &[T] {
        &self.data
    }

    /// Provides mutable access to the flattened storage.
    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.data
    }

    /// Returns a reference to a single element when it exists.
    #[inline(always)]
    pub fn get(&self, row: usize, col: usize, d: usize) -> Option<&T> {
        if row < self.rows && col < self.cols && d < self.depth {
            Some(&self.data[self.bucket_start(row, col) + d])
        } else {
            None
        }
    }

    /// Returns a mutable reference to a single element when it exists.
    #[inline(always)]
    pub fn get_mut(&mut self, row: usize, col: usize, d: usize) -> Option<&mut T> {
        if row < self.rows && col < self.cols && d < self.depth {
            let idx = self.bucket_start(row, col) + d;
            Some(&mut self.data[idx])
        } else {
            None
        }
    }

    /// Returns the `(row, col)` bucket slice when it exists.
    #[inline(always)]
    pub fn bucket(&self, row: usize, col: usize) -> Option<&[T]> {
        if row < self.rows && col < self.cols {
            let start = self.bucket_start(row, col);
            Some(&self.data[start..start + self.depth])
        } else {
            None
        }
    }

    /// Returns the `(row, col)` bucket slice mutably when it exists.
    #[inline(always)]
    pub fn bucket_mut(&mut self, row: usize, col: usize) -> Option<&mut [T]> {
        if row < self.rows && col < self.cols {
            let start = self.bucket_start(row, col);
            Some(&mut self.data[start..start + self.depth])
        } else {
            None
        }
    }

    /// Returns the `(row, col)` bucket slice, debug-asserting bounds.
    /// Faster sibling of [`Self::bucket`].
    #[inline(always)]
    pub fn bucket_slice(&self, row: usize, col: usize) -> &[T] {
        debug_assert!(row < self.rows && col < self.cols, "bucket out of bounds");
        let start = self.bucket_start(row, col);
        &self.data[start..start + self.depth]
    }

    /// Mutable sibling of [`Self::bucket_slice`].
    #[inline(always)]
    pub fn bucket_slice_mut(&mut self, row: usize, col: usize) -> &mut [T] {
        debug_assert!(row < self.rows && col < self.cols, "bucket out of bounds");
        let start = self.bucket_start(row, col);
        &mut self.data[start..start + self.depth]
    }

    /// Applies an update to a single element via the supplied operator.
    #[inline(always)]
    pub fn update_one_counter<F, V>(&mut self, row: usize, col: usize, d: usize, op: F, value: V)
    where
        F: Fn(&mut T, V),
    {
        let idx = self.bucket_start(row, col) + d;
        op(&mut self.data[idx], value);
    }

    /// get the number of bits required to cover the col size
    #[inline(always)]
    /// Returns the bit width needed to represent a column index.
    pub fn get_mask_bits(&self) -> u32 {
        mask_bits_for_cols(self.cols)
    }

    /// get the number of bits required for hashed value
    /// only three case possible: 32, 64, 128
    #[inline]
    /// Returns the packed hash width needed for all rows.
    pub fn get_required_bits(&self) -> usize {
        let mut bits_required = self.get_mask_bits() as usize;
        bits_required *= self.rows;
        bits_required = 32 << ((bits_required > 32) as u32 + (bits_required > 64) as u32);
        bits_required = bits_required.min(128);
        bits_required
    }

    /// Inserts along every row using a hashed column selection.
    ///
    /// For each row a column is selected from `hashed_val`, yielding one
    /// `(row, col)` bucket; the closure receives that **bucket slice**, the
    /// value, and the current row index. This is the three-dimensional analogue
    /// of [`crate::Vector2D::fast_insert`], where the per-row target is a whole
    /// bucket rather than a single counter.
    #[inline(always)]
    pub fn fast_insert<Hash, F, V>(&mut self, op: F, value: V, hashed_val: &Hash)
    where
        Hash: MatrixFastHash,
        F: Fn(&mut [T], &V, usize),
        V: Clone,
    {
        for row in 0..self.rows {
            let col = self.col_for_row(hashed_val, row);
            let start = self.bucket_start(row, col);
            let end = start + self.depth;
            op(&mut self.data[start..end], &value, row);
        }
    }

    #[inline(always)]
    /// Decrements the Nitro skip counter by `c`.
    pub fn reduce_nitro_skip(&mut self, c: usize) {
        self.nitro.reduce_to_skip_by_count(c)
    }

    #[inline(always)]
    /// Sets the Nitro skip counter to `c`.
    pub fn update_nitro_skip(&mut self, c: usize) {
        self.nitro.to_skip = c
    }

    #[inline(always)]
    /// Returns the current Nitro skip counter.
    pub fn get_nitro_skip(&mut self) -> usize {
        self.nitro.to_skip
    }

    /// Reads a single element by `(row, col, d)`.
    #[inline(always)]
    pub fn query_one_counter(&self, row: usize, col: usize, d: usize) -> T
    where
        T: Clone,
    {
        self.data[self.bucket_start(row, col) + d].clone()
    }

    /// Queries all rows using precomputed hashed values to find the minimum.
    ///
    /// The closure receives: bucket slice, row index, and hash value.
    #[inline(always)]
    pub fn fast_query_min<Hash, F, R>(&self, hashed_val: &Hash, op: F) -> R
    where
        Hash: MatrixFastHash,
        F: Fn(&[T], usize, &Hash) -> R,
        R: PartialOrd,
    {
        let c0 = self.col_for_row(hashed_val, 0);
        let mut min = op(self.bucket_slice(0, c0), 0, hashed_val);
        for row in 1..self.rows {
            let col = self.col_for_row(hashed_val, row);
            let candidate = op(self.bucket_slice(row, col), row, hashed_val);
            if candidate < min {
                min = candidate;
            }
        }
        min
    }

    /// Queries all rows using precomputed hashed values to find the median.
    ///
    /// The closure receives: bucket slice, row index, and hash value, and
    /// returns `f64` values which are collected and reduced to a median.
    #[inline(always)]
    pub fn fast_query_median<Hash, F>(&self, hashed_val: &Hash, op: F) -> f64
    where
        Hash: MatrixFastHash,
        F: Fn(&[T], usize, &Hash) -> f64,
    {
        let mut estimates = Vec::with_capacity(self.rows);
        for row in 0..self.rows {
            let col = self.col_for_row(hashed_val, row);
            estimates.push(op(self.bucket_slice(row, col), row, hashed_val));
        }
        compute_median_inline_f64(&mut estimates)
    }

    /// Queries all rows using precomputed hashed values to find the maximum.
    ///
    /// The closure receives: bucket slice, row index, and hash value.
    #[inline(always)]
    pub fn fast_query_max<F, R>(&self, hashed_val: &MatrixHashType, op: F) -> R
    where
        F: Fn(&[T], usize, &MatrixHashType) -> R,
        R: PartialOrd,
    {
        let c0 = self.col_for_row(hashed_val, 0);
        let mut max = op(self.bucket_slice(0, c0), 0, hashed_val);
        for row in 1..self.rows {
            let col = self.col_for_row(hashed_val, row);
            let candidate = op(self.bucket_slice(row, col), row, hashed_val);
            if candidate > max {
                max = candidate;
            }
        }
        max
    }

    /// Queries all rows to find the minimum with a query key.
    ///
    /// The closure receives: bucket slice, query key, row index, and hash value.
    #[inline(always)]
    pub fn fast_query_min_with_key<F, Q, R>(
        &self,
        hashed_val: &MatrixHashType,
        query_key: &Q,
        op: F,
    ) -> R
    where
        F: Fn(&[T], &Q, usize, &MatrixHashType) -> R,
        R: PartialOrd,
    {
        let c0 = self.col_for_row(hashed_val, 0);
        let mut min = op(self.bucket_slice(0, c0), query_key, 0, hashed_val);
        for row in 1..self.rows {
            let col = self.col_for_row(hashed_val, row);
            let candidate = op(self.bucket_slice(row, col), query_key, row, hashed_val);
            if candidate < min {
                min = candidate;
            }
        }
        min
    }

    /// Queries all rows to find the maximum with a query key.
    ///
    /// The closure receives: bucket slice, query key, row index, and hash value.
    #[inline(always)]
    pub fn fast_query_max_with_key<F, Q, R>(
        &self,
        hashed_val: &MatrixHashType,
        query_key: &Q,
        op: F,
    ) -> R
    where
        F: Fn(&[T], &Q, usize, &MatrixHashType) -> R,
        R: PartialOrd,
    {
        let c0 = self.col_for_row(hashed_val, 0);
        let mut max = op(self.bucket_slice(0, c0), query_key, 0, hashed_val);
        for row in 1..self.rows {
            let col = self.col_for_row(hashed_val, row);
            let candidate = op(self.bucket_slice(row, col), query_key, row, hashed_val);
            if candidate > max {
                max = candidate;
            }
        }
        max
    }

    /// Queries all rows to find the median with a query key.
    ///
    /// The closure receives: bucket slice, query key, row index, and hash value.
    #[inline(always)]
    pub fn fast_query_median_with_key<F, Q>(
        &self,
        hashed_val: &MatrixHashType,
        query_key: &Q,
        op: F,
    ) -> f64
    where
        F: Fn(&[T], &Q, usize, &MatrixHashType) -> f64,
    {
        let mut estimates = Vec::with_capacity(self.rows);
        for row in 0..self.rows {
            let col = self.col_for_row(hashed_val, row);
            estimates.push(op(self.bucket_slice(row, col), query_key, row, hashed_val));
        }
        compute_median_inline_f64(&mut estimates)
    }

    /// Queries all rows with custom aggregation logic (fold/reduce pattern).
    ///
    /// The closure receives: accumulator, bucket slice, query key, row index, and
    /// hash value.
    #[inline(always)]
    pub fn fast_query_aggregate<F, Q, R>(
        &self,
        hashed_val: &MatrixHashType,
        query_key: &Q,
        init: R,
        fold_fn: F,
    ) -> R
    where
        F: Fn(R, &[T], &Q, usize, &MatrixHashType) -> R,
    {
        let mut acc = init;
        for row in 0..self.rows {
            let col = self.col_for_row(hashed_val, row);
            acc = fold_fn(acc, self.bucket_slice(row, col), query_key, row, hashed_val);
        }
        acc
    }

    /// Returns an immutable slice corresponding to a full row plane
    /// (`cols * depth` elements).
    #[inline(always)]
    pub fn row_slice(&self, row: usize) -> &[T] {
        debug_assert!(row < self.rows, "row index out of bounds");
        let start = row * self.cols * self.depth;
        let end = start + self.cols * self.depth;
        &self.data[start..end]
    }

    /// Returns a mutable slice corresponding to a full row plane.
    #[inline(always)]
    pub fn row_slice_mut(&mut self, row: usize) -> &mut [T] {
        debug_assert!(row < self.rows, "row index out of bounds");
        let start = row * self.cols * self.depth;
        let end = start + self.cols * self.depth;
        &mut self.data[start..end]
    }

    /// Returns the number of rows (legacy helper).
    #[inline(always)]
    pub fn get_row(&self) -> usize {
        self.rows
    }

    /// Returns the number of columns (legacy helper).
    #[inline(always)]
    pub fn get_col(&self) -> usize {
        self.cols
    }

    /// Returns the per-bucket depth (legacy helper).
    #[inline(always)]
    pub fn get_depth(&self) -> usize {
        self.depth
    }
}

impl<T> Index<usize> for Vector3D<T> {
    type Output = [T];

    fn index(&self, index: usize) -> &Self::Output {
        self.row_slice(index)
    }
}

impl<T> IndexMut<usize> for Vector3D<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.row_slice_mut(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_bits_match_expected_thresholds() {
        let default_dims: Vector3D<u64> = Vector3D::init(3, 4096, 8);
        assert_eq!(default_dims.get_required_bits(), 64);

        let smaller_cols: Vector3D<u64> = Vector3D::init(3, 64, 8);
        assert_eq!(smaller_cols.get_required_bits(), 32);

        let larger_shape: Vector3D<u64> = Vector3D::init(5, 1_048_576, 8);
        assert_eq!(larger_shape.get_required_bits(), 128);
    }

    #[test]
    fn fill_initializes_every_cell() {
        let mut v: Vector3D<u8> = Vector3D::init(2, 4, 3);
        v.fill(0);
        assert_eq!(v.len(), 2 * 4 * 3);
        assert!(!v.is_empty());
        assert!(v.as_slice().iter().all(|&x| x == 0));
        assert_eq!(v.rows(), 2);
        assert_eq!(v.cols(), 4);
        assert_eq!(v.depth(), 3);
    }

    #[test]
    fn from_fn_addresses_every_position() {
        let v = Vector3D::from_fn(2, 3, 2, |r, c, d| (r * 100 + c * 10 + d) as u32);
        assert_eq!(v.get(0, 0, 0), Some(&0));
        assert_eq!(v.get(1, 2, 1), Some(&121));
        assert_eq!(v.get(2, 0, 0), None);
        assert_eq!(v.bucket(1, 2), Some([120u32, 121u32].as_slice()));
        assert_eq!(v.bucket(0, 3), None);
    }

    #[test]
    fn bucket_and_element_mutation_round_trips() {
        let mut v: Vector3D<u8> = Vector3D::init(2, 2, 4);
        v.fill(0);
        v.bucket_slice_mut(1, 0)[2] = 7;
        v.update_one_counter(0, 1, 3, |a, b| *a = b, 9);
        assert_eq!(v.query_one_counter(1, 0, 2), 7);
        assert_eq!(v.get(0, 1, 3), Some(&9));
        // Untouched bucket stays zero.
        assert!(v.bucket_slice(0, 0).iter().all(|&x| x == 0));
        // Row plane spans cols * depth elements.
        assert_eq!(v.row_slice(0).len(), 2 * 4);
        assert_eq!(v[1].len(), 2 * 4);
    }
}
