
use super::matrix_storage::cols_mask_bits;
use crate::MatrixFastHash;

/// Shared thin wrapper over `Vec<T>` for sketches whose every matrix cell is a
/// fixed-length record.
///
/// `Vector3D` models a `rows * cols` grid where each `(row, col)` cell is a
/// contiguous run of `bucket_len` elements — a **bucket**. Storage is a single flat
/// `Vec<T>` in bucket-major order; the element at `(row, col, d)` lives at
/// `(row * cols + col) * bucket_len + d`.
///
/// Row and column addressing mirrors [`crate::Vector2D`]: the same
/// [`MatrixFastHash`] machinery selects one column per row, and the third
/// dimension is addressed within the selected bucket.
///
/// # When to reach for it
///
/// Use `Vector3D<T>` when, and only when, each cell is made of **several
/// mutually coupled fields** — that is, when the estimator or the update must
/// read or write those fields as a unit. When a cell is just `k` independent
/// counters, `k` separate [`Vector2D`](crate::Vector2D)s are equivalent and
/// simpler.
///
/// # Why not `Vector2D<[T; N]>`
///
/// `Vector2D<[T; N]>` has exactly the same memory layout. The one thing
/// `Vector3D` adds is a **`bucket_len` chosen at run time**: it comes from a
/// configuration value (a HyperLogLog precision, a sub-window count), which a
/// const generic cannot express.
///
/// # Layout
///
/// The layout is array-of-structs: a bucket's elements are adjacent, and
/// consecutive buckets are adjacent to each other. A per-item path touches
/// one bucket, and a full-table sweep over
/// `as_mut_slice().chunks_exact_mut(bucket_len)` walks the storage in order.
///
/// How much of a bucket a per-item path actually reads is the sketch's
/// business, not this type's — one caller here rewrites the whole record on
/// every insert, the other touches a single byte of a 256-byte one. What
/// the layout guarantees is contiguity, not that a bucket fits a cache line.
/// A sketch whose hot path touches *one* field across *all* cells is not a
/// fit either way.
#[derive(Clone, Debug)]
pub struct Vector3D<T> {
    data: Vec<T>,
    rows: usize,
    cols: usize,
    bucket_len: usize,
    mask_bits: u32,
    mask: u128,
}

impl<T> Vector3D<T> {
    /// Creates a container sized for `rows * cols * bucket_len` elements, with the
    /// storage left empty.
    ///
    /// The bucket accessors are only valid once [`Self::fill`] has populated
    /// the storage; until then [`Self::len`] reports `0`.
    ///
    /// Panics if any dimension is zero, or if `rows * cols * bucket_len` overflows
    /// `usize`.
    pub fn unfilled(rows: usize, cols: usize, bucket_len: usize) -> Self {
        assert!(
            rows > 0 && cols > 0 && bucket_len > 0,
            "Vector3D dimensions must be non-zero, got rows={rows}, cols={cols}, bucket_len={bucket_len}"
        );
        let len = rows
            .checked_mul(cols)
            .and_then(|n| n.checked_mul(bucket_len))
            .unwrap_or_else(|| {
                panic!(
                    "Vector3D rows * cols * bucket_len overflows usize, got \
                     rows={rows}, cols={cols}, bucket_len={bucket_len}"
                )
            });
        let mask_bits = cols_mask_bits(cols);
        Self {
            data: Vec::with_capacity(len),
            rows,
            cols,
            bucket_len,
            mask_bits,
            mask: (1u128 << mask_bits) - 1,
        }
    }

    /// Replaces the entire container with `rows * cols * bucket_len` clones of
    /// `value`, reusing the existing allocation.
    pub fn fill(&mut self, value: T)
    where
        T: Clone,
    {
        // The product is checked in `init`, which is the only way to set these
        // three fields.
        let len = self.rows * self.cols * self.bucket_len;
        self.data.clear();
        self.data.resize(len, value);
    }

    #[inline(always)]
    fn col_for_row<Hash: MatrixFastHash>(&self, hashed_val: &Hash, row: usize) -> usize {
        // Decode with the (mask_bits, mask) pair cached at construction, as
        // `Vector2D` does; `fold_to_col` owns the power-of-two skip.
        let raw = hashed_val.row_hash(row, self.mask_bits, self.mask);
        super::matrix_storage::fold_to_col(raw, self.cols)
    }

    #[inline(always)]
    fn bucket_start(&self, row: usize, col: usize) -> usize {
        (row * self.cols + col) * self.bucket_len
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

    /// Returns the per-bucket bucket_len (length of each `(row, col)` cell).
    #[inline(always)]
    pub fn bucket_len(&self) -> usize {
        self.bucket_len
    }

    /// Returns the total number of elements.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` when the container stores no elements.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Provides immutable access to the flattened storage.
    ///
    /// Iterating `as_slice().chunks_exact(bucket_len())` walks every bucket in
    /// `(row, col)` order.
    #[inline(always)]
    pub fn as_slice(&self) -> &[T] {
        &self.data
    }

    /// Provides mutable access to the flattened storage.
    ///
    /// Iterating `as_mut_slice().chunks_exact_mut(bucket_len())` walks every bucket
    /// in `(row, col)` order — the shape a periodic full-table sweep wants.
    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.data
    }

    /// Returns the `(row, col)` bucket slice.
    ///
    /// Panics if either index is out of range. The slice index below does not
    /// catch it: an out-of-range `col` still lands inside `data` and would
    /// hand back a different row's bucket.
    #[inline(always)]
    pub fn bucket_slice(&self, row: usize, col: usize) -> &[T] {
        assert!(
            row < self.rows && col < self.cols,
            "bucket ({row}, {col}) out of bounds for a {} x {} grid",
            self.rows,
            self.cols
        );
        let start = self.bucket_start(row, col);
        &self.data[start..start + self.bucket_len]
    }

    /// Returns the bit width needed to represent one column index.
    ///
    /// A packed fast-path hash carries `rows * mask_bits()` bits of column
    /// selection; callers that pack all rows into one `u128` must check that
    /// product against 128 before hashing.
    #[inline(always)]
    pub fn mask_bits(&self) -> u32 {
        self.mask_bits
    }

    /// Inserts along every row using a hashed column selection.
    ///
    /// For each row a column is selected from `hashed_val`, yielding one
    /// `(row, col)` bucket; the closure receives that **bucket slice**, the
    /// value, and the row index. This is the three-dimensional analogue of
    /// [`crate::Vector2D::fast_insert`], where the per-row target is a whole
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
            let end = start + self.bucket_len;
            op(&mut self.data[start..end], &value, row);
        }
    }

    /// Queries every row through a hashed column selection and returns the
    /// minimum of the per-row results.
    ///
    /// The closure receives the bucket slice and the row index.
    #[inline(always)]
    pub fn fast_query_min<Hash, F, R>(&self, hashed_val: &Hash, op: F) -> R
    where
        Hash: MatrixFastHash,
        F: Fn(&[T], usize) -> R,
        R: PartialOrd,
    {
        let c0 = self.col_for_row(hashed_val, 0);
        let mut min = op(self.bucket_slice(0, c0), 0);
        for row in 1..self.rows {
            let col = self.col_for_row(hashed_val, row);
            let candidate = op(self.bucket_slice(row, col), row);
            if candidate < min {
                min = candidate;
            }
        }
        min
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MatrixHash;

    #[test]
    fn fill_initializes_every_cell() {
        let mut v: Vector3D<u8> = Vector3D::unfilled(2, 4, 3);
        assert_eq!(v.len(), 0, "init leaves storage empty until fill");
        assert!(v.is_empty());
        v.fill(0);
        assert_eq!(v.len(), 2 * 4 * 3);
        assert!(!v.is_empty());
        assert!(v.as_slice().iter().all(|&x| x == 0));
        assert_eq!((v.rows(), v.cols(), v.bucket_len()), (2, 4, 3));
    }

    #[test]
    #[should_panic(expected = "dimensions must be non-zero")]
    fn unfilled_rejects_zero_bucket_len() {
        let _: Vector3D<u8> = Vector3D::unfilled(2, 4, 0);
    }

    #[test]
    fn buckets_tile_the_storage_without_gaps_or_overlap() {
        let mut v: Vector3D<u16> = Vector3D::unfilled(3, 5, 4);
        v.fill(0);
        // Stamp each bucket with a unique marker through the bucket accessor,
        // then verify the flat storage is exactly the expected tiling.
        let bucket_len = v.bucket_len();
        for (index, bucket) in v.as_mut_slice().chunks_exact_mut(bucket_len).enumerate() {
            for slot in bucket.iter_mut() {
                *slot = index as u16 + 1;
            }
        }
        let expected: Vec<u16> = (1..=15u16)
            .flat_map(|m| std::iter::repeat_n(m, 4))
            .collect();
        assert_eq!(v.as_slice(), expected.as_slice());
        // First and last buckets land where the index arithmetic says.
        assert_eq!(v.bucket_slice(0, 0), &[1, 1, 1, 1]);
        assert_eq!(v.bucket_slice(2, 4), &[15, 15, 15, 15]);
        // A sweep over chunks_exact sees every bucket exactly once.
        assert_eq!(v.as_slice().chunks_exact(v.bucket_len()).count(), 15);
    }

    #[test]
    fn depth_one_degenerates_to_a_plain_matrix() {
        let mut v: Vector3D<i32> = Vector3D::unfilled(2, 3, 1);
        v.fill(0);
        for (index, slot) in v.as_mut_slice().iter_mut().enumerate() {
            *slot = index as i32;
        }
        // With bucket_len 1 the flat storage is exactly the row-major matrix.
        assert_eq!(v.as_slice(), &[0, 1, 2, 3, 4, 5]);
        assert_eq!(v.len(), v.rows() * v.cols());
    }

    #[test]
    fn mask_bits_cover_the_column_index() {
        assert_eq!(Vector3D::<u8>::unfilled(1, 64, 1).mask_bits(), 6);
        assert_eq!(Vector3D::<u8>::unfilled(1, 1, 1).mask_bits(), 0);
        // Non-power-of-two widths round up to the next bit.
        assert_eq!(Vector3D::<u8>::unfilled(1, 17, 1).mask_bits(), 5);
        assert_eq!(Vector3D::<u8>::unfilled(1, 4096, 1).mask_bits(), 12);
    }

    #[test]
    fn fast_insert_and_query_min_agree_on_the_same_columns() {
        let mut v: Vector3D<u32> = Vector3D::unfilled(3, 64, 2);
        v.fill(0);
        let hash = MatrixHash::Packed64(0x0123_4567_89AB_CDEF);
        // Write a per-row marker into the selected bucket of every row.
        v.fast_insert(
            |bucket, &delta: &u32, row| {
                bucket[0] += delta;
                bucket[1] = row as u32;
            },
            7u32,
            &hash,
        );
        // The minimum over the same hash sees exactly what was written.
        let min_first: u32 = v.fast_query_min(&hash, |bucket, _| bucket[0]);
        assert_eq!(min_first, 7);
        let min_row: u32 = v.fast_query_min(&hash, |bucket, _| bucket[1]);
        assert_eq!(min_row, 0, "row 0's marker is the smallest");
        // Exactly one bucket per row was touched.
        let touched = v.as_slice().chunks_exact(2).filter(|b| b[0] == 7).count();
        assert_eq!(touched, 3);
    }

    #[test]
    #[should_panic(expected = "overflows usize")]
    fn unfilled_rejects_dimensions_whose_product_overflows() {
        // Each dimension is legal on its own; the product is not. Without the
        // check `Vec::with_capacity` receives the wrapped product and the
        // container reports a shape its storage cannot hold.
        let _: Vector3D<u8> = Vector3D::unfilled(3, usize::MAX / 2, 1);
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn bucket_slice_rejects_a_column_past_the_row() {
        let mut v: Vector3D<u16> = Vector3D::unfilled(2, 4, 3);
        v.fill(0);
        // In bounds of `data`, so the slice index alone would return row 1's
        // bucket instead of panicking.
        let _ = v.bucket_slice(0, 5);
    }

}
