use serde::{Deserialize, Serialize};
use std::ops::{Index, IndexMut, Range};

/// Shared thin wrapper over `Vec<T>` tailored for sketches.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Vector1D<T> {
    data: Vec<T>,
}

impl<T> Vector1D<T> {
    /// Creates an empty vector with reserved capacity.
    pub fn init(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    /// Creates a vector by cloning `value` `len` times.
    pub fn filled(len: usize, value: T) -> Self
    where
        T: Clone,
    {
        Self {
            data: vec![value; len],
        }
    }

    /// Replaces the contents with `len` clones of `value`.
    pub fn fill(&mut self, value: T)
    where
        T: Clone,
    {
        let l = self.data.len();
        self.data.clear();
        self.data.resize(l, value);
    }

    /// Builds a vector from supplied storage.
    pub fn from_vec(vec: Vec<T>) -> Self {
        Self { data: vec }
    }

    /// Returns the number of stored elements.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn insert(&mut self, pos: usize, val: T) {
        self.data.insert(pos, val);
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

    pub fn last_mut(&mut self) -> Option<&mut T> {
        self.data.last_mut()
    }

    /// Returns a raw mutable pointer to the backing storage.
    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.data.as_mut_ptr()
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
    #[inline(always)]
    pub fn update_if_greater(&mut self, pos: usize, val: T)
    where
        T: Copy + Ord,
    {
        self.data[pos] = self.data[pos].max(val);
    }

    /// Update value at ```pos``` if ```val``` is greater
    #[inline(always)]
    pub fn update_if_smaller(&mut self, pos: usize, val: T)
    where
        T: Copy + Ord,
    {
        self.data[pos] = self.data[pos].min(val);
    }

    /// Applies an update to a single cell via the supplied operator.
    #[inline(always)]
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
    }

    /// Truncates the vector to the specified length.
    pub fn truncate(&mut self, len: usize) {
        self.data.truncate(len);
    }

    /// Moves all elements from `other` into `self`, leaving `other` empty.
    pub fn append(&mut self, other: &mut Vec<T>) {
        self.data.append(other);
    }

    /// Clones and appends all elements in a slice to the vector.
    pub fn extend_from_slice(&mut self, other: &[T])
    where
        T: Clone,
    {
        self.data.extend_from_slice(other);
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

    /// Sorts without preserving order but without allocations.
    pub fn sort_unstable_by<F>(&mut self, compare: F)
    where
        F: FnMut(&T, &T) -> std::cmp::Ordering,
    {
        self.data.sort_unstable_by(compare);
    }

    /// Clears the vector, removing all values.
    pub fn clear(&mut self) {
        self.data.clear();
    }
}

impl<T> Index<usize> for Vector1D<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        debug_assert!(index < self.data.len(), "index out of bounds");
        &self.data[index]
    }
}

impl<T> IndexMut<usize> for Vector1D<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        debug_assert!(index < self.data.len(), "index out of bounds");
        &mut self.data[index]
    }
}

impl<T> Index<Range<usize>> for Vector1D<T> {
    type Output = [T];

    fn index(&self, range: Range<usize>) -> &Self::Output {
        debug_assert!(range.end <= self.data.len(), "range end out of bounds");
        &self.data[range]
    }
}

impl<T> IndexMut<Range<usize>> for Vector1D<T> {
    fn index_mut(&mut self, range: Range<usize>) -> &mut Self::Output {
        debug_assert!(range.end <= self.data.len(), "range end out of bounds");
        &mut self.data[range]
    }
}

impl<'a, T> IntoIterator for &'a Vector1D<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.iter()
    }
}
