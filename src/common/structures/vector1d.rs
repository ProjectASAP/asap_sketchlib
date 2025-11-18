use serde::{Deserialize, Serialize};
use std::ops::{Index, IndexMut};

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
