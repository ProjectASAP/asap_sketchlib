use serde::{Deserialize, Serialize};
use std::ops::{Index, IndexMut};

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
