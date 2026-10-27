use std::ops::{Index, IndexMut};

/// Trait defining heap ordering behavior.
/// Implement this trait to define custom heap orderings.
pub trait CommonHeapOrder<T> {
    /// Returns true if parent and child should be swapped.
    /// This determines which value sits at the root.
    fn should_swap(&self, parent: &T, child: &T) -> bool;

    /// Returns true if the new value should replace the root when heap is at capacity.
    fn should_replace_root(&self, root: &T, new_value: &T) -> bool;
}

/// Root is the smallest value; bounded heap retains the largest values.
#[derive(Clone, Debug)]
pub struct RetainLargest;

impl<T: Ord> CommonHeapOrder<T> for RetainLargest {
    #[inline(always)]
    fn should_swap(&self, parent: &T, child: &T) -> bool {
        child < parent
    }

    #[inline(always)]
    fn should_replace_root(&self, root: &T, new_value: &T) -> bool {
        // Replace root (smallest) if new value is larger.
        new_value > root
    }
}

/// Root is the largest value; bounded heap retains the smallest values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetainSmallest;

impl<T: Ord> CommonHeapOrder<T> for RetainSmallest {
    #[inline(always)]
    fn should_swap(&self, parent: &T, child: &T) -> bool {
        child > parent
    }

    #[inline(always)]
    fn should_replace_root(&self, root: &T, new_value: &T) -> bool {
        // Replace root (largest) if new value is smaller.
        new_value < root
    }
}

#[derive(Debug, PartialEq, Eq)]
/// Capacity-bounded binary heap with pluggable ordering.
pub struct CommonHeap<T, O: CommonHeapOrder<T>> {
    data: Vec<T>,
    size: usize,
    order: O,
}

impl<T: Clone, O: CommonHeapOrder<T> + Clone> Clone for CommonHeap<T, O> {
    /// Keeps the allocated slots, so a clone grows as its source would.
    fn clone(&self) -> Self {
        let mut data = Vec::with_capacity(self.data.capacity());
        data.extend_from_slice(&self.data);
        Self {
            data,
            size: self.size,
            order: self.order.clone(),
        }
    }
}

/// Slots reserved up front. A heap whose capacity is larger grows on demand.
pub(crate) const PREALLOCATED_SLOTS: usize = 1024;

impl<T, O: CommonHeapOrder<T>> CommonHeap<T, O> {
    /// Creates a new heap with the specified capacity and ordering.
    pub fn with_capacity(capacity: usize, order: O) -> Self {
        Self::with_expected_len(capacity, capacity, order)
    }

    /// Creates a heap of `capacity` that reserves for `expected_len` elements
    /// rather than for the capacity, so a caller that already knows how many
    /// elements it is about to push does not reserve for a capacity it will not
    /// fill. `capacity` still fixes [`Self::capacity`] and [`Self::is_full`].
    pub fn with_expected_len(capacity: usize, expected_len: usize, order: O) -> Self {
        Self {
            data: Vec::with_capacity(expected_len.min(capacity).min(PREALLOCATED_SLOTS)),
            size: capacity,
            order,
        }
    }

    /// Wraps `data` that already satisfies the heap invariant for `order`,
    /// holding the slots a heap built by [`Self::with_capacity`] holds after
    /// as many pushes.
    pub(crate) fn from_heap_vec(data: Vec<T>, capacity: usize, order: O) -> Self {
        let slots = crate::common::heap::vec_slots_after_pushes(
            capacity.min(PREALLOCATED_SLOTS),
            data.len(),
            size_of::<T>(),
        );
        let mut data = data;
        if data.capacity() > slots {
            data.shrink_to(slots);
        } else {
            data.reserve_exact(slots - data.len());
        }
        Self {
            data,
            size: capacity,
            order,
        }
    }

    /// Replaces the contents with `other`'s, keeping this heap's allocation and
    /// growing it as pushes would.
    pub(crate) fn assign_from(&mut self, other: &Self)
    where
        T: Clone,
        O: Clone,
    {
        let slots = crate::common::heap::vec_slots_after_pushes(
            self.data.capacity(),
            other.data.len(),
            size_of::<T>(),
        );
        self.data.clear();
        self.data.reserve_exact(slots);
        self.data.extend_from_slice(&other.data);
        self.size = other.size;
        self.order = other.order.clone();
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

    /// Element slots the heap has allocated: at least [`Self::len`], growing
    /// on demand toward [`Self::capacity`].
    #[inline]
    pub(crate) fn allocated_slots(&self) -> usize {
        self.data.capacity()
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

    /// Returns a reference to the root element without removing it.
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
            self.push_back_with(value, &mut |_, _| {});
        } else if !self.data.is_empty() && self.order.should_replace_root(&self.data[0], &value) {
            self.replace_root_with(value, &mut |_, _| {});
        }
    }

    /// Appends `value` and sifts it up, reporting every index swap.
    ///
    /// `on_swap(i, j)` fires for each swap the sift performs, so a caller
    /// holding an index beside the heap can patch it instead of rebuilding.
    /// The value is appended at `len()` before any swap fires.
    ///
    /// The caller is responsible for the capacity check; this appends
    /// unconditionally.
    pub fn push_back_with(&mut self, value: T, on_swap: &mut impl FnMut(usize, usize)) {
        self.data.push(value);
        self.bubble_up_with(self.data.len() - 1, on_swap);
    }

    /// Replaces the root with `value`, sifts it down, and returns the
    /// displaced root. Swaps are reported as in [`Self::push_back_with`], and
    /// the write to index 0 happens before any swap fires.
    ///
    /// Panics if the heap is empty.
    pub fn replace_root_with(&mut self, value: T, on_swap: &mut impl FnMut(usize, usize)) -> T {
        let displaced = std::mem::replace(&mut self.data[0], value);
        self.bubble_down_with(0, on_swap);
        displaced
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

    /// Re-sifts the element at the given index to restore the heap property.
    /// Returns false if `index` is out of range, true otherwise.
    #[inline]
    pub fn update_at(&mut self, index: usize) -> bool {
        self.update_at_with(index, &mut |_, _| {})
    }

    /// Re-sifts the element at `index`, reporting every index swap as in
    /// [`Self::push_back_with`].
    ///
    /// Returns false if `index` is out of range, true otherwise.
    #[inline]
    pub fn update_at_with(&mut self, index: usize, on_swap: &mut impl FnMut(usize, usize)) -> bool {
        if index >= self.data.len() {
            return false;
        }
        if !self.bubble_down_with(index, on_swap) {
            self.bubble_up_with(index, on_swap);
        }
        true
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
    fn bubble_down(&mut self, idx: usize) -> bool {
        self.bubble_down_with(idx, &mut |_, _| {})
    }

    fn bubble_down_with(&mut self, mut idx: usize, on_swap: &mut impl FnMut(usize, usize)) -> bool {
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
            on_swap(idx, target);
            idx = target;
        }

        idx != start_idx
    }

    /// Bubbles an element up to maintain heap property.
    fn bubble_up_with(&mut self, mut idx: usize, on_swap: &mut impl FnMut(usize, usize)) {
        while idx > 0 {
            let parent_idx = Self::parent(idx);
            if self
                .order
                .should_swap(&self.data[parent_idx], &self.data[idx])
            {
                self.data.swap(parent_idx, idx);
                on_swap(parent_idx, idx);
                idx = parent_idx;
            } else {
                break;
            }
        }
    }
}

// Convenience constructors for common heap types
impl<T: Ord> CommonHeap<T, RetainLargest> {
    /// Creates a new min-heap with the specified capacity.
    #[inline]
    pub fn new_min(capacity: usize) -> Self {
        Self::with_capacity(capacity, RetainLargest)
    }
}

impl<T: Ord> CommonHeap<T, RetainSmallest> {
    /// Creates a new max-heap with the specified capacity.
    #[inline]
    pub fn new_max(capacity: usize) -> Self {
        Self::with_capacity(capacity, RetainSmallest)
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
