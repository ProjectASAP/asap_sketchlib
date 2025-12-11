//! heavy hitter heap that can be used by various
//! sketches or sketch_framework
//! basically, the HHHeap is a wrapper around CommonHeap
//! where the HHHeap is a Min Heap that can take in
//! HHItem defined in crate::common::input::HHItem

use crate::common::input::HHItem;
use crate::common::{CommonHeap, CommonMinHeap};
use crate::{hash_it_to_64, hash_item_to_64, input_to_owned, HeapItem, SketchInput};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Wrapper around CommonHeap for HHItem with TopK heavy hitter tracking.
/// Modern replacement for TopKHeap using the generic CommonHeap structure.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HHHeap {
    heap: CommonHeap<HHItem, CommonMinHeap>,
    positions: HashMap<u64, Vec<(HeapItem, usize)>>,
    k: usize,
}

impl HHHeap {
    /// Creates a new HHHeap with capacity k.
    pub fn new(k: usize) -> Self {
        HHHeap {
            heap: CommonHeap::new_min(k),
            positions: HashMap::with_capacity(k),
            k,
        }
    }

    /// Finds an item by key, returns the index if found.
    pub fn find(&self, key: &SketchInput) -> Option<usize> {
        let slot = self.slot_for_input(key);
        self.positions.get(&slot).and_then(|bucket| {
            bucket
                .iter()
                .find_map(|(value, idx)| if value == key { Some(*idx) } else { None })
        })
    }

    pub fn find_heap_item(&self, key: &HeapItem) -> Option<usize> {
        let slot = self.slot_for_item(key);
        self.positions.get(&slot).and_then(|bucket| {
            bucket
                .iter()
                .find_map(|(value, idx)| if value == key { Some(*idx) } else { None })
        })
    }

    /// Updates an existing item's count or inserts a new item.
    pub fn update<'k>(&mut self, key: &SketchInput, count: i64) -> bool {
        if let Some(idx) = self.find(key) {
            self.heap[idx].count = count;
            self.heap.update_at(idx);
            self.refresh_positions();
            return true;
        }

        if !self.should_accept_new(count) {
            return true;
        }

        let owned = input_to_owned(key);
        self.heap.push(HHItem::create_item(owned, count));
        self.refresh_positions();
        true
    }

    pub fn update_heap_item(&mut self, key: &HeapItem, count: i64) -> bool {
        if let Some(idx) = self.find_heap_item(key) {
            self.heap[idx].count = count;
            self.heap.update_at(idx);
            self.refresh_positions();
            return true;
        }

        if !self.should_accept_new(count) {
            return true;
        }

        self.heap.push(HHItem::create_item(key.to_owned(), count));
        self.refresh_positions();
        true
    }

    /// Provides access to the underlying data as a slice.
    /// Named `heap` for API compatibility with TopKHeap.
    pub fn heap(&self) -> &[HHItem] {
        self.heap.as_slice()
    }

    /// Prints all items in the heap.
    pub fn print_heap(&self) {
        println!("======== Beginning of Heap ========");
        for item in self.heap.iter() {
            item.print_item();
        }
        println!("============ Heap Ends ============");
    }

    /// Returns the memory used by the heap in bytes.
    // pub fn get_memory_bytes(&self) -> f64 {
    //     let mut total = 0.0;
    //     for item in self.heap.iter() {
    //         total += item.key.len() as f64 + 8.0; // key length + i64 count
    //     }
    //     total
    // }

    /// Clears the heap.
    pub fn clear(&mut self) {
        self.heap.clear();
        self.positions.clear();
    }

    /// Returns the number of items in the heap.
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// Returns true if the heap is empty.
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Creates a copy of another HHHeap.
    pub fn from_heap(other: &HHHeap) -> Self {
        other.clone()
    }

    /// Returns the capacity of the heap.
    pub fn capacity(&self) -> usize {
        self.k
    }

    #[inline]
    fn should_accept_new(&self, count: i64) -> bool {
        if self.heap.len() < self.k {
            return true;
        }
        self.heap
            .peek()
            .map(|min_item| count > min_item.count)
            .unwrap_or(true)
    }

    fn refresh_positions(&mut self) {
        self.positions.clear();
        for (idx, item) in self.heap.iter().enumerate() {
            let slot = self.slot_for_item(&item.key);
            self.positions
                .entry(slot)
                .or_default()
                .push((item.key.clone(), idx));
        }
    }

    #[inline]
    fn slot_for_input(&self, key: &SketchInput) -> u64 {
        hash_it_to_64(0, key)
    }

    #[inline]
    fn slot_for_item(&self, key: &HeapItem) -> u64 {
        hash_item_to_64(0, key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CommonHeap, CommonHeapOrder, CommonMaxHeap, CommonMinHeap, HeapItem, SketchInput,
        common::input::HHItem,
    };

    fn heap_item_from_str(value: &str) -> HeapItem {
        HeapItem::String(value.to_string())
    }

    #[test]
    fn heap_retains_top_k_items_by_count() {
        // confirm inserting beyond capacity keeps only the k largest counts
        let mut heap = HHHeap::new(3);
        for i in 1..=5 {
            let key = format!("key-{i}");
            let key_item = heap_item_from_str(&key);
            heap.update_heap_item(&key_item, i as i64);
        }

        assert_eq!(heap.heap.len(), 3);
        let mut counts: Vec<i64> = heap.heap.iter().map(|item| item.count).collect();
        counts.sort_unstable();
        assert_eq!(counts, vec![3, 4, 5]);
    }

    #[test]
    fn update_count_increments_existing_entry() {
        // ensure update_count bumps stored counter instead of replacing the entry
        let mut heap = HHHeap::new(4);
        let key_item = heap_item_from_str("alpha");
        let mut count = 0;
        for _ in 0..3 {
            count += 1;
            heap.update_heap_item(&key_item, count);
        }

        let idx = heap.find_heap_item(&key_item).expect("alpha present");
        assert_eq!(heap.heap[idx].count, 3);
    }

    #[test]
    fn clean_resets_heap_state() {
        // cleaning should drop all entries and reclaim capacity
        let mut heap = HHHeap::new(2);
        let key_a = heap_item_from_str("a");
        let key_b = heap_item_from_str("b");
        heap.update_heap_item(&key_a, 5);
        heap.update_heap_item(&key_b, 6);
        assert_eq!(heap.heap.len(), 2);

        heap.clear();
        assert!(heap.heap.is_empty());
    }

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
        heap.push(HHItem::new(SketchInput::String("five".to_owned()), 5));
        heap.push(HHItem::new(SketchInput::String("three".to_owned()), 3));
        heap.push(HHItem::new(SketchInput::String("seven".to_owned()), 7));

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
            heap.push(HHItem::new(
                SketchInput::String(format!("key-{i}").to_owned()),
                i,
            ));
        }

        // Should keep top 3: counts 3, 4, 5
        assert_eq!(heap.len(), 3);
        let mut counts: Vec<i64> = heap.iter().map(|item| item.count).collect();
        counts.sort_unstable();
        assert_eq!(counts, vec![3, 4, 5]);

        // Test finding an item (linear search like TopKHeap::find)
        let found = heap
            .iter()
            .find(|item| item.key == HeapItem::String("key-4".to_owned()));
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

        heap.push(HHItem::new(SketchInput::String("a".to_owned()), 5));
        heap.push(HHItem::new(SketchInput::String("b".to_owned()), 3));
        heap.push(HHItem::new(SketchInput::String("c".to_owned()), 7));
        heap.push(HHItem::new(SketchInput::String("d".to_owned()), 1)); // Won't be added
        heap.push(HHItem::new(SketchInput::String("e".to_owned()), 10)); // Will replace min

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
                let idx_opt = heap
                    .iter()
                    .position(|item| item.key == HeapItem::String(key.to_owned()));

                if let Some(idx) = idx_opt {
                    // Found: update count
                    heap[idx].count = count;
                    heap.update_at(idx);
                } else {
                    // Not found: insert (TopKHeap::insert equivalent)
                    heap.push(HHItem::new(SketchInput::Str(key), count));
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
        let found = heap
            .iter()
            .find(|item| item.key == HeapItem::String("key-4".to_owned()));
        assert!(found.is_some());
        assert_eq!(found.unwrap().count, 4);

        // TopKHeap::clean() equivalent:
        heap.clear();
        assert!(heap.is_empty());
    }
}
