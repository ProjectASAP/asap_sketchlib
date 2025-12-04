//! Common data structure that is served as basic building block
//! Vector1D:
//! Vector2D:
//! Vector3D:
//! CommonHeap:
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng, rng};
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

/// DPDK member sketch implementation. Reference:
/// <https://github.com/DPDK/dpdk/blob/main/lib/member/rte_member_sketch.c>.
/// Structure to hold data for Nitro Mode
/// Default to be off (i.e., not Nitro Mode)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Nitro {
    pub is_nitro_mode: bool,
    sampling_rate: f64,
    pub to_skip: usize,
    /// Precomputed: 1.0 / ln(1 - sampling_rate) for geometric sampling
    inv_ln_one_minus_p: f64,
    #[serde(skip)]
    #[serde(default = "new_small_rng")]
    generator: SmallRng,
    pub delta: u64,
}

fn new_small_rng() -> SmallRng {
    let mut seed_rng = rng();
    SmallRng::from_rng(&mut seed_rng)
}

impl Default for Nitro {
    fn default() -> Self {
        Self {
            is_nitro_mode: false,
            sampling_rate: 0.0,
            to_skip: 0,
            inv_ln_one_minus_p: 0.0, // not used unless Nitro mode is enabled
            generator: new_small_rng(), // not used unless Nitro mode is enabled
            delta: 0,
        }
    }
}

impl Nitro {
    pub fn init_nitro(rate: f64) -> Self {
        assert!(
            !rate.is_nan() && rate > 0.0 && rate <= 1.0,
            "sample_rate must be within (0.0, 1.0]"
        );
        let inv_ln = if (rate - 1.0).abs() <= f64::EPSILON {
            0.0 // Not used for full sampling
        } else {
            1.0 / (1.0 - rate).ln()
        };
        let mut nitro = Self {
            is_nitro_mode: true,
            sampling_rate: rate,
            to_skip: 0,
            inv_ln_one_minus_p: inv_ln,
            generator: new_small_rng(),
            delta: 0,
        };
        nitro.delta = nitro.scaled_increment(1);
        nitro
    }

    // for profiling
    #[inline(never)]
    pub fn draw_geometric(&mut self) {
        if self.is_full_sampling() {
            self.to_skip = 0;
            return;
        }
        let k = loop {
            let r = self.generator.random::<f64>();
            if r != 0.0_f64 && r != 1.0_f64 {
                break r;
            }
        };
        self.to_skip = ((1.0 - k).ln() * self.inv_ln_one_minus_p).ceil() as usize;
    }

    #[inline(always)]
    pub fn reduce_to_skip(&mut self) {
        self.to_skip -= 1;
    }

    #[inline(always)]
    pub fn reduce_to_skip_by_count(&mut self, c: usize) {
        self.to_skip -= c;
    }

    #[inline(always)]
    pub fn get_sampling_rate(&self) -> f64 {
        self.sampling_rate
    }

    // #[inline]
    #[inline(always)]
    pub fn scaled_increment(&self, weight: u64) -> u64 {
        if self.is_full_sampling() {
            weight
        } else {
            ((weight as f64) / self.sampling_rate).ceil() as u64
        }
    }

    // #[inline]
    #[inline(always)]
    fn is_full_sampling(&self) -> bool {
        (self.sampling_rate - 1.0).abs() <= f64::EPSILON
    }
}

/// Trait defining heap ordering behavior.
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
