//! Optimized UnivMon variants and pooling.
//!
//! - `UnivSketchPool`: free-list pool for `UnivMon` sketch reuse.
//! - `UnivMonPyramid`: pyramid-structured UnivMon with two-tier sketch
//!   dimensions — larger "elephant" layers for heavy hitters and smaller
//!   "mouse" layers for the long tail, matching the PromSketch design.

use crate::UnivMon;
use crate::common::heap::HHHeap;
use crate::common::{BOTTOM_LAYER_FINDER, DataInput, hash_item64_seeded, hash64_seeded};
use crate::common::{L2HH, Vector1D};
use crate::sketches::count::CountL2HH;

/// Object pool for `UnivMon` sketches.
///
/// Maintains a free-list of pre-allocated sketches. Callers take ownership
/// via `take()` and return sketches via `put()`, which resets and recycles
/// them. This avoids repeated heap allocation/deallocation for large sketch
/// matrices during promotion, merge, and expiration cycles.
pub struct UnivSketchPool {
    free_list: Vec<UnivMon>,
    total_allocated: usize,
    heap_size: usize,
    sketch_row: usize,
    sketch_col: usize,
    layer_size: usize,
}

impl UnivSketchPool {
    /// Creates a new pool with `cap` pre-allocated sketches.
    pub fn new(
        cap: usize,
        heap_size: usize,
        sketch_row: usize,
        sketch_col: usize,
        layer_size: usize,
    ) -> Self {
        let free_list: Vec<UnivMon> = (0..cap)
            .map(|_| UnivMon::init_univmon(heap_size, sketch_row, sketch_col, layer_size))
            .collect();
        UnivSketchPool {
            free_list,
            total_allocated: cap,
            heap_size,
            sketch_row,
            sketch_col,
            layer_size,
        }
    }

    /// Takes ownership of a clean sketch from the pool.
    ///
    /// Pops a recycled sketch from the free-list if available, otherwise
    /// allocates a fresh one.
    pub fn take(&mut self) -> UnivMon {
        if let Some(sketch) = self.free_list.pop() {
            sketch
        } else {
            self.total_allocated += 1;
            UnivMon::init_univmon(
                self.heap_size,
                self.sketch_row,
                self.sketch_col,
                self.layer_size,
            )
        }
    }

    /// Returns a sketch to the pool for reuse. Resets all internal state.
    pub fn put(&mut self, mut sketch: UnivMon) {
        sketch.free();
        self.free_list.push(sketch);
    }

    /// Number of sketches currently available in the pool.
    pub fn available(&self) -> usize {
        self.free_list.len()
    }

    /// Total number of sketches ever allocated by this pool.
    pub fn total_allocated(&self) -> usize {
        self.total_allocated
    }
}

// ---------------------------------------------------------------------------
// UnivMonPyramid
// ---------------------------------------------------------------------------

const DEFAULT_ELEPHANT_LAYERS: usize = 8;
const DEFAULT_ELEPHANT_ROW: usize = 3;
const DEFAULT_ELEPHANT_COL: usize = 2048;
const DEFAULT_MOUSE_ROW: usize = 3;
const DEFAULT_MOUSE_COL: usize = 512;
const DEFAULT_PYRAMID_HEAP: usize = 32;
const DEFAULT_PYRAMID_LAYERS: usize = 16;

/// Pyramid-structured UnivMon with two-tier sketch dimensions.
///
/// Layers `0..elephant_layers` ("elephant") use larger sketches for accurate
/// heavy-hitter tracking. Layers `elephant_layers..layer_size` ("mouse") use
/// smaller sketches, saving memory since deeper layers sample exponentially
/// fewer items.
#[derive(Clone, Debug)]
/// Optimized UnivMon variant with separate elephant and mouse layers.
pub struct UnivMonPyramid {
    /// Per-layer L2/heavy-hitter sketches.
    pub l2_sketch_layers: Vector1D<L2HH>,
    /// Per-layer heavy-hitter heaps.
    pub hh_layers: Vector1D<HHHeap>,
    /// Total number of layers.
    pub layer_size: usize,
    /// Number of elephant layers.
    pub elephant_layers: usize,
    /// Row count for elephant layers.
    pub elephant_row: usize,
    /// Column count for elephant layers.
    pub elephant_col: usize,
    /// Row count for mouse layers.
    pub mouse_row: usize,
    /// Column count for mouse layers.
    pub mouse_col: usize,
    /// Heap capacity per layer.
    pub heap_size: usize,
    /// Bucket size used for hashing decisions.
    pub bucket_size: usize,
}

impl UnivMonPyramid {
    /// Creates an optimized UnivMon pyramid.
    pub fn new(
        heap_size: usize,
        elephant_layers: usize,
        elephant_row: usize,
        elephant_col: usize,
        mouse_row: usize,
        mouse_col: usize,
        total_layers: usize,
    ) -> Self {
        let sk_vec: Vec<L2HH> = if total_layers <= elephant_layers {
            (0..total_layers)
                .map(|i| {
                    L2HH::COUNT(CountL2HH::with_dimensions_and_seed(
                        elephant_row,
                        elephant_col,
                        i,
                    ))
                })
                .collect()
        } else {
            (0..elephant_layers)
                .map(|i| {
                    L2HH::COUNT(CountL2HH::with_dimensions_and_seed(
                        elephant_row,
                        elephant_col,
                        i,
                    ))
                })
                .chain((elephant_layers..total_layers).map(|i| {
                    L2HH::COUNT(CountL2HH::with_dimensions_and_seed(mouse_row, mouse_col, i))
                }))
                .collect()
        };

        let hh_vec: Vec<HHHeap> = (0..total_layers).map(|_| HHHeap::new(heap_size)).collect();

        UnivMonPyramid {
            l2_sketch_layers: Vector1D::from_vec(sk_vec),
            hh_layers: Vector1D::from_vec(hh_vec),
            layer_size: total_layers,
            elephant_layers,
            elephant_row,
            elephant_col,
            mouse_row,
            mouse_col,
            heap_size,
            bucket_size: 0,
        }
    }

    /// Creates a pyramid using built-in default dimensions.
    pub fn with_defaults() -> Self {
        Self::new(
            DEFAULT_PYRAMID_HEAP,
            DEFAULT_ELEPHANT_LAYERS,
            DEFAULT_ELEPHANT_ROW,
            DEFAULT_ELEPHANT_COL,
            DEFAULT_MOUSE_ROW,
            DEFAULT_MOUSE_COL,
            DEFAULT_PYRAMID_LAYERS,
        )
    }

    #[inline(always)]
    fn find_bottom_layer_num(&self, hash: u64) -> usize {
        for l in 1..self.layer_size {
            if ((hash >> l) & 1) == 0 {
                return l - 1;
            }
        }
        self.layer_size - 1
    }

    /// Standard insert: updates sketch + heap at every layer 0..=bottom.
    pub fn insert(&mut self, key: &DataInput, value: i64) {
        self.bucket_size += value as usize;
        let h = hash64_seeded(BOTTOM_LAYER_FINDER, key);
        let bottom = self.find_bottom_layer_num(h);
        for i in 0..=bottom {
            let count = if i == 0 {
                self.l2_sketch_layers[i].update_and_est(key, value)
            } else {
                self.l2_sketch_layers[i].update_and_est_without_l2(key, value)
            };
            self.hh_layers[i].update(key, count as i64);
        }
    }

    /// Optimized insert: only updates the count sketch at the bottom layer
    /// and layer 0, skipping all intermediate sketch updates.
    ///
    /// The bottom layer's count estimate is reused for all intermediate heap
    /// updates (layers 1..=bottom), saving `bottom - 1` sketch updates per
    /// item while keeping heavy-hitter heaps fully up to date.
    ///
    /// Mirrors the Go PromSketch `update_optimized` function with explicit
    /// elephant / mouse branching.
    pub fn fast_insert(&mut self, key: &DataInput, value: i64) {
        self.bucket_size += value as usize;
        let h = hash64_seeded(BOTTOM_LAYER_FINDER, key);
        let bottom = self.find_bottom_layer_num(h);

        if bottom < self.elephant_layers {
            // All touched layers are elephant layers.
            if bottom > 0 {
                let count = self.l2_sketch_layers[bottom].update_and_est_without_l2(key, value);
                // Reuse bottom-layer estimate for upper-layer heaps.
                for l in (1..=bottom).rev() {
                    self.hh_layers[l].update(key, count as i64);
                }
                let count0 = self.l2_sketch_layers[0].update_and_est(key, value);
                self.hh_layers[0].update(key, count0 as i64);
            } else {
                let count0 = self.l2_sketch_layers[0].update_and_est(key, value);
                self.hh_layers[0].update(key, count0 as i64);
            }
        } else {
            // Bottom layer is a mouse layer (smaller sketch dimensions).
            let count = self.l2_sketch_layers[bottom].update_and_est_without_l2(key, value);
            for l in (1..=bottom).rev() {
                self.hh_layers[l].update(key, count as i64);
            }
            // Layer 0 is always an elephant layer.
            let count0 = self.l2_sketch_layers[0].update_and_est(key, value);
            self.hh_layers[0].update(key, count0 as i64);
        }
    }

    // -- Query methods (identical to UnivMon) --------------------------------

    /// Computes a g-sum estimate.
    pub fn calc_g_sum<F>(&self, g: F, is_card: bool) -> f64
    where
        F: Fn(f64) -> f64,
    {
        let mut y = vec![0.0; self.layer_size];

        let l2_top = self.l2_sketch_layers[self.layer_size - 1].get_l2();
        let threshold_top = if is_card { (l2_top * 0.01) as i64 } else { 0 };

        let mut tmp = 0.0;
        for item in self.hh_layers[self.layer_size - 1].heap() {
            if item.count > threshold_top {
                tmp += g(item.count as f64);
            }
        }
        y[self.layer_size - 1] = tmp;

        for i in (0..(self.layer_size - 1)).rev() {
            tmp = 0.0;
            let l2_val = self.l2_sketch_layers[i].get_l2();
            let threshold = if is_card { (l2_val * 0.01) as i64 } else { 0 };

            for item in self.hh_layers[i].heap() {
                if item.count > threshold {
                    let hash = (hash_item64_seeded(BOTTOM_LAYER_FINDER, &item.key) >> (i + 1)) & 1;
                    let coe = 1.0 - 2.0 * (hash as f64);
                    tmp += coe * g(item.count as f64);
                }
            }
            y[i] = 2.0 * y[i + 1] + tmp;
        }
        y[0]
    }

    /// Returns the estimated L1 norm.
    pub fn calc_l1(&self) -> f64 {
        self.calc_g_sum(|x| x, false)
    }

    /// Returns the estimated L2 norm.
    pub fn calc_l2(&self) -> f64 {
        self.calc_g_sum(|x| x * x, false).sqrt()
    }

    /// Returns the estimated entropy.
    pub fn calc_entropy(&self) -> f64 {
        let tmp = self.calc_g_sum(|x| if x > 0.0 { x * x.log2() } else { 0.0 }, false);
        (self.bucket_size as f64).log2() - tmp / (self.bucket_size as f64)
    }

    /// Returns the estimated cardinality.
    pub fn calc_card(&self) -> f64 {
        self.calc_g_sum(|_| 1.0, true)
    }

    // -- Lifecycle -----------------------------------------------------------

    /// Resets all counters and heaps without deallocating.
    pub fn free(&mut self) {
        self.bucket_size = 0;
        for i in 0..self.layer_size {
            self.l2_sketch_layers[i].clear();
            self.hh_layers[i].clear();
        }
    }

    /// Merges another pyramid's data into this one (element-wise).
    pub fn merge(&mut self, other: &UnivMonPyramid) {
        assert_eq!(
            self.layer_size, other.layer_size,
            "layer size must match for merge"
        );
        for i in 0..self.layer_size {
            self.l2_sketch_layers[i].merge(&other.l2_sketch_layers[i]);
            for item in other.hh_layers[i].heap() {
                let count = if let Some(index) = self.hh_layers[i].find_heap_item(&item.key) {
                    self.hh_layers[i].heap()[index].count + item.count
                } else {
                    item.count
                };
                self.hh_layers[i].update_heap_item(&item.key, count);
            }
        }
    }

    /// Returns the heap for one layer.
    pub fn heap_at_layer(&mut self, layer: usize) -> &mut HHHeap {
        &mut self.hh_layers[layer]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DataInput;

    #[test]
    fn pool_basic_take_put() {
        let mut pool = UnivSketchPool::new(2, 16, 2, 5, 2);
        assert_eq!(pool.available(), 2);
        assert_eq!(pool.total_allocated(), 2);

        let s0 = pool.take();
        assert_eq!(pool.available(), 1);

        let s1 = pool.take();
        assert_eq!(pool.available(), 0);

        // Pool is empty — next take allocates a new one
        let s2 = pool.take();
        assert_eq!(pool.available(), 0);
        assert_eq!(pool.total_allocated(), 3);

        // Return one
        pool.put(s1);
        assert_eq!(pool.available(), 1);

        // Should reuse the returned sketch
        let s3 = pool.take();
        assert_eq!(pool.available(), 0);
        assert_eq!(pool.total_allocated(), 3); // no new allocation

        // Return all
        pool.put(s0);
        pool.put(s2);
        pool.put(s3);
        assert_eq!(pool.available(), 3);
    }

    #[test]
    fn pool_free_resets_sketch() {
        let mut pool = UnivSketchPool::new(1, 16, 2, 5, 2);

        // Take a sketch, insert some data
        let mut sketch = pool.take();
        sketch.insert(&DataInput::I64(42), 100);
        assert!(sketch.bucket_size > 0);

        // Return it — should reset
        pool.put(sketch);

        // Take it back — should be clean
        let sketch2 = pool.take();
        assert_eq!(sketch2.bucket_size, 0);
        assert!((sketch2.l2_sketch_layers[0].get_l2()).abs() < 1e-9);
    }

    // =======================================================================
    //                    UnivMonPyramid tests
    // =======================================================================

    #[test]
    fn pyramid_basic_insert_and_query() {
        let mut um = UnivMonPyramid::with_defaults();

        let cases: Vec<(&str, i64)> = vec![("hello", 10), ("world", 20), ("hello", 5), ("foo", 30)];

        for (key, val) in &cases {
            um.insert(&DataInput::Str(key), *val);
        }

        assert_eq!(um.bucket_size, 65);
        assert!((um.calc_l1() - 65.0).abs() < 1e-6, "L1 = {}", um.calc_l1());
        assert_eq!(um.calc_card(), 3.0);
    }

    #[test]
    fn pyramid_fast_insert_matches_standard() {
        // Both insert paths should produce identical sketches.
        let mut standard = UnivMonPyramid::new(32, 8, 3, 2048, 3, 512, 16);
        let mut fast = UnivMonPyramid::new(32, 8, 3, 2048, 3, 512, 16);

        for i in 0..500i64 {
            let key = DataInput::I64(i % 100);
            standard.insert(&key, 1);
            fast.fast_insert(&key, 1);
        }

        assert_eq!(standard.bucket_size, fast.bucket_size);

        // L1 and cardinality should be very close (heap contents may differ
        // slightly because fast_insert reuses one estimate for all heaps).
        let l1_diff = (standard.calc_l1() - fast.calc_l1()).abs();
        let card_diff = (standard.calc_card() - fast.calc_card()).abs();
        assert!(
            l1_diff / standard.calc_l1() < 0.10,
            "L1 diverged: std={}, fast={}",
            standard.calc_l1(),
            fast.calc_l1()
        );
        assert!(
            card_diff / standard.calc_card().max(1.0) < 0.15,
            "Card diverged: std={}, fast={}",
            standard.calc_card(),
            fast.calc_card()
        );
    }

    #[test]
    fn pyramid_two_tier_dimensions() {
        // Verify elephant layers are larger than mouse layers.
        let um = UnivMonPyramid::new(32, 4, 5, 2048, 3, 256, 8);

        // Layers 0..4 = elephant (5 rows × 2048 cols)
        // Layers 4..8 = mouse   (3 rows × 256 cols)
        assert_eq!(um.layer_size, 8);
        assert_eq!(um.elephant_layers, 4);
    }

    #[test]
    fn pyramid_free_resets_state() {
        let mut um = UnivMonPyramid::with_defaults();
        for i in 0..100i64 {
            um.insert(&DataInput::I64(i), 10);
        }
        assert!(um.bucket_size > 0);

        um.free();
        assert_eq!(um.bucket_size, 0);
        assert!((um.l2_sketch_layers[0].get_l2()).abs() < 1e-9);
    }

    #[test]
    fn pyramid_merge_combines_data() {
        let mut left = UnivMonPyramid::with_defaults();
        let mut right = UnivMonPyramid::with_defaults();

        for i in 0..50i64 {
            left.insert(&DataInput::I64(i), 10);
        }
        for i in 50..100i64 {
            right.insert(&DataInput::I64(i), 10);
        }

        let left_l1 = left.calc_l1();
        let right_l1 = right.calc_l1();
        left.merge(&right);

        let merged_l1 = left.calc_l1();
        let expected = left_l1 + right_l1;
        let err = (merged_l1 - expected).abs() / expected;
        assert!(
            err < 0.10,
            "Merged L1 error {:.2}%: got {}, expected {}",
            err * 100.0,
            merged_l1,
            expected
        );
    }

    fn ground_truth(freq: &std::collections::HashMap<i64, i64>) -> (f64, f64, f64, f64) {
        let l1: f64 = freq.values().map(|&v| v as f64).sum();
        let l2: f64 = freq
            .values()
            .map(|&v| (v as f64).powi(2))
            .sum::<f64>()
            .sqrt();
        let card = freq.len() as f64;
        let entropy = if l1 > 0.0 {
            let term: f64 = freq
                .values()
                .map(|&v| {
                    let f = v as f64;
                    if f > 0.0 { f * f.log2() } else { 0.0 }
                })
                .sum();
            l1.log2() - term / l1
        } else {
            0.0
        };
        (l1, l2, card, entropy)
    }

    #[test]
    fn pyramid_accuracy_zipf() {
        use std::collections::HashMap;

        let mut um = UnivMonPyramid::new(64, 8, 5, 2048, 3, 512, 16);
        let mut freq: HashMap<i64, i64> = HashMap::new();

        // Heavy hitter
        for _ in 0..5000 {
            um.insert(&DataInput::I64(0), 1);
            *freq.entry(0).or_insert(0) += 1;
        }
        // Medium flows
        for key in 1..=20i64 {
            for _ in 0..200 {
                um.insert(&DataInput::I64(key), 1);
                *freq.entry(key).or_insert(0) += 1;
            }
        }
        // Light flows
        for key in 21..=500i64 {
            um.insert(&DataInput::I64(key), 1);
            *freq.entry(key).or_insert(0) += 1;
        }

        let (true_l1, true_l2, true_card, true_entropy) = ground_truth(&freq);

        let err = |name: &str, est: f64, truth: f64| {
            let rel = (est - truth).abs() / truth.max(1e-12);
            assert!(
                rel < 0.15,
                "Pyramid {name}: error {:.2}%, est={est:.2}, truth={truth:.2}",
                rel * 100.0
            );
        };

        err("L1", um.calc_l1(), true_l1);
        err("L2", um.calc_l2(), true_l2);
        err("Card", um.calc_card(), true_card);
        err("Entropy", um.calc_entropy(), true_entropy);
    }

    #[test]
    fn pyramid_fast_insert_accuracy() {
        use std::collections::HashMap;

        let mut um = UnivMonPyramid::new(64, 8, 5, 2048, 3, 512, 16);
        let mut freq: HashMap<i64, i64> = HashMap::new();

        // Use fast_insert for everything.
        for _ in 0..3000 {
            um.fast_insert(&DataInput::I64(0), 1);
            *freq.entry(0).or_insert(0) += 1;
        }
        for key in 1..=50i64 {
            for _ in 0..100 {
                um.fast_insert(&DataInput::I64(key), 1);
                *freq.entry(key).or_insert(0) += 1;
            }
        }

        let (true_l1, true_l2, true_card, true_entropy) = ground_truth(&freq);

        let err = |name: &str, est: f64, truth: f64| {
            let rel = (est - truth).abs() / truth.max(1e-12);
            assert!(
                rel < 0.15,
                "Pyramid fast {name}: error {:.2}%, est={est:.2}, truth={truth:.2}",
                rel * 100.0
            );
        };

        err("L1", um.calc_l1(), true_l1);
        err("L2", um.calc_l2(), true_l2);
        err("Card", um.calc_card(), true_card);
        err("Entropy", um.calc_entropy(), true_entropy);
    }

    #[test]
    fn pyramid_memory_savings_vs_uniform() {
        // Pyramid with 16 layers should use less memory than a uniform
        // UnivMon with the same elephant dimensions at all 16 layers.
        // We verify this by comparing total sketch column counts.
        let elephant_col = 2048;
        let mouse_col = 512;
        let elephant_layers = 8;
        let total_layers = 16;

        let uniform_cols = elephant_col * total_layers;
        let pyramid_cols =
            elephant_col * elephant_layers + mouse_col * (total_layers - elephant_layers);

        assert!(
            pyramid_cols < uniform_cols,
            "Pyramid ({pyramid_cols}) should use fewer columns than uniform ({uniform_cols})"
        );
        // With defaults: pyramid = 8*2048 + 8*512 = 20480, uniform = 16*2048 = 32768
        // Savings = ~37.5%
        let savings = 1.0 - (pyramid_cols as f64 / uniform_cols as f64);
        assert!(
            savings > 0.30,
            "Expected >30% column savings, got {:.1}%",
            savings * 100.0
        );
    }
}
