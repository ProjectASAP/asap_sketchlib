//! UnivMon (Universal Monitoring) Implementation
//!
//! This module provides an implementation of the UnivMon algorithm as described in:
//! "Universal Sketches for the Next Generation of Real-time Network Data Analytics"
//! by Liu et al. (ACM SIGCOMM 2016).
//! <https://dl.acm.org/doi/10.1145/2934872.2934906>
//!
//! UnivMon is a universal sketch framework that enables the estimation of multiple
//! network flow metrics—such as L1/L2 norms, Shannon entropy, and cardinality—using
//! a single, hierarchical sampling structure.
//!
//! # Architecture
//! The implementation consists of a "Sketch Pyramid" where:
//! * Each layer $i$ samples a subset of the stream from layer $i-1$ with probability 1/2.
//! * Each layer maintains a functional sketch (e.g., Count-Min Sketch) for frequency estimation.
//! * Each layer maintains a Heavy Hitter heap to track the most frequent elements at that sampling level.
//!
//! # Capabilities
//! * **L1/L2 Norm**: Estimation of total flow volume and second moments.
//! * **Entropy**: Calculation of flow distribution complexity.
//! * **Cardinality**: Estimation of the number of distinct elements.
//! * **Heavy Hitters**: Tracking top flows across different sampling granularities.
//!
//! This implementation is part of the `asap_sketchlib` library.

use crate::common::heap::HHHeap;
use crate::common::{BOTTOM_LAYER_FINDER, DataInput, hash_item64_seeded, hash64_seeded};
use crate::common::{L2HH, Vector1D};
use crate::sketches::count::CountL2HH;
use rmp_serde::{
    decode::Error as RmpDecodeError, encode::Error as RmpEncodeError, from_slice, to_vec_named,
};
use serde::{Deserialize, Serialize};

const DEFAULT_SKETCH_ROW: usize = 5;
const DEFAULT_SKETCH_COL: usize = 2048;
const DEFAULT_HEAP_SIZE: usize = 32;
const DEFAULT_LAYER_SIZE: usize = 8;

#[derive(Serialize, Deserialize, Clone, Debug)]
/// UnivMon sketch pyramid for multi-metric stream estimation.
pub struct UnivMon {
    /// Per-layer L2/heavy-hitter sketches.
    pub l2_sketch_layers: Vector1D<L2HH>,
    /// Per-layer heavy-hitter heaps.
    pub hh_layers: Vector1D<HHHeap>,
    /// Number of pyramid layers.
    pub layer_size: usize,
    /// Row count of each underlying sketch.
    pub sketch_row: usize,
    /// Column count of each underlying sketch.
    pub sketch_col: usize,
    /// Heap capacity per layer.
    pub heap_size: usize,
    /// Bucket size used for hashing decisions.
    pub bucket_size: usize,
}

impl Default for UnivMon {
    fn default() -> Self {
        UnivMon::init_univmon(
            DEFAULT_HEAP_SIZE,
            DEFAULT_SKETCH_ROW,
            DEFAULT_SKETCH_COL,
            DEFAULT_LAYER_SIZE,
        )
    }
}

impl UnivMon {
    /// Creates a UnivMon instance with explicit dimensions.
    pub fn init_univmon(
        heap_size: usize,
        sketch_row: usize,
        sketch_col: usize,
        layer_size: usize,
    ) -> Self {
        let sk_vec: Vec<L2HH> = (0..layer_size)
            .map(|i| {
                L2HH::COUNT(CountL2HH::with_dimensions_and_seed(
                    sketch_row, sketch_col, i,
                ))
            })
            .collect();

        let hh_vec: Vec<HHHeap> = (0..layer_size).map(|_| HHHeap::new(heap_size)).collect();

        UnivMon {
            l2_sketch_layers: Vector1D::from_vec(sk_vec),
            hh_layers: Vector1D::from_vec(hh_vec),
            layer_size,
            sketch_row,
            sketch_col,
            heap_size,
            bucket_size: 0,
        }
    }

    #[inline(always)]
    fn find_bottom_layer_num(&self, hash: u64, layer: usize) -> usize {
        for l in 1..layer {
            if ((hash >> l) & 1) == 0 {
                return l - 1;
            }
        }
        layer - 1
    }

    #[inline(always)]
    fn update(&mut self, key: &DataInput, value: i64, bottom_layer_num: usize) {
        for i in 0..=bottom_layer_num {
            let count = if i == 0 {
                self.l2_sketch_layers[i].update_and_est(key, value)
            } else {
                self.l2_sketch_layers[i].update_and_est_without_l2(key, value)
            };
            self.hh_layers[i].update(key, count as i64);
        }
    }

    #[inline(always)]
    fn process_univmon(&mut self, key: &DataInput, value: i64, bottom_layer_num: usize) {
        self.bucket_size += value as usize;
        self.update(key, value, bottom_layer_num);
    }

    /// Inserts one weighted update.
    pub fn insert(&mut self, key: &DataInput, value: i64) {
        let h = hash64_seeded(BOTTOM_LAYER_FINDER, key);
        let bottom_layer_num = self.find_bottom_layer_num(h, self.layer_size);
        self.process_univmon(key, value, bottom_layer_num)
    }

    /// Inserts one weighted update using fast-path hashing.
    pub fn fast_insert(&mut self, key: &DataInput, value: i64) {
        self.bucket_size += value as usize;
        let h = hash64_seeded(BOTTOM_LAYER_FINDER, key);
        let bottom_layer_num = self.find_bottom_layer_num(h, self.layer_size);
        let count = self.l2_sketch_layers[bottom_layer_num].update_and_est(key, value);
        for i in 0..=bottom_layer_num {
            self.hh_layers[i].update(key, count as i64);
        }
    }

    /// Prints all heavy-hitter layers for debugging.
    pub fn print_hh_layer(&self) {
        print!("Print HH_Layer: ");
        for i in 0..self.layer_size {
            println!("layer {}: ", i);
            self.hh_layers[i].print_heap();
        }
    }

    /// Computes a g-sum estimate using the heuristic recurrence.
    pub fn calc_g_sum_heuristic<F>(&self, g: F, is_card: bool) -> f64
    where
        F: Fn(f64) -> f64,
    {
        let mut y = vec![0.0; self.layer_size];
        let mut tmp: f64;

        let l2_value = self.l2_sketch_layers[self.layer_size - 1].get_l2();
        let mut threshold = (l2_value * 0.01) as i64;
        if !is_card {
            threshold = 0;
        }

        tmp = 0.0;
        for item in self.hh_layers[self.layer_size - 1].heap() {
            if item.count > threshold {
                tmp += g(item.count as f64);
            }
        }
        y[self.layer_size - 1] = tmp;

        for i in (0..(self.layer_size - 1)).rev() {
            tmp = 0.0;
            let l2_value = self.l2_sketch_layers[i].get_l2();
            let mut threshold = (l2_value * 0.01) as i64;
            if !is_card {
                threshold = 0;
            }

            for item in self.hh_layers[i].heap() {
                if item.count > threshold {
                    // let hash = (hash64_seeded(CANONICAL_HASH_SEED, &item.key) >> (i+1)) & 1;
                    // let hash = (hash64_seeded(CANONICAL_HASH_SEED, &DataInput::Str(&item.key)) >> (i + 1)) & 1;
                    let hash = (hash_item64_seeded(BOTTOM_LAYER_FINDER, &item.key) >> (i + 1)) & 1;
                    let coe = 1.0 - 2.0 * (hash as f64);
                    tmp += coe * g(item.count as f64);
                }
            }
            y[i] = 2.0 * y[i + 1] + tmp;
        }

        y[0]
    }

    /// Computes a g-sum estimate.
    pub fn calc_g_sum<F>(&self, g: F, is_card: bool) -> f64
    where
        F: Fn(f64) -> f64,
    {
        self.calc_g_sum_heuristic(g, is_card)
    }

    /// Returns the estimated L1 norm.
    pub fn calc_l1(&self) -> f64 {
        self.calc_g_sum(|x| x, false)
    }

    /// Returns the estimated L2 norm.
    pub fn calc_l2(&self) -> f64 {
        let tmp = self.calc_g_sum(|x| x * x, false);
        tmp.sqrt()
    }

    /// Returns the estimated entropy.
    pub fn calc_entropy(&self) -> f64 {
        let tmp = self.calc_g_sum(
            |x| {
                if x > 0.0 { x * x.log2() } else { 0.0 }
            },
            false,
        );
        (self.bucket_size as f64).log2() - tmp / (self.bucket_size as f64)
    }

    /// Returns the estimated cardinality.
    pub fn calc_card(&self) -> f64 {
        self.calc_g_sum(|_| 1.0, true)
    }

    /// Resets the sketch to its initial state without reallocating.
    /// Zeroes all counters and clears all heaps, matching the Go `Free()` method.
    pub fn free(&mut self) {
        self.bucket_size = 0;
        for i in 0..self.layer_size {
            self.l2_sketch_layers[i].clear();
            self.hh_layers[i].clear();
        }
    }

    /// Merges another UnivMon into this one.
    pub fn merge(&mut self, other: &UnivMon) {
        assert_eq!(
            self.layer_size, other.layer_size,
            "layer size should be equal to merge"
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

    /// Serializes the UnivMon sketch into MessagePack bytes.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        to_vec_named(self)
    }

    /// Deserializes a UnivMon sketch from MessagePack bytes.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataInput, HeapItem};
    use core::f64;
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use std::collections::HashMap;

    #[test]
    fn univmon_round_trip_serialization() {
        let mut um = UnivMon::init_univmon(12, 3, 64, 4);
        let flows = [
            ("alpha", 5),
            ("beta", 7),
            ("gamma", 9),
            ("alpha", 3),
            ("delta", 11),
        ];

        for (key, count) in flows {
            um.insert(&DataInput::String(key.to_string()), count);
        }

        let bucket_size_before = um.bucket_size;
        let l1_before = um.calc_l1();
        let l2_before = um.calc_l2();
        let entropy_before = um.calc_entropy();
        let card_before = um.calc_card();

        let encoded = um
            .serialize_to_bytes()
            .expect("serialize UnivMon into MessagePack");
        assert!(!encoded.is_empty(), "serialized bytes should not be empty");
        let data = encoded.clone();

        let decoded =
            UnivMon::deserialize_from_bytes(&data).expect("deserialize UnivMon from MessagePack");

        assert_eq!(um.layer_size, decoded.layer_size);
        assert_eq!(um.sketch_row, decoded.sketch_row);
        assert_eq!(um.sketch_col, decoded.sketch_col);
        assert_eq!(um.heap_size, decoded.heap_size);
        assert_eq!(bucket_size_before, decoded.bucket_size);
        assert!(
            (decoded.calc_l1() - l1_before).abs() < 1e-6,
            "L1 changed after round trip"
        );
        assert!(
            (decoded.calc_l2() - l2_before).abs() < 1e-6,
            "L2 changed after round trip"
        );
        assert!(
            (decoded.calc_entropy() - entropy_before).abs() < 1e-6,
            "entropy changed after round trip"
        );
        assert!(
            (decoded.calc_card() - card_before).abs() < f64::EPSILON,
            "cardinality changed after round trip"
        );
    }

    // fn bottom_layer_for(um: &UnivMon, key: &str) -> usize {
    //     let hash = hash64_seeded(BOTTOM_LAYER_FINDER, &DataInput::Str(key));
    //     um.find_bottom_layer_num(hash, um.layer)
    // }

    #[test]
    fn update_populates_bucket_size_and_heavy_hitters() {
        // processing a single hot key should record its weight in the heavy hitter layers
        let mut um = UnivMon::init_univmon(16, 3, 32, 4);
        let key = "alpha";

        // let bottom = bottom_layer_for(&um, key);

        for _ in 0..40 {
            // um.univmon_processing(key, 1, bottom);
            um.insert(&DataInput::Str(key), 1);
        }

        assert_eq!(um.bucket_size, 40);

        let idx = um.hh_layers[0]
            .find_heap_item(&HeapItem::String(key.to_owned()))
            .expect("heavy hitter should track key");
        assert!(
            um.hh_layers[0].heap()[idx].count >= 20,
            "expected significant count for heavy hitter, got {}",
            um.hh_layers[0].heap()[idx].count
        );
        assert!(
            um.calc_l1() == 40.0,
            "L1 Norm: get {}, expecting 1",
            um.calc_l1()
        );
        assert!(
            um.calc_card() == 1.0,
            "Cardinality: get {}, expecting 1",
            um.calc_card()
        );
    }

    #[test]
    fn merge_with_combines_heavy_hitters() {
        // merging two sketches should keep contributions from both sides
        let mut left = UnivMon::init_univmon(16, 3, 32, 4);
        let mut right = UnivMon::init_univmon(16, 3, 32, 4);

        let key_left = "left";
        let key_right = "right";

        // let bottom_left = bottom_layer_for(&left, key_left);
        // let bottom_right = bottom_layer_for(&right, key_right);

        for _ in 0..25 {
            // left.univmon_processing(key_left, 1, bottom_left);
            left.insert(&DataInput::Str(key_left), 1);
        }
        for _ in 0..30 {
            // right.univmon_processing(key_right, 1, bottom_right);
            right.insert(&DataInput::Str(key_right), 1);
        }

        left.merge(&right);

        let left_heap = left.heap_at_layer(00);
        let right_heap = right.heap_at_layer(0);
        // let right_heap = right.heap_at_layer(00);
        let idx_left = left_heap
            .find_heap_item(&HeapItem::String(key_left.to_owned()))
            .expect("left key present");
        let idx_right_in_left = left_heap
            .find_heap_item(&HeapItem::String(key_right.to_owned()))
            .expect("left key present");
        let idx_right = right_heap
            .find_heap_item(&HeapItem::String(key_right.to_owned()))
            .expect("right key present");
        assert!(
            left_heap.heap()[idx_left].count == 25,
            "left in left is: {}",
            left_heap.heap()[idx_left].count
        );
        assert!(
            right_heap.heap()[idx_right].count == 30,
            "right in right is: {}",
            right_heap.heap()[idx_right].count
        );
        assert!(
            left_heap.heap()[idx_right_in_left].count == 30,
            "right in left is: {}",
            left_heap.heap()[idx_right_in_left].count
        );
        // assert!(left.hh_layers[0].heap()[idx_right].count > 0);
    }

    #[test]
    fn univmon_layers_use_different_seeds() {
        // Verify that different layers in UnivMon use different seeds
        // by checking they produce different hash values
        use crate::common::hash128_seeded;

        let _um = UnivMon::init_univmon(20, 3, 1024, 4);

        // Hash the same key with different seed indices (as used by different layers)
        let test_key = DataInput::Str("test_flow");

        // Hash the same key with different seed indices (as used by different layers)
        let hash_0 = hash128_seeded(0, &test_key);
        let hash_1 = hash128_seeded(1, &test_key);
        let hash_2 = hash128_seeded(2, &test_key);
        let hash_3 = hash128_seeded(3, &test_key);

        // All should be different
        assert_ne!(hash_0, hash_1, "Layers 0 and 1 should use different seeds");
        assert_ne!(hash_0, hash_2, "Layers 0 and 2 should use different seeds");
        assert_ne!(hash_0, hash_3, "Layers 0 and 3 should use different seeds");
        assert_ne!(hash_1, hash_2, "Layers 1 and 2 should use different seeds");
        assert_ne!(hash_1, hash_3, "Layers 1 and 3 should use different seeds");
        assert_ne!(hash_2, hash_3, "Layers 2 and 3 should use different seeds");
    }

    #[test]
    fn univmon_cardinality_is_positive() {
        // Basic sanity test: cardinality should be positive after insertions
        let mut um = UnivMon::init_univmon(20, 3, 2048, 8);

        for i in 0..20 {
            let key = format!("flow_{i}");
            // let bottom = bottom_layer_for(&um, &key);
            // um.univmon_processing(&key, 10, bottom);
            um.insert(&DataInput::String(key), 1);
        }

        let card = um.calc_card();
        assert!(
            card == 20.0,
            "Cardinality should be positive after insertions, got {card}"
        );
    }

    #[test]
    fn univmon_bucket_size_tracked_correctly() {
        // Verify that bucket_size is correctly tracked with seed configuration
        let mut um = UnivMon::init_univmon(20, 3, 1024, 6);

        let flows = [("flow_a", 100), ("flow_b", 200), ("flow_c", 150)];
        let expected_total = 450;

        for (key, count) in &flows {
            // let bottom = bottom_layer_for(&um, key);
            // um.univmon_processing(key, *count, bottom);
            um.insert(&DataInput::Str(key), *count);
        }

        assert_eq!(
            um.bucket_size, expected_total,
            "Bucket size should equal sum of all counts"
        );
    }

    #[test]
    fn univmon_basic_operation() {
        let cases: Vec<(String, i64)> = vec![
            ("notfound", 1),
            ("hello", 1),
            ("count", 3),
            ("min", 4),
            ("world", 10),
            ("cheatcheat", 3),
            ("cheatcheat", 7),
            ("min", 2),
            ("hello", 2),
            ("tigger", 34),
            ("flow", 9),
            ("miss", 4),
            ("hello", 30),
            ("world", 10),
            ("hello", 10),
            ("mom", 1),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        let mut um = UnivMon::init_univmon(100, 3, 2048, 16);
        for case in cases {
            // let h = hash64_seeded(BOTTOM_LAYER_FINDER, &DataInput::Str(&case.0));
            // let bln = um.find_bottom_layer_num(h, 16);
            // um.univmon_processing(&case.0, case.1, bln);
            um.insert(&DataInput::String(case.0), case.1);
        }

        assert_eq!(um.calc_card(), 10.0, "Cardinality estimation incorrect");
        assert_eq!(um.calc_l1(), 131.0, "L1 estimation incorrect");
    }

    // #[test]
    // fn univmon_different_seeds_maintain_accuracy() {
    //     // Verify that using different seed indices doesn't break basic accuracy
    //     // Create two UnivMons with same config but verify both maintain accuracy

    //     let mut um1 = UnivMon::new_univmon_pyramid(20, 3, 2048, 10, 0);
    //     let mut um2 = UnivMon::new_univmon_pyramid(20, 3, 2048, 10, 1); // Different pool_idx

    //     // Insert same data into both with more flows for better stability
    //     let flows = [
    //         ("flow_a", 150),
    //         ("flow_b", 200),
    //         ("flow_c", 100),
    //         ("flow_d", 180),
    //         ("flow_e", 120),
    //     ];

    //     let true_l1 = 750f64;

    //     for (key, count) in &flows {
    //         let bottom1 = bottom_layer_for(&um1, key);
    //         let bottom2 = bottom_layer_for(&um2, key);
    //         um1.univmon_processing(key, *count, bottom1);
    //         um2.univmon_processing(key, *count, bottom2);
    //     }

    //     // Both should estimate L1 with reasonable accuracy
    //     let est_l1_1 = um1.calc_l1();
    //     let est_l1_2 = um2.calc_l1();

    //     let error_1 = ((est_l1_1 - true_l1).abs()) / true_l1;
    //     let error_2 = ((est_l1_2 - true_l1).abs()) / true_l1;

    //     assert!(
    //         est_l1_1 == true_l1,
    //         "UnivMon 1 L1 estimate {} should be reasonably accurate (error: {:.2}%)",
    //         est_l1_1,
    //         error_1 * 100.0
    //     );
    //     assert!(
    //         est_l1_1 == true_l1,
    //         "UnivMon 2 L1 estimate {} should be reasonably accurate (error: {:.2}%)",
    //         est_l1_2,
    //         error_2 * 100.0
    //     );
    // }

    // #[test]
    // fn test_layer_update_correctness() {
    //     // 1. Initialize UnivMon with enough layers
    //     let layers = 8;
    //     // Small dimensions to make debugging easier, but enough to avoid collisions in this simple test
    //     let mut um = UnivMon::init_univmon(10, 5, 128, layers, 0);

    //     let key = "test_key_layer_logic";
    //     let value = 10;

    //     // 2. Pre-calculate the expected bottom layer for this key
    //     // We use the same hasher the struct uses internally
    //     let hash = hash64_seeded(BOTTOM_LAYER_FINDER, &DataInput::Str(key));
    //     let expected_bottom = um.find_bottom_layer_num(hash, layers);

    //     // 3. Perform Update
    //     um.univmon_processing(key, value, expected_bottom);

    //     // 4. Verification Loop
    //     for i in 0..layers {
    //         // Check Heap Presence
    //         let in_heap = um.hh_layers[i].find(key).is_some();

    //         // Check Sketch Estimate
    //         // We use estimate() to see if the counter was incremented
    //         let count_est = um.cs_layers[i].get_estimate(&DataInput::Str(key));

    //         if i <= expected_bottom {
    //             // Case A: Layers the item SHOULD exist in
    //             assert!(in_heap, "Key should be in heap for layer {}", i);
    //             assert_eq!(count_est, value, "Sketch at layer {} should track count", i);
    //         } else {
    //             // Case B: Layers the item should NOT exist in (it was sampled out)
    //             assert!(!in_heap, "Key should NOT be in heap for layer {}", i);
    //             // Ideally 0, but technically collisions could occur.
    //             // With 'value=10' and empty sketch, it should be 0.
    //             assert_eq!(count_est, 0, "Sketch at layer {} should be empty", i);
    //         }
    //     }
    // }

    #[test]
    fn test_statistical_accuracy() {
        // 1. Setup: Larger sketch for statistical significance
        // k=50 (top-k size), r=5 (rows), c=1024 (cols), l=10 (layers)
        let mut um = UnivMon::init_univmon(50, 5, 1024, 10);

        // 2. Generate Data: A simple skewed distribution
        // 1 heavy hitter (count 1000), 10 medium (count 100), 100 noise (count 1)
        let mut true_l2_sq = 0.0;
        let mut true_entropy_term = 0.0;
        let mut total_count = 0.0;

        let scenarios = vec![("heavy", 1000, 1), ("medium", 100, 10), ("noise", 1, 100)];

        for (prefix, count, repeat) in scenarios {
            for i in 0..repeat {
                let key = format!("{}_{}", prefix, i);
                let val = count as i64;
                let val_f = val as f64;

                // Ground Truth Calculation
                true_l2_sq += val_f * val_f;
                true_entropy_term += val_f * val_f.log2();
                total_count += val_f;

                // Update Sketch
                // let hash = hash64_seeded(BOTTOM_LAYER_FINDER, &DataInput::Str(&key));
                // let bln = um.find_bottom_layer_num(hash, 10);
                // um.univmon_processing(&key, val, bln);
                um.insert(&DataInput::String(key), val);
            }
        }

        // 3. Calculate True Metrics
        let true_l2 = true_l2_sq.sqrt();
        let true_entropy = total_count.log2() - (true_entropy_term / total_count);

        // 4. Get Estimates
        let est_l2 = um.calc_l2();
        let est_entropy = um.calc_entropy();

        // 5. Assertions (Allowing ~10% error for test-sized sketches)
        let l2_err = (est_l2 - true_l2).abs() / true_l2;
        let ent_err = (est_entropy - true_entropy).abs() / true_entropy;

        println!(
            "True L2: {:.2}, Est L2: {:.2}, Error: {:.2}%",
            true_l2,
            est_l2,
            l2_err * 100.0
        );
        println!(
            "True Ent: {:.2}, Est Ent: {:.2}, Error: {:.2}%",
            true_entropy,
            est_entropy,
            ent_err * 100.0
        );

        // UnivMon is generally very accurate for L2
        assert!(l2_err < 0.15, "L2 Error too high: {:.2}%", l2_err * 100.0);

        // Entropy is harder, usually requires higher k, allowing slightly looser bound
        assert!(
            ent_err < 0.15,
            "Entropy Error too high: {:.2}%",
            ent_err * 100.0
        );
    }

    #[test]
    fn univmon_random_data_matches_ground_truth_within_five_percent() {
        let mut rng = StdRng::seed_from_u64(0xDEADBEEF);
        let mut um = UnivMon::init_univmon(256, 6, 8192, 16);
        let mut truth: HashMap<String, i64> = HashMap::new();

        for _ in 0..10_000 {
            let key_id = rng.random::<u32>() % 5000;
            let key = format!("key_{key_id}");
            let value = (rng.random::<u32>() % 100 + 1) as i64;
            *truth.entry(key.clone()).or_insert(0) += value;
            um.insert(&DataInput::String(key), value);
        }

        let total_mass: f64 = truth.values().map(|&v| v as f64).sum();
        let true_l1 = total_mass;
        let true_l2 = truth
            .values()
            .map(|&v| {
                let val = v as f64;
                val * val
            })
            .sum::<f64>()
            .sqrt();
        let true_card = truth.len() as f64;
        let entropy_term = truth
            .values()
            .map(|&v| {
                let val = v as f64;
                if val > 0.0 { val * val.log2() } else { 0.0 }
            })
            .sum::<f64>();
        let true_entropy = total_mass.log2() - entropy_term / total_mass;

        let to_check = [
            ("cardinality", um.calc_card(), true_card),
            ("l1", um.calc_l1(), true_l1),
            ("l2", um.calc_l2(), true_l2),
            ("entropy", um.calc_entropy(), true_entropy),
        ];

        for (name, estimate, expected) in to_check {
            let rel_err = (estimate - expected).abs() / expected;
            assert!(
                rel_err <= 0.05,
                "{name} relative error {:.2}% exceeds 5%: est={estimate}, expected={expected}",
                rel_err * 100.0
            );
        }
    }
}

// following out-dated code contains pyramid optimization that is potentially useful
// #[derive(Serialize, Deserialize, Clone, Debug)]
// pub struct UnivMon {
//     pub k: usize,
//     pub row: usize,
//     pub col: usize,
//     pub layer: usize,
//     pub cs_layers: Vector1D<L2HH>,
//     pub hh_layers: Vector1D<HHHeap>,
//     pub pool_idx: i64,
//     pub heap_update: i32,
//     pub bucket_size: usize,
// }
//
// impl UnivMon {
//     pub fn init_univmon(k: usize, r: usize, c: usize, l: usize, p_idx: i64) -> Self {
//         // Create cs_layers - each layer needs different seeds
//         // Layer i uses SEEDLIST[i] for hashing
//         let cs_vec: Vec<L2HH> = (0..l)
//             .map(|i| L2HH::COUNT(CountL2HH::with_dimensions_and_seed(r, c, i)))
//             .collect();
//         // Create hh_layers
//         let hh_vec: Vec<HHHeap> = (0..l).map(|_| HHHeap::new(k)).collect();
//
//         UnivMon {
//             k,
//             row: r,
//             col: c,
//             layer: l,
//             cs_layers: Vector1D::from_vec(cs_vec),
//             hh_layers: Vector1D::from_vec(hh_vec),
//             pool_idx: p_idx,
//             heap_update: 0,
//             bucket_size: 0,
//         }
//     }
//
//     pub fn get_bucket_size(&self) -> usize {
//         self.bucket_size
//     }
//
//     pub fn new_univmon_pyramid(k: usize, r: usize, c: usize, l: usize, p_idx: i64) -> Self {
//         // 8 is ELEPHANT_LAYER in PromSketch
//         // Each layer i uses SEEDLIST[i] for hashing
//         let cs_vec: Vec<L2HH> = if l <= 8 {
//             (0..l)
//                 .map(|i| L2HH::COUNT(CountL2HH::with_dimensions_and_seed(3, 2048, i)))
//                 .collect()
//         } else {
//             (0..8)
//                 .map(|i| L2HH::COUNT(CountL2HH::with_dimensions_and_seed(3, 2048, i)))
//                 .chain((8..l).map(|i| L2HH::COUNT(CountL2HH::with_dimensions_and_seed(3, 512, i))))
//                 .collect()
//         };
//
//         let hh_vec: Vec<HHHeap> = if l <= 8 {
//             (0..l).map(|_| HHHeap::new(k)).collect()
//         } else {
//             (0..l).map(|_| HHHeap::new(100)).collect()
//         };
//
//         UnivMon {
//             k,
//             row: r,
//             col: c,
//             layer: l,
//             cs_layers: Vector1D::from_vec(cs_vec),
//             hh_layers: Vector1D::from_vec(hh_vec),
//             pool_idx: p_idx,
//             heap_update: 0,
//             bucket_size: 0,
//         }
//     }
//
//     // pub fn free(&mut self) {
//     //     self.bucket_size = 0;
//
//     //     self.cs_layers.clear();
//     //     self.hh_layers.clear();
//     // }
//
//     // well... I'm not confident about this function
//     // pub fn get_memory_kb(&self) -> f64 {
//     //     let mut total = 0.0;
//     //     for i in 0..self.layer {
//     //         total += self.hh_layers[i].get_memory_bytes();
//     //     }
//     //     return (2048.0 * 3.0 * (self.layer as f64) * 8.0 + total) / 1024.0;
//     // }
//
//     // pub fn get_memory_kb_pyramid(&self) -> f64 {
//     //     let mut total = 0.0;
//     //     for i in 0..self.layer {
//     //         total += self.hh_layers[i].get_memory_bytes();
//     //     }
//     //     // again, hard code the ELEPHANT_LAYER for now
//     //     if self.layer <= 8 {
//     //         return (2048.0 * 3.0 * (self.layer as f64) * 8.0 + total) / 1024.0;
//     //     } else {
//     //         return ((2048.0 * 3.0 * 8.0 + 512.0 * 3.0 * (self.layer as f64 - 8.0)) * 8.0 + total)
//     //             / 1024.0;
//     //     }
//     // }
//
//     // update univmon
//     pub fn find_bottom_layer_num(&self, hash: u64, layer: usize) -> usize {
//         for l in 1..layer {
//             if ((hash >> l) & 1) == 0 {
//                 return l - 1;
//             }
//         }
//         layer - 1
//     }
//
//     pub fn update(&mut self, key: &str, value: i64, bottom_layer_num: usize) {
//         for i in 0..=bottom_layer_num {
//             let count = if i == 0 {
//                 self.cs_layers[i].update_and_est(&DataInput::Str(key), value)
//             } else {
//                 self.cs_layers[i].update_and_est_without_l2(&DataInput::Str(key), value)
//             };
//             self.hh_layers[i].update(key, count as i64);
//         }
//     }
//
//     pub fn update_optimized(&mut self, key: &str, value: i64, bottom_layer_num: usize) {
//         // hardcode again
//         if bottom_layer_num < 8 {
//             if bottom_layer_num > 0 {
//                 // let mut median = self.cs_layers[bottom_layer_num].update_and_est_without_l2(key, value);
//                 let mut median = self.cs_layers[bottom_layer_num]
//                     .update_and_est_without_l2(&DataInput::Str(key), value);
//                 for l in (1..=bottom_layer_num).rev() {
//                     self.hh_layers[l].update(key, median as i64);
//                 }
//                 // median = self.cs_layers[0].update_and_est(key, value);
//                 median = self.cs_layers[0].update_and_est(&DataInput::Str(key), value);
//                 self.hh_layers[0].update(key, median as i64);
//             } else {
//                 // let median = self.cs_layers[0].update_and_est(key, value);
//                 let median = self.cs_layers[0].update_and_est(&DataInput::Str(key), value);
//                 self.hh_layers[0].update(key, median as i64);
//             }
//         } else {
//             // let mut median = self.cs_layers[bottom_layer_num].update_and_est_without_l2(key, value);
//             let mut median = self.cs_layers[bottom_layer_num]
//                 .update_and_est_without_l2(&DataInput::Str(key), value);
//             for l in (1..=bottom_layer_num).rev() {
//                 self.hh_layers[l].update(key, median as i64);
//             }
//             // median = self.cs_layers[0].update_and_est(key, value);
//             median = self.cs_layers[0].update_and_est(&DataInput::Str(key), value);
//             self.hh_layers[0].update(key, median as i64);
//         }
//     }
//
//     pub fn update_pyramid(&mut self, key: &str, value: i64, bottom_layer_num: usize) {
//         // hardcode one more time
//         if bottom_layer_num < 8 {
//             for l in (0..=bottom_layer_num).rev() {
//                 let median = if l == 0 {
//                     self.cs_layers[l].update_and_est(&DataInput::Str(key), value)
//                 } else {
//                     self.cs_layers[l].update_and_est_without_l2(&DataInput::Str(key), value)
//                 };
//                 self.hh_layers[l].update(key, median as i64);
//             }
//         } else {
//             let mut median;
//             for l in (0..=7).rev() {
//                 if l == 0 {
//                     // median = self.cs_layers[l].update_and_est(key, value);
//                     median = self.cs_layers[l].update_and_est(&DataInput::Str(key), value);
//                 } else {
//                     // median = self.cs_layers[l].update_and_est_without_l2(key, value);
//                     median =
//                         self.cs_layers[l].update_and_est_without_l2(&DataInput::Str(key), value);
//                 }
//                 self.hh_layers[l].update(key, median as i64);
//             }
//             for l in (8..=bottom_layer_num).rev() {
//                 // median = self.cs_layers[l].update_and_est_without_l2(key, value);
//                 median = self.cs_layers[l].update_and_est_without_l2(&DataInput::Str(key), value);
//                 self.hh_layers[l].update(key, median as i64);
//             }
//         }
//     }
//
//     pub fn univmon_processing(&mut self, key: &str, value: i64, bottom_layer_num: usize) {
//         self.bucket_size += value as usize;
//         self.update(key, value, bottom_layer_num);
//     }
//
//     pub fn univmon_processing_optimized(&mut self, key: &str, value: i64, bottom_layer_num: usize) {
//         self.bucket_size += value as usize;
//         self.update_optimized(key, value, bottom_layer_num);
//     }
//
//     // pub fn print_hh_layer(&self) {
//     //     print!("Print HH_Layer: ");
//     //     for i in 0..self.layer {
//     //         println!("layer {}: ", i);
//     //         self.hh_layers[i].print_heap();
//     //     }
//     // }
//
//     pub fn calc_g_sum_heuristic<F>(&self, g: F, is_card: bool) -> f64
//     where
//         F: Fn(f64) -> f64,
//     {
//         let mut y = vec![0.0; self.layer];
//         let mut tmp: f64;
//
//         let l2_value = self.cs_layers[self.layer - 1].get_l2();
//         let mut threshold = (l2_value * 0.01) as i64;
//         if !is_card {
//             threshold = 0;
//         }
//
//         tmp = 0.0;
//         for item in self.hh_layers[self.layer - 1].heap() {
//             if item.count > threshold {
//                 tmp += g(item.count as f64);
//             }
//         }
//         y[self.layer - 1] = tmp;
//
//         for i in (0..(self.layer - 1)).rev() {
//             tmp = 0.0;
//             let l2_value = self.cs_layers[i].get_l2();
//             let mut threshold = (l2_value * 0.01) as i64;
//             if !is_card {
//                 threshold = 0;
//             }
//
//             for item in self.hh_layers[i].heap() {
//                 if item.count > threshold {
//                     // let hash = (hash64_seeded(CANONICAL_HASH_SEED, &item.key) >> (i+1)) & 1;
//                     // let hash = (hash64_seeded(CANONICAL_HASH_SEED, &DataInput::Str(&item.key)) >> (i + 1)) & 1;
//                     let hash =
//                         (hash64_seeded(BOTTOM_LAYER_FINDER, &DataInput::Str(&item.key)) >> (i + 1)) & 1;
//                     let coe = 1.0 - 2.0 * (hash as f64);
//                     tmp += coe * g(item.count as f64);
//                 }
//             }
//             y[i] = 2.0 * y[i + 1] + tmp;
//         }
//
//         y[0]
//     }
//
//     pub fn calc_g_sum<F>(&self, g: F, is_card: bool) -> f64
//     where
//         F: Fn(f64) -> f64,
//     {
//         self.calc_g_sum_heuristic(g, is_card)
//     }
//
//     pub fn calc_l1(&self) -> f64 {
//         self.calc_g_sum(|x| x, false)
//     }
//
//     pub fn calc_l2(&self) -> f64 {
//         let tmp = self.calc_g_sum(|x| x * x, false);
//         tmp.sqrt()
//     }
//
//     pub fn calc_entropy(&self) -> f64 {
//         let tmp = self.calc_g_sum(
//             |x| {
//                 if x > 0.0 { x * x.log2() } else { 0.0 }
//             },
//             false,
//         );
//         (self.bucket_size as f64).log2() - tmp / (self.bucket_size as f64)
//     }
//
//     pub fn calc_card(&self) -> f64 {
//         self.calc_g_sum(|_| 1.0, true)
//     }
//
//     pub fn merge_with(&mut self, other: &UnivMon) {
//         for i in 0..self.layer {
//             self.cs_layers[i].merge(&other.cs_layers[i]);
//
//             let mut topk = HHHeap::new(self.k);
//             for item in self.hh_layers[i].heap() {
//                 topk.update(&item.key, item.count);
//             }
//
//             for item in other.hh_layers[i].heap() {
//                 let count = if let Some(index) = topk.find(&item.key) {
//                     topk.heap()[index].count + item.count
//                 } else {
//                     item.count
//                 };
//                 topk.update(&item.key, count);
//             }
//
//             self.hh_layers[i] = topk;
//         }
//     }
// }
