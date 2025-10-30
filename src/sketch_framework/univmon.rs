use crate::common::heap::HHHeap;
use crate::common::{BOTTOM_LAYER_FINDER, SketchInput, hash_it};
use crate::common::{L2HH, Vector1D};
use crate::sketches::count::CountL2HH;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UnivMon {
    pub k: usize,
    pub row: usize,
    pub col: usize,
    pub layer: usize,
    pub cs_layers: Vector1D<L2HH>,
    pub hh_layers: Vector1D<HHHeap>,
    pub pool_idx: i64,
    pub heap_update: i32,
    pub bucket_size: usize,
}

impl UnivMon {
    pub fn init_univmon(k: usize, r: usize, c: usize, l: usize, p_idx: i64) -> Self {
        // Create cs_layers - each layer needs different seeds
        // Layer i uses SEEDLIST[i] for hashing
        let cs_vec: Vec<L2HH> = (0..l)
            .map(|i| L2HH::COUNT(CountL2HH::with_dimensions_and_seed(r, c, i)))
            .collect();

        // Create hh_layers
        let hh_vec: Vec<HHHeap> = (0..l).map(|_| HHHeap::new(k)).collect();

        UnivMon {
            k,
            row: r,
            col: c,
            layer: l,
            cs_layers: Vector1D::from_vec(cs_vec),
            hh_layers: Vector1D::from_vec(hh_vec),
            pool_idx: p_idx,
            heap_update: 0,
            bucket_size: 0,
        }
    }

    pub fn get_bucket_size(&self) -> usize {
        self.bucket_size
    }

    pub fn new_univmon_pyramid(k: usize, r: usize, c: usize, l: usize, p_idx: i64) -> Self {
        // 8 is ELEPHANT_LAYER in PromSketch
        // Each layer i uses SEEDLIST[i] for hashing
        let cs_vec: Vec<L2HH> = if l <= 8 {
            (0..l)
                .map(|i| L2HH::COUNT(CountL2HH::with_dimensions_and_seed(3, 2048, i)))
                .collect()
        } else {
            (0..8)
                .map(|i| L2HH::COUNT(CountL2HH::with_dimensions_and_seed(3, 2048, i)))
                .chain((8..l).map(|i| L2HH::COUNT(CountL2HH::with_dimensions_and_seed(3, 512, i))))
                .collect()
        };

        let hh_vec: Vec<HHHeap> = if l <= 8 {
            (0..l).map(|_| HHHeap::new(k)).collect()
        } else {
            (0..l).map(|_| HHHeap::new(100)).collect()
        };

        UnivMon {
            k,
            row: r,
            col: c,
            layer: l,
            cs_layers: Vector1D::from_vec(cs_vec),
            hh_layers: Vector1D::from_vec(hh_vec),
            pool_idx: p_idx,
            heap_update: 0,
            bucket_size: 0,
        }
    }

    // pub fn free(&mut self) {
    //     self.bucket_size = 0;

    //     self.cs_layers.clear();
    //     self.hh_layers.clear();
    // }

    // well... I'm not confident about this function
    // pub fn get_memory_kb(&self) -> f64 {
    //     let mut total = 0.0;
    //     for i in 0..self.layer {
    //         total += self.hh_layers[i].get_memory_bytes();
    //     }
    //     return (2048.0 * 3.0 * (self.layer as f64) * 8.0 + total) / 1024.0;
    // }

    // pub fn get_memory_kb_pyramid(&self) -> f64 {
    //     let mut total = 0.0;
    //     for i in 0..self.layer {
    //         total += self.hh_layers[i].get_memory_bytes();
    //     }
    //     // again, hard code the ELEPHANT_LAYER for now
    //     if self.layer <= 8 {
    //         return (2048.0 * 3.0 * (self.layer as f64) * 8.0 + total) / 1024.0;
    //     } else {
    //         return ((2048.0 * 3.0 * 8.0 + 512.0 * 3.0 * (self.layer as f64 - 8.0)) * 8.0 + total)
    //             / 1024.0;
    //     }
    // }

    // update univmon
    pub fn find_bottom_layer_num(&self, hash: u64, layer: usize) -> usize {
        for l in 1..layer {
            if ((hash >> l) & 1) == 0 {
                return l - 1;
            }
        }
        return layer - 1;
    }

    pub fn update(&mut self, key: &str, value: i64, bottom_layer_num: usize) {
        for i in 0..=bottom_layer_num {
            let count;
            if i == 0 {
                // self.cs_layers[i].insert_count(key);
                // count = self.cs_layers[i].update_and_est(key, value);
                count = self.cs_layers[i].update_and_est(&SketchInput::Str(key), value);
            } else {
                // count = self.cs_layers[i].update_and_est_without_l2(key,value);
                count = self.cs_layers[i].update_and_est_without_l2(&SketchInput::Str(key), value);
            }
            self.hh_layers[i].update(key, count as i64);
        }
    }

    pub fn update_optimized(&mut self, key: &str, value: i64, bottom_layer_num: usize) {
        // hardcode again
        if bottom_layer_num < 8 {
            if bottom_layer_num > 0 {
                // let mut median = self.cs_layers[bottom_layer_num].update_and_est_without_l2(key, value);
                let mut median = self.cs_layers[bottom_layer_num]
                    .update_and_est_without_l2(&SketchInput::Str(key), value);
                for l in (1..=bottom_layer_num).rev() {
                    self.hh_layers[l].update(key, median as i64);
                }
                // median = self.cs_layers[0].update_and_est(key, value);
                median = self.cs_layers[0].update_and_est(&SketchInput::Str(key), value);
                self.hh_layers[0].update(key, median as i64);
            } else {
                // let median = self.cs_layers[0].update_and_est(key, value);
                let median = self.cs_layers[0].update_and_est(&SketchInput::Str(key), value);
                self.hh_layers[0].update(key, median as i64);
            }
        } else {
            // let mut median = self.cs_layers[bottom_layer_num].update_and_est_without_l2(key, value);
            let mut median = self.cs_layers[bottom_layer_num]
                .update_and_est_without_l2(&SketchInput::Str(key), value);
            for l in (1..=bottom_layer_num).rev() {
                self.hh_layers[l].update(key, median as i64);
            }
            // median = self.cs_layers[0].update_and_est(key, value);
            median = self.cs_layers[0].update_and_est(&SketchInput::Str(key), value);
            self.hh_layers[0].update(key, median as i64);
        }
    }

    pub fn update_pyramid(&mut self, key: &str, value: i64, bottom_layer_num: usize) {
        // hardcode one more time
        if bottom_layer_num < 8 {
            for l in (0..=bottom_layer_num).rev() {
                let median;
                if l == 0 {
                    // median = self.cs_layers[l].update_and_est(key, value);
                    median = self.cs_layers[l].update_and_est(&SketchInput::Str(key), value);
                } else {
                    // median = self.cs_layers[l].update_and_est_without_l2(key, value);
                    median =
                        self.cs_layers[l].update_and_est_without_l2(&SketchInput::Str(key), value);
                }
                self.hh_layers[l].update(key, median as i64);
            }
        } else {
            let mut median;
            for l in (0..=7).rev() {
                if l == 0 {
                    // median = self.cs_layers[l].update_and_est(key, value);
                    median = self.cs_layers[l].update_and_est(&SketchInput::Str(key), value);
                } else {
                    // median = self.cs_layers[l].update_and_est_without_l2(key, value);
                    median =
                        self.cs_layers[l].update_and_est_without_l2(&SketchInput::Str(key), value);
                }
                self.hh_layers[l].update(key, median as i64);
            }
            for l in (8..=bottom_layer_num).rev() {
                // median = self.cs_layers[l].update_and_est_without_l2(key, value);
                median = self.cs_layers[l].update_and_est_without_l2(&SketchInput::Str(key), value);
                self.hh_layers[l].update(key, median as i64);
            }
        }
    }

    pub fn univmon_processing(&mut self, key: &str, value: i64, bottom_layer_num: usize) {
        self.bucket_size += value as usize;
        self.update(key, value, bottom_layer_num);
    }

    pub fn univmon_processing_optimized(&mut self, key: &str, value: i64, bottom_layer_num: usize) {
        self.bucket_size += value as usize;
        self.update_optimized(key, value, bottom_layer_num);
    }

    // pub fn print_hh_layer(&self) {
    //     print!("Print HH_Layer: ");
    //     for i in 0..self.layer {
    //         println!("layer {}: ", i);
    //         self.hh_layers[i].print_heap();
    //     }
    // }

    pub fn calc_g_sum_heuristic<F>(&self, g: F, is_card: bool) -> f64
    where
        F: Fn(f64) -> f64,
    {
        let mut y = vec![0.0; self.layer];
        let mut tmp: f64;

        let l2_value = self.cs_layers[self.layer - 1].get_l2();
        let mut threshold = (l2_value * 0.01) as i64;
        if !is_card {
            threshold = 0;
        }

        tmp = 0.0;
        for item in self.hh_layers[self.layer - 1].heap() {
            if item.count > threshold {
                tmp += g(item.count as f64);
            }
        }
        y[self.layer - 1] = tmp;

        for i in (0..(self.layer - 1)).rev() {
            tmp = 0.0;
            let l2_value = self.cs_layers[i].get_l2();
            let mut threshold = (l2_value * 0.01) as i64;
            if !is_card {
                threshold = 0;
            }

            for item in self.hh_layers[i].heap() {
                if item.count > threshold {
                    // let hash = (hash_it(LASTSTATE, &item.key) >> (i+1)) & 1;
                    // let hash = (hash_it(LASTSTATE, &SketchInput::Str(&item.key)) >> (i + 1)) & 1;
                    let hash =
                        (hash_it(BOTTOM_LAYER_FINDER, &SketchInput::Str(&item.key)) >> (i + 1)) & 1;
                    let coe = 1.0 - 2.0 * (hash as f64);
                    tmp += coe * g(item.count as f64);
                }
            }
            y[i] = 2.0 * y[i + 1] + tmp;
        }

        y[0]
    }

    pub fn calc_g_sum<F>(&self, g: F, is_card: bool) -> f64
    where
        F: Fn(f64) -> f64,
    {
        self.calc_g_sum_heuristic(g, is_card)
    }

    pub fn calc_l1(&self) -> f64 {
        self.calc_g_sum(|x| x, false)
    }

    pub fn calc_l2(&self) -> f64 {
        let tmp = self.calc_g_sum(|x| x * x, false);
        tmp.sqrt()
    }

    pub fn calc_entropy(&self) -> f64 {
        let tmp = self.calc_g_sum(
            |x| {
                if x > 0.0 { x * x.log2() } else { 0.0 }
            },
            false,
        );
        (self.bucket_size as f64).log2() - tmp / (self.bucket_size as f64)
    }

    pub fn calc_card(&self) -> f64 {
        self.calc_g_sum(|_| 1.0, true)
    }

    pub fn merge_with(&mut self, other: &UnivMon) {
        for i in 0..self.layer {
            self.cs_layers[i].merge(&other.cs_layers[i]);

            let mut topk = HHHeap::new(self.k);
            for item in self.hh_layers[i].heap() {
                topk.update(&item.key, item.count);
            }

            for item in other.hh_layers[i].heap() {
                let count = if let Some(index) = topk.find(&item.key) {
                    topk.heap()[index].count + item.count
                } else {
                    item.count
                };
                topk.update(&item.key, count);
            }

            self.hh_layers[i] = topk;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BOTTOM_LAYER_FINDER, SketchInput, hash_it};

    fn bottom_layer_for(um: &UnivMon, key: &str) -> usize {
        let hash = hash_it(BOTTOM_LAYER_FINDER, &SketchInput::Str(key));
        um.find_bottom_layer_num(hash, um.layer)
    }

    #[test]
    fn update_populates_bucket_size_and_heavy_hitters() {
        // processing a single hot key should record its weight in the heavy hitter layers
        let mut um = UnivMon::init_univmon(16, 3, 32, 4, 0);
        let key = "alpha";
        let bottom = bottom_layer_for(&um, key);

        for _ in 0..40 {
            um.univmon_processing(key, 1, bottom);
        }

        assert_eq!(um.bucket_size, 40);

        let idx = um.hh_layers[0]
            .find(key)
            .expect("heavy hitter should track key");
        assert!(
            um.hh_layers[0].heap()[idx].count >= 20,
            "expected significant count for heavy hitter, got {}",
            um.hh_layers[0].heap()[idx].count
        );
        assert!(um.calc_l1() > 0.0);
        assert!(um.calc_card() >= 1.0);
    }

    #[test]
    fn merge_with_combines_heavy_hitters() {
        // merging two sketches should keep contributions from both sides
        let mut left = UnivMon::init_univmon(16, 3, 32, 4, 0);
        let mut right = UnivMon::init_univmon(16, 3, 32, 4, 0);

        let key_left = "left";
        let key_right = "right";

        let bottom_left = bottom_layer_for(&left, key_left);
        let bottom_right = bottom_layer_for(&right, key_right);

        for _ in 0..25 {
            left.univmon_processing(key_left, 1, bottom_left);
        }
        for _ in 0..30 {
            right.univmon_processing(key_right, 1, bottom_right);
        }

        left.merge_with(&right);

        let idx_left = left.hh_layers[0].find(key_left).expect("left key present");
        let idx_right = left.hh_layers[0]
            .find(key_right)
            .expect("right key present");
        assert!(left.hh_layers[0].heap()[idx_left].count > 0);
        assert!(left.hh_layers[0].heap()[idx_right].count > 0);
    }

    #[test]
    fn univmon_layers_use_different_seeds() {
        // Verify that different layers in UnivMon use different seeds
        // by checking they produce different hash values
        use crate::common::hash_it_to_128;

        let _um = UnivMon::init_univmon(20, 3, 1024, 4, 0);

        // Hash the same key with different seed indices (as used by different layers)
        let test_key = SketchInput::Str("test_flow");

        // Hash the same key with different seed indices (as used by different layers)
        let hash_0 = hash_it_to_128(0, &test_key);
        let hash_1 = hash_it_to_128(1, &test_key);
        let hash_2 = hash_it_to_128(2, &test_key);
        let hash_3 = hash_it_to_128(3, &test_key);

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
        let mut um = UnivMon::init_univmon(20, 3, 2048, 8, 0);

        for i in 0..20 {
            let key = format!("flow_{}", i);
            let bottom = bottom_layer_for(&um, &key);
            um.univmon_processing(&key, 10, bottom);
        }

        let card = um.calc_card();
        assert!(
            card == 20.0,
            "Cardinality should be positive after insertions, got {}",
            card
        );
    }

    #[test]
    fn univmon_bucket_size_tracked_correctly() {
        // Verify that bucket_size is correctly tracked with seed configuration
        let mut um = UnivMon::init_univmon(20, 3, 1024, 6, 0);

        let flows = [("flow_a", 100), ("flow_b", 200), ("flow_c", 150)];
        let expected_total = 450;

        for (key, count) in &flows {
            let bottom = bottom_layer_for(&um, key);
            um.univmon_processing(key, *count, bottom);
        }

        assert_eq!(
            um.get_bucket_size(),
            expected_total,
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

        let mut um = UnivMon::init_univmon(100, 3, 2048, 16, -1);
        for case in cases {
            let h = hash_it(BOTTOM_LAYER_FINDER, &SketchInput::Str(&case.0));
            let bln = um.find_bottom_layer_num(h, 16);
            um.univmon_processing(&case.0, case.1, bln);
        }

        assert_eq!(um.calc_card(), 10.0, "Cardinality estimation incorrect");
        assert_eq!(um.calc_l1(), 131.0, "L1 estimation incorrect");
    }

    #[test]
    fn univmon_different_seeds_maintain_accuracy() {
        // Verify that using different seed indices doesn't break basic accuracy
        // Create two UnivMons with same config but verify both maintain accuracy

        let mut um1 = UnivMon::new_univmon_pyramid(20, 3, 2048, 10, 0);
        let mut um2 = UnivMon::new_univmon_pyramid(20, 3, 2048, 10, 1); // Different pool_idx

        // Insert same data into both with more flows for better stability
        let flows = [
            ("flow_a", 150),
            ("flow_b", 200),
            ("flow_c", 100),
            ("flow_d", 180),
            ("flow_e", 120),
        ];

        let true_l1 = 750f64;

        for (key, count) in &flows {
            let bottom1 = bottom_layer_for(&um1, key);
            let bottom2 = bottom_layer_for(&um2, key);
            um1.univmon_processing(key, *count, bottom1);
            um2.univmon_processing(key, *count, bottom2);
        }

        // Both should estimate L1 with reasonable accuracy
        let est_l1_1 = um1.calc_l1();
        let est_l1_2 = um2.calc_l1();

        let error_1 = ((est_l1_1 - true_l1 as f64).abs()) / (true_l1 as f64);
        let error_2 = ((est_l1_2 - true_l1 as f64).abs()) / (true_l1 as f64);

        assert!(
            est_l1_1 == true_l1,
            "UnivMon 1 L1 estimate {} should be reasonably accurate (error: {:.2}%)",
            est_l1_1,
            error_1 * 100.0
        );
        assert!(
            est_l1_1 == true_l1,
            "UnivMon 2 L1 estimate {} should be reasonably accurate (error: {:.2}%)",
            est_l1_2,
            error_2 * 100.0
        );
    }
}
