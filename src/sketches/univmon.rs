use super::{CountUniv, TopKHeap, utils::LASTSTATE, utils::SketchInput, utils::hash_it};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UnivMon {
    pub k: usize, // topK?
    pub row: usize,
    pub col: usize,
    pub layer: usize,
    pub cs_layers: Vec<CountUniv>,
    pub hh_layers: Vec<TopKHeap>,
    pub max_time: usize, // as most machine will be 64 bit, usize should suffice
    pub min_time: usize, // for sliding window model
    pub pool_idx: i64,
    pub heap_update: i32,
    pub bucket_size: usize,
}

impl UnivMon {
    pub fn init_univmon(k: usize, r: usize, c: usize, l: usize, p_idx: i64) -> Self {
        let mut um = UnivMon {
            k: k,
            row: r,
            col: c,
            layer: l,
            cs_layers: Vec::new(),
            hh_layers: Vec::new(),
            max_time: 0,
            min_time: 0,
            pool_idx: p_idx,
            heap_update: 0,
            bucket_size: 0,
        };
        for _ in 0..l {
            // every Count sketch will have different seeds
            // not sure if this is going to be a problem
            um.cs_layers.push(CountUniv::init_count());
            um.hh_layers.push(TopKHeap::init_heap(k as u32));
        }
        um
    }

    pub fn get_bucket_size(&self) -> usize {
        self.bucket_size
    }

    pub fn new_univmon_pytamid(k: usize, r: usize, c: usize, l: usize, p_idx: i64) -> Self {
        let mut um = UnivMon {
            k: k,
            row: r,
            col: c,
            layer: l,
            cs_layers: Vec::new(),
            hh_layers: Vec::new(),
            max_time: 0,
            min_time: 0,
            pool_idx: p_idx,
            heap_update: 0,
            bucket_size: 0,
        };
        // 8 is ELEPHANT_LAYER in PromSketch
        // hardcode now
        if l <= 8 {
            for _ in 0..l {
                // every Count sketch will have different seeds
                // not sure if this is going to be a problem
                um.cs_layers
                    .push(CountUniv::init_countuniv_with_rc(3, 2048));
                um.hh_layers.push(TopKHeap::init_heap(k as u32));
            }
        } else {
            for _ in 0..8 {
                um.cs_layers
                    .push(CountUniv::init_countuniv_with_rc(3, 2048));
                um.hh_layers.push(TopKHeap::init_heap(100));
            }
            for _ in 8..l {
                um.cs_layers.push(CountUniv::init_countuniv_with_rc(3, 512));
                um.hh_layers.push(TopKHeap::init_heap(100));
            }
        }
        um
    }

    pub fn free(&mut self) {
        self.bucket_size = 0;

        self.cs_layers.clear();
        self.hh_layers.clear();
    }

    // well... I'm not confident about this function
    pub fn get_memory_kb(&self) -> f64 {
        let mut total = 0.0;
        for i in 0..self.layer {
            total += self.hh_layers[i].get_memory_bytes();
        }
        return (2048.0 * 3.0 * (self.layer as f64) * 8.0 + total) / 1024.0;
    }

    pub fn get_memory_kb_pyramid(&self) -> f64 {
        let mut total = 0.0;
        for i in 0..self.layer {
            total += self.hh_layers[i].get_memory_bytes();
        }
        // again, hard code the ELEPHANT_LAYER for now
        if self.layer <= 8 {
            return (2048.0 * 3.0 * (self.layer as f64) * 8.0 + total) / 1024.0;
        } else {
            return ((2048.0 * 3.0 * 8.0 + 512.0 * 3.0 * (self.layer as f64 - 8.0)) * 8.0 + total)
                / 1024.0;
        }
    }

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

    pub fn print_hh_layer(&self) {
        print!("Print HH_Layer: ");
        for i in 0..self.layer {
            println!("layer {}: ", i);
            self.hh_layers[i].print_heap();
        }
    }

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
        for item in &self.hh_layers[self.layer - 1].heap {
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

            for item in &self.hh_layers[i].heap {
                if item.count > threshold {
                    // let hash = (hash_it(LASTSTATE, &item.key) >> (i+1)) & 1;
                    let hash = (hash_it(LASTSTATE, &SketchInput::Str(&item.key)) >> (i + 1)) & 1;
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
            let row = self.cs_layers[i].row;
            let col = self.cs_layers[i].col;

            for j in 0..row {
                for k in 0..col {
                    self.cs_layers[i].matrix[j][k] += other.cs_layers[i].matrix[j][k];
                }
            }

            let mut topk = TopKHeap::init_heap(self.k as u32);
            for item in &self.hh_layers[i].heap {
                topk.update(&item.key, item.count);
            }

            for item in &other.hh_layers[i].heap {
                let count = if let Some(index) = topk.find(&item.key) {
                    topk.heap[index].count + item.count
                } else {
                    item.count
                };
                topk.update(&item.key, count);
            }

            self.hh_layers[i] = TopKHeap::init_heap_from_heap(&topk);
        }
    }
}
