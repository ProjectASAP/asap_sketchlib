// use super::chapter::L2HH;
// use crate::{
//     TopKHeap,
//     common::{LASTSTATE, SketchInput, hash_it},
// };

// pub struct UM {
//     pub k: usize,
//     pub row: usize,
//     pub col: usize,
//     pub layer: usize,
//     pub layers: Vec<L2HH>,
//     pub hh_layers: Vec<TopKHeap>,
//     pub pool_idx: i64,
//     // pub heap_update: i32,
//     pub bucket_size: usize,
// }

// impl UM {
//     pub fn init_um(k: usize, r: usize, c: usize, l: usize, p_idx: i64, sketch_type: L2HH) -> Self {
//         let mut um = UM {
//             k: k,
//             row: r,
//             col: c,
//             layer: l,
//             layers: Vec::new(),
//             hh_layers: Vec::new(),
//             pool_idx: p_idx,
//             bucket_size: 0,
//         };
//         for _ in 0..l {
//             um.layers.push(sketch_type.clone());
//             um.hh_layers.push(TopKHeap::init_heap(k as u32));
//         }
//         um
//     }

//     /// need to understand what this pyramid is trying to do first
//     // pub fn new_univmon_pyramid(k: usize, r: usize, c: usize, l: usize, p_idx: i64, sketch_type: Chapter<'a>) -> Self {
//     //     let mut um = UM {
//     //         k: k,
//     //         row: r,
//     //         col: c,
//     //         layer: l,
//     //         layers: Vec::new(),
//     //         hh_layers: Vec::new(),
//     //         pool_idx: p_idx,
//     //     };
//     //     // 8 is ELEPHANT_LAYER in PromSketch
//     //     // hardcode now
//     //     if l <= 8 {
//     //         for _ in 0..l {
//     //             // every Count sketch will have different seeds
//     //             // not sure if this is going to be a problem
//     //             um.layers.push(sketch_type.clone());
//     //             um.hh_layers.push(TopKHeap::init_heap(k as u32));
//     //         }
//     //     } else {
//     //         for _ in 0..8 {
//     //             um.layers
//     //                 .push(CountUniv::init_countuniv_with_rc(3, 2048));
//     //             um.hh_layers.push(TopKHeap::init_heap(100));
//     //         }
//     //         for _ in 8..l {
//     //             um.cs_layers.push(CountUniv::init_countuniv_with_rc(3, 512));
//     //             um.hh_layers.push(TopKHeap::init_heap(100));
//     //         }
//     //     }
//     //     um
//     // }

//     pub fn find_bottom_layer_num(&self, hash: u64, layer: usize) -> usize {
//         for l in 1..layer {
//             if ((hash >> l) & 1) == 0 {
//                 return l - 1;
//             }
//         }
//         return layer - 1;
//     }

//     pub fn update(&mut self, key: &str, value: i64, bottom_layer_num: usize) {
//         if value == 0 || self.layer == 0 {
//             return;
//         }

//         if value >= 0 {
//             self.bucket_size = self.bucket_size.saturating_add(value as usize);
//         } else {
//             let magnitude = value.saturating_abs() as usize;
//             self.bucket_size = self.bucket_size.saturating_sub(magnitude);
//         }

//         let sketch_input = SketchInput::Str(key);
//         let max_level = std::cmp::min(bottom_layer_num, self.layer.saturating_sub(1));
//         for level in 0..=max_level {
//             let estimate = self.layers[level].update(&sketch_input, value);
//             self.hh_layers[level].update(key, estimate.round() as i64);
//         }
//     }

//     pub fn calc_g_sum_heuristic<F>(&self, g: F, is_card: bool) -> f64
//     where
//         F: Fn(f64) -> f64,
//     {
//         let mut y = vec![0.0; self.layer];
//         let mut tmp: f64;

//         let l2_value = self.layers[self.layer - 1].get_l2();
//         let mut threshold = (l2_value * 0.01) as i64;
//         if !is_card {
//             threshold = 0;
//         }

//         tmp = 0.0;
//         for item in &self.hh_layers[self.layer - 1].heap {
//             if item.count > threshold {
//                 tmp += g(item.count as f64);
//             }
//         }
//         y[self.layer - 1] = tmp;

//         for i in (0..(self.layer - 1)).rev() {
//             tmp = 0.0;
//             let l2_value = self.layers[i].get_l2();
//             let mut threshold = (l2_value * 0.01) as i64;
//             if !is_card {
//                 threshold = 0;
//             }

//             for item in &self.hh_layers[i].heap {
//                 if item.count > threshold {
//                     // let hash = (hash_it(LASTSTATE, &item.key) >> (i+1)) & 1;
//                     let hash = (hash_it(LASTSTATE, &SketchInput::Str(&item.key)) >> (i + 1)) & 1;
//                     let coe = 1.0 - 2.0 * (hash as f64);
//                     tmp += coe * g(item.count as f64);
//                 }
//             }
//             y[i] = 2.0 * y[i + 1] + tmp;
//         }

//         y[0]
//     }

//     pub fn calc_g_sum<F>(&self, g: F, is_card: bool) -> f64
//     where
//         F: Fn(f64) -> f64,
//     {
//         self.calc_g_sum_heuristic(g, is_card)
//     }

//     pub fn calc_l1(&self) -> f64 {
//         self.calc_g_sum(|x| x, false)
//     }

//     pub fn calc_l2(&self) -> f64 {
//         let tmp = self.calc_g_sum(|x| x * x, false);
//         tmp.sqrt()
//     }

//     pub fn calc_entropy(&self) -> f64 {
//         if self.bucket_size == 0 {
//             return 0.0;
//         }

//         let tmp = self.calc_g_sum(
//             |x| {
//                 if x > 0.0 { x * x.log2() } else { 0.0 }
//             },
//             false,
//         );
//         (self.bucket_size as f64).log2() - tmp / (self.bucket_size as f64)
//     }

//     pub fn calc_card(&self) -> f64 {
//         self.calc_g_sum(|_| 1.0, true)
//     }

//     pub fn merge_with(&mut self, other: &UM) {
//         for i in 0..self.layer {
//             self.layers[i].merge(&other.layers[i]);

//             let mut topk = TopKHeap::init_heap(self.k as u32);
//             for item in &self.hh_layers[i].heap {
//                 topk.update(&item.key, item.count);
//             }

//             for item in &other.hh_layers[i].heap {
//                 let count = if let Some(index) = topk.find(&item.key) {
//                     topk.heap[index].count + item.count
//                 } else {
//                     item.count
//                 };
//                 topk.update(&item.key, count);
//             }

//             self.hh_layers[i] = TopKHeap::init_heap_from_heap(&topk);
//         }
//     }
// }
