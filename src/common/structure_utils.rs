//! Common data structure that is served as basic building block
//! Vector1D:
//! Vector2D:
//! Vector3D:
//! CommonHeap:
// use rand::rngs::SmallRng;
// use rand::{Rng, SeedableRng, rng};
use serde::{Deserialize, Serialize};

// use crate::PRECOMPUTED_SAMPLE;
use crate::PRECOMPUTED_SAMPLE_RATE_1PERCENT;
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
    // #[serde(skip)]
    // #[serde(default = "new_small_rng")]
    // // generator: SmallRng,
    pub delta: u64,
    idx: usize,
    mask: usize,
}

// fn new_small_rng() -> SmallRng {
//     let mut seed_rng = rng();
//     SmallRng::from_rng(&mut seed_rng)
// }

impl Default for Nitro {
    fn default() -> Self {
        Self {
            is_nitro_mode: false,
            sampling_rate: 0.0,
            to_skip: 0,
            inv_ln_one_minus_p: 0.0, // not used unless Nitro mode is enabled
            // generator: new_small_rng(), // not used unless Nitro mode is enabled
            delta: 0,
            idx: 0,
            mask: 0x10000,
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
            // generator: new_small_rng(),
            delta: 0,
            idx: 0,
            mask: 0x10000,
        };
        nitro.delta = nitro.scaled_increment(1);
        nitro
    }

    // for profiling
    #[inline(always)]
    pub fn draw_geometric(&mut self) {
        if self.is_full_sampling() {
            self.to_skip = 0;
            return;
        }
        // let k = loop {
        //     let r = self.generator.random::<f64>();
        //     if r != 0.0_f64 && r != 1.0_f64 {
        //         break r;
        //     }
        // };
        // self.to_skip = ((1.0 - k).ln() * self.inv_ln_one_minus_p).ceil() as usize;

        // self.to_skip = (PRECOMPUTED_SAMPLE[self.idx] * self.inv_ln_one_minus_p).ceil() as usize;

        self.to_skip = PRECOMPUTED_SAMPLE_RATE_1PERCENT[self.idx].ceil() as usize;
        self.idx = (self.idx + 1) & self.mask;
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

    #[inline(always)]
    pub fn get_ctx(&self) -> (usize, f64, usize, usize) {
        (self.idx, self.inv_ln_one_minus_p, self.to_skip, self.mask)
    }

    #[inline(always)]
    pub fn commit_ctx(&mut self, idx: usize, to_skip: usize) {
        self.idx = idx;
        self.to_skip = to_skip;
    }
}

/// Compute median from a mutable slice of f64 values (inline helper)
/// This is used by query_median_with_custom_hash for HydraCounter queries
#[inline(always)]
pub fn compute_median_inline_f64(values: &mut [f64]) -> f64 {
    match values.len() {
        0 => 0.0,
        1 => values[0],
        2 => (values[0] + values[1]) / 2.0,
        // starting here is an assumption that LLVM and compiler
        // will load var into register and perform simple register swap
        // no heavy sort or memory swap
        3 => {
            let (mut v0, mut v1, v2) = (values[0], values[1], values[2]);
            // ensure v0 is smaller than v1
            if v0 > v1 {
                std::mem::swap(&mut v0, &mut v1);
            }
            // ensure v1 is smaller than v2, and ignore the actual v2 value
            if v1 > v2 {
                v1 = v2;
            }
            // ensure v1 is still greater than v0
            if v0 > v1 {
                v1 = v0;
            }
            v1
        }
        4 => {
            let (mut v0, mut v1, mut v2, mut v3) = (values[0], values[1], values[2], values[3]);
            // ensure the order of v0 and v1
            if v0 > v1 {
                std::mem::swap(&mut v0, &mut v1);
            }
            // ensure the order of v2 and v3
            if v2 > v3 {
                std::mem::swap(&mut v2, &mut v3);
            }
            // the smaller of v0 and v2 will be smaller than v1 anyway
            // ignore the smaller one, which will be min (dropped)
            if v0 > v2 {
                v2 = v0;
            }
            // the greater of v1 and v3 will be greater than v2 anyway
            // ignore the greeater one, which will be max (dropped)
            if v1 > v3 {
                v1 = v3;
            }
            (v1 + v2) / 2.0
        }
        5 => {
            let (mut v0, mut v1, mut v2, mut v3, mut v4) =
                (values[0], values[1], values[2], values[3], values[4]);
            // ensure the order of v0 and v1
            if v0 > v1 {
                std::mem::swap(&mut v0, &mut v1);
            }
            // ensure the order of v3 and v4
            if v3 > v4 {
                std::mem::swap(&mut v3, &mut v4);
            }
            // the smaller of v0 v3 will be smaller than v1 v4 and the other
            // smaller than 3 value, so not median of 5
            if v0 > v3 {
                v3 = v0;
            }
            // the greater of v1 v4 will be greater than v0 v3 and the other
            // greater than 3 value, so not median of 5
            if v1 > v4 {
                v1 = v4;
            }
            // median of 5 is reduced to median of v1 v2 v3
            // v0 and v4 will not change the order
            // v0 will be one of the two smallest
            // v4 will be one of the two greatest
            // safely ignored
            if v1 > v2 {
                std::mem::swap(&mut v1, &mut v2);
            }
            if v2 > v3 {
                v2 = v3;
            }
            if v1 > v2 {
                v2 = v1;
            }
            v2
        }
        _ => {
            values.sort_unstable_by(f64::total_cmp);
            let mid = values.len() / 2;
            if values.len() % 2 == 1 {
                values[mid]
            } else {
                (values[mid - 1] + values[mid]) / 2.0
            }
        }
    }
}

/// Trait defining heap ordering behavior.
#[cfg(test)]
mod tests {

    use super::*;
    use rand::{Rng, SeedableRng, rngs::StdRng};

    fn build_three() -> Vec<[f64; 3]> {
        let mut rng = StdRng::seed_from_u64(0x5eed_c0de_1234_5678);
        (0..1_000)
            .map(|_| {
                [
                    rng.random::<f64>(),
                    rng.random::<f64>(),
                    rng.random::<f64>(),
                ]
            })
            .collect()
    }

    fn build_four() -> Vec<[f64; 4]> {
        let mut rng = StdRng::seed_from_u64(0x5eed_c0de_1234_5678);
        (0..1_000)
            .map(|_| {
                [
                    rng.random::<f64>(),
                    rng.random::<f64>(),
                    rng.random::<f64>(),
                    rng.random::<f64>(),
                ]
            })
            .collect()
    }

    fn build_five() -> Vec<[f64; 5]> {
        let mut rng = StdRng::seed_from_u64(0x5eed_c0de_1234_5678);
        (0..1_000)
            .map(|_| {
                [
                    rng.random::<f64>(),
                    rng.random::<f64>(),
                    rng.random::<f64>(),
                    rng.random::<f64>(),
                    rng.random::<f64>(),
                ]
            })
            .collect()
    }

    fn median_three_sort(values: &mut [f64; 3]) -> f64 {
        values.sort_unstable_by(f64::total_cmp);
        let mid = values.len() / 2;
        if values.len() % 2 == 1 {
            values[mid]
        } else {
            (values[mid - 1] + values[mid]) / 2.0
        }
    }

    fn median_four_sort(values: &mut [f64; 4]) -> f64 {
        values.sort_unstable_by(f64::total_cmp);
        let mid = values.len() / 2;
        if values.len() % 2 == 1 {
            values[mid]
        } else {
            (values[mid - 1] + values[mid]) / 2.0
        }
    }

    fn median_five_sort(values: &mut [f64; 5]) -> f64 {
        values.sort_unstable_by(f64::total_cmp);
        let mid = values.len() / 2;
        if values.len() % 2 == 1 {
            values[mid]
        } else {
            (values[mid - 1] + values[mid]) / 2.0
        }
    }

    #[test]
    fn median_test() {
        let mut three_vec = build_three();
        let mut four_vec = build_four();
        let mut five_vec = build_five();
        for v in &mut three_vec {
            let fast_median = compute_median_inline_f64(v);
            let sort_median = median_three_sort(v);
            assert_eq!(
                fast_median, sort_median,
                "median for sort is {sort_median} but fast gives {fast_median}, input is {:?}",
                v
            );
        }
        for v in &mut four_vec {
            let fast_median = compute_median_inline_f64(v);
            let sort_median = median_four_sort(v);
            assert_eq!(
                fast_median, sort_median,
                "median for sort is {sort_median} but fast gives {fast_median}, input is {:?}",
                v
            );
        }
        for v in &mut five_vec {
            let fast_median = compute_median_inline_f64(v);
            let sort_median = median_five_sort(v);
            assert_eq!(
                fast_median, sort_median,
                "median for sort is {sort_median} but fast gives {fast_median}, input is {:?}",
                v
            );
        }
    }
}
