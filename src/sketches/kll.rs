//! A translation of kll golang implementation
//! https://github.com/dgryski/go-kll
use rand::{Rng, rng};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Coin {
    st: u64,
    mask: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Compactor {
    items: Vec<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Quantile {
    quantile: f64,
    value: f64,
}
// pub type CDF = Vec<Quantile>;
// Cumulative Distribution Function
pub struct CDF {
    quantile_list: Vec<Quantile>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KLL {
    compactors: Vec<Compactor>,
    k: i32,
    compactor_count: i32,
    size: i32,     // number of current data
    max_size: i32, // max number of data at current height
    co: Coin,
}

// removing as sampler is only for optimization
// impl Sampler {
//     pub fn update(&mut self, x: f64, w: u64, mut to: Vec<f64>) -> Vec<f64> {
//         let ph = 1u64 << self.h;
//         let mut rng = rand::thread_rng();

//         if self.w + w <= ph {
//             self.w += w;
//             if rng.r#gen::<f64>() * (w as f64) < self.w as f64 {
//                 self.y = x;
//             }
//             if self.w == ph {
//                 self.w = 0;
//                 to.push(self.y);
//                 return to;
//             }
//         } else if self.w < w {
//             if rng.r#gen::<f64>() * (w as f64) < ph as f64 {
//                 to.push(x);
//                 return to;
//             }
//         } else {
//             self.w = w;
//             self.y = x;
//             if rng.r#gen::<f64>() * (w as f64) < ph as f64 {
//                 to.push(x);
//                 return to;
//             }
//         }
//         to
//     }

//     pub fn grow(&mut self) {
//         self.h += 1;
//     }
// }

// the coin is... not intuitive
// well... well... well...
// whatever, it seems to be usable
impl Coin {
    pub fn new() -> Self {
        let mut rng = rng();
        Coin {
            st: rng.random::<u64>(),
            mask: 0,
        }
    }

    pub fn xorshift_mult64(mut x: u64) -> u64 {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        x.wrapping_mul(2685821657736338717)
    }

    pub fn toss(&mut self) -> i32 {
        if self.mask == 0 {
            self.st = Coin::xorshift_mult64(self.st);
            self.mask = 1;
        }
        let v = if self.st & self.mask > 0 { 1 } else { 0 };
        self.mask <<= 1;
        v
    }
}

impl Compactor {
    pub fn new() -> Self {
        Compactor { items: Vec::new() }
    }

    // what is the difference between :-(
    // mut co: &Coin
    // co: &mut Coin
    pub fn compact(&mut self, co: &mut Coin, mut dst: Vec<f64>) -> Vec<f64> {
        let l = self.items.len();
        if l == 0 || l == 1 {
        } else if l == 2 {
            if self.items[0] > self.items[1] {
                // let temp = self.items[0];
                // self.items[0] = self.items[1];
                // self.items[1] = temp;
                self.items.swap(0, 1);
            }
        } else {
            self.items.sort_by(|a, b| a.partial_cmp(b).unwrap());
        }
        // ok, from this part is converted by ChatGPT
        // I have no idea what the hell it is
        // looks like just, allocate extra memory...? maybe ignore for now
        // let free = dst.capacity() - dst.len();
        // if free < self.items.len() / 2 {
        //     let extra = self.items.len() / 2 - free;
        //     dst.reserve(extra);
        // }
        // let offs = co.toss() as usize;
        // while self.items.len() >= 2 {
        //     let l = self.items.len() - 2;
        //     dst.push(self.items[l + offs]);
        //     self.items.truncate(l);
        // }
        let keep = co.toss() as usize;
        for i in 0..self.items.len() {
            if i % 2 == keep {
                dst.push(self.items[i]);
            }
        }
        dst
    }

    pub fn print_compactor(&self) {
        println!("compactor: {:?}", self.items);
    }

    pub fn append_all(&mut self, to_add: &mut Vec<f64>) {
        self.items.append(to_add);
    }

    // probably helper function to save some effort
    pub fn size(&self) -> i32 {
        self.items.len() as i32
    }
}

impl KLL {
    pub fn init_kll(k: i32) -> Self {
        let mut kll = KLL {
            compactors: vec![Compactor::new()],
            // compactors: Vec::new(),
            k,
            compactor_count: 1,
            size: 0,
            max_size: 0,
            co: Coin::new(),
        };
        kll.max_size = kll.capacity(0);
        kll
    }

    fn set_max_size(&mut self) {
        self.max_size = 0;
        for i in 0..self.compactor_count {
            self.max_size += KLL::capacity(self, i);
        }
    }

    fn grow(&mut self) {
        self.compactors.push(Compactor { items: Vec::new() });
        self.compactor_count = self.compactors.len() as i32;
        self.set_max_size();
    }

    fn capacity(&self, i: i32) -> i32 {
        let height = KLL::compute_height(self.compactors.len() as i32 - i - 1);
        (f64::ceil(self.k as f64 * height) as i32) + 1
    }

    fn compute_height(i: i32) -> f64 {
        // in golang implementation, there is a cache for this thing
        f64::powf(2.0 / 3.0, i as f64)
    }

    pub fn update(&mut self, x: f64) {
        self.compactors[0].items.push(x);
        self.size += 1;
        // for i in 0..self.compactors.len() {
        //     self.compactors[i].print_compactor();
        // }
        self.compact();
        // println!("update with: {}", x);
        // self.print_compactors();
    }

    pub fn print_compactors(&self) {
        for c in self.compactors.iter() {
            c.print_compactor();
            // println!("{}th compactor: {:?}", i, self.compactors[i].items);
        }
    }

    pub fn compact(&mut self) {
        // I will have some empty compactors...
        // that is... possible... right?
        // did I just compact too aggresively?
        for i in 0..self.compactor_count {
            // I think the capacity can be reached, right?
            // so, just, >, not >=... right?
            if self.compactors[i as usize].size() > self.capacity(i) {
                // do I really want to grow at this moment?
                if i + 1 >= self.compactor_count {
                    self.grow();
                }
                let to_compact_size = self.compactors[i as usize].size();
                let next_to_compact_size = self.compactors[i as usize + 1].size();
                let mut new_items = self.compactors[i as usize].compact(&mut self.co, Vec::new());
                self.compactors[i as usize + 1].append_all(&mut new_items);
                self.compactors[i as usize].items = Vec::new();
                self.size = self.size
                    + self.compactors[i as usize].size()
                    + self.compactors[i as usize + 1].size();
                self.size = self.size - to_compact_size - next_to_compact_size;
                // if self.size < self.max_size {
                //     break;
                // }
            }
        }
        // why the filter doesn't work??? what...? why???
        // self.compactors = self.compactors.iter().filter(| c | (**c).size() > 0).collect();
        // I don't want the first compactor to be kicked out

        // println!("before shrink: ");
        // self.print_compactors();

        // if self.compactors[0].size() > 0 {
        // self.compactors.retain(|c| c.size() > 0);
        // }
        // self.compactors = self
        // .compactors
        // .drain(..)
        // .enumerate()
        // .filter(|(i, c)| *i == 0 || c.size() > 0)
        // .map(|(_, c)| c)
        // .collect();
        // self.compactor_count = self.compactors.len() as i32;
        // self.set_max_size();

        // let mut new_compactors = Vec::new();
        // for i in 0..self.compactor_count {
        //     if self.compactors[i as usize].size() != 0 {
        //         let new_compactor = self.compactors[i as usize].clone();
        //         new_compactors.push(new_compactor);
        //     } else if i == 0 {
        //         let new_compactor = self.compactors[0].clone();
        //         new_compactors.push(new_compactor);
        //     }
        // }
        // self.compactors = new_compactors;
        // self.compactor_count = self.compactors.len() as i32;
        // self.set_max_size();

        // println!("after shrink: ");
        // self.print_compactors();

        // // the following is a translation of golang implementation
        // // I think it is not accurate
        // // whatever, maybe I'm wrong... again!
        // while self.size >= self.max_size {
        //     for i in 0..self.compactors.len() {
        //         if self.compactors[i].size() >= self.capacity(i as i32) {
        //             if i as i32 +1 >= self.compactor_count {
        //                 self.grow();
        //             }
        //             let prev_h = self.compactors[i].size();
        //             let prev_h1 = self.compactors[i+1].size();
        //             let mut new_items = self.compactors[i].compact(&mut self.co, Vec::new());
        //             self.compactors[i+1].append_all(&mut new_items);
        //             // after compact, the old compactor is supposed to be empty, right?
        //             // then, why the golang implementation doesn't have that at all?
        //             self.compactors[i].items = Vec::new();
        //             self.size = self.size + self.compactors[i].size() + self.compactors[i+1].size();
        //             self.size = self.size - prev_h - prev_h1;
        //             self.set_max_size();
        //             if self.size < self.max_size {
        //                 break;
        //             }
        //         }
        //     }
        // }
    }

    pub fn update_size(&mut self) {
        self.size = self.compactors.iter().map(|c| c.items.len() as i32).sum();
    }

    pub fn merge(&mut self, other: &KLL) {
        while self.compactor_count < other.compactor_count {
            self.grow();
        }

        for (h, c_other) in other.compactors.iter().enumerate() {
            self.compactors[h].items.extend_from_slice(&c_other.items);
        }

        self.update_size();
        self.compact();
    }

    pub fn rank(&self, x: f64) -> usize {
        let mut r = 0;
        for (h, c) in self.compactors.iter().enumerate() {
            let weight = 1 << h;
            r += c.items.iter().filter(|&&v| v <= x).count() * weight;
        }
        r
    }

    pub fn count(&self) -> usize {
        self.compactors
            .iter()
            .enumerate()
            .map(|(h, c)| c.items.len() * (1 << h))
            .sum()
    }

    pub fn quantile(&self, x: f64) -> f64 {
        let mut r = 0;
        let mut n = 0;
        for (h, c) in self.compactors.iter().enumerate() {
            let weight = 1 << h;
            for &v in &c.items {
                if v <= x {
                    r += weight;
                }
                n += weight;
            }
        }
        if n == 0 { 0.0 } else { r as f64 / n as f64 }
    }

    pub fn cdf(&self) -> CDF {
        let mut q: CDF = CDF {
            quantile_list: Vec::with_capacity(self.size as usize),
        };

        let mut total_w = 0.0;
        for (h, c) in self.compactors.iter().enumerate() {
            let weight = (1 << h) as f64;
            for &v in &c.items {
                q.quantile_list.push(Quantile {
                    quantile: weight,
                    value: v,
                });
            }
            total_w += c.items.len() as f64 * weight;
        }

        // Sort by value
        q.quantile_list
            .sort_by(|a, b| a.value.partial_cmp(&b.value).unwrap());

        // Convert q to cumulative distribution
        let mut cur_w = 0.0;
        for entry in &mut q.quantile_list {
            cur_w += entry.quantile;
            entry.quantile = cur_w / total_w;
        }

        q
    }
}

impl CDF {
    pub fn quantile(&self, x: f64) -> f64 {
        match self
            .quantile_list
            .binary_search_by(|e| e.value.partial_cmp(&x).unwrap_or(std::cmp::Ordering::Less))
        {
            Ok(idx) => self.quantile_list[idx].quantile,
            Err(0) => 0.0,
            Err(idx) => self.quantile_list[idx - 1].quantile,
        }
    }

    /// Returns the estimated value corresponding to quantile `p`
    pub fn query(&self, p: f64) -> f64 {
        match self.quantile_list.binary_search_by(|e| {
            e.quantile
                .partial_cmp(&p)
                .unwrap_or(std::cmp::Ordering::Less)
        }) {
            Ok(idx) => self.quantile_list[idx].value,
            Err(idx) if idx == self.quantile_list.len() => {
                self.quantile_list[self.quantile_list.len() - 1].value
            }
            Err(idx) => self.quantile_list[idx].value,
        }
    }

    /// Quantile estimation of value `x` using linear interpolation
    pub fn quantile_li(&self, x: f64) -> f64 {
        let idx = self.quantile_list.partition_point(|e| e.value < x);
        if idx == self.quantile_list.len() {
            return 1.0;
        }
        if idx == 0 {
            return 0.0;
        }
        let a = self.quantile_list[idx - 1].value;
        let aq = self.quantile_list[idx - 1].quantile;
        let b = self.quantile_list[idx].value;
        let bq = self.quantile_list[idx].quantile;
        ((a - x) * bq + (x - b) * aq) / (a - b)
    }

    /// Value estimation given quantile `p`, using linear interpolation
    pub fn query_li(&self, p: f64) -> f64 {
        let idx = self.quantile_list.partition_point(|e| e.quantile < p);
        if idx == self.quantile_list.len() {
            return self.quantile_list[self.quantile_list.len() - 1].value;
        }
        if idx == 0 {
            return self.quantile_list[0].value;
        }
        let a = self.quantile_list[idx - 1].value;
        let aq = self.quantile_list[idx - 1].quantile;
        let b = self.quantile_list[idx].value;
        let bq = self.quantile_list[idx].quantile;
        ((aq - p) * b + (p - bq) * a) / (aq - bq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{sample_uniform_f64, sample_zipf_f64};

    #[derive(Clone, Copy)]
    enum TestDistribution {
        Uniform {
            min: f64,
            max: f64,
        },
        Zipf {
            min: f64,
            max: f64,
            domain: usize,
            exponent: f64,
        },
    }

    const SKETCH_K: i32 = 512;

    fn build_kll_with_distribution(
        k: i32,
        sample_size: usize,
        distribution: TestDistribution,
        seed: u64,
    ) -> (KLL, Vec<f64>) {
        let mut sketch = KLL::init_kll(k);
        let values = match distribution {
            TestDistribution::Uniform { min, max } => {
                sample_uniform_f64(min, max, sample_size, seed)
            }
            TestDistribution::Zipf {
                min,
                max,
                domain,
                exponent,
            } => sample_zipf_f64(min, max, domain, exponent, sample_size, seed),
        };

        for &value in &values {
            sketch.update(value);
        }

        (sketch, values)
    }

    fn quantile_from_sorted(data: &[f64], quantile: f64) -> f64 {
        assert!(!data.is_empty(), "data set must not be empty");
        if quantile <= 0.0 {
            return data[0];
        }
        if quantile >= 1.0 {
            return data[data.len() - 1];
        }
        let position = quantile * (data.len() - 1) as f64;
        let lower_index = position.floor() as usize;
        let upper_index = position.ceil() as usize;
        if lower_index == upper_index {
            return data[lower_index];
        }
        let lower_value = data[lower_index];
        let upper_value = data[upper_index];
        let weight = position - lower_index as f64;
        lower_value + (upper_value - lower_value) * weight
    }

    fn assert_quantiles_within_error(
        sketch: &KLL,
        sorted_truth: &[f64],
        quantiles: &[(f64, &str)],
        tolerance: f64,
    ) {
        let cdf = sketch.cdf();
        for &(quantile, label) in quantiles {
            let truth = quantile_from_sorted(sorted_truth, quantile);
            let estimate = cdf.query(quantile);
            let rel_error = (estimate - truth).abs() / truth.abs();
            assert!(
                rel_error < tolerance,
                "{label} exceeded tolerance: truth={truth:.4},
                estimate={estimate:.4}, rel_error={rel_error:.4}, 
                total_length={}",
                sorted_truth.len()
            );
        }
    }

    #[test]
    fn uniform_distribution_quantiles_within_five_percent() {
        const TOLERANCE: f64 = 0.05;
        const DISTRIBUTION: TestDistribution = TestDistribution::Uniform {
            min: 1_000_000.0,
            max: 10_000_000.0,
        };
        const QUANTILES: &[(f64, &str)] = &[
            (0.0, "min"),
            (0.10, "p10"),
            (0.25, "p25"),
            (0.50, "p50"),
            (0.75, "p75"),
            (0.90, "p90"),
            (1.0, "max"),
        ];

        for (idx, sample_size) in [1_000usize, 5_000usize, 20_000usize]
            .into_iter()
            .enumerate()
        {
            let seed = 0xA5A5_0000_u64 + idx as u64;
            let (sketch, mut values) =
                build_kll_with_distribution(SKETCH_K, sample_size, DISTRIBUTION, seed);
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            assert_quantiles_within_error(&sketch, &values, QUANTILES, TOLERANCE);
        }
    }

    #[test]
    fn zipf_distribution_quantiles_within_five_percent() {
        const TOLERANCE: f64 = 0.05;
        const DISTRIBUTION: TestDistribution = TestDistribution::Zipf {
            min: 1_000_000.0,
            max: 10_000_000.0,
            domain: 8_192,
            exponent: 1.1,
        };
        const QUANTILES: &[(f64, &str)] = &[
            (0.0, "min"),
            (0.10, "p10"),
            (0.25, "p25"),
            (0.50, "p50"),
            (0.75, "p75"),
            (0.90, "p90"),
            (1.0, "max"),
        ];

        for (idx, sample_size) in [1_000usize, 5_000usize, 20_000usize]
            .into_iter()
            .enumerate()
        {
            let seed = 0xB4B4_0000_u64 + idx as u64;
            let (sketch, mut values) =
                build_kll_with_distribution(SKETCH_K, sample_size, DISTRIBUTION, seed);
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            assert_quantiles_within_error(&sketch, &values, QUANTILES, TOLERANCE);
        }
    }
}
