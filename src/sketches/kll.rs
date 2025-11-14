//! A translation of kll golang implementation
//! https://github.com/dgryski/go-kll
use rand::{Rng, rng};
use serde::{Deserialize, Serialize};

use crate::{SketchInput, Vector1D};

/// Convert SketchInput to f64 for KLL sketch
/// Returns an error if the input is not numeric
fn sketch_input_to_f64(input: &SketchInput) -> Result<f64, &'static str> {
    match input {
        SketchInput::I8(v) => Ok(*v as f64),
        SketchInput::I16(v) => Ok(*v as f64),
        SketchInput::I32(v) => Ok(*v as f64),
        SketchInput::I64(v) => Ok(*v as f64),
        SketchInput::I128(v) => Ok(*v as f64),
        SketchInput::ISIZE(v) => Ok(*v as f64),
        SketchInput::U8(v) => Ok(*v as f64),
        SketchInput::U16(v) => Ok(*v as f64),
        SketchInput::U32(v) => Ok(*v as f64),
        SketchInput::U64(v) => Ok(*v as f64),
        SketchInput::U128(v) => Ok(*v as f64),
        SketchInput::USIZE(v) => Ok(*v as f64),
        SketchInput::F32(v) => Ok(*v as f64),
        SketchInput::F64(v) => Ok(*v),
        SketchInput::Str(_) | SketchInput::String(_) | SketchInput::Bytes(_) => {
            Err("KLL sketch only accepts numeric inputs")
        }
    }
}

/// Coin to ensure that the coin toss is consistent
/// across multiple flip
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Coin {
    st: u64,
    mask: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Quantile {
    quantile: f64,
    value: f64,
}
// pub type CDF = Vec<Quantile>;
// Cumulative Distribution Function
pub struct CDF {
    quantile_list: Vector1D<Quantile>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KLL {
    compactors: Vector1D<Vector1D<f64>>,
    k: usize,
    total_count: usize,
    co: Coin,
}

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

impl Default for KLL {
    fn default() -> Self {
        Self::init_kll(20)
    }
}

impl KLL {
    pub fn init_kll(k: i32) -> Self {
        let norm_k = k.max(2) as usize;
        let mut compactors = Vector1D::init(0);
        let base_capacity = KLL::capacity_for(0, 1, norm_k);
        compactors.push(Vector1D::from_vec(Vec::with_capacity(base_capacity)));
        KLL {
            compactors,
            k: norm_k,
            total_count: 0,
            co: Coin::new(),
        }
    }

    fn grow(&mut self) {
        let level_idx = self.compactors.len();
        let total_levels = level_idx + 1;
        let capacity = KLL::capacity_for(level_idx, total_levels, self.k);
        self.compactors
            .push(Vector1D::from_vec(Vec::with_capacity(capacity)));
    }

    fn capacity(&self, level: usize) -> usize {
        KLL::capacity_for(level, self.compactors.len(), self.k)
    }

    fn capacity_for(level: usize, total_levels: usize, k: usize) -> usize {
        let height = KLL::compute_height(total_levels as i32 - level as i32 - 1);
        (f64::ceil(k as f64 * height) as usize) + 1
    }

    fn ensure_level(&mut self, level: usize) {
        while level >= self.compactors.len() {
            self.grow();
        }
    }

    fn buffer_size(&self) -> usize {
        self.compactors.iter().map(|c| c.len()).sum()
    }

    fn compute_height(i: i32) -> f64 {
        // in golang implementation, there is a cache for this thing
        f64::powf(2.0 / 3.0, i as f64)
    }

    /// Update the sketch with a numeric value from SketchInput
    /// Returns an error if the input is not numeric
    pub fn update(&mut self, x: &SketchInput) -> Result<(), &'static str> {
        let value = sketch_input_to_f64(x)?;
        self.compactors[0].push(value);
        self.total_count += 1;
        self.compact_from_level(0);
        Ok(())
    }

    /// Update the sketch with a raw f64 value (for internal use and testing)
    pub fn update_f64(&mut self, x: f64) {
        self.compactors[0].push(x);
        self.total_count += 1;
        self.compact_from_level(0);
    }

    pub fn print_compactors(&self) {
        for c in self.compactors.iter() {
            println!("{:?}", c);
        }
    }

    pub fn compact(&mut self) {
        self.compact_from_level(0);
    }

    fn compact_from_level(&mut self, start_level: usize) {
        let mut level = start_level;
        while level < self.compactors.len() {
            let capacity = self.capacity(level);
            if self.compactors[level].len() <= capacity {
                level += 1;
                continue;
            }
            self.compact_level(level);
            level += 1;
        }
    }

    fn compact_level(&mut self, level: usize) {
        self.ensure_level(level + 1);

        let (left, right) = self.compactors.as_mut_slice().split_at_mut(level + 1);
        let current = &mut left[level];
        if current.len() <= 1 {
            return;
        }
        current.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let keep = self.co.toss() as usize & 1;
        let next = &mut right[0];

        for (idx, &value) in current.iter().enumerate() {
            if (idx & 1) == keep {
                next.push(value);
            }
        }
        current.clear();
    }

    pub fn merge(&mut self, other: &KLL) {
        for idx in 0..other.compactors.len() {
            self.ensure_level(idx);
            let other_level = &other.compactors[idx];
            if other_level.is_empty() {
                continue;
            }
            self.compactors[idx].extend_from_slice(other_level.as_slice());
        }
        self.total_count += other.total_count;
        self.compact_from_level(0);
    }

    pub fn rank(&self, x: f64) -> usize {
        let mut r = 0;
        for (h, c) in self.compactors.iter().enumerate() {
            let weight = 1 << h;
            r += c.iter().filter(|&&v| v <= x).count() * weight;
        }
        r
    }

    pub fn count(&self) -> usize {
        self.compactors
            .iter()
            .enumerate()
            .map(|(h, c)| c.len() * (1 << h))
            .sum()
    }

    pub fn quantile(&self, x: f64) -> f64 {
        let mut r = 0;
        let mut n = 0;
        for (h, c) in self.compactors.iter().enumerate() {
            let weight = 1 << h;
            for &v in c {
                if v <= x {
                    r += weight;
                }
                n += weight;
            }
        }
        if n == 0 { 0.0 } else { r as f64 / n as f64 }
    }

    pub fn cdf(&self) -> CDF {
        let mut q = CDF {
            quantile_list: Vector1D::from_vec(Vec::with_capacity(self.buffer_size())),
        };

        let mut total_w = 0.0;
        for (h, c) in self.compactors.iter().enumerate() {
            let weight = (1 << h) as f64;
            for &v in c {
                q.quantile_list.push(Quantile {
                    quantile: weight,
                    value: v,
                });
            }
            total_w += c.len() as f64 * weight;
        }

        if total_w == 0.0 {
            return q;
        }

        // Sort by value
        q.quantile_list
            .sort_by(|a, b| a.value.partial_cmp(&b.value).unwrap());

        // Convert q to cumulative distribution
        let mut cur_w = 0.0;
        for entry in q.quantile_list.iter_mut() {
            cur_w += entry.quantile;
            entry.quantile = cur_w / total_w;
        }

        q
    }
}

impl CDF {
    pub fn quantile(&self, x: f64) -> f64 {
        if self.quantile_list.is_empty() {
            return 0.0;
        }
        let slice = self.quantile_list.as_slice();
        match slice.binary_search_by(|e| {
            e.value
                .partial_cmp(&x)
                .unwrap_or(std::cmp::Ordering::Less)
        }) {
            Ok(idx) => slice[idx].quantile,
            Err(0) => 0.0,
            Err(idx) => slice[idx - 1].quantile,
        }
    }

    /// Returns the estimated value corresponding to quantile `p`
    pub fn query(&self, p: f64) -> f64 {
        if self.quantile_list.is_empty() {
            return 0.0;
        }
        let slice = self.quantile_list.as_slice();
        match slice.binary_search_by(|e| {
            e.quantile
                .partial_cmp(&p)
                .unwrap_or(std::cmp::Ordering::Less)
        }) {
            Ok(idx) => slice[idx].value,
            Err(idx) if idx == slice.len() => slice[slice.len() - 1].value,
            Err(idx) => slice[idx].value,
        }
    }

    /// Quantile estimation of value `x` using linear interpolation
    pub fn quantile_li(&self, x: f64) -> f64 {
        let slice = self.quantile_list.as_slice();
        if slice.is_empty() {
            return 0.0;
        }
        let idx = slice.partition_point(|e| e.value < x);
        if idx == slice.len() {
            return 1.0;
        }
        if idx == 0 {
            return 0.0;
        }
        let a = slice[idx - 1].value;
        let aq = slice[idx - 1].quantile;
        let b = slice[idx].value;
        let bq = slice[idx].quantile;
        ((a - x) * bq + (x - b) * aq) / (a - b)
    }

    /// Value estimation given quantile `p`, using linear interpolation
    pub fn query_li(&self, p: f64) -> f64 {
        let slice = self.quantile_list.as_slice();
        if slice.is_empty() {
            return 0.0;
        }
        let idx = slice.partition_point(|e| e.quantile < p);
        if idx == slice.len() {
            return slice[slice.len() - 1].value;
        }
        if idx == 0 {
            return slice[0].value;
        }
        let a = slice[idx - 1].value;
        let aq = slice[idx - 1].quantile;
        let b = slice[idx].value;
        let bq = slice[idx].quantile;
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
            sketch.update_f64(value);
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
        // const TOLERANCE: f64 = 0.5;
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
    fn test_sketch_input_api() {
        let mut kll = KLL::init_kll(128);

        // Test with different numeric types
        kll.update(&SketchInput::I32(10)).unwrap();
        kll.update(&SketchInput::I64(20)).unwrap();
        kll.update(&SketchInput::F64(30.5)).unwrap();
        kll.update(&SketchInput::F32(40.2)).unwrap();
        kll.update(&SketchInput::U32(50)).unwrap();

        // Query quantiles
        let cdf = kll.cdf();
        let median = cdf.query(0.5);

        // Median should be around 30
        assert!(median > 20.0 && median < 40.0, "Median = {}", median);

        // Test error handling for non-numeric input
        let result = kll.update(&SketchInput::String("not a number".to_string()));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "KLL sketch only accepts numeric inputs"
        );
    }

    #[test]
    fn zipf_distribution_quantiles_within_five_percent() {
        // const TOLERANCE: f64 = 0.5;
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

    #[test]
    fn merge_preserves_quantiles_within_tolerance() {
        const TOLERANCE: f64 = 0.05;
        const QUANTILES: &[(f64, &str)] = &[
            (0.0, "min"),
            (0.10, "p10"),
            (0.25, "p25"),
            (0.50, "p50"),
            (0.75, "p75"),
            (0.90, "p90"),
            (1.0, "max"),
        ];

        let values = sample_uniform_f64(1_000_000.0, 10_000_000.0, 10_000, 0xC0FFEE);
        let mut sketch_a = KLL::init_kll(SKETCH_K);
        let mut sketch_b = KLL::init_kll(SKETCH_K);

        for (idx, value) in values.iter().copied().enumerate() {
            if idx % 2 == 0 {
                sketch_a.update_f64(value);
            } else {
                sketch_b.update_f64(value);
            }
        }

        sketch_a.merge(&sketch_b);

        let mut sorted = values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_quantiles_within_error(&sketch_a, &sorted, QUANTILES, TOLERANCE);
    }

    #[test]
    fn cdf_handles_empty_sketch() {
        let sketch = KLL::init_kll(64);
        let cdf = sketch.cdf();
        assert_eq!(cdf.quantile(123.0), 0.0);
        assert_eq!(cdf.query(0.5), 0.0);
        assert_eq!(cdf.query_li(0.5), 0.0);
    }
}
