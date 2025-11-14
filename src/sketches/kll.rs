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

/// Coin generates deterministic pseudo-random coin flips while amortizing
/// calls to the RNG by consuming one bit at a time from a 64-bit buffer.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Coin {
    state: u64,
    bit_cache: u64,
    #[serde(default)]
    remaining_bits: u8,
}

impl Coin {
    pub fn new() -> Self {
        let mut rng = rng();
        Self::from_seed(rng.random::<u64>())
    }

    pub fn xorshift_mult64(mut x: u64) -> u64 {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        x.wrapping_mul(2685821657736338717)
    }

    fn from_seed(seed: u64) -> Self {
        Self {
            state: Self::normalize_seed(seed),
            bit_cache: 0,
            remaining_bits: 0,
        }
    }

    #[inline]
    fn normalize_seed(seed: u64) -> u64 {
        const FALLBACK: u64 = 0x9e37_79b9_7f4a_7c15;
        if seed == 0 { FALLBACK } else { seed }
    }

    #[inline]
    fn refill(&mut self) {
        self.state = Self::normalize_seed(Self::xorshift_mult64(self.state));
        self.bit_cache = self.state;
        self.remaining_bits = u64::BITS as u8;
    }

    pub fn toss(&mut self) -> bool {
        if self.remaining_bits == 0 {
            self.refill();
        }
        let bit = (self.bit_cache & 1) != 0;
        self.bit_cache >>= 1;
        self.remaining_bits -= 1;
        bit
    }
}

/// One entry in the cumulative distribution, storing a value and its mass.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdfEntry {
    value: f64,
    quantile: f64,
}

/// KLL sketch with level compactors, the accuracy parameter `k`, running count,
/// and a reusable coin flip source for deterministic compaction.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KLL {
    compactors: Vector1D<Vector1D<f64>>,
    k: usize,
    total_count: usize,
    co: Coin,
}

impl Default for KLL {
    fn default() -> Self {
        Self::init_kll(20)
    }
}

impl KLL {
    pub fn init_kll(k: i32) -> Self {
        let norm_k = k.max(2) as usize;
        let mut compactors = Vector1D::init(5);
        let capacity = KLL::compactor_capacity(0, norm_k);
        compactors.push(Vector1D::init(capacity));
        KLL {
            compactors,
            k: norm_k,
            total_count: 0,
            co: Coin::new(),
        }
    }

    /// capacity of a compactor is sololy based on the height of the compactor and k
    #[inline(always)]
    fn compactor_capacity(height: i32, k: usize) -> usize {
        let scale = (2.0_f64 / 3.0_f64).powi(height);
        let capacity = (k as f64 * scale).ceil() as usize;
        capacity.max(1)
    }

    /// push a new compactor to the end
    fn grow(&mut self) {
        // let level_idx = self.compactors.len() + 1;
        // let capacity = KLL::compactor_capacity(level_idx as i32, self.k);
        self.compactors.push(Vector1D::init(self.k));
    }

    /// ensure the level-th compactor exists
    fn ensure_level(&mut self, level: usize) {
        while level >= self.compactors.len() {
            self.grow();
        }
    }

    /// number of items in all compactors
    fn buffer_size(&self) -> usize {
        self.compactors.iter().map(|c| c.len()).sum()
    }

    /// Update the sketch with a numeric value from SketchInput
    /// Returns an error if the input is not numeric
    pub fn update(&mut self, x: &SketchInput) -> Result<(), &'static str> {
        let value = sketch_input_to_f64(x)?;
        self.update_f64(value);
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

    /// perform compaction from `start_level`, typically from level 0
    fn compact_from_level(&mut self, start_level: usize) {
        let mut level = start_level;
        while level < self.compactors.len() {
            let capacity =
                KLL::compactor_capacity((self.compactors.len() - 1 - level) as i32, self.k);
            if self.compactors[level].len() > capacity {
                self.compact_level(level);
            }
            level += 1;
        }
    }

    /// only care about compaction at level
    /// potential compaction at level+1 will be taken care by compact_from_level()
    fn compact_level(&mut self, level: usize) {
        self.ensure_level(level + 1);
        let (left, right) = self.compactors.as_mut_slice().split_at_mut(level + 1);
        let source = &mut left[level];
        let destination = &mut right[0];
        source.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let keep = usize::from(self.co.toss());
        for (idx, value) in source.iter().enumerate() {
            if (idx & 1) == keep {
                destination.push(*value);
            }
        }
        source.clear();
    }

    pub fn merge(&mut self, other: &KLL) {
        for idx in 0..other.compactors.len() {
            let other_level = &other.compactors[idx];
            if other_level.is_empty() {
                continue;
            }
            self.ensure_level(idx);
            self.compactors[idx].extend_from_slice(other_level.as_slice());
        }
        self.total_count += other.total_count;
        self.compact_from_level(0);
    }

    /// Serialize the sketch into MessagePack bytes.
    pub fn serialize(&self) -> Option<Vec<u8>> {
        rmp_serde::to_vec(self).ok()
    }

    /// Deserialize a sketch from MessagePack bytes.
    pub fn deserialize(bytes: &[u8]) -> Option<Self> {
        rmp_serde::from_slice(bytes).ok()
    }

    /// get the rank of input x by counting how many input is smaller than x
    pub fn rank(&self, x: f64) -> usize {
        let mut r = 0;
        for (h, c) in self.compactors.iter().enumerate() {
            let weight = 1 << h;
            r += c.iter().filter(|&&v| v <= x).count() * weight;
        }
        r
    }

    /// the number of data represented in the sketch
    /// may differ from total_count (due to item lost during compaction)
    pub fn count(&self) -> usize {
        self.compactors
            .iter()
            .enumerate()
            .map(|(h, c)| c.len() * (1 << h))
            .sum()
    }

    /// get the quantile of input x
    /// notice the difference: this is not calculating p99/p50/etc.
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

    /// calculate the CDF for query()
    pub fn cdf(&self) -> CDF {
        let mut cdf = CDF {
            entries: Vector1D::init(self.buffer_size()),
        };
        let mut total_w = 0;

        for (h, c) in self.compactors.iter().enumerate() {
            let weight = 1 << h;
            for &v in c {
                cdf.entries.push(CdfEntry {
                    value: v,
                    quantile: weight as f64,
                });
            }
            total_w += c.len() * weight;
        }

        // empty
        if total_w == 0 {
            return cdf;
        }

        cdf.entries
            .sort_by(|a, b| a.value.partial_cmp(&b.value).unwrap());

        let mut cur_w = 0.0;
        for entry in cdf.entries.iter_mut() {
            cur_w += entry.quantile;
            entry.quantile = cur_w / total_w as f64;
        }

        cdf
    }
}

/// the CDF for query quantile
pub struct CDF {
    entries: Vector1D<CdfEntry>,
}

impl CDF {
    pub fn quantile(&self, x: f64) -> f64 {
        if self.entries.is_empty() {
            return 0.0;
        }
        let slice = self.entries.as_slice();
        match slice
            .binary_search_by(|e| e.value.partial_cmp(&x).unwrap_or(std::cmp::Ordering::Less))
        {
            Ok(idx) => slice[idx].quantile,
            Err(0) => 0.0,
            Err(idx) => slice[idx - 1].quantile,
        }
    }

    /// Returns the estimated value corresponding to quantile `p`
    pub fn query(&self, p: f64) -> f64 {
        if self.entries.is_empty() {
            return 0.0;
        }
        let slice = self.entries.as_slice();
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
        let slice = self.entries.as_slice();
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
        let slice = self.entries.as_slice();
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

    // Ensure each 64-bit chunk is consumed bit-by-bit before refilling.
    #[test]
    fn coin_bit_cache_behavior() {
        let seed = 0x0123_4567_89ab_cdef;
        let mut coin = Coin::from_seed(seed);
        let mut expected_state = Coin::normalize_seed(seed);

        for block in 0..3 {
            expected_state = Coin::normalize_seed(Coin::xorshift_mult64(expected_state));
            for bit in 0..64 {
                let expected = ((expected_state >> bit) & 1) != 0;
                assert_eq!(
                    coin.toss(),
                    expected,
                    "mismatch at block {block}, bit {bit}"
                );
            }
        }
    }

    // Zero seeds must map to a valid state and never fall back to zero.
    #[test]
    fn coin_state_never_zero() {
        let mut coin = Coin::from_seed(0);
        assert_ne!(coin.state, 0);

        for _ in 0..128 {
            coin.toss();
            assert_ne!(coin.state, 0);
        }
    }

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

    const SKETCH_K: i32 = 200;

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

    // return element from input with given quantile
    fn quantile_from_sorted(data: &[f64], quantile: f64) -> f64 {
        assert!(!data.is_empty(), "data set must not be empty");
        if quantile <= 0.0 {
            return data[0];
        }
        if quantile >= 1.0 {
            return data[data.len() - 1];
        }
        let n = data.len();
        let idx = ((quantile * n as f64).ceil() as isize - 1)
            .clamp(0, (n - 1) as isize) as usize;
        data[idx]
    }

    fn assert_quantiles_within_error(
        sketch: &KLL,
        sorted_truth: &[f64],
        quantiles: &[(f64, &str)],
        tolerance: f64,
        context: &str,
        sample_size: usize,
        seed: u64,
    ) {
        let cdf = sketch.cdf();
        for &(quantile, label) in quantiles {
            let lower_q = (quantile - tolerance).max(0.0);
            let upper_q = (quantile + tolerance).min(1.0);
            let truth_min = quantile_from_sorted(sorted_truth, lower_q);
            let truth_max = quantile_from_sorted(sorted_truth, upper_q);
            let estimate = cdf.query(quantile);
            assert!(
                (truth_min..=truth_max).contains(&estimate),
                "{label} exceeded tolerance: context={context}, sample_size={sample_size}, seed=0x{seed:08x}, \
                quantile={quantile:.4}, truth_min={truth_min:.4}, truth_max={truth_max:.4}, \
                estimate={estimate:.4}, tolerance={tolerance:.4}, total_length={}",
                sorted_truth.len()
            );
        }
    }

    #[test]
    fn distributions_quantiles_stay_within_rank_error() {
        const TOLERANCE: f64 = 0.02;
        const SAMPLE_SIZES: &[usize] = &[
            1_000, 5_000, 20_000, 100_000, 1_000_000, 5_000_000,
        ];
        const QUANTILES: &[(f64, &str)] = &[
            (0.0, "min"),
            (0.10, "p10"),
            (0.25, "p25"),
            (0.50, "p50"),
            (0.75, "p75"),
            (0.90, "p90"),
            (1.0, "max"),
        ];

        struct Case {
            name: &'static str,
            distribution: TestDistribution,
            seed_base: u64,
        }

        let cases = [
            Case {
                name: "uniform",
                distribution: TestDistribution::Uniform {
                    min: 0.0,
                    max: 100_000_000.0,
                },
                seed_base: 0xA5A5_0000,
            },
            Case {
                name: "zipf",
                distribution: TestDistribution::Zipf {
                    min: 1_000_000.0,
                    max: 10_000_000.0,
                    domain: 8_192,
                    exponent: 1.1,
                },
                seed_base: 0xB4B4_0000,
            },
        ];

        for case in cases {
            for (idx, &sample_size) in SAMPLE_SIZES.iter().enumerate() {
                let seed = case.seed_base + idx as u64;
                let (sketch, mut values) =
                    build_kll_with_distribution(SKETCH_K, sample_size, case.distribution, seed);
                values.sort_by(|a, b| a.partial_cmp(b).unwrap());
                assert_quantiles_within_error(
                    &sketch,
                    &values,
                    QUANTILES,
                    TOLERANCE,
                    case.name,
                    sample_size,
                    seed,
                );
            }
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
    fn merge_preserves_quantiles_within_tolerance() {
        const TOLERANCE: f64 = 0.02;
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
        assert_quantiles_within_error(
            &sketch_a,
            &sorted,
            QUANTILES,
            TOLERANCE,
            "merge",
            values.len(),
            0x00C0_FFEE,
        );
    }

    #[test]
    fn cdf_handles_empty_sketch() {
        let sketch = KLL::init_kll(64);
        let cdf = sketch.cdf();
        assert_eq!(cdf.quantile(123.0), 0.0);
        assert_eq!(cdf.query(0.5), 0.0);
        assert_eq!(cdf.query_li(0.5), 0.0);
    }

    #[test]
    fn kll_round_trip_rmp() {
        let mut sketch = KLL::init_kll(256);
        let samples = sample_uniform_f64(0.0, 1_000_000.0, 5_000, 0xDEAD_BEEF);
        for value in &samples {
            sketch.update_f64(*value);
        }

        let bytes = sketch.serialize().expect("serialize KLL with rmp");
        assert!(!bytes.is_empty(), "serialized bytes should not be empty");

        let restored = KLL::deserialize(&bytes).expect("deserialize KLL with rmp");
        assert_eq!(sketch.k, restored.k);
        assert_eq!(sketch.total_count, restored.total_count);
        assert_eq!(sketch.compactors.len(), restored.compactors.len());

        for (original, recovered) in sketch.compactors.iter().zip(restored.compactors.iter()) {
            assert_eq!(
                original.as_slice(),
                recovered.as_slice(),
                "compactor contents differ after round-trip"
            );
        }

        let quantiles = [0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0];
        let original_cdf = sketch.cdf();
        let restored_cdf = restored.cdf();
        for &q in &quantiles {
            assert!(
                (original_cdf.query(q) - restored_cdf.query(q)).abs() < f64::EPSILON,
                "quantile mismatch at p={q}: original={}, restored={}",
                original_cdf.query(q),
                restored_cdf.query(q)
            );
        }
    }
}
