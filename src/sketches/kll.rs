//! KLL quantile sketch (compact / insert-optimized variant).
//!
//! Insertion and compaction follow the compact KLL layout from:
//! "Insert-optimized implementation of streaming data sketches" (Pfeil et al., 2025).
//! CDF construction is adapted from dgryski's Go implementation.
//!
//! References:
//! - https://www.amazon.science/publications/insert-optimized-implementation-of-streaming-data-sketches
//! - https://github.com/dgryski/go-kll

use rand::{Rng, rng};
use rmp_serde::decode::Error as RmpDecodeError;
use rmp_serde::encode::Error as RmpEncodeError;
use serde::{Deserialize, Serialize};

use crate::common::input::sketch_input_to_f64;
use crate::{SketchInput, Vector1D};

const CAPACITY_CACHE_LEN: usize = 20;
const MAX_CACHEABLE_K: usize = 26_602;
const CAPACITY_DECAY: f64 = 2.0 / 3.0;
const DEFAULT_K: i32 = 200;

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

/// KLL quantile sketch using a compact, insert-optimized layout.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KLL {
    items: Vector1D<f64>, // compactors, packed
    /// Stores the START index of each level in `items`.
    levels: Vector1D<usize>,
    k: usize,
    m: usize, // Minimum buffer size (usually 8)
    num_levels: usize,
    co: Coin,
    /// Cached level capacities by height (from top to bottom)
    #[serde(skip)]
    capacity_cache: [u32; CAPACITY_CACHE_LEN],
    /// Tracks current top height so we can index into the cache quickly.
    #[serde(skip)]
    top_height: usize,
    /// Cached capacity for level 0 to speed up hot-path updates
    #[serde(skip)]
    level0_capacity: usize,
}

impl Default for KLL {
    fn default() -> Self {
        Self::init_kll(DEFAULT_K)
    }
}

impl KLL {
    /// Creates a KLL sketch with the given `k` and `m` parameters.
    pub fn init(k: usize, m: usize) -> Self {
        let mut norm_m = m.min(MAX_CACHEABLE_K);
        norm_m = norm_m.max(2);
        let mut norm_k = k.max(norm_m);
        if norm_k > MAX_CACHEABLE_K {
            norm_k = MAX_CACHEABLE_K;
        }
        let mut s = Self {
            items: Vector1D::init(norm_k * 3),
            levels: Vector1D::filled(2, 0),
            k: norm_k,
            m: norm_m,
            num_levels: 1,
            co: Coin::new(),
            capacity_cache: [0; CAPACITY_CACHE_LEN],
            top_height: 0,
            level0_capacity: 0,
        };
        s.rebuild_capacity_cache();
        s
    }

    /// Creates a KLL sketch with default `m` and the provided `k`.
    pub fn init_kll(k: i32) -> Self {
        Self::init(k as usize, 8)
    }

    fn push_value(&mut self, value: f64) {
        self.items.push(value);

        if let Some(last) = self.levels.last_mut() {
            *last = self.items.len();
        }

        let levels_slice = self.levels.as_slice();
        let l0_start = levels_slice[self.num_levels - 1];
        let l0_count = self.items.len() - l0_start;

        if l0_count > self.level0_capacity {
            self.compress_while_needed();
        }
    }

    /// The hot path: O(1) insertion at the end of the vector.
    pub fn update(&mut self, val: &SketchInput) -> Result<(), &'static str> {
        let value = sketch_input_to_f64(val)?;
        self.push_value(value);
        Ok(())
    }

    /// Loops to maintain the KLL invariant.
    fn compress_while_needed(&mut self) {
        let mut h = 0;
        loop {
            let level_idx = self.num_levels - 1 - h;
            let cap = self.capacity_for_level(h);

            let size = self.level_size(h);

            if size <= cap {
                break;
            }

            if level_idx == 0 {
                self.add_new_top_level();
                continue;
            }

            self.compact(h);
            h += 1;
        }
    }

    fn capacity_for_level(&self, level: usize) -> usize {
        if self.num_levels == 0 {
            return self.m;
        }
        let height_from_top = self.top_height.saturating_sub(level);
        let idx = height_from_top.min(CAPACITY_CACHE_LEN - 1);
        self.capacity_cache[idx] as usize
    }

    fn rebuild_capacity_cache(&mut self) {
        self.top_height = self.num_levels.saturating_sub(1);
        let mut scale = 1.0_f64;
        for idx in 0..CAPACITY_CACHE_LEN {
            let scaled = ((self.k as f64) * scale).ceil() as usize;
            let cap = scaled.max(self.m);
            self.capacity_cache[idx] = cap as u32;
            scale *= CAPACITY_DECAY;
        }
        self.level0_capacity = self.capacity_for_level(0);
    }

    #[inline]
    fn level_size(&self, h: usize) -> usize {
        let idx = self.num_levels - 1 - h;
        let slice = self.levels.as_slice();
        slice[idx + 1] - slice[idx]
    }

    fn add_new_top_level(&mut self) {
        self.levels.insert(0, 0);
        if let Some(last) = self.levels.last_mut() {
            *last = self.items.len();
        }
        self.num_levels += 1;
        self.top_height = self.num_levels - 1;
        self.level0_capacity = self.capacity_for_level(0);
    }

    fn compact(&mut self, h: usize) {
        let cur_lvl_idx = self.num_levels - 1 - h;

        // Get raw indices first
        let levels_slice = self.levels.as_mut_slice();
        let start = levels_slice[cur_lvl_idx];
        let end = levels_slice[cur_lvl_idx + 1];
        let count = end - start;

        let items = self.items.as_mut_slice();

        items[start..end].sort_unstable_by(f64::total_cmp);

        let offset = usize::from(self.co.toss());
        let mut survivors = 0;
        let mut i = offset;

        while i < count {
            items[start + survivors] = items[start + i];
            survivors += 1;
            i += 2;
        }

        let garbage_len = count - survivors;
        let start_garbage = start + survivors;
        let end_garbage = end;
        let tail_len = items.len() - end_garbage;

        if tail_len > 0 {
            // Safety: source and destination ranges may overlap, but `ptr::copy` handles overlap.
            // The ranges are within `items` and `tail_len` ensures we stay in-bounds.
            unsafe {
                let ptr = items.as_mut_ptr();
                std::ptr::copy(ptr.add(end_garbage), ptr.add(start_garbage), tail_len);
            }
        }

        let new_len = items.len() - garbage_len;
        self.items.truncate(new_len);

        // Update level pointers after shift
        let levels_slice = self.levels.as_mut_slice();
        levels_slice[cur_lvl_idx] = start + survivors;

        for pos in levels_slice
            .iter_mut()
            .take(self.num_levels + 1)
            .skip(cur_lvl_idx + 1)
        {
            *pos -= garbage_len;
        }

        // Sync last pointer just in case (should be covered by loop, but ensures safety)
        levels_slice[self.num_levels] = self.items.len();
    }

    /// Reset the sketch to its initial state, preserving `k`, `m`, and the
    /// backing `items` allocation. After clearing, the sketch behaves as if
    /// freshly constructed.
    pub fn clear(&mut self) {
        self.items.clear();
        self.levels = Vector1D::filled(2, 0);
        self.num_levels = 1;
        self.co = Coin::new();
        self.rebuild_capacity_cache();
    }

    /// Prints the compactors for debugging.
    pub fn print_compactors(&self) {
        println!(
            "KLL Packed (k={}, levels={}, items={})",
            self.k,
            self.num_levels,
            self.items.len()
        );
        let levels = self.levels.as_slice();
        let items = self.items.as_slice();
        for h in (0..self.num_levels).rev() {
            let idx = self.num_levels - 1 - h;
            let start = levels[idx];
            let end = levels[idx + 1];
            println!("  L{}: {:?}", h, &items[start..end]);
        }
    }

    /// Builds a CDF representation of the sketch.
    pub fn cdf(&self) -> Cdf {
        let mut cdf = Cdf {
            entries: Vector1D::init(self.buffer_size()),
        };
        let mut total_w = 0usize;

        let levels = self.levels.as_slice();
        let items = self.items.as_slice();

        for h in 0..self.num_levels {
            let idx = self.num_levels - 1 - h;
            let start = levels[idx];
            let end = levels[idx + 1];
            let weight = 1 << h;
            for &value in &items[start..end] {
                cdf.entries.push(CdfEntry {
                    value,
                    quantile: weight as f64,
                });
            }
            total_w += (end - start) * weight;
        }

        if total_w == 0 {
            return cdf;
        }

        cdf.entries
            .as_mut_slice()
            .sort_by(|a, b| a.value.partial_cmp(&b.value).unwrap());

        let mut cur_w = 0.0;
        for entry in cdf.entries.as_mut_slice() {
            cur_w += entry.quantile;
            entry.quantile = cur_w / total_w as f64;
        }

        cdf
    }

    /// Merges another sketch into this one.
    pub fn merge(&mut self, other: &KLL) {
        for &value in other.items.as_slice() {
            self.push_value(value);
        }
    }

    /// Returns the estimated value at quantile `q`.
    pub fn quantile(&self, q: f64) -> f64 {
        let cdf = self.cdf();
        cdf.query(q)
    }

    /// Returns the estimated rank of value `x`.
    pub fn rank(&self, x: f64) -> usize {
        let mut r = 0;
        let levels = self.levels.as_slice();
        let items = self.items.as_slice();

        for h in 0..self.num_levels {
            let idx = self.num_levels - 1 - h;
            let start = levels[idx];
            let end = levels[idx + 1];
            let weight = 1 << h;

            // Using iter check is faster than full slice copy
            for &val in &items[start..end] {
                if val <= x {
                    r += weight;
                }
            }
        }
        r
    }

    /// Returns the total count of observations seen by the sketch.
    pub fn count(&self) -> usize {
        let mut total = 0;
        for h in 0..self.num_levels {
            total += self.level_size(h) * (1 << h);
        }
        total
    }

    /// Number of stored samples across all levels.
    fn buffer_size(&self) -> usize {
        self.items.len()
    }

    /// Serialize the sketch into MessagePack bytes.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        rmp_serde::to_vec(self)
    }

    /// Deserialize a sketch from MessagePack bytes.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        rmp_serde::from_slice(bytes).map(|mut sketch: KLL| {
            sketch.rebuild_capacity_cache();
            sketch
        })
    }
}

/// The CDF for quantile queries.
pub struct Cdf {
    entries: Vector1D<CdfEntry>,
}

impl Cdf {
    /// Returns the quantile for value `x` using the CDF table.
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

    /// Prints the CDF entries for debugging.
    pub fn print_entries(&self) {
        println!("entries: {:?}", self.entries);
    }

    /// Returns the estimated value corresponding to quantile `p`.
    pub fn query(&self, p: f64) -> f64 {
        // println!("{:?}", self.entries);
        if self.entries.is_empty() {
            return 0.0;
        }
        let slice = self.entries.as_slice();
        match slice.binary_search_by(|e| {
            e.quantile
                .partial_cmp(&p)
                .unwrap_or(std::cmp::Ordering::Less)
        }) {
            Ok(idx) => {
                // println!("idx: {idx}");
                slice[idx].value
            }
            Err(idx) if idx == slice.len() => {
                // println!("ERR1: idx: {idx}");
                slice[slice.len() - 1].value
            }
            Err(idx) => {
                // println!("ERR2: idx: {idx}");
                slice[idx].value
            }
        }
    }

    /// Quantile estimation of value `x` using linear interpolation.
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

    /// Value estimation given quantile `p`, using linear interpolation.
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
            sketch.update(&SketchInput::F64(value)).unwrap();
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
        let idx = ((quantile * n as f64).ceil() as isize - 1).clamp(0, (n - 1) as isize) as usize;
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
        const SAMPLE_SIZES: &[usize] = &[1_000, 5_000, 20_000, 100_000, 1_000_000, 5_000_000];
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
        // kll.print_compactors();
        let median = cdf.query(0.5);

        // Median should be 30.5
        assert!(median > 20.0 && median < 40.2, "Median = {}", median);

        // Test error handling for non-numeric input
        let result = kll.update(&SketchInput::String("not a number".to_string()));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "KLL sketch only accepts numeric inputs"
        );
    }

    #[test]
    fn test_forced_compact() {
        // force compaction to happen with small k/m
        let mut kll = KLL::init(3, 3);
        // kll.print_compactors();
        kll.update(&SketchInput::F64(10.0)).unwrap();
        // kll.print_compactors();
        kll.update(&SketchInput::F64(20.0)).unwrap();
        // kll.print_compactors();
        kll.update(&SketchInput::F64(30.0)).unwrap();
        // kll.print_compactors();
        kll.update(&SketchInput::F64(40.0)).unwrap();
        // kll.print_compactors();
        kll.update(&SketchInput::F64(50.0)).unwrap();
        // kll.print_compactors();
        let cdf = kll.cdf();
        // cdf.print_entries();
        let median = cdf.query(0.5);
        // only 30 and 40 is possible
        assert!(median == 30.0 || median == 40.0, "Median = {}", median);
    }

    #[test]
    fn test_no_compact() {
        // no compaction should happen
        let mut kll = KLL::init_kll(8);
        // kll.print_compactors();
        kll.update(&SketchInput::F64(10.0)).unwrap();
        // kll.print_compactors();
        kll.update(&SketchInput::F64(20.0)).unwrap();
        // kll.print_compactors();
        kll.update(&SketchInput::F64(30.0)).unwrap();
        // kll.print_compactors();
        kll.update(&SketchInput::F64(40.0)).unwrap();
        // kll.print_compactors();
        kll.update(&SketchInput::F64(50.0)).unwrap();
        // kll.print_compactors();

        // Query quantiles
        let cdf = kll.cdf();
        // cdf.print_entries();
        // kll.print_compactors();
        let median = cdf.query(0.5);
        // Median should be 30
        assert!(median == 30.0, "Median = {}", median);
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
                sketch_a.update(&SketchInput::F64(value)).unwrap();
            } else {
                sketch_b.update(&SketchInput::F64(value)).unwrap();
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
            sketch.update(&SketchInput::F64(*value)).unwrap();
        }

        let bytes = sketch.serialize_to_bytes().expect("serialize KLL with rmp");
        assert!(!bytes.is_empty(), "serialized bytes should not be empty");

        let restored = KLL::deserialize_from_bytes(&bytes).expect("deserialize KLL with rmp");
        assert_eq!(sketch.k, restored.k);
        assert_eq!(sketch.m, restored.m);
        assert_eq!(sketch.num_levels, restored.num_levels);
        assert_eq!(sketch.top_height, restored.top_height);
        assert_eq!(sketch.level0_capacity, restored.level0_capacity);
        assert_eq!(
            sketch.levels.as_slice(),
            restored.levels.as_slice(),
            "level boundaries changed after round-trip"
        );
        assert_eq!(
            sketch.items.as_slice(),
            restored.items.as_slice(),
            "packed items changed after round-trip"
        );

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
