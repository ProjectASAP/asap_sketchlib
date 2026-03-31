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

const MAX_LEVELS: usize = 61;

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

/// Computes the maximum number of items the sketch can hold across all
/// levels for the given `k` and `m`. The buffer is pre-allocated to this
/// size so that no dynamic reallocation ever occurs.
fn compute_max_capacity(k: usize, m: usize) -> usize {
    let mut total = 0;
    let mut scale = 1.0_f64;
    for _ in 0..MAX_LEVELS {
        total += ((k as f64) * scale).ceil().max(m as f64) as usize;
        scale *= CAPACITY_DECAY;
    }
    total
}

/// Halves a sorted run, placing survivors in the **upper** (right) half of
/// `items[begin..begin+pop]` so they are contiguous with the level above.
/// Traverses backwards to avoid overwriting unread source elements.
#[inline]
fn randomly_halve_up(items: &mut [f64], begin: usize, pop: usize, offset: usize) -> usize {
    let num_survivors = (pop - offset + 1) / 2;
    let dest = begin + pop - num_survivors;
    for d in (0..num_survivors).rev() {
        items[dest + d] = items[begin + offset + 2 * d];
    }
    num_survivors
}

/// Merges two contiguous sorted runs in `slice` using `f64::total_cmp`.
/// `slice[..left_len]` is the first sorted run, `slice[left_len..]` is the
/// second.  `buf` is a reusable scratch buffer.
#[inline]
fn merge_sorted_runs(slice: &mut [f64], left_len: usize, buf: &mut Vec<f64>) {
    let total = slice.len();
    if left_len == 0 || left_len >= total {
        return;
    }
    if slice[left_len - 1].total_cmp(&slice[left_len]).is_le() {
        return;
    }

    let right_len = total - left_len;
    buf.clear();

    if left_len <= right_len {
        buf.extend_from_slice(&slice[..left_len]);
        let mut i = 0;
        let mut j = left_len;
        let mut k = 0;
        while i < buf.len() && j < total {
            if buf[i].total_cmp(&slice[j]).is_le() {
                slice[k] = buf[i];
                i += 1;
            } else {
                slice[k] = slice[j];
                j += 1;
            }
            k += 1;
        }
        if i < buf.len() {
            slice[k..k + (buf.len() - i)].copy_from_slice(&buf[i..]);
        }
    } else {
        buf.extend_from_slice(&slice[left_len..]);
        let mut i = left_len;
        let mut j = buf.len();
        let mut k = total;
        while i > 0 && j > 0 {
            k -= 1;
            if buf[j - 1].total_cmp(&slice[i - 1]).is_ge() {
                slice[k] = buf[j - 1];
                j -= 1;
            } else {
                slice[k] = slice[i - 1];
                i -= 1;
            }
        }
        if j > 0 {
            slice[..j].copy_from_slice(&buf[..j]);
        }
    }
}

// ---------------------------------------------------------------------------
// KLL sketch
// ---------------------------------------------------------------------------

/// KLL quantile sketch using a compact, insert-optimized, grow-downward layout.
///
/// Memory layout (grows leftward):
/// ```text
/// items: [ free ← | L0 (unsorted) | L1 | L2 | … | L_top ]
///         0        levels[0]                        levels[num_levels]
/// ```
///
/// `levels[h]` = start of level h.  `levels[h+1] - levels[h]` = size of level h.
#[derive(Clone, Debug)]
pub struct KLL {
    items: Box<[f64]>,
    levels: Box<[usize]>,
    k: usize,
    m: usize,
    num_levels: usize,
    max_capacity: usize,
    co: Coin,
    capacity_cache: [u32; CAPACITY_CACHE_LEN],
    top_height: usize,
    level0_capacity: usize,
    merge_buf: Vec<f64>,
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
        let max_cap = compute_max_capacity(norm_k, norm_m);
        let mut s = Self {
            items: vec![0.0_f64; max_cap].into_boxed_slice(),
            levels: {
                let mut v = vec![0usize; MAX_LEVELS + 1];
                v[0] = max_cap;
                v[1] = max_cap;
                v.into_boxed_slice()
            },
            k: norm_k,
            m: norm_m,
            num_levels: 1,
            max_capacity: max_cap,
            co: Coin::new(),
            capacity_cache: [0; CAPACITY_CACHE_LEN],
            top_height: 0,
            level0_capacity: 0,
            merge_buf: Vec::with_capacity(norm_k),
        };
        s.rebuild_capacity_cache();
        s
    }

    /// Creates a KLL sketch with default `m` and the provided `k`.
    pub fn init_kll(k: i32) -> Self {
        Self::init(k as usize, 8)
    }

    /// Hot-path insert: decrement `levels[0]`, write item, check capacity.
    #[inline]
    fn push_value(&mut self, value: f64) {
        if self.levels[0] == 0 {
            self.compress_while_updating();
        }
        self.levels[0] -= 1;
        self.items[self.levels[0]] = value;

        if self.levels[1] - self.levels[0] > self.level0_capacity {
            self.compress_while_updating();
        }
    }

    pub fn update(&mut self, val: &SketchInput) -> Result<(), &'static str> {
        let value = sketch_input_to_f64(val)?;
        self.push_value(value);
        Ok(())
    }

    // -- Compaction ----------------------------------------------------------

    fn compress_while_updating(&mut self) {
        let mut h = 0;
        loop {
            let pop = self.level_size(h);
            let cap = self.capacity_for_level(h);
            if pop <= cap {
                break;
            }
            if h + 1 == self.num_levels {
                self.add_new_top_level();
            }
            self.compact(h);
            h += 1;
        }
    }

    fn compact(&mut self, h: usize) {
        let beg = self.levels[h];
        let end = self.levels[h + 1];
        let pop = end - beg;

        if h == 0 {
            self.items[beg..end].sort_unstable_by(f64::total_cmp);
        }

        let offset = usize::from(self.co.toss());
        let num_survivors = randomly_halve_up(&mut self.items, beg, pop, offset);
        let surv_start = beg + pop - num_survivors;

        let pop_above = self.levels[h + 2] - end;
        if pop_above > 0 {
            merge_sorted_runs(
                &mut self.items[surv_start..end + pop_above],
                num_survivors,
                &mut self.merge_buf,
            );
        }

        let delta = surv_start - beg;
        if delta > 0 && h > 0 {
            let lo = self.levels[0];
            let hi = beg;
            if hi > lo {
                self.items.copy_within(lo..hi, lo + delta);
            }
            for lvl in self.levels[..h].iter_mut() {
                *lvl += delta;
            }
        }

        self.levels[h] = surv_start;
        self.levels[h + 1] = surv_start;
    }

    fn add_new_top_level(&mut self) {
        let sentinel = self.levels[self.num_levels];
        self.num_levels += 1;
        self.levels[self.num_levels] = sentinel;
        self.top_height = self.num_levels - 1;
        self.level0_capacity = self.capacity_for_level(0);
    }

    // -- Capacity helpers ----------------------------------------------------

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
        self.levels[h + 1] - self.levels[h]
    }

    // -- Query-side ----------------------------------------------------------

    pub fn cdf(&self) -> Cdf {
        let mut cdf = Cdf {
            entries: Vector1D::init(self.buffer_size()),
        };
        let mut total_w = 0usize;

        for h in 0..self.num_levels {
            let start = self.levels[h];
            let end = self.levels[h + 1];
            let weight = 1 << h;
            for &value in &self.items[start..end] {
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

    pub fn merge(&mut self, other: &KLL) {
        let used_start = other.levels[0];
        let used_end = other.levels[other.num_levels];
        for &value in &other.items[used_start..used_end] {
            self.push_value(value);
        }
    }

    pub fn quantile(&self, q: f64) -> f64 {
        let cdf = self.cdf();
        cdf.query(q)
    }

    pub fn rank(&self, x: f64) -> usize {
        let mut r = 0;
        for h in 0..self.num_levels {
            let start = self.levels[h];
            let end = self.levels[h + 1];
            let weight = 1 << h;
            for &val in &self.items[start..end] {
                if val <= x {
                    r += weight;
                }
            }
        }
        r
    }

    pub fn count(&self) -> usize {
        let mut total = 0;
        for h in 0..self.num_levels {
            total += self.level_size(h) * (1 << h);
        }
        total
    }

    fn buffer_size(&self) -> usize {
        self.levels[self.num_levels] - self.levels[0]
    }

    // -- Lifecycle -----------------------------------------------------------

    pub fn clear(&mut self) {
        let mc = self.max_capacity;
        self.levels[0] = mc;
        self.levels[1] = mc;
        self.num_levels = 1;
        self.co = Coin::new();
        self.rebuild_capacity_cache();
    }

    pub fn print_compactors(&self) {
        println!(
            "KLL Packed (k={}, levels={}, items={})",
            self.k,
            self.num_levels,
            self.buffer_size()
        );
        for h in (0..self.num_levels).rev() {
            let start = self.levels[h];
            let end = self.levels[h + 1];
            println!("  L{}: {:?}", h, &self.items[start..end]);
        }
    }

    // -- Serialization -------------------------------------------------------

    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        rmp_serde::to_vec(self)
    }

    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        rmp_serde::from_slice(bytes)
    }

    fn ensure_levels_sorted(&mut self) {
        if self.num_levels <= 1 {
            return;
        }
        for h in 1..self.num_levels {
            let s = self.levels[h];
            let e = self.levels[h + 1];
            if s < e {
                self.items[s..e].sort_unstable_by(f64::total_cmp);
            }
        }
    }
}

/// Wire format for serialization (only the used portion of the buffer).
#[derive(Serialize, Deserialize)]
struct KLLWire {
    items: Vec<f64>,
    levels: Vec<usize>,
    k: usize,
    m: usize,
    num_levels: usize,
    co: Coin,
}

impl Serialize for KLL {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let used_start = self.levels[0];
        let used_end = self.levels[self.num_levels];
        let wire = KLLWire {
            items: self.items[used_start..used_end].to_vec(),
            levels: self.levels[..=self.num_levels].iter().map(|&l| l - used_start).collect(),
            k: self.k,
            m: self.m,
            num_levels: self.num_levels,
            co: self.co.clone(),
        };
        wire.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for KLL {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = KLLWire::deserialize(deserializer)?;
        let max_cap = compute_max_capacity(wire.k, wire.m);
        let used_len = wire.items.len();
        let offset = max_cap - used_len;

        let mut items = vec![0.0_f64; max_cap].into_boxed_slice();
        items[offset..offset + used_len].copy_from_slice(&wire.items);

        let mut levels = vec![0usize; MAX_LEVELS + 1].into_boxed_slice();
        for (i, &l) in wire.levels.iter().enumerate() {
            levels[i] = l + offset;
        }

        let mut sketch = KLL {
            items,
            levels,
            k: wire.k,
            m: wire.m,
            num_levels: wire.num_levels,
            max_capacity: max_cap,
            co: wire.co,
            capacity_cache: [0; CAPACITY_CACHE_LEN],
            top_height: 0,
            level0_capacity: 0,
            merge_buf: Vec::with_capacity(wire.k),
        };
        sketch.rebuild_capacity_cache();
        sketch.ensure_levels_sorted();
        Ok(sketch)
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
            sketch.levels, restored.levels,
            "level boundaries changed after round-trip"
        );

        let s_start = sketch.levels[0];
        let s_end = sketch.levels[sketch.num_levels];
        let r_start = restored.levels[0];
        let r_end = restored.levels[restored.num_levels];
        assert_eq!(
            &sketch.items[s_start..s_end],
            &restored.items[r_start..r_end],
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
