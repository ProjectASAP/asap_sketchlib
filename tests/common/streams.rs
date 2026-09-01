//! Input data generation for the E2E suites.
//!
//! Every generator is seeded and deterministic so failures reproduce exactly.
//! A suite that needs a stream calls one function here and gets the whole
//! dataset back; nothing in this file touches a sketch.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

pub struct ZipfConfig {
    pub count: usize,
    pub domain: usize,
    pub exponent: f64,
    pub seed: u64,
}

pub struct UniformConfig {
    pub count: usize,
    pub domain: u64,
    pub seed: u64,
}

pub struct NormalConfig {
    pub count: usize,
    pub mean: f64,
    pub std_dev: f64,
    pub seed: u64,
}

pub struct ExponentialConfig {
    pub count: usize,
    pub lambda: f64,
    pub seed: u64,
}

pub trait DiscreteValue: Sized {
    fn from_u64(value: u64) -> Self;
}

macro_rules! discrete_value {
    ($($ty:ty),+ $(,)?) => { $(
        impl DiscreteValue for $ty {
            fn from_u64(value: u64) -> Self { value as Self }
        }
    )+ };
}

discrete_value!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64
);

pub trait FloatValue: Sized {
    fn from_f64(value: f64) -> Self;
}

impl FloatValue for f32 {
    fn from_f64(value: f64) -> Self {
        value as f32
    }
}

impl FloatValue for f64 {
    fn from_f64(value: f64) -> Self {
        value
    }
}

pub struct ZipfGenerator;

impl ZipfGenerator {
    pub fn generate<T: DiscreteValue>(config: &ZipfConfig) -> Vec<T> {
        let mut cdf: Vec<f64> = (0..config.domain)
            .map(|i| 1.0 / (i as f64 + 1.0).powf(config.exponent))
            .collect();
        for i in 1..cdf.len() {
            cdf[i] += cdf[i - 1];
        }
        let total = cdf[config.domain - 1];
        for value in &mut cdf {
            *value /= total;
        }
        let mut rng = StdRng::seed_from_u64(config.seed);
        (0..config.count)
            .map(|_| {
                let draw: f64 = rng.random();
                let index = match cdf.binary_search_by(|p| p.partial_cmp(&draw).unwrap()) {
                    Ok(index) | Err(index) => index.min(config.domain - 1),
                };
                T::from_u64(index as u64)
            })
            .collect()
    }
}

pub struct UniformGenerator;

impl UniformGenerator {
    pub fn generate<T: DiscreteValue>(config: &UniformConfig) -> Vec<T> {
        let mut rng = StdRng::seed_from_u64(config.seed);
        (0..config.count)
            .map(|_| T::from_u64(rng.random::<u64>() % config.domain))
            .collect()
    }
}

pub struct NormalGenerator;

impl NormalGenerator {
    pub fn generate<T: FloatValue>(config: &NormalConfig) -> Vec<T> {
        let mut rng = StdRng::seed_from_u64(config.seed);
        (0..config.count)
            .map(|_| {
                let u1 = rng.random::<f64>().max(1e-12);
                let u2 = rng.random::<f64>();
                T::from_f64(
                    config.mean
                        + config.std_dev
                            * (-2.0 * u1.ln()).sqrt()
                            * (std::f64::consts::TAU * u2).cos(),
                )
            })
            .collect()
    }
}

pub struct ExponentialGenerator;

impl ExponentialGenerator {
    pub fn generate<T: FloatValue>(config: &ExponentialConfig) -> Vec<T> {
        let mut rng = StdRng::seed_from_u64(config.seed);
        (0..config.count)
            .map(|_| T::from_f64(-rng.random::<f64>().max(1e-12).ln() / config.lambda))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Distributions
// ---------------------------------------------------------------------------

/// `n` draws from Zipf(s) over `[0, domain)`.
pub fn zipf_u64(n: usize, domain: usize, exponent: f64, seed: u64) -> Vec<u64> {
    ZipfGenerator::generate(&ZipfConfig {
        count: n,
        domain,
        exponent,
        seed,
    })
}

/// `zipf_u64` as signed keys, for the suites whose ground truth is keyed on
/// `i64`.
pub fn zipf_i64(n: usize, domain: usize, exponent: f64, seed: u64) -> Vec<i64> {
    zipf_u64(n, domain, exponent, seed)
        .into_iter()
        .map(|v| v as i64)
        .collect()
}

/// `n` draws from Zipf(s) mapped onto a fixed value grid spanning
/// `[lo, hi]`, so a heavy-tailed key distribution becomes a heavy-tailed
/// *value* distribution usable by quantile sketches.
pub fn zipf_f64(n: usize, domain: usize, exponent: f64, lo: f64, hi: f64, seed: u64) -> Vec<f64> {
    let step = (hi - lo) / (domain - 1) as f64;
    zipf_u64(n, domain, exponent, seed)
        .into_iter()
        .map(|i| lo + step * i as f64)
        .collect()
}

/// `n` draws from Uniform{[0, domain)}.
pub fn uniform_u64(n: usize, domain: u64, seed: u64) -> Vec<u64> {
    UniformGenerator::generate(&UniformConfig {
        count: n,
        domain,
        seed,
    })
}

/// `uniform_u64` widened to `f64`, which is exact for every `domain` a test
/// uses here.
pub fn uniform_f64(n: usize, domain: u64, seed: u64) -> Vec<f64> {
    uniform_u64(n, domain, seed)
        .into_iter()
        .map(|v| v as f64)
        .collect()
}

/// `n` iid Normal(mean, std) samples (Box-Muller).
pub fn normal_f64(n: usize, mean: f64, std: f64, seed: u64) -> Vec<f64> {
    NormalGenerator::generate(&NormalConfig {
        count: n,
        mean,
        std_dev: std,
        seed,
    })
}

/// `n` iid Exponential(lambda) samples.
pub fn exponential_f64(n: usize, lambda: f64, seed: u64) -> Vec<f64> {
    ExponentialGenerator::generate(&ExponentialConfig {
        count: n,
        lambda,
        seed,
    })
}

/// Log-uniform adversarial values: `gamma^k * (1 + frac*(gamma-1))`, mixing
/// bucket-edge hits (frac ≈ 0) with interior values. Targets DDSketch mappings.
pub fn log_uniform_f64(n: usize, gamma: f64, k_range: std::ops::Range<i32>, seed: u64) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n)
        .map(|_| {
            let k = rng.random_range(k_range.start..k_range.end);
            let frac: f64 = if rng.random::<f64>() < 0.2 {
                1e-9
            } else {
                rng.random()
            };
            gamma.powi(k) * (1.0 + frac * (gamma - 1.0))
        })
        .collect()
}

/// `n` values drawn from a small domain so most values repeat many times.
/// Rank-error checks must survive heavy ties, where one value legitimately
/// spans a wide rank interval.
pub fn duplicate_heavy_f64(n: usize, distinct: usize, seed: u64) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n)
        .map(|_| (rng.random::<u64>() % distinct as u64) as f64)
        .collect()
}

/// Strictly increasing values: the worst case for a compactor that assumes
/// arrivals are unordered.
pub fn monotonic_f64(n: usize, start: f64, step: f64) -> Vec<f64> {
    (0..n).map(|i| start + step * i as f64).collect()
}

/// Adversarial ordering of a fixed multiset: the sorted values are emitted
/// alternately from the two ends inward, so every compaction sees a stream
/// whose local order is maximally unlike its global order.
pub fn outside_in_ordering(mut values: Vec<f64>) -> Vec<f64> {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let (mut lo, mut hi) = (0usize, values.len());
    let mut out = Vec::with_capacity(values.len());
    while lo < hi {
        out.push(values[lo]);
        lo += 1;
        if lo < hi {
            hi -= 1;
            out.push(values[hi]);
        }
    }
    out
}
