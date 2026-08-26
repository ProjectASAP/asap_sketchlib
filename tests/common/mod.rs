//! Shared synthetic data generation, ground-truth tracking, and assertion
//! helpers for the E2E sketch test suites.
//!
//! All generators are seeded and deterministic so failures reproduce exactly.
//! Ground truth is tracked exactly while the stream is generated, then used
//! to assert sketch outputs against theory-based tolerances.

// Which helpers are "used" varies by feature flags and per-suite coverage.
#![allow(dead_code)]

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Stream generators
// ---------------------------------------------------------------------------

/// `n` draws from Zipf(s) over `[0, domain)`.
pub fn zipf_u64(n: usize, domain: usize, exponent: f64, seed: u64) -> Vec<u64> {
    let mut cdf: Vec<f64> = (0..domain)
        .map(|i| 1.0 / (i as f64 + 1.0).powf(exponent))
        .collect();
    for i in 1..cdf.len() {
        cdf[i] += cdf[i - 1];
    }
    let total = cdf[domain - 1];
    for x in cdf.iter_mut() {
        *x /= total;
    }
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n)
        .map(|_| {
            // Draw the variate ONCE; comparing inside the binary-search
            // closure would use a fresh random per probe and corrupt results.
            let u: f64 = rng.random();
            match cdf.binary_search_by(|p| p.partial_cmp(&u).unwrap()) {
                Ok(i) | Err(i) => (i as u64).min(domain as u64 - 1),
            }
        })
        .collect()
}

/// `n` draws from Uniform{[0, domain)}.
pub fn uniform_u64(n: usize, domain: u64, seed: u64) -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n).map(|_| rng.random::<u64>() % domain).collect()
}

/// `n` iid Normal(mean, std) samples (Box-Muller).
pub fn normal_f64(n: usize, mean: f64, std: f64, seed: u64) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n)
        .map(|_| {
            let u1: f64 = rng.random::<f64>().max(1e-12);
            let u2: f64 = rng.random::<f64>();
            mean + std * (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
        })
        .collect()
}

/// `n` iid Exponential(lambda) samples.
pub fn exponential_f64(n: usize, lambda: f64, seed: u64) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n)
        .map(|_| -rng.random::<f64>().max(1e-12).ln() / lambda)
        .collect()
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

// ---------------------------------------------------------------------------
// Ground truth trackers
// ---------------------------------------------------------------------------

/// Exact frequency-vector ground truth for integer-keyed streams.
#[derive(Default)]
pub struct FreqTruth {
    counts: HashMap<i64, i64>,
}

impl FreqTruth {
    pub fn observe(&mut self, key: i64) {
        *self.counts.entry(key).or_insert(0) += 1;
    }

    pub fn observe_weighted(&mut self, key: i64, weight: i64) {
        *self.counts.entry(key).or_insert(0) += weight;
    }

    pub fn get(&self, key: i64) -> i64 {
        self.counts.get(&key).copied().unwrap_or(0)
    }

    pub fn total(&self) -> i64 {
        self.counts.values().sum()
    }

    pub fn distinct(&self) -> usize {
        self.counts.len()
    }

    /// Exact F2 = Σ count².
    pub fn f2(&self) -> f64 {
        self.counts
            .values()
            .map(|c| (*c as f64) * (*c as f64))
            .sum()
    }

    /// Exact L2 norm.
    pub fn l2_norm(&self) -> f64 {
        self.f2().sqrt()
    }

    /// Exact Shannon entropy of the empirical distribution.
    pub fn entropy(&self, base_bits: bool) -> f64 {
        let total = self.total() as f64;
        -self
            .counts
            .values()
            .filter(|c| **c > 0)
            .map(|c| {
                let p = *c as f64 / total;
                p * if base_bits { p.log2() } else { p.ln() }
            })
            .sum::<f64>()
    }

    /// True top-k by count, descending.
    pub fn top_k(&self, k: usize) -> Vec<(i64, i64)> {
        let mut v: Vec<(i64, i64)> = self.counts.iter().map(|(a, b)| (*a, *b)).collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        v.truncate(k);
        v
    }

    pub fn pairs(&self) -> Vec<(i64, i64)> {
        self.counts.iter().map(|(a, b)| (*a, *b)).collect()
    }
}

/// Exact order-statistic ground truth for numeric streams.
pub struct NumericTruth {
    sorted: Vec<f64>,
}

impl NumericTruth {
    pub fn new(mut values: Vec<f64>) -> Self {
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        Self { sorted: values }
    }

    pub fn len(&self) -> usize {
        self.sorted.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sorted.is_empty()
    }

    pub fn min(&self) -> f64 {
        self.sorted[0]
    }

    pub fn max(&self) -> f64 {
        self.sorted[self.sorted.len() - 1]
    }

    /// Nearest-rank quantile: smallest value with CDF >= q (ceil convention).
    pub fn quantile(&self, q: f64) -> f64 {
        let n = self.sorted.len();
        let idx = ((q.clamp(0.0, 1.0) * n as f64).ceil() as usize).clamp(1, n);
        self.sorted[idx - 1]
    }

    /// Empirical share of observations <= x.
    pub fn cdf(&self, x: f64) -> f64 {
        self.sorted.iter().filter(|v| **v <= x).count() as f64 / self.sorted.len() as f64
    }

    /// Allowed value band for a quantile query with rank tolerance `tol`.
    pub fn quantile_band(&self, q: f64, tol: f64) -> (f64, f64) {
        (
            self.quantile((q - tol).clamp(0.0, 1.0)),
            self.quantile((q + tol).clamp(0.0, 1.0)),
        )
    }
}

// ---------------------------------------------------------------------------
// Assertion helpers (panic with expected-vs-actual context)
// ---------------------------------------------------------------------------

pub fn assert_rel_close(actual: f64, expected: f64, rel_tol: f64, label: &str) {
    let rel = if expected == 0.0 {
        actual.abs()
    } else {
        ((actual - expected) / expected).abs()
    };
    assert!(
        rel <= rel_tol,
        "{label}: expected ~{expected:.6}, got {actual:.6} (rel err {rel:.5} > tol {rel_tol})"
    );
}

pub fn assert_between(actual: f64, lo: f64, hi: f64, label: &str) {
    assert!(
        actual >= lo && actual <= hi,
        "{label}: got {actual:.6}, outside allowed band [{lo:.6}, {hi:.6}]"
    );
}

/// Quantile query must land inside the truth's rank-tolerance value band.
pub fn assert_in_rank_band(est: f64, truth: &NumericTruth, q: f64, tol: f64, label: &str) {
    let (lo, hi) = truth.quantile_band(q, tol);
    assert!(
        est >= lo && est <= hi,
        "{label}: q={q} estimate {est:.4} outside rank band [{lo:.4}, {hi:.4}]"
    );
}
