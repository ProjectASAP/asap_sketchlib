//! Input data generation for the E2E suites.
//!
//! Every generator is seeded and deterministic so failures reproduce exactly.
//! A suite that needs a stream calls one function here and gets the whole
//! dataset back; nothing in this file touches a sketch.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use asap_sketchlib::DataInput;

use super::FreqTruth;

// ---------------------------------------------------------------------------
// Distributions
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
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n).map(|_| rng.random::<u64>() % domain).collect()
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

// ---------------------------------------------------------------------------
// Streams paired with their exact ground truth
// ---------------------------------------------------------------------------

/// Exact per-key counts for an integer-keyed stream.
pub fn freq_truth(stream: &[i64]) -> FreqTruth {
    let mut truth = FreqTruth::default();
    for key in stream {
        truth.observe(*key);
    }
    truth
}

/// A Zipf key stream and the exact counts of the keys in it.
pub fn zipf_stream_with_truth(
    n: usize,
    domain: usize,
    exponent: f64,
    seed: u64,
) -> (Vec<u64>, FreqTruth) {
    let stream = zipf_u64(n, domain, exponent, seed);
    let mut truth = FreqTruth::default();
    for k in &stream {
        truth.observe(*k as i64);
    }
    (stream, truth)
}

// ---------------------------------------------------------------------------
// Frequency / heavy-hitter key streams
// ---------------------------------------------------------------------------

/// Bounded integer draws mapped onto distinct f64 values in `[100, 1000)`,
/// identified by bit pattern so exact counts stay comparable.
pub fn uniform_f64_key(v: u64) -> i64 {
    (100.0 + v as f64 * (900.0 / 4096.0)).to_bits() as i64
}

pub fn f64_input(key: i64) -> DataInput<'static> {
    DataInput::F64(f64::from_bits(key as u64))
}

pub fn u64_input(key: i64) -> DataInput<'static> {
    DataInput::U64(key as u64)
}

/// `u64` keys as the `DataInput` a sketch is fed.
pub fn u64_inputs(keys: &[u64]) -> Vec<DataInput<'static>> {
    keys.iter().copied().map(DataInput::U64).collect()
}

/// A named key stream paired with the `DataInput` constructor for its keys.
pub type BoundStream = (&'static str, Vec<i64>, fn(i64) -> DataInput<'static>);

/// Zipf over `u64` keys and uniform over `f64` keys for one trial, exercising
/// both `DataInput` hashing paths. Each trial uses a disjoint key domain so
/// its collisions are unrelated to the previous trial's.
pub fn bound_streams(trial: u64, n: usize) -> [BoundStream; 2] {
    [
        (
            "zipf/u64",
            zipf_u64(n, 8192, 1.1, 1005 + trial * 977)
                .into_iter()
                .map(|v| v as i64 + (trial as i64) * 100_000)
                .collect(),
            u64_input as fn(i64) -> DataInput<'static>,
        ),
        (
            "uniform/f64",
            uniform_u64(n, 4096, 1006 + trial * 977)
                .into_iter()
                .map(|v| uniform_f64_key(v + trial * 8192))
                .collect(),
            f64_input as fn(i64) -> DataInput<'static>,
        ),
    ]
}

/// Domain of `heavy_hitter_stream`, for the tests that size a summary against
/// it.
pub const HEAVY_HITTER_DOMAIN: usize = 2_048;

/// The skewed key stream the heavy-hitter and conformance suites score their
/// summaries on.
pub fn heavy_hitter_stream() -> Vec<i64> {
    zipf_i64(60_000, HEAVY_HITTER_DOMAIN, 1.1, 9_001)
}

// ---------------------------------------------------------------------------
// Membership key sets
// ---------------------------------------------------------------------------

pub const BLOOM_MEMBERS: i64 = 20_000;
pub const BLOOM_PROBES: i64 = 200_000;

/// The keys inserted into a Bloom filter.
pub fn bloom_members() -> Vec<i64> {
    (0..BLOOM_MEMBERS).collect()
}

/// Keys disjoint from `bloom_members`, for measuring a false-positive rate.
pub fn bloom_probes() -> Vec<i64> {
    (10_000_000..10_000_000 + BLOOM_PROBES).collect()
}

// ---------------------------------------------------------------------------
// Quantile value streams
// ---------------------------------------------------------------------------

/// Named stream shapes with a fixed seed each. Covers the light-tailed,
/// heavy-tailed, tie-dense, sorted and adversarially-ordered cases a
/// compaction scheme can behave differently on.
pub fn rank_streams(trial: usize, n: usize) -> Vec<(&'static str, Vec<f64>)> {
    let s = 0xA5A5_0000u64 + trial as u64 * 7919;
    vec![
        ("uniform", uniform_f64(n, 100_000_000, s)),
        ("normal", normal_f64(n, 1_000.0, 250.0, s + 1)),
        ("zipf", zipf_f64(n, 8_192, 1.1, 1e6, 1e7, s + 2)),
        // Fifty distinct values over tens of thousands of observations: a
        // single value legitimately spans several percent of the rank space,
        // which is exactly where a value-error check would misfire and a
        // rank-interval check must not.
        ("duplicate-heavy", duplicate_heavy_f64(n, 50, s + 3)),
        // Sorted input: every compaction sees a run that is already in global
        // order.
        ("monotonic", monotonic_f64(n, 0.0, 1.0)),
        // Adversarial ordering: the same multiset emitted from both ends
        // inward, so no prefix resembles the whole.
        (
            "outside-in",
            outside_in_ordering(normal_f64(n, 5_000.0, 900.0, s + 4)),
        ),
    ]
}

/// The `rank_streams` shapes plus three the compaction path can behave
/// differently on: two heavy-tailed spreads, and a run short enough that no
/// compactor ever fills.
pub fn bulk_cases(trial: usize, n: usize) -> Vec<(&'static str, Vec<f64>)> {
    let alpha = 0.01;
    let gamma = (1.0 + alpha) / (1.0 - alpha);
    let mut cases = rank_streams(trial, n);
    cases.push(("exponential", exponential_f64(n, 1e-3, 3007)));
    cases.push(("log-uniform", log_uniform_f64(n, gamma, 5..40, 3005)));
    cases.push((
        "sequential-10",
        (0..10).map(|i| i as f64 * 1.7 + 11.0).collect(),
    ));
    cases
}

/// Streams for the relative-error battery. `adversarial` places a fifth of its
/// mass exactly on bucket lower edges, where the mapping's error is at its
/// maximum of exactly `alpha` and one ULP of drift decides the bucket.
pub fn dds_streams(alpha: f64, n: usize, seed: u64) -> Vec<(&'static str, Vec<f64>)> {
    let gamma = (1.0 + alpha) / (1.0 - alpha);
    vec![
        (
            "adversarial-bucket-edges",
            log_uniform_f64(n, gamma, 5..40, seed),
        ),
        (
            "normal",
            normal_f64(n, 1_000.0, 250.0, seed + 1)
                .into_iter()
                .filter(|v| *v > 0.0)
                .collect(),
        ),
        ("exponential", exponential_f64(n, 1e-3, seed + 2)),
        (
            "uniform",
            uniform_u64(n, 9_000_000, seed + 3)
                .into_iter()
                .map(|v| 1_000_000.0 + v as f64)
                .collect(),
        ),
        ("zipf", zipf_f64(n, 8_192, 1.1, 1e6, 1e7, seed + 4)),
        // Nine decades in one stream: the bucket store spans a large index
        // range and the mapping is exercised far from 1.0 in both directions.
        (
            "wide-dynamic-range",
            uniform_u64(n, 1_000_000, seed + 5)
                .into_iter()
                .enumerate()
                .map(|(i, v)| 10f64.powi((i % 10) as i32 - 4) * (1.0 + v as f64 / 1_000_000.0))
                .collect(),
        ),
    ]
}

/// The three mass regimes the ordered-query bound is scored on: a diffuse
/// stream with no heavy head, a sharp Zipf head, and a heavy head over a broad
/// diffuse tail.
pub fn univmonq_ordered_regimes() -> Vec<(&'static str, Vec<f64>)> {
    let diffuse = uniform_f64(200_000, 10_000_000, 0x0DDE_0001);
    let heavy = zipf_f64(200_000, 4_096, 1.4, 1.0, 1e6, 0x0DDE_0002);
    let mut mixed = zipf_f64(60_000, 64, 1.6, 1.0, 1e3, 0x0DDE_0003);
    mixed.extend(
        uniform_u64(140_000, 5_000_000, 0x0DDE_0004)
            .into_iter()
            .map(|v| 1e4 + v as f64),
    );
    vec![("diffuse", diffuse), ("heavy", heavy), ("mixed", mixed)]
}

// ---------------------------------------------------------------------------
// Multi-column labelled streams (Hydra)
// ---------------------------------------------------------------------------

/// `src_region` and `dst_region` share `REGIONS`, so `{src = eu-west}` and
/// `{dst = eu-west}` are distinct subpopulations over an identical value.
pub const SCHEMA: [&str; 3] = ["src_region", "dst_region", "status"];
pub const REGIONS: [&str; 4] = ["eu-west", "us-east", "apac", "sa-east"];
pub const STATUSES: [&str; 3] = ["200", "404", "500"];
pub const ENDPOINTS: [&str; 4] = ["/login", "/checkout", "/query", "/asset"];

/// One stream row: a full-width key plus the value the counters measure.
pub struct Record {
    pub key: [&'static str; 3],
    pub endpoint: &'static str,
}

/// Skewed traffic over four independently seeded columns. Consumes `seed`
/// through `seed + 3`, so call sites space their seeds by at least four.
pub fn labelled_stream(n: usize, seed: u64) -> Vec<Record> {
    let src = zipf_u64(n, REGIONS.len(), 0.8, seed);
    let dst = zipf_u64(n, REGIONS.len(), 0.5, seed + 1);
    let statuses = zipf_u64(n, STATUSES.len(), 1.2, seed + 2);
    let endpoints = zipf_u64(n, ENDPOINTS.len(), 0.4, seed + 3);
    (0..n)
        .map(|i| Record {
            key: [
                REGIONS[src[i] as usize],
                REGIONS[dst[i] as usize],
                STATUSES[statuses[i] as usize],
            ],
            endpoint: ENDPOINTS[endpoints[i] as usize],
        })
        .collect()
}

/// Two key columns over 2-value domains: 4 singles and 4 pairs = 8 subkeys,
/// sparse against `col_num`.
pub const H2_REGIONS: [&str; 2] = ["eu-west", "us-east"];
pub const H2_SERVICES: [&str; 2] = ["auth", "cart"];

pub fn h2_keys(n: usize, seed: u64) -> Vec<(&'static str, &'static str)> {
    let regions = zipf_u64(n, H2_REGIONS.len(), 0.6, seed);
    let services = zipf_u64(n, H2_SERVICES.len(), 0.6, seed + 1);
    (0..n)
        .map(|i| {
            (
                H2_REGIONS[regions[i] as usize],
                H2_SERVICES[services[i] as usize],
            )
        })
        .collect()
}
