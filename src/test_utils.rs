//! Common Functionality for Unit Tests
//! Generate a atream of f64 under Zipf Distribution

use rand::SeedableRng;
use rand::distr::{self, Distribution, Uniform};
use rand::rngs::StdRng;

pub fn sample_uniform_f64(min: f64, max: f64, sample_size: usize, seed: u64) -> Vec<f64> {
    assert!(sample_size > 0, "sample size must be positive");
    assert!(
        max >= min,
        "uniform distribution expects max >= min (got min={min}, max={max})"
    );
    let mut rng = StdRng::seed_from_u64(seed);
    let dist = Uniform::new_inclusive(min, max).expect("uniform bounds should be valid");
    (0..sample_size).map(|_| dist.sample(&mut rng)).collect()
}

fn sample_zipf_indices(domain: usize, exponent: f64, sample_size: usize, seed: u64) -> Vec<usize> {
    assert!(domain > 0, "zipf domain must be positive");
    assert!(sample_size > 0, "sample size must be positive");
    let mut rng = StdRng::seed_from_u64(seed);
    let weights: Vec<f64> = (1..=domain)
        .map(|k| 1.0 / (k as f64).powf(exponent))
        .collect();
    let dist = distr::weighted::WeightedIndex::new(weights).expect("zipf weights should be valid");
    (0..sample_size).map(|_| dist.sample(&mut rng)).collect()
}

pub fn sample_zipf_u64(domain: usize, exponent: f64, sample_size: usize, seed: u64) -> Vec<u64> {
    sample_zipf_indices(domain, exponent, sample_size, seed)
        .into_iter()
        .map(|idx| idx as u64)
        .collect()
}

pub fn sample_zipf_f64(
    min: f64,
    max: f64,
    domain: usize,
    exponent: f64,
    sample_size: usize,
    seed: u64,
) -> Vec<f64> {
    assert!(
        max >= min,
        "zipf distribution expects max >= min (got min={min}, max={max})"
    );
    let step = if domain > 1 {
        (max - min) / (domain as f64 - 1.0)
    } else {
        0.0
    };
    sample_zipf_indices(domain, exponent, sample_size, seed)
        .into_iter()
        .map(|idx| min + step * idx as f64)
        .collect()
}

pub fn sample_normal_f64(mean: f64, std: f64, sample_size: usize, seed: u64) -> Vec<f64> {
    assert!(sample_size > 0, "sample size must be positive");
    assert!(std >= 0.0, "std must be nonnegative");
    let eps = 1e-12;

    // need an even count of uniforms - just using uniform sampler
    let need = ((sample_size + 1) / 2) * 2;
    let us = sample_uniform_f64(eps, 1.0 - eps, need, seed);

    let mut vals = Vec::with_capacity(sample_size);
    let mut i = 0;
    while i < need {
        let u1 = us[i];
        let u2 = us[i + 1];

        // Box–Muller transform
        let r = (-2.0 * u1.ln()).sqrt();
        let theta = 2.0 * std::f64::consts::PI * u2;
        let z0 = r * theta.cos();
        let z1 = r * theta.sin();

        vals.push(mean + std * z0);
        if vals.len() < sample_size {
            vals.push(mean + std * z1);
        }
        i += 2;
    }
    vals
}

pub fn sample_binomial_f64(
    min: f64,
    max: f64,
    trials: usize,
    p: f64,
    sample_size: usize,
    seed: u64,
) -> Vec<f64> {
    assert!(sample_size > 0, "sample size must be positive");
    assert!((0.0..=1.0).contains(&p), "p must be in [0,1]");
    assert!(max >= min, "expects max >= min");

    let need = sample_size
        .checked_mul(trials)
        .expect("sample_size * trials overflow");
    let mut us = sample_uniform_f64(0.0, 1.0, need, seed).into_iter();

    let span = if trials > 0 {
        (max - min) / trials as f64
    } else {
        0.0
    };
    let mut out = Vec::with_capacity(sample_size);

    for _ in 0..sample_size {
        let mut k = 0usize;
        for _ in 0..trials {
            if us.next().unwrap() < p {
                k += 1;
            }
        }
        out.push(min + span * (k as f64));
    }

    out
}

pub fn sample_exponential_f64(lambda: f64, sample_size: usize, seed: u64) -> Vec<f64> {
    assert!(lambda > 0.0, "lambda must be positive");
    assert!(sample_size > 0, "sample size must be positive");

    // generate uniform samples in (0,1)
    let eps = 1e-12; //padding of sorts to avoid log(0)
    let us = sample_uniform_f64(eps, 1.0 - eps, sample_size, seed);

    // Apply inverse-CDF transform: X = -ln(U) / λ
    us.into_iter().map(|u| -u.ln() / lambda).collect() //iterator map and collect
}
