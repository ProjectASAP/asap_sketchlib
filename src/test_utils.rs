//! Common Functionality for Unit Tests
//! Generate a atream of f64 under Zipf Distribution

#![cfg(test)]

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
