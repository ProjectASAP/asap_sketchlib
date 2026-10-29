//! Shared helpers of the analytics tests.

#![allow(dead_code)]

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// Failures allowed out of `trials` at a true rate of at most `delta`: the
/// mean plus four standard deviations, plus one.
pub fn allowed_failures(trials: usize, delta: f64) -> usize {
    let n = trials as f64;
    (n * delta + 4.0 * (n * delta * (1.0 - delta)).sqrt() + 1.0).floor() as usize
}

/// A seeded generator for trial `trial` of a test named by `salt`.
pub fn rng(salt: u64, trial: u64) -> StdRng {
    StdRng::seed_from_u64(salt.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ trial)
}

/// `distinct` fresh random keys, each with a Zipf-like weight `⌈s/rank⌉`, so
/// every trial hashes a different key set.
pub fn zipf_keys(rng: &mut StdRng, distinct: usize, scale: u64) -> Vec<(u64, u64)> {
    (0..distinct)
        .map(|rank| (rng.random(), scale.div_ceil(rank as u64 + 1)))
        .collect()
}

/// A Zipf head of `head` keys at scale `scale` over `tail` fresh unit-weight
/// keys, so collisions carry real mass.
pub fn mixed_keys(rng: &mut StdRng, head: usize, scale: u64, tail: usize) -> Vec<(u64, u64)> {
    let mut keys = zipf_keys(rng, head, scale);
    keys.extend((0..tail).map(|_| (rng.random(), 1)));
    keys
}

/// `N`, the total weight of `keys`.
pub fn l1(keys: &[(u64, u64)]) -> f64 {
    keys.iter().map(|&(_, w)| w as f64).sum()
}

/// `‖f‖₂` of `keys`.
pub fn l2(keys: &[(u64, u64)]) -> f64 {
    keys.iter()
        .map(|&(_, w)| (w as f64).powi(2))
        .sum::<f64>()
        .sqrt()
}

/// Asserts the ops `f` performs are at most `bound`.
#[cfg(feature = "op-count")]
#[track_caller]
pub fn assert_ops_within<R>(what: &str, bound: f64, f: impl FnOnce() -> R) -> R {
    let (result, ops) = asap_sketchlib::analytics::ops::count(f);
    assert!(
        ops as f64 <= bound,
        "{what}: counted {ops} ops, formula says at most {bound}"
    );
    result
}
