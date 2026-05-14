//! Lazily-built table of `ln(1 - u)` draws used to amortise geometric-skip
//! sampling.
//!
//! Storing this as a [`LazyLock`] instead of a 64K-entry literal array keeps
//! the crate small enough to publish to crates.io while preserving the same
//! `PRECOMPUTED_SAMPLE[i]` access pattern. The table is generated once per
//! process from a [`SmallRng`] with a fixed seed, so values are stable
//! across runs and across versions of this crate.

use std::sync::LazyLock;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

/// Number of precomputed entries. `PRECOMPUTED_SAMPLE[i]` is defined for
/// `i` in `0..PRECOMPUTED_SAMPLE_LEN`.
pub const PRECOMPUTED_SAMPLE_LEN: usize = 0x10000;

/// Fixed seed used to make the table reproducible across runs / versions.
const SEED: u64 = 0xA5A0_5A71_B11B_C0DE_u64;

/// Precomputes part of geometric sampling: `ln(1 - u)` with
/// `u` drawn uniformly from the open interval `(0, 1)`.
///
/// The table is materialised lazily on first access through [`LazyLock`].
/// Indexing (`PRECOMPUTED_SAMPLE[i]`), iteration (`PRECOMPUTED_SAMPLE.iter()`),
/// and length (`PRECOMPUTED_SAMPLE.len()`) all work via `Deref` to the
/// underlying slice.
pub static PRECOMPUTED_SAMPLE: LazyLock<Box<[f64]>> =
    LazyLock::new(|| build_ln_one_minus_u_table(1.0));

/// Builds a length-`PRECOMPUTED_SAMPLE_LEN` boxed slice whose i-th entry is
/// `ln(1 - u_i) * scale`, where `u_i ∈ (0, 1)` is drawn from a `SmallRng`
/// seeded with [`SEED`].
///
/// Shared by [`PRECOMPUTED_SAMPLE`] and
/// [`super::precompute_sample2::PRECOMPUTED_SAMPLE_RATE_1PERCENT`].
pub(super) fn build_ln_one_minus_u_table(scale: f64) -> Box<[f64]> {
    let mut generator = SmallRng::seed_from_u64(SEED);
    (0..PRECOMPUTED_SAMPLE_LEN)
        .map(|_| {
            let k = loop {
                let r: f64 = generator.random::<f64>();
                if r != 0.0 && r != 1.0 {
                    break r;
                }
            };
            (1.0 - k).ln() * scale
        })
        .collect()
}
