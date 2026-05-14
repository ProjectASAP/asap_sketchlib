//! Lazily-built table of geometric-skip distances for a 1% sampling rate.
//!
//! Storing this as a [`LazyLock`] instead of a 64K-entry literal array keeps
//! the crate small enough to publish to crates.io while preserving the same
//! `PRECOMPUTED_SAMPLE_RATE_1PERCENT[i]` access pattern. The table is
//! generated once per process from a [`rand::rngs::SmallRng`] with the same
//! fixed seed as [`super::precompute_sample::PRECOMPUTED_SAMPLE`], so values
//! are stable across runs and across versions of this crate.

use std::sync::LazyLock;

use super::precompute_sample::build_ln_one_minus_u_table;

/// Precomputed geometric-skip distances for sampling-rate `p = 0.01`, i.e.
/// `ln(1 - u) / ln(1 - p) = ln(1 - u) / ln(0.99)` for `u ∈ (0, 1)`.
///
/// The table is materialised lazily on first access through [`LazyLock`].
/// Indexing (`PRECOMPUTED_SAMPLE_RATE_1PERCENT[i]`),
/// iteration (`PRECOMPUTED_SAMPLE_RATE_1PERCENT.iter()`), and length
/// (`PRECOMPUTED_SAMPLE_RATE_1PERCENT.len()`) all work via `Deref` to the
/// underlying slice.
pub static PRECOMPUTED_SAMPLE_RATE_1PERCENT: LazyLock<Box<[f64]>> =
    LazyLock::new(|| build_ln_one_minus_u_table(1.0 / 0.99_f64.ln()));
