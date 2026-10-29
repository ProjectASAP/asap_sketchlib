//! Theoretical error, memory and op counts per sketch, as
//! `docs/sketch_analytics.md` defines them.
//!
//! Each sketch type implements [`SketchAnalytics`]. A caller with parameters
//! calls its functions directly, and a caller with a target `(ε, δ)` first
//! calls [`SketchAnalytics::params_for`].

pub mod cardinality;
pub mod count_matrix;
pub mod heavy_hitters;
pub mod ops;
pub mod top_k;

pub use crate::capability::StatisticKind;
pub use cardinality::{BloomParams, DistinctData, HllData, HllParams, KmvParams, SetData};
pub use count_matrix::MatrixParams;
#[cfg(feature = "experimental")]
pub use count_matrix::{CountMinHllParams, MicroCmData, MicroCmParams};
pub use heavy_hitters::{CocoParams, ElasticParams, FlowData, SpaceSavingData, SpaceSavingParams};
pub use top_k::{FoldData, FoldParams, MatrixHeapParams, MatrixSpaceSavingParams};

use crate::{FastPath, RegularPath};

/// Whether a matrix sketch's hash path is `FastPath`.
pub trait HashPath {
    const FAST: bool;
}

impl HashPath for RegularPath {
    const FAST: bool = false;
}

impl HashPath for FastPath {
    const FAST: bool = true;
}

/// The `δ` a contract whose `δ` the caller chooses uses when
/// [`SketchAnalytics::theoretical_error`] gets `delta = None`.
pub const DEFAULT_DELTA: f64 = 0.01;

/// The stream norm an [`ErrorMetric::Absolute`] error is stated in multiples of.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NormScale {
    /// `N`, the total update weight.
    StreamL1,
    /// `‖f‖₂`, the square root of the sum of squared key weights.
    StreamL2,
}

/// The error quantity an [`ErrorBound`] bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorMetric {
    /// Off by at most `ε`, or `ε` times the stream norm `scale`.
    Absolute { scale: Option<NormScale> },
    /// Off by at most `ε` times the true value.
    Relative,
    /// The returned value's rank is within `ε·n` of the requested rank.
    Rank,
}

impl ErrorMetric {
    /// `Absolute { scale: Some(StreamL1) }`.
    pub const L1: Self = Self::Absolute {
        scale: Some(NormScale::StreamL1),
    };
    /// `Absolute { scale: Some(StreamL2) }`.
    pub const L2: Self = Self::Absolute {
        scale: Some(NormScale::StreamL2),
    };
    /// `Absolute { scale: None }`.
    pub const ABSOLUTE: Self = Self::Absolute { scale: None };
}

/// With probability at least `1 − δ`, one estimate from one state is off by
/// at most `ε` in `metric`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ErrorBound {
    pub metric: ErrorMetric,
    pub epsilon: f64,
    /// `None` when the analysis gives only a standard error.
    pub delta: Option<f64>,
    /// The analysis, named as ASAPPlanner names it where it has the same one.
    pub contract: &'static str,
}

impl ErrorBound {
    /// An exact readout.
    pub const EXACT: Self = Self {
        metric: ErrorMetric::ABSOLUTE,
        epsilon: 0.0,
        delta: Some(0.0),
        contract: "exact_v1",
    };
}

/// Theoretical error, memory and ops of one sketch type.
///
/// `Data` holds the data inputs a sketch's formulas read, each an `Option`.
/// When one is unknown, memory and ops use its upper bound, and
/// [`Self::theoretical_error`] and [`Self::params_for`] return `None` if they
/// need it.
pub trait SketchAnalytics {
    type Params;
    type Data: Default;

    /// Bytes a state with `params` holds after the updates `data` describes.
    fn theoretical_memory_bytes(params: &Self::Params, data: &Self::Data) -> usize;

    /// Most ops one plain update costs, or its average where the sketch's
    /// section marks it average.
    fn theoretical_update_ops(params: &Self::Params, data: &Self::Data) -> f64;

    /// Most ops one query of `statistic` costs; `None` when the sketch does
    /// not answer it.
    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &Self::Params,
        data: &Self::Data,
    ) -> Option<f64>;

    /// Most ops merging one state into another costs; `None` when the sketch
    /// does not merge.
    fn theoretical_merge_ops(params: &Self::Params, data: &Self::Data) -> Option<f64>;

    /// The error of one `statistic` estimate.
    ///
    /// For a contract with a fixed `δ`, `delta = None` or a `delta` at or
    /// above it returns that `δ`, and a `delta` below it returns `None`. For a
    /// contract whose `δ` the caller chooses, it returns the smallest `ε` at
    /// `delta`, or at [`DEFAULT_DELTA`] when `delta` is `None`. `None` when
    /// the statistic has no bound.
    fn theoretical_error(
        statistic: StatisticKind,
        params: &Self::Params,
        data: &Self::Data,
        delta: Option<f64>,
    ) -> Option<ErrorBound>;

    /// Parameters that meet `(epsilon, delta)` for `statistic`; `None` when
    /// no parameters the constructor accepts can.
    fn params_for(
        statistic: StatisticKind,
        epsilon: f64,
        delta: f64,
        data: &Self::Data,
    ) -> Option<Self::Params>;
}

/// `⌈log2 x⌉`, and 0 for `x ≤ 1`.
pub fn lg(x: f64) -> f64 {
    if x <= 1.0 { 0.0 } else { x.log2().ceil() }
}

/// `⌊log2 k⌋`, and 0 for `k ≤ 1`.
pub fn floor_log2(k: usize) -> f64 {
    if k <= 1 { 0.0 } else { k.ilog2() as f64 }
}

/// `p·lg p`, the cost of sorting `p` items or taking their median.
pub fn sort_ops(p: f64) -> f64 {
    p * lg(p)
}

/// Ops a Count-Min estimate over `d` rows costs: the minimum of `d` counters.
pub fn est_count_min(d: usize) -> f64 {
    d as f64
}

/// Ops a Count Sketch estimate over `d` rows costs: copy `d` counters into a
/// buffer and take their median.
pub fn est_count_sketch(d: usize) -> f64 {
    2.0 * d as f64 + sort_ops(d as f64)
}

/// `h(k)`, the most ops one `TopKHeap` update costs at capacity `k`.
pub fn heap_update_ops(k: usize) -> f64 {
    8.0 + 14.0 * floor_log2(k)
}

/// `P_med(d, β) = P(Binomial(d, β) ≥ ⌈d/2⌉)`: the chance a median over `d`
/// rows, each wrong independently with probability `β`, is wrong.
pub fn p_med(d: usize, beta: f64) -> f64 {
    if d == 0 {
        return 1.0;
    }
    let beta = beta.clamp(0.0, 1.0);
    let need = d.div_ceil(2);
    let mut total = 0.0;
    for j in need..=d {
        total += (ln_choose(d, j) + j as f64 * beta.ln() + (d - j) as f64 * (-beta).ln_1p()).exp();
    }
    total.min(1.0)
}

/// The largest per-row failure probability `β` with `P_med(d, β) ≤ delta`.
pub fn max_row_failure(d: usize, delta: f64) -> f64 {
    if delta >= 1.0 {
        return 1.0;
    }
    if delta <= 0.0 {
        return 0.0;
    }
    let (mut lo, mut hi) = (0.0f64, 1.0f64);
    for _ in 0..200 {
        let mid = 0.5 * (lo + hi);
        if p_med(d, mid) <= delta {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    lo
}

fn ln_choose(n: usize, k: usize) -> f64 {
    let k = k.min(n - k);
    (0..k).map(|i| ((n - i) as f64 / (i + 1) as f64).ln()).sum()
}

/// The standard normal quantile `z(p)` for `p` in (0, 1).
pub fn normal_quantile(p: f64) -> f64 {
    let (mut lo, mut hi) = (-40.0f64, 40.0f64);
    for _ in 0..200 {
        let mid = 0.5 * (lo + hi);
        if normal_cdf(mid) < p {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    0.5 * (lo + hi)
}

fn normal_cdf(x: f64) -> f64 {
    0.5 * erfc(-x / std::f64::consts::SQRT_2)
}

// Numerical Recipes' erfc, relative error below 1.2e-7.
fn erfc(x: f64) -> f64 {
    let z = x.abs();
    let t = 1.0 / (1.0 + 0.5 * z);
    let poly = -z * z - 1.265_512_23
        + t * (1.000_023_68
            + t * (0.374_091_96
                + t * (0.096_784_18
                    + t * (-0.186_288_06
                        + t * (0.278_868_07
                            + t * (-1.135_203_98
                                + t * (1.488_515_87 + t * (-0.822_152_23 + t * 0.170_872_77))))))));
    let r = t * poly.exp();
    if x >= 0.0 { r } else { 2.0 - r }
}

/// Resolves a requested `delta` against a contract's fixed `δ`: the fixed
/// `δ` when `delta` is `None` or at least it, `None` when below it.
pub fn fixed_delta(fixed: f64, delta: Option<f64>) -> Option<f64> {
    match delta {
        Some(requested) if requested < fixed => None,
        _ => Some(fixed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p_med_matches_hand_computed_binomial_tails() {
        assert_eq!(p_med(1, 0.25), 0.25);
        // P(Bin(3, 0.1) ≥ 2) = 3·0.01·0.9 + 0.001.
        assert!((p_med(3, 0.1) - 0.028).abs() < 1e-12);
        // Even d: P(Bin(2, 0.5) ≥ 1) = 0.75.
        assert!((p_med(2, 0.5) - 0.75).abs() < 1e-12);
    }

    #[test]
    fn max_row_failure_inverts_p_med() {
        for d in [1, 3, 5, 9, 19] {
            for delta in [0.1, 0.01, 1e-4] {
                let beta = max_row_failure(d, delta);
                assert!(p_med(d, beta) <= delta);
                assert!(p_med(d, beta * 1.001) > delta);
            }
        }
    }

    #[test]
    fn normal_quantile_hits_known_values() {
        assert!((normal_quantile(0.995) - 2.575_829).abs() < 1e-5);
        assert!((normal_quantile(0.975) - 1.959_964).abs() < 1e-5);
        assert!(normal_quantile(0.5).abs() < 1e-6);
    }

    #[test]
    fn lg_and_floor_log2_round_the_documented_ways() {
        assert_eq!(lg(1.0), 0.0);
        assert_eq!(lg(5.0), 3.0);
        assert_eq!(lg(8.0), 3.0);
        assert_eq!(floor_log2(1), 0.0);
        assert_eq!(floor_log2(5), 2.0);
        assert_eq!(heap_update_ops(8), 50.0);
    }
}
