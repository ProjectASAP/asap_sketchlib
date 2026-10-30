//! Analytics of the quantile sketches Kll, KllDynamic and DdSketch, and the
//! experimental UniformSampling.

use super::{
    DEFAULT_DELTA, ErrorBound, ErrorMetric, SketchAnalytics, StatisticKind, fixed_delta,
    is_positive, lg, normal_upper_quantile,
};
use crate::common::numerical::NumericalValue;
use crate::sketches::ddsketch::ddsketch_indexable_bounds;
use crate::sketches::kll::{
    CAPACITY_CACHE_LEN, DEFAULT_K, DEFAULT_M, MAX_K, MAX_LEVELS, capacity_table, levels_after,
    normalized_k_m,
};
use crate::{DdSketch, Kll, KllDynamic};

/// `k` and `m` of a KLL sketch, as its constructor takes them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KllParams {
    pub k: usize,
    pub m: usize,
}

impl Default for KllParams {
    fn default() -> Self {
        Self {
            k: DEFAULT_K as usize,
            m: DEFAULT_M,
        }
    }
}

/// `n`, the updates a state has received.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamData {
    pub n: Option<u64>,
}

impl StreamData {
    fn n(&self) -> Option<usize> {
        self.n.map(|n| usize::try_from(n).unwrap_or(usize::MAX))
    }
}

const KLL_QUANTILE_CONTRACT: &str = "kll_quantile_empirical_99_v1";
const KLL_QUANTILE_SCALED_CONTRACT: &str = "kll_quantile_empirical_99_delta_scaled_v1";
const KLL_RANK_CONTRACT: &str = "kll_rank_measured_99_v1";
const KLL_RANK_SCALED_CONTRACT: &str = "kll_rank_measured_99_delta_scaled_v1";

/// The smallest `m` the KLL fits were measured at.
const KLL_FIT_MIN_M: usize = 8;

/// `ε = c·s(δ)/k^p` of a KLL fit, for `k ≥ min_k`.
#[derive(Clone, Copy)]
struct KllFit {
    constant: f64,
    power: f64,
    min_k: usize,
    contract: &'static str,
    scaled_contract: &'static str,
}

const KLL_QUANTILE_FIT: KllFit = KllFit {
    constant: 2.296,
    power: 0.9723,
    min_k: DEFAULT_M,
    contract: KLL_QUANTILE_CONTRACT,
    scaled_contract: KLL_QUANTILE_SCALED_CONTRACT,
};

/// Measured for `k` from 32 to 4096.
const KLL_RANK_FIT: KllFit = KllFit {
    constant: 2.07,
    power: 0.9646,
    min_k: 32,
    contract: KLL_RANK_CONTRACT,
    scaled_contract: KLL_RANK_SCALED_CONTRACT,
};

/// `s(δ)`: 1 for `δ ≥ 0.01`, and `z(1 − δ/2)/z(1 − 0.005)` below.
pub fn kll_delta_scale(delta: f64) -> f64 {
    if delta >= 0.01 {
        1.0
    } else {
        normal_upper_quantile(delta / 2.0) / normal_upper_quantile(0.005)
    }
}

fn kll_error(
    fit: KllFit,
    metric: ErrorMetric,
    params: &KllParams,
    delta: Option<f64>,
) -> Option<ErrorBound> {
    let (k, m) = normalized_k_m(params.k, params.m);
    let delta = delta.unwrap_or(DEFAULT_DELTA);
    (k >= fit.min_k && m >= KLL_FIT_MIN_M && delta > 0.0 && delta < 1.0).then_some(())?;
    Some(ErrorBound {
        metric,
        epsilon: fit.constant * kll_delta_scale(delta) / (k as f64).powf(fit.power),
        delta: Some(delta),
        contract: if delta < 0.01 {
            fit.scaled_contract
        } else {
            fit.contract
        },
    })
}

fn kll_params_for(fit: KllFit, epsilon: f64, delta: f64) -> Option<KllParams> {
    (epsilon > 0.0 && delta > 0.0 && delta < 1.0).then_some(())?;
    let k = (fit.constant * kll_delta_scale(delta) / epsilon)
        .powf(1.0 / fit.power)
        .ceil();
    (k <= MAX_K as f64).then(|| KllParams {
        k: (k as usize).max(fit.min_k),
        m: DEFAULT_M,
    })
}

fn kll_error_for(
    statistic: StatisticKind,
    params: &KllParams,
    delta: Option<f64>,
) -> Option<ErrorBound> {
    match statistic {
        StatisticKind::Quantile => kll_error(KLL_QUANTILE_FIT, ErrorMetric::Rank, params, delta),
        StatisticKind::Rank => kll_error(KLL_RANK_FIT, ErrorMetric::L1, params, delta),
        _ => None,
    }
}

fn kll_sizing(statistic: StatisticKind, epsilon: f64, delta: f64) -> Option<KllParams> {
    match statistic {
        StatisticKind::Quantile => kll_params_for(KLL_QUANTILE_FIT, epsilon, delta),
        StatisticKind::Rank => kll_params_for(KLL_RANK_FIT, epsilon, delta),
        _ => None,
    }
}

/// Bounds on one KLL state's shape after `n` updates, or after any number
/// when `n` is unknown.
struct KllShape {
    /// `cap(i)`, height `i` from the top.
    caps: [u32; CAPACITY_CACHE_LEN],
    /// `L̂`: `L(n) + 1` once `n > k`, and 1 before.
    levels: usize,
    /// `R̂ = min(n, cap(0) + … + cap(L̂ − 1))`.
    retained: usize,
    /// `c_b = cap(L(n) − 1)`.
    bottom: usize,
    /// `ĉ_b = min(n, cap(L(n) − 2))`, the most the bottom level holds.
    bottom_max: usize,
}

impl KllShape {
    fn new(params: &KllParams, n: Option<usize>) -> Self {
        let (k, m) = normalized_k_m(params.k, params.m);
        let caps = capacity_table(k, m);
        let cap = |height: usize| caps[height.min(CAPACITY_CACHE_LEN - 1)] as usize;
        let n_or_max = n.unwrap_or(usize::MAX);
        let base = levels_after(k, n_or_max);
        let levels = if n_or_max > k {
            (base + 1).min(MAX_LEVELS)
        } else {
            1
        };
        let retained = n_or_max.min((0..levels).map(cap).sum());
        // Unknown `n` takes the largest bottom any `n` gives.
        let (bottom, bottom_max) = match n {
            Some(n) => (cap(base - 1), n.min(cap(base.saturating_sub(2)))),
            None => (cap(0), cap(0)),
        };
        Self {
            caps,
            levels,
            retained,
            bottom,
            bottom_max,
        }
    }

    /// `Σ s·lg s` over the levels, each `s` at most its capacity and `n`.
    fn level_sort_ops(&self, n: Option<usize>) -> f64 {
        let n = n.unwrap_or(usize::MAX);
        (0..self.levels)
            .map(|height| {
                let size = (self.caps[height.min(CAPACITY_CACHE_LEN - 1)] as usize).min(n) as f64;
                size * lg(size)
            })
            .sum()
    }
}

/// `L̂`, the most levels one KLL state holds after `n` updates.
pub(crate) fn kll_levels(params: &KllParams, n: Option<u64>) -> usize {
    KllShape::new(params, n.map(|n| usize::try_from(n).unwrap_or(usize::MAX))).levels
}

/// `3R + R·lg R + 2L + lg R + 2`: build the CDF and binary-search it.
fn kll_quantile_ops(shape: &KllShape) -> f64 {
    let (r, l) = (shape.retained as f64, shape.levels as f64);
    3.0 * r + r * lg(r) + 2.0 * l + lg(r) + 2.0
}

fn kll_query_ops(statistic: StatisticKind, params: &KllParams, data: &StreamData) -> Option<f64> {
    let shape = KllShape::new(params, data.n());
    match statistic {
        StatisticKind::Quantile => Some(kll_quantile_ops(&shape)),
        StatisticKind::Rank => Some(shape.retained as f64 + 2.0 * shape.levels as f64),
        _ => None,
    }
}

/// `R`, `L` of a merge of two states of `n` updates each.
fn kll_merge_sizes(params: &KllParams, n: Option<usize>) -> (KllShape, f64, f64) {
    let shape = KllShape::new(params, n);
    let merged = KllShape::new(params, n.map(|n| n.saturating_mul(2)));
    let r = 2.0 * shape.retained as f64;
    (shape, r, merged.levels as f64)
}

impl<T: NumericalValue> SketchAnalytics for Kll<T> {
    type Params = KllParams;
    type Data = StreamData;

    fn theoretical_memory_bytes(params: &KllParams, data: &StreamData) -> Option<usize> {
        Some(Kll::<T>::theoretical_memory_bytes(
            params.k,
            params.m,
            data.n().unwrap_or(usize::MAX),
        ))
    }

    fn theoretical_update_ops(params: &KllParams, data: &StreamData) -> f64 {
        15.0 + lg(KllShape::new(params, data.n()).bottom as f64 + 1.0)
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &KllParams,
        data: &StreamData,
    ) -> Option<f64> {
        kll_query_ops(statistic, params, data)
    }

    fn theoretical_merge_ops(params: &KllParams, data: &StreamData) -> Option<f64> {
        let (shape, r, l) = kll_merge_sizes(params, data.n());
        let b = 2.0 * shape.bottom_max as f64;
        Some(b * lg(b) + 16.0 * r + 8.0 * l + 40.0)
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &KllParams,
        _: &StreamData,
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        kll_error_for(statistic, params, delta)
    }

    fn params_for(
        statistic: StatisticKind,
        epsilon: f64,
        delta: f64,
        _: &StreamData,
    ) -> Option<KllParams> {
        kll_sizing(statistic, epsilon, delta)
    }
}

impl<T: NumericalValue> SketchAnalytics for KllDynamic<T> {
    type Params = KllParams;
    type Data = StreamData;

    /// `size_of::<Self>() + size_of::<T>()·max(3k, 2(R̂ + 1)) + 16·(L̂ + 1)`.
    fn theoretical_memory_bytes(params: &KllParams, data: &StreamData) -> Option<usize> {
        let (k, _) = normalized_k_m(params.k, params.m);
        let shape = KllShape::new(params, data.n());
        let items = (3 * k).max(2 * (shape.retained.saturating_add(1)));
        Some(
            size_of::<Self>()
                .saturating_add(items.saturating_mul(size_of::<T>()))
                .saturating_add(2 * (shape.levels + 1) * size_of::<usize>()),
        )
    }

    fn theoretical_update_ops(params: &KllParams, data: &StreamData) -> f64 {
        11.0 + 2.0 * lg(KllShape::new(params, data.n()).bottom as f64 + 1.0)
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &KllParams,
        data: &StreamData,
    ) -> Option<f64> {
        kll_query_ops(statistic, params, data)
    }

    fn theoretical_merge_ops(params: &KllParams, data: &StreamData) -> Option<f64> {
        let (shape, r, l) = kll_merge_sizes(params, data.n());
        Some(2.0 * shape.level_sort_ops(data.n()) + 16.0 * r + 8.0 * l)
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &KllParams,
        _: &StreamData,
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        kll_error_for(statistic, params, delta)
    }

    fn params_for(
        statistic: StatisticKind,
        epsilon: f64,
        delta: f64,
        _: &StreamData,
    ) -> Option<KllParams> {
        kll_sizing(statistic, epsilon, delta)
    }
}

/// `α`, DdSketch's relative accuracy.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DdParams {
    pub alpha: f64,
}

impl Default for DdParams {
    fn default() -> Self {
        Self { alpha: 0.01 }
    }
}

/// DdSketch's data inputs.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct DdData {
    pub n: Option<u64>,
    /// The smallest nonzero magnitude of the values.
    pub min_magnitude: Option<f64>,
    /// `A`, the largest magnitude of the values.
    pub max_magnitude: Option<f64>,
    /// Whether negative values occur.
    pub negatives: Option<bool>,
}

const DD_QUANTILE_CONTRACT: &str = "ddsketch_relative_error_alpha_v1";
const DD_INTERPOLATED_RELATIVE_CONTRACT: &str = "ddsketch_interpolated_relative_alpha_v1";
const DD_INTERPOLATED_ABSOLUTE_CONTRACT: &str = "ddsketch_interpolated_absolute_alpha_v1";

/// The largest `α` sizing returns.
const DD_MAX_ALPHA: f64 = 0.5;

/// Whether `α` is in (0, 1) and the known magnitudes are nonnegative and
/// within `ddsketch_indexable_bounds(α)`.
fn dd_valid(alpha: f64, data: &DdData) -> bool {
    if !(alpha > 0.0 && alpha < 1.0) {
        return false;
    }
    let (low, high) = ddsketch_indexable_bounds(alpha);
    data.min_magnitude.is_none_or(|min| min >= low)
        && data
            .max_magnitude
            .is_none_or(|max| max >= 0.0 && max <= high)
}

/// `B`, the most bucket slots both stores of one state hold.
fn dd_slots(params: &DdParams, data: &DdData) -> f64 {
    match <DdSketch as SketchAnalytics>::theoretical_memory_bytes(params, data) {
        Some(bytes) if bytes < usize::MAX => {
            ((bytes - size_of::<DdSketch>()) / size_of::<u64>()) as f64
        }
        _ => f64::INFINITY,
    }
}

impl SketchAnalytics for DdSketch {
    type Params = DdParams;
    type Data = DdData;

    fn theoretical_memory_bytes(params: &DdParams, data: &DdData) -> Option<usize> {
        (params.alpha > 0.0 && params.alpha < 1.0).then_some(())?;
        Some(DdSketch::theoretical_memory_bytes(
            params.alpha,
            data.n
                .map_or(usize::MAX, |n| usize::try_from(n).unwrap_or(usize::MAX)),
            data.min_magnitude.unwrap_or(0.0),
            data.max_magnitude.unwrap_or(f64::INFINITY),
            data.negatives.unwrap_or(true),
        ))
    }

    fn theoretical_update_ops(_: &DdParams, _: &DdData) -> f64 {
        4.0
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &DdParams,
        data: &DdData,
    ) -> Option<f64> {
        let b = dd_slots(params, data);
        match statistic {
            StatisticKind::Quantile => Some(b + 4.0),
            StatisticKind::QuantileInterpolated => Some(2.0 * b + 7.0),
            _ => None,
        }
    }

    fn theoretical_merge_ops(params: &DdParams, data: &DdData) -> Option<f64> {
        Some(4.0 * dd_slots(params, data) + 11.0)
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &DdParams,
        data: &DdData,
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        dd_valid(params.alpha, data).then_some(())?;
        let delta = Some(fixed_delta(0.0, delta)?);
        let relative = |contract| ErrorBound {
            metric: ErrorMetric::Relative,
            epsilon: params.alpha,
            delta,
            contract,
        };
        match statistic {
            StatisticKind::Quantile => Some(relative(DD_QUANTILE_CONTRACT)),
            StatisticKind::QuantileInterpolated if data.negatives == Some(false) => {
                Some(relative(DD_INTERPOLATED_RELATIVE_CONTRACT))
            }
            StatisticKind::QuantileInterpolated => Some(ErrorBound {
                metric: ErrorMetric::ABSOLUTE,
                epsilon: params.alpha * data.max_magnitude?,
                delta,
                contract: DD_INTERPOLATED_ABSOLUTE_CONTRACT,
            }),
            _ => None,
        }
    }

    fn params_for(
        statistic: StatisticKind,
        epsilon: f64,
        _: f64,
        data: &DdData,
    ) -> Option<DdParams> {
        is_positive(epsilon).then_some(())?;
        let alpha = match statistic {
            StatisticKind::Quantile => epsilon,
            StatisticKind::QuantileInterpolated if data.negatives == Some(false) => epsilon,
            StatisticKind::QuantileInterpolated => epsilon / data.max_magnitude?,
            _ => return None,
        };
        let alpha = alpha.min(DD_MAX_ALPHA);
        dd_valid(alpha, data).then_some(DdParams { alpha })
    }
}

#[cfg(feature = "experimental")]
pub use uniform::UniformParams;

#[cfg(feature = "experimental")]
mod uniform {
    use super::{SketchAnalytics, StatisticKind, StreamData, lg};
    use crate::UniformSampling;

    /// `ρ`, the sample rate.
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub struct UniformParams {
        pub sample_rate: f64,
    }

    impl Default for UniformParams {
        fn default() -> Self {
            Self { sample_rate: 0.1 }
        }
    }

    /// `s = ⌈ρ·n⌉`, the sample size after `n` updates; infinite when `n` is
    /// unknown.
    fn sample_size(params: &UniformParams, data: &StreamData) -> f64 {
        data.n
            .map_or(f64::INFINITY, |n| (params.sample_rate * n as f64).ceil())
    }

    impl SketchAnalytics for UniformSampling {
        type Params = UniformParams;
        type Data = StreamData;

        /// `size_of::<Self>() + 16·max(4, 2s)`.
        fn theoretical_memory_bytes(params: &UniformParams, data: &StreamData) -> Option<usize> {
            data.n?;
            let slots = (2.0 * sample_size(params, data)).max(4.0);
            let bytes = size_of::<Self>() as f64 + 16.0 * slots;
            Some(if bytes >= usize::MAX as f64 {
                usize::MAX
            } else {
                bytes as usize
            })
        }

        fn theoretical_update_ops(params: &UniformParams, data: &StreamData) -> f64 {
            let s = sample_size(params, data);
            3.0 + lg(s) + 2.0 * s
        }

        fn theoretical_query_ops(
            statistic: StatisticKind,
            params: &UniformParams,
            data: &StreamData,
        ) -> Option<f64> {
            (statistic == StatisticKind::Sample).then(|| 2.0 * sample_size(params, data))
        }

        fn theoretical_merge_ops(params: &UniformParams, data: &StreamData) -> Option<f64> {
            let both = 2.0 * sample_size(params, data);
            Some(3.0 + 2.0 * both + both * lg(both))
        }

        fn theoretical_error(
            _: StatisticKind,
            _: &UniformParams,
            _: &StreamData,
            _: Option<f64>,
        ) -> Option<crate::ErrorBound> {
            None
        }

        fn params_for(_: StatisticKind, _: f64, _: f64, _: &StreamData) -> Option<UniformParams> {
            None
        }
    }
}
