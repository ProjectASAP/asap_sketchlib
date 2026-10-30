//! Analytics of the cardinality and membership sketches: HyperLogLog,
//! HyperLogLogHip, Kmv, SetAggregator and Bloom.

use super::{
    DEFAULT_DELTA, ErrorBound, ErrorMetric, HashPath, SketchAnalytics, StatisticKind, fixed_delta,
    floor_log2,
};
use crate::common::heap::hash_map_bytes_after_inserts;
use crate::sketches::bloom::partitioned_fpp;
use crate::sketches::hll::{HyperLogLogHipImpl, HyperLogLogImpl};
use crate::sketches::kmv::KMV_DEFAULT_K;
use crate::{
    BLOOM_DEFAULT_COLS, BLOOM_DEFAULT_ROWS, BLOOM_MAX_BITS, BLOOM_MAX_SLICES, Bloom, Classic,
    DefaultXxHasher, Ertl, HllRegisterStorage, HllRegistersP14, Kmv, SetAggregator, SketchHasher,
};

/// HyperLogLog precision `p`, with `m = 2^p` registers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HllParams {
    pub precision: usize,
}

impl Default for HllParams {
    fn default() -> Self {
        Self {
            precision: HllRegistersP14::PRECISION,
        }
    }
}

/// HyperLogLog's data inputs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HllData {
    /// An upper bound on `D`.
    pub max_distinct: Option<u64>,
}

/// `D`, the data input of Kmv and Bloom.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DistinctData {
    /// `D`, the distinct keys inserted.
    pub distinct: Option<u64>,
}

const HLL_RSE: f64 = 1.04;
const GENERIC_HLL_CONTRACT: &str = "generic_hll_rse_only_no_confidence_v1";
const CLASSIC_COLLISION_CONTRACT: &str = "classic_hll_linear_counting_collision_bound_v1";
const HIP_CONTRACT: &str = "hip_hll_rse_only_no_confidence_v1";

/// The smallest precision Flajolet et al.'s standard error covers.
const HLL_MIN_PRECISION: usize = 4;

/// Flajolet et al.'s `β_m` for `m = 16, 32, 64, 128`.
const HLL_SMALL_M_RSE: [f64; 4] = [1.106, 1.070, 1.054, 1.046];

/// Precisions and `max_distinct` the linear-counting collision bound covers.
const COLLISION_MIN_PRECISION: usize = 4;
const COLLISION_MAX_PRECISION: usize = 18;
const COLLISION_MAX_DISTINCT: u64 = 1 << 17;

fn register_count(precision: usize) -> f64 {
    2f64.powi(precision as i32)
}

/// A standard error `constant/√m`, with no `δ`.
fn rse_error(constant: f64, precision: usize, contract: &'static str) -> ErrorBound {
    ErrorBound {
        metric: ErrorMetric::Relative,
        epsilon: constant / register_count(precision).sqrt(),
        delta: None,
        contract,
    }
}

/// `β_m` of HyperLogLog's standard error `β_m/√m`: the table for `p ≤ 7`,
/// 1.04 above, and `None` for `p < 4`.
fn hll_rse_constant(precision: usize) -> Option<f64> {
    match precision {
        p if p < HLL_MIN_PRECISION => None,
        p if p < HLL_MIN_PRECISION + HLL_SMALL_M_RSE.len() => {
            Some(HLL_SMALL_M_RSE[p - HLL_MIN_PRECISION])
        }
        _ => Some(HLL_RSE),
    }
}

/// HyperLogLog's standard error `β_m/√m`; `None` for `p < 4`.
fn hll_rse_error(precision: usize) -> Option<ErrorBound> {
    Some(rse_error(
        hll_rse_constant(precision)?,
        precision,
        GENERIC_HLL_CONTRACT,
    ))
}

/// The smallest `p ≥ 4` with `β_m/√m ≤ ε`.
fn hll_rse_params_for(epsilon: f64) -> Option<HllParams> {
    let mut precision = rse_params_for(HLL_RSE, epsilon)?
        .precision
        .max(HLL_MIN_PRECISION);
    while hll_rse_error(precision)?.epsilon > epsilon {
        precision += 1;
    }
    Some(HllParams { precision })
}

/// `p = ⌈2·log2(constant/ε)⌉`, at least 1; `None` past 63.
fn rse_params_for(constant: f64, epsilon: f64) -> Option<HllParams> {
    if epsilon.is_nan() || epsilon <= 0.0 {
        return None;
    }
    let precision = (2.0 * (constant / epsilon).log2()).ceil().max(1.0);
    (precision <= 63.0).then_some(HllParams {
        precision: precision as usize,
    })
}

/// `ln(max over 2 ≤ D ≤ max_distinct of λ^t/t!)`, with `λ = D(D − 1)/(2m)`
/// and `t = ⌊εD⌋ + 1`, for `ε ≤ 1`; `ln_fact[i]` is `ln i!`.
fn collision_ln_delta(m: f64, max_distinct: usize, epsilon: f64, ln_fact: &[f64]) -> f64 {
    (2..=max_distinct)
        .map(|d| {
            let d = d as f64;
            let lambda = d * (d - 1.0) / (2.0 * m);
            let t = (epsilon * d).floor() as usize + 1;
            t as f64 * lambda.ln() - ln_fact[t]
        })
        .fold(f64::NEG_INFINITY, f64::max)
}

/// Classic's linear-counting bound: the smallest `ε` with
/// `max_distinct/(2(m − max_distinct)) ≤ ε ≤ 1` whose `δ` is at most
/// `delta`; `None` outside the conditions or when no such `ε` exists.
fn classic_collision_error(
    precision: usize,
    max_distinct: Option<u64>,
    delta: Option<f64>,
) -> Option<ErrorBound> {
    let max_distinct = max_distinct?;
    let m = register_count(precision);
    if !(COLLISION_MIN_PRECISION..=COLLISION_MAX_PRECISION).contains(&precision)
        || max_distinct > COLLISION_MAX_DISTINCT
        || max_distinct as f64 > m / 2.0
    {
        return None;
    }
    let delta = delta.unwrap_or(DEFAULT_DELTA);
    if delta.is_nan() || delta <= 0.0 {
        return None;
    }
    let max_distinct = max_distinct as usize;
    let floor = max_distinct as f64 / (2.0 * (m - max_distinct as f64));
    let mut ln_fact = vec![0.0; max_distinct + 3];
    for i in 1..ln_fact.len() {
        ln_fact[i] = ln_fact[i - 1] + (i as f64).ln();
    }
    let target = delta.ln();
    let meets = |epsilon: f64| collision_ln_delta(m, max_distinct, epsilon, &ln_fact) <= target;
    let epsilon = if meets(floor) {
        floor
    } else if !meets(1.0) {
        return None;
    } else {
        let (mut lo, mut hi) = (floor, 1.0f64);
        for _ in 0..60 {
            let mid = 0.5 * (lo + hi);
            if meets(mid) {
                hi = mid;
            } else {
                lo = mid;
            }
        }
        hi
    };
    Some(ErrorBound {
        metric: ErrorMetric::Relative,
        epsilon,
        delta: Some(delta),
        contract: CLASSIC_COLLISION_CONTRACT,
    })
}

/// Classic's bound: the collision bound when it applies and its `ε` is at
/// most the standard error's, otherwise the standard error.
fn classic_error(
    precision: usize,
    max_distinct: Option<u64>,
    delta: Option<f64>,
) -> Option<ErrorBound> {
    let rse = hll_rse_error(precision)?;
    Some(
        classic_collision_error(precision, max_distinct, delta)
            .filter(|bound| bound.epsilon <= rse.epsilon)
            .unwrap_or(rse),
    )
}

impl<R: HllRegisterStorage, H: SketchHasher> SketchAnalytics for HyperLogLogImpl<Classic, R, H> {
    type Params = HllParams;
    type Data = HllData;

    fn theoretical_memory_bytes(params: &HllParams, _: &HllData) -> Option<usize> {
        Some(Self::theoretical_memory_bytes(params.precision))
    }

    fn theoretical_update_ops(_: &HllParams, _: &HllData) -> f64 {
        1.0
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &HllParams,
        _: &HllData,
    ) -> Option<f64> {
        (statistic == StatisticKind::Distinct).then(|| register_count(params.precision))
    }

    fn theoretical_merge_ops(params: &HllParams, _: &HllData) -> Option<f64> {
        Some(2.0 * register_count(params.precision))
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &HllParams,
        data: &HllData,
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        (statistic == StatisticKind::Distinct).then_some(())?;
        classic_error(params.precision, data.max_distinct, delta)
    }

    fn params_for(
        statistic: StatisticKind,
        epsilon: f64,
        delta: f64,
        data: &HllData,
    ) -> Option<HllParams> {
        (statistic == StatisticKind::Distinct).then_some(())?;
        (COLLISION_MIN_PRECISION..=COLLISION_MAX_PRECISION)
            .find(|&precision| {
                classic_error(precision, data.max_distinct, Some(delta)).is_some_and(|bound| {
                    bound.contract == CLASSIC_COLLISION_CONTRACT && bound.epsilon <= epsilon
                })
            })
            .map(|precision| HllParams { precision })
            .or_else(|| hll_rse_params_for(epsilon))
    }
}

impl<R: HllRegisterStorage, H: SketchHasher> SketchAnalytics for HyperLogLogImpl<Ertl, R, H> {
    type Params = HllParams;
    type Data = HllData;

    fn theoretical_memory_bytes(params: &HllParams, _: &HllData) -> Option<usize> {
        Some(Self::theoretical_memory_bytes(params.precision))
    }

    fn theoretical_update_ops(_: &HllParams, _: &HllData) -> f64 {
        1.0
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &HllParams,
        _: &HllData,
    ) -> Option<f64> {
        (statistic == StatisticKind::Distinct)
            .then(|| 2.0 * register_count(params.precision) + 66.0 - params.precision as f64)
    }

    fn theoretical_merge_ops(params: &HllParams, _: &HllData) -> Option<f64> {
        Some(2.0 * register_count(params.precision))
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &HllParams,
        _: &HllData,
        _: Option<f64>,
    ) -> Option<ErrorBound> {
        (statistic == StatisticKind::Distinct).then_some(())?;
        hll_rse_error(params.precision)
    }

    fn params_for(
        statistic: StatisticKind,
        epsilon: f64,
        _: f64,
        _: &HllData,
    ) -> Option<HllParams> {
        (statistic == StatisticKind::Distinct).then_some(())?;
        hll_rse_params_for(epsilon)
    }
}

impl<R: HllRegisterStorage> SketchAnalytics for HyperLogLogHipImpl<R> {
    type Params = HllParams;
    type Data = ();

    fn theoretical_memory_bytes(params: &HllParams, _: &()) -> Option<usize> {
        Some(Self::theoretical_memory_bytes(params.precision))
    }

    fn theoretical_update_ops(_: &HllParams, _: &()) -> f64 {
        6.0
    }

    fn theoretical_query_ops(statistic: StatisticKind, _: &HllParams, _: &()) -> Option<f64> {
        (statistic == StatisticKind::Distinct).then_some(1.0)
    }

    fn theoretical_merge_ops(_: &HllParams, _: &()) -> Option<f64> {
        None
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &HllParams,
        _: &(),
        _: Option<f64>,
    ) -> Option<ErrorBound> {
        (statistic == StatisticKind::Distinct).then(|| {
            rse_error(
                std::f64::consts::LN_2.sqrt(),
                params.precision,
                HIP_CONTRACT,
            )
        })
    }

    fn params_for(statistic: StatisticKind, epsilon: f64, _: f64, _: &()) -> Option<HllParams> {
        (statistic == StatisticKind::Distinct).then_some(())?;
        rse_params_for(std::f64::consts::LN_2.sqrt(), epsilon)
    }
}

/// `k`, the hashes a Kmv sketch keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KmvParams {
    pub k: usize,
}

impl Default for KmvParams {
    fn default() -> Self {
        Self { k: KMV_DEFAULT_K }
    }
}

const KMV_CONTRACT: &str = "kmv_exact_below_k_chernoff_v1";

/// Kmv's `δ` at `ε`: Chernoff bounds on over- and underestimating by more
/// than `ε`, the second 0 for `ε ≥ 1`.
pub fn kmv_delta(epsilon: f64, k: usize) -> f64 {
    let (e, n) = (epsilon, k.saturating_sub(1) as f64);
    let over = (-e * e * n / ((1.0 + e) * (2.0 + e))).exp();
    let under = if e < 1.0 {
        (-e * e * n / (2.0 * (1.0 - e))).exp()
    } else {
        0.0
    };
    over + under
}

/// The smallest `ε` with `kmv_delta(ε, k) ≤ delta`; `None` when none is.
fn kmv_epsilon(k: usize, delta: f64) -> Option<f64> {
    let mut hi = 1.0f64;
    while kmv_delta(hi, k) > delta {
        hi *= 2.0;
        if hi > 1e12 {
            return None;
        }
    }
    let mut lo = 0.0f64;
    for _ in 0..100 {
        let mid = 0.5 * (lo + hi);
        if kmv_delta(mid, k) <= delta {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    Some(hi)
}

/// The smallest `k ≥ 2` with `kmv_delta(ε, k) ≤ delta`.
fn kmv_k(epsilon: f64, delta: f64) -> Option<usize> {
    if epsilon.is_nan() || epsilon <= 0.0 || delta.is_nan() || delta <= 0.0 {
        return None;
    }
    let mut hi = 2usize;
    while kmv_delta(epsilon, hi) > delta {
        hi = hi.checked_mul(2)?;
    }
    let mut lo = hi / 2;
    if hi == 2 {
        return Some(2);
    }
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if kmv_delta(epsilon, mid) <= delta {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    Some(hi)
}

impl<H: SketchHasher> SketchAnalytics for Kmv<H> {
    type Params = KmvParams;
    type Data = DistinctData;

    fn theoretical_memory_bytes(params: &KmvParams, data: &DistinctData) -> Option<usize> {
        let distinct = data
            .distinct
            .map_or(usize::MAX, |d| usize::try_from(d).unwrap_or(usize::MAX));
        Some(Kmv::<DefaultXxHasher>::theoretical_memory_bytes(
            params.k, distinct,
        ))
    }

    fn theoretical_update_ops(params: &KmvParams, _: &DistinctData) -> f64 {
        params.k as f64 + 3.0 + 6.0 * floor_log2(params.k)
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        _: &KmvParams,
        _: &DistinctData,
    ) -> Option<f64> {
        (statistic == StatisticKind::Distinct).then_some(1.0)
    }

    fn theoretical_merge_ops(params: &KmvParams, _: &DistinctData) -> Option<f64> {
        let k = params.k as f64;
        Some(k * (k + 4.0 + 6.0 * floor_log2(params.k)))
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &KmvParams,
        data: &DistinctData,
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        (statistic == StatisticKind::Distinct && params.k >= 2).then_some(())?;
        if delta.is_some_and(|delta| delta.is_nan() || delta < 0.0) {
            return None;
        }
        if data.distinct.is_some_and(|d| d < params.k as u64) {
            return Some(ErrorBound {
                metric: ErrorMetric::Relative,
                epsilon: 0.0,
                delta: Some(0.0),
                contract: KMV_CONTRACT,
            });
        }
        let delta = delta.unwrap_or(DEFAULT_DELTA);
        (delta > 0.0).then_some(())?;
        Some(ErrorBound {
            metric: ErrorMetric::Relative,
            epsilon: kmv_epsilon(params.k, delta)?,
            delta: Some(delta),
            contract: KMV_CONTRACT,
        })
    }

    fn params_for(
        statistic: StatisticKind,
        epsilon: f64,
        delta: f64,
        data: &DistinctData,
    ) -> Option<KmvParams> {
        (statistic == StatisticKind::Distinct).then_some(())?;
        let exact = data
            .distinct
            .and_then(|d| usize::try_from(d).ok()?.checked_add(1))
            .map(|k| k.max(2));
        let bounded = kmv_k(epsilon, delta);
        let k = match (exact, bounded) {
            (Some(a), Some(b)) => a.min(b),
            (a, b) => a.or(b)?,
        };
        Some(KmvParams { k })
    }
}

impl SketchAnalytics for SetAggregator {
    type Params = ();
    type Data = SetData;

    fn theoretical_memory_bytes(_: &(), data: &SetData) -> Option<usize> {
        let distinct = usize::try_from(data.distinct?).unwrap_or(usize::MAX);
        let key_bytes = (distinct as f64 * data.mean_key_bytes?).ceil();
        Some(
            size_of::<Self>()
                .saturating_add(hash_map_bytes_after_inserts::<String>(distinct))
                .saturating_add(if key_bytes >= usize::MAX as f64 {
                    usize::MAX
                } else {
                    key_bytes as usize
                }),
        )
    }

    fn theoretical_update_ops(_: &(), _: &SetData) -> f64 {
        1.0
    }

    fn theoretical_query_ops(statistic: StatisticKind, _: &(), _: &SetData) -> Option<f64> {
        match statistic {
            StatisticKind::Distinct => Some(0.0),
            StatisticKind::Membership => Some(1.0),
            _ => None,
        }
    }

    fn theoretical_merge_ops(_: &(), data: &SetData) -> Option<f64> {
        Some(data.distinct.map_or(f64::INFINITY, |d| 2.0 * d as f64))
    }

    fn theoretical_error(
        statistic: StatisticKind,
        _: &(),
        _: &SetData,
        _: Option<f64>,
    ) -> Option<ErrorBound> {
        matches!(
            statistic,
            StatisticKind::Distinct | StatisticKind::Membership
        )
        .then_some(ErrorBound::EXACT)
    }

    fn params_for(statistic: StatisticKind, _: f64, _: f64, _: &SetData) -> Option<()> {
        matches!(
            statistic,
            StatisticKind::Distinct | StatisticKind::Membership
        )
        .then_some(())
    }
}

/// SetAggregator's data inputs.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct SetData {
    /// `D`, the distinct keys. Unknown counts as unbounded.
    pub distinct: Option<u64>,
    /// `ℓ`, the mean key length in bytes.
    pub mean_key_bytes: Option<f64>,
}

/// `r` slices of `c` bits of a Bloom filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BloomParams {
    pub rows: usize,
    pub cols: usize,
}

impl Default for BloomParams {
    fn default() -> Self {
        Self {
            rows: BLOOM_DEFAULT_ROWS,
            cols: BLOOM_DEFAULT_COLS,
        }
    }
}

const BLOOM_CONTRACT: &str = "bloom_partitioned_fpr_v1";

/// `(1 − (1 − 1/c)^D)^r`, and 0 for `D = 0`.
fn bloom_rate(rows: usize, cols: usize, distinct: u64) -> f64 {
    if distinct == 0 {
        0.0
    } else {
        partitioned_fpp(rows, cols, distinct as f64)
    }
}

impl<Mode: HashPath, H: SketchHasher> SketchAnalytics for Bloom<Mode, H> {
    type Params = BloomParams;
    type Data = DistinctData;

    fn theoretical_memory_bytes(params: &BloomParams, _: &DistinctData) -> Option<usize> {
        Some(Self::theoretical_memory_bytes(params.rows, params.cols))
    }

    fn theoretical_update_ops(params: &BloomParams, _: &DistinctData) -> f64 {
        params.rows as f64
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &BloomParams,
        _: &DistinctData,
    ) -> Option<f64> {
        (statistic == StatisticKind::Membership).then_some(params.rows as f64)
    }

    fn theoretical_merge_ops(params: &BloomParams, _: &DistinctData) -> Option<f64> {
        Some(2.0 * (params.rows * params.cols.div_ceil(64)) as f64)
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &BloomParams,
        data: &DistinctData,
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        (statistic == StatisticKind::Membership
            && params.cols.is_power_of_two()
            && params.cols as u64 <= 1 << 32)
            .then_some(())?;
        let rate = bloom_rate(params.rows, params.cols, data.distinct?);
        Some(ErrorBound {
            metric: ErrorMetric::ABSOLUTE,
            epsilon: 0.0,
            delta: Some(fixed_delta(rate, delta)?),
            contract: BLOOM_CONTRACT,
        })
    }

    /// The `r ≤ BLOOM_MAX_SLICES` and power-of-two `c`, with `r·c` at most
    /// `BLOOM_MAX_BITS`, that minimize `r·c` and meet `delta`.
    fn params_for(
        statistic: StatisticKind,
        epsilon: f64,
        delta: f64,
        data: &DistinctData,
    ) -> Option<BloomParams> {
        (statistic == StatisticKind::Membership && epsilon >= 0.0).then_some(())?;
        let distinct = data.distinct?;
        let mut best: Option<BloomParams> = None;
        for rows in 1..=BLOOM_MAX_SLICES {
            let mut cols = 1usize;
            while rows * cols <= BLOOM_MAX_BITS && bloom_rate(rows, cols, distinct) > delta {
                cols *= 2;
            }
            if rows * cols <= BLOOM_MAX_BITS && best.is_none_or(|b| rows * cols < b.rows * b.cols) {
                best = Some(BloomParams { rows, cols });
            }
        }
        best
    }
}
