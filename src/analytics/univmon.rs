//! Analytics of the UnivMon family: UnivMon, UnivMonTwoTier and UnivMonQ.

use super::count_matrix::{
    L2_NORM_CONTRACT, MatrixParams, count_sketch_error, l2_norm_error, l2_norm_params_for,
};
use super::heavy_hitters::FlowData;
use super::{
    DEFAULT_DELTA, ErrorBound, ErrorMetric, SketchAnalytics, StatisticKind, est_count_sketch,
    floor_log2, heap_update_ops, lg, max_row_failure,
};
use crate::common::heap::DEFAULT_HEAP_SIZE;
use crate::sketch_framework::univmon::{UNIVMON_MAX_LAYERS, wire::check_univmon_dimensions};
use crate::sketch_framework::univmon_q::{config_is_valid, layer_cols};
use crate::sketch_framework::{univmon, univmon_two_tier};
use crate::{
    CountSketchWithL2Norm, DataInput, SketchHasher, UnivMon, UnivMonQ, UnivMonQConfig,
    UnivMonTwoTier,
};
use std::f64::consts::E;

/// Heap capacity `k`, `d` rows, `w` columns and `L` layers of a UnivMon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnivMonParams {
    pub heap_size: usize,
    pub rows: usize,
    pub cols: usize,
    pub layers: usize,
}

impl Default for UnivMonParams {
    fn default() -> Self {
        Self {
            heap_size: DEFAULT_HEAP_SIZE,
            rows: univmon::DEFAULT_ROWS,
            cols: univmon::DEFAULT_COLS,
            layers: univmon::DEFAULT_LAYERS,
        }
    }
}

/// Heap capacity `k`, `L_e` elephant layers of `d_e × w_e`, mouse layers of
/// `d_m × w_m`, and `L` layers of a UnivMonTwoTier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TwoTierParams {
    pub heap_size: usize,
    pub elephant_layers: usize,
    pub elephant_rows: usize,
    pub elephant_cols: usize,
    pub mouse_rows: usize,
    pub mouse_cols: usize,
    pub layers: usize,
}

impl Default for TwoTierParams {
    fn default() -> Self {
        Self {
            heap_size: DEFAULT_HEAP_SIZE,
            elephant_layers: univmon_two_tier::DEFAULT_ELEPHANT_LAYERS,
            elephant_rows: univmon_two_tier::DEFAULT_ELEPHANT_ROWS,
            elephant_cols: univmon_two_tier::DEFAULT_ELEPHANT_COLS,
            mouse_rows: univmon_two_tier::DEFAULT_MOUSE_ROWS,
            mouse_cols: univmon_two_tier::DEFAULT_MOUSE_COLS,
            layers: univmon_two_tier::DEFAULT_LAYERS,
        }
    }
}

impl TwoTierParams {
    /// `(d, w)` of layer `j`.
    fn layer(&self, j: usize) -> (usize, usize) {
        if j < self.elephant_layers {
            (self.elephant_rows, self.elephant_cols)
        } else {
            (self.mouse_rows, self.mouse_cols)
        }
    }
}

/// The data inputs of UnivMonQ.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UnivMonQData {
    /// `n`, the updates.
    pub n: Option<u64>,
    /// Whether the ordered readouts' heavy set is empty, as
    /// [`UnivMonQOrderedQueryDiagnostics::heavy`](crate::UnivMonQOrderedQueryDiagnostics)
    /// reports it. The Quantile and Rank bounds need it `true`.
    pub heavy_set_empty: Option<bool>,
}

const TOTAL_CONTRACT: &str = "univmon_unit_update_total";
const DISTINCT_CONTRACT: &str = "univmon_distinct_complete_top_layer_chebyshev_v1";
const ITEM_CONTRACT: &str = "univmonq_terminal_count_sketch_l2_median_chosen_row_rate_v1";
const SAMPLE_CONTRACT: &str = "univmonq_rank_diffuse_occurrence_hoeffding_v1";

/// The heap estimator's variance, in units of `D²/k`.
const DISTINCT_VARIANCE: f64 = 1.35;

/// The smallest heap the Distinct bound holds for.
const DISTINCT_MIN_HEAP: usize = 8;

/// `λ(L) = 2 − 2^(1−L)`, the layers an update reaches on average.
fn reached_layers(layers: usize) -> f64 {
    2.0 - 2f64.powi(1 - layers as i32)
}

/// `ψ`: 0 when `D ≤ k`, `(e/4)^(k/2)` when `L ≥ lg(D/k) + 2`, else 1.
fn overflow_probability(heap_size: usize, layers: usize, distinct: u64) -> f64 {
    let ratio = distinct as f64 / heap_size as f64;
    if ratio <= 1.0 {
        0.0
    } else if layers as f64 >= lg(ratio) + 2.0 {
        (E / 4.0).powf(heap_size as f64 / 2.0)
    } else {
        1.0
    }
}

/// Distinct: `Relative`, `ε = √(1.35/(k·(δ − ψ)))`, `δ` chosen, `k ≥ 8`;
/// `None` without `D` or when `δ ≤ ψ`.
fn distinct_error(
    heap_size: usize,
    layers: usize,
    data: &FlowData,
    delta: Option<f64>,
) -> Option<ErrorBound> {
    (heap_size >= DISTINCT_MIN_HEAP).then_some(())?;
    let delta = delta.unwrap_or(DEFAULT_DELTA);
    let room = delta - overflow_probability(heap_size, layers, data.distinct?);
    (room > 0.0).then(|| ErrorBound {
        metric: ErrorMetric::Relative,
        epsilon: (DISTINCT_VARIANCE / (heap_size as f64 * room)).sqrt(),
        delta: Some(delta),
        contract: DISTINCT_CONTRACT,
    })
}

/// Distinct sizing: the smallest `k ≥ 8` with
/// `1.35/(k·ε²) + (e/4)^(k/2) ≤ δ`, and `L = lg(D/k) + 2`; `None` past
/// [`UNIVMON_MAX_LAYERS`].
fn distinct_params_for(epsilon: f64, delta: f64, data: &FlowData) -> Option<(usize, usize)> {
    if !(epsilon > 0.0) || !(delta > 0.0) {
        return None;
    }
    let distinct = data.distinct?;
    let fails = |k: usize| {
        let k = k as f64;
        DISTINCT_VARIANCE / (k * epsilon * epsilon) + (E / 4.0).powf(k / 2.0) > delta
    };
    let mut high = DISTINCT_MIN_HEAP;
    while fails(high) {
        high = high.checked_mul(2).filter(|&k| k <= u32::MAX as usize)?;
    }
    let mut low = DISTINCT_MIN_HEAP;
    while low < high {
        let mid = low + (high - low) / 2;
        if fails(mid) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    let layers = lg(distinct as f64 / low as f64) as usize + 2;
    (layers <= UNIVMON_MAX_LAYERS).then_some((low, layers))
}

/// `k` capped by `D`: the most keys one layer's heap holds.
fn heap_keys(heap_size: usize, data: &FlowData) -> f64 {
    data.distinct
        .map_or(heap_size as f64, |d| (d as f64).min(heap_size as f64))
}

/// One layer's share of an update: `3d` counter and row-sum ops, an
/// estimate, a heap update and the candidate flag.
fn layer_update_ops(rows: usize, heap_size: usize) -> f64 {
    3.0 * rows as f64 + est_count_sketch(rows) + heap_update_ops(heap_size) + 1.0
}

/// One layer's share of a merge: the counters, the two flags, both heaps
/// read into a set, the heap cleared, each of up to `2c` keys re-estimated
/// into it, and the flag written.
fn layer_merge_ops(rows: usize, cols: usize, heap_size: usize, keys: f64) -> f64 {
    (3 * rows * cols + rows) as f64
        + 6.0
        + keys * (4.0 + 2.0 * (1.0 + est_count_sketch(rows) + heap_update_ops(heap_size)))
}

/// The readouts both pyramids share, from layer 0's dimensions.
fn pyramid_error(
    statistic: StatisticKind,
    layer0: MatrixParams,
    heap_size: usize,
    layers: usize,
    data: &FlowData,
    delta: Option<f64>,
) -> Option<ErrorBound> {
    match statistic {
        StatisticKind::Total => Some(ErrorBound {
            contract: TOTAL_CONTRACT,
            ..ErrorBound::EXACT
        }),
        StatisticKind::FrequencyL2 => l2_norm_error(&layer0, delta, L2_NORM_CONTRACT),
        StatisticKind::Distinct => distinct_error(heap_size, layers, data, delta),
        _ => None,
    }
}

/// The query ops both pyramids share; `layer_rows` gives each layer's `d`.
fn pyramid_query_ops(
    statistic: StatisticKind,
    heap_size: usize,
    layers: usize,
    layer_rows: impl Fn(usize) -> usize,
    data: &FlowData,
) -> Option<f64> {
    match statistic {
        StatisticKind::Total => Some(1.0),
        StatisticKind::FrequencyL2 => Some(est_count_sketch(layer_rows(0))),
        StatisticKind::Distinct | StatisticKind::GSum | StatisticKind::FrequencyEntropy => {
            let keys = heap_keys(heap_size, data);
            let reads: f64 = (0..layers)
                .map(|j| 1.0 + est_count_sketch(layer_rows(j)))
                .sum();
            Some(4.0 + 2.0 * layers as f64 + keys * reads)
        }
        _ => None,
    }
}

impl SketchAnalytics for UnivMon {
    type Params = UnivMonParams;
    type Data = FlowData;

    fn theoretical_memory_bytes(params: &UnivMonParams, data: &FlowData) -> usize {
        let key = vec![0u8; data.key_bytes.unwrap_or(0)];
        UnivMon::theoretical_memory_bytes(
            params.heap_size,
            params.rows,
            params.cols,
            params.layers,
            &DataInput::Bytes(&key),
        )
    }

    fn theoretical_update_ops(params: &UnivMonParams, _: &FlowData) -> f64 {
        1.0 + reached_layers(params.layers) * layer_update_ops(params.rows, params.heap_size)
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &UnivMonParams,
        data: &FlowData,
    ) -> Option<f64> {
        pyramid_query_ops(
            statistic,
            params.heap_size,
            params.layers,
            |_| params.rows,
            data,
        )
    }

    fn theoretical_merge_ops(params: &UnivMonParams, data: &FlowData) -> Option<f64> {
        let keys = heap_keys(params.heap_size, data);
        let layer = layer_merge_ops(params.rows, params.cols, params.heap_size, keys);
        Some(2.0 + params.layers as f64 * layer)
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &UnivMonParams,
        data: &FlowData,
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        let layer0 = MatrixParams {
            rows: params.rows,
            cols: params.cols,
        };
        pyramid_error(
            statistic,
            layer0,
            params.heap_size,
            params.layers,
            data,
            delta,
        )
    }

    fn params_for(
        statistic: StatisticKind,
        epsilon: f64,
        delta: f64,
        data: &FlowData,
    ) -> Option<UnivMonParams> {
        let params = match statistic {
            StatisticKind::Total => UnivMonParams::default(),
            StatisticKind::FrequencyL2 => {
                let shape = l2_norm_params_for(epsilon, delta)?;
                UnivMonParams {
                    rows: shape.rows,
                    cols: shape.cols,
                    ..UnivMonParams::default()
                }
            }
            StatisticKind::Distinct => {
                let (heap_size, layers) = distinct_params_for(epsilon, delta, data)?;
                UnivMonParams {
                    heap_size,
                    layers,
                    ..UnivMonParams::default()
                }
            }
            _ => return None,
        };
        UnivMon::check_shape(params.heap_size, params.rows, params.cols, params.layers)
            .ok()
            .map(|()| params)
    }
}

impl SketchAnalytics for UnivMonTwoTier {
    type Params = TwoTierParams;
    type Data = FlowData;

    fn theoretical_memory_bytes(params: &TwoTierParams, data: &FlowData) -> usize {
        let key = vec![0u8; data.key_bytes.unwrap_or(0)];
        UnivMonTwoTier::theoretical_memory_bytes(
            params.heap_size,
            params.elephant_layers,
            params.elephant_rows,
            params.elephant_cols,
            params.mouse_rows,
            params.mouse_cols,
            params.layers,
            &DataInput::Bytes(&key),
        )
    }

    fn theoretical_update_ops(params: &TwoTierParams, _: &FlowData) -> f64 {
        let elephant = reached_layers(params.elephant_layers.min(params.layers));
        let mouse = reached_layers(params.layers) - elephant;
        1.0 + elephant * layer_update_ops(params.elephant_rows, params.heap_size)
            + mouse * layer_update_ops(params.mouse_rows, params.heap_size)
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &TwoTierParams,
        data: &FlowData,
    ) -> Option<f64> {
        pyramid_query_ops(
            statistic,
            params.heap_size,
            params.layers,
            |j| params.layer(j).0,
            data,
        )
    }

    fn theoretical_merge_ops(params: &TwoTierParams, data: &FlowData) -> Option<f64> {
        let keys = heap_keys(params.heap_size, data);
        let layers: f64 = (0..params.layers)
            .map(|j| {
                let (rows, cols) = params.layer(j);
                layer_merge_ops(rows, cols, params.heap_size, keys)
            })
            .sum();
        Some(2.0 + layers)
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &TwoTierParams,
        data: &FlowData,
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        let (rows, cols) = params.layer(0);
        pyramid_error(
            statistic,
            MatrixParams { rows, cols },
            params.heap_size,
            params.layers,
            data,
            delta,
        )
    }

    fn params_for(
        statistic: StatisticKind,
        epsilon: f64,
        delta: f64,
        data: &FlowData,
    ) -> Option<TwoTierParams> {
        let params = match statistic {
            StatisticKind::Total => TwoTierParams::default(),
            StatisticKind::FrequencyL2 => {
                let shape = l2_norm_params_for(epsilon, delta)?;
                TwoTierParams {
                    elephant_rows: shape.rows,
                    elephant_cols: shape.cols,
                    ..TwoTierParams::default()
                }
            }
            StatisticKind::Distinct => {
                let (heap_size, layers) = distinct_params_for(epsilon, delta, data)?;
                TwoTierParams {
                    heap_size,
                    layers,
                    ..TwoTierParams::default()
                }
            }
            _ => return None,
        };
        let fits = (0..params.layers).all(|j| {
            let (rows, cols) = params.layer(j);
            check_univmon_dimensions(params.layers, rows, cols, params.heap_size).is_ok()
                && CountSketchWithL2Norm::<crate::DefaultXxHasher>::check_hash_bits(rows, cols)
                    .is_ok()
        });
        (fits && params.layers <= UNIVMON_MAX_LAYERS).then_some(params)
    }
}

/// `q` of UnivMonQ: the most collision probability over its layers, `1/w_j`
/// for a power-of-two `w_j` and `min(1/w_j + 2^(−lg W), 9/(8w_j))` otherwise.
fn univmonq_collision_probability(config: &UnivMonQConfig) -> f64 {
    let slice = 2f64.powi(-(lg(config.top_layer_cols as f64) as i32));
    (0..config.layers)
        .map(|level| {
            let cols = layer_cols(*config, level);
            let w = cols as f64;
            if cols.is_power_of_two() {
                1.0 / w
            } else {
                (1.0 / w + slice).min(9.0 / (8.0 * w))
            }
        })
        .fold(0.0, f64::max)
}

/// Quantile and Rank: `ε = √(ln(2/δ)/(2·min(M, n)))`, `δ` chosen, when the
/// heavy set is empty.
fn sample_error(
    metric: ErrorMetric,
    config: &UnivMonQConfig,
    data: &UnivMonQData,
    delta: Option<f64>,
) -> Option<ErrorBound> {
    (data.heavy_set_empty? && config.ordered_samples > 0).then_some(())?;
    let sampled = (config.ordered_samples as f64).min(data.n? as f64);
    let delta = delta.unwrap_or(DEFAULT_DELTA);
    (sampled > 0.0 && delta > 0.0 && delta <= 1.0).then(|| ErrorBound {
        metric,
        epsilon: ((2.0 / delta).ln() / (2.0 * sampled)).sqrt(),
        delta: Some(delta),
        contract: SAMPLE_CONTRACT,
    })
}

/// ItemFrequency sizing: the odd `d` and power-of-two `W`, with no halving,
/// that meet `(ε, δ)` with the fewest counters `d·W`.
fn univmonq_item_params_for(epsilon: f64, delta: f64) -> Option<UnivMonQConfig> {
    if !(epsilon > 0.0) || !(delta > 0.0) {
        return None;
    }
    let mut best: Option<UnivMonQConfig> = None;
    for rows in (1..=63).step_by(2) {
        let target = max_row_failure(rows, delta) * epsilon * epsilon;
        if !(target > 0.0) || 1.0 / target > (1u64 << 32) as f64 {
            continue;
        }
        let config = UnivMonQConfig {
            rows,
            top_layer_cols: ((1.0 / target).ceil() as usize).next_power_of_two(),
            cols_halving_period: 0,
            ..UnivMonQConfig::default()
        };
        if config_is_valid(config)
            && best.is_none_or(|b| rows * config.top_layer_cols < b.rows * b.top_layer_cols)
        {
            best = Some(config);
        }
    }
    best
}

impl<H: SketchHasher> SketchAnalytics for UnivMonQ<H> {
    type Params = UnivMonQConfig;
    type Data = UnivMonQData;

    fn theoretical_memory_bytes(config: &UnivMonQConfig, _: &UnivMonQData) -> usize {
        UnivMonQ::<H>::theoretical_memory_bytes(*config)
    }

    fn theoretical_update_ops(config: &UnivMonQConfig, _: &UnivMonQData) -> f64 {
        let d = config.rows as f64;
        let k = config.heap_size as f64;
        let levels = floor_log2(config.heap_size);
        let sample = if config.ordered_samples > 0 {
            8.0 + 7.0 * floor_log2(config.ordered_samples)
        } else {
            0.0
        };
        18.0 + d + est_count_sketch(config.rows) + 7.0 * levels + k * (9.0 + 7.0 * levels) + sample
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        config: &UnivMonQConfig,
        _: &UnivMonQData,
    ) -> Option<f64> {
        (statistic == StatisticKind::ItemFrequency).then(|| 1.0 + est_count_sketch(config.rows))
    }

    fn theoretical_merge_ops(config: &UnivMonQConfig, _: &UnivMonQData) -> Option<f64> {
        let d = config.rows as f64;
        let k = config.heap_size as f64;
        let m = config.ordered_samples as f64;
        let counters: f64 = (0..config.layers)
            .map(|level| 2.0 * d * layer_cols(*config, level) as f64)
            .sum();
        let per_key = 15.0
            + 2.0 * est_count_sketch(config.rows)
            + 2.0 * lg(k)
            + 2.0 * floor_log2(config.heap_size);
        let layers = config.layers as f64 * (4.0 + k * per_key);
        let samples = m * (9.0 + 7.0 * floor_log2(config.ordered_samples));
        Some(6.0 + counters + layers + samples)
    }

    fn theoretical_error(
        statistic: StatisticKind,
        config: &UnivMonQConfig,
        data: &UnivMonQData,
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        match statistic {
            StatisticKind::ItemFrequency => count_sketch_error(
                config.rows,
                univmonq_collision_probability(config),
                delta,
                ITEM_CONTRACT,
            ),
            StatisticKind::Quantile => sample_error(ErrorMetric::Rank, config, data, delta),
            StatisticKind::Rank => sample_error(ErrorMetric::L1, config, data, delta),
            _ => None,
        }
    }

    fn params_for(
        statistic: StatisticKind,
        epsilon: f64,
        delta: f64,
        data: &UnivMonQData,
    ) -> Option<UnivMonQConfig> {
        match statistic {
            StatisticKind::ItemFrequency => univmonq_item_params_for(epsilon, delta),
            StatisticKind::Quantile | StatisticKind::Rank => {
                if !(epsilon > 0.0) || !(delta > 0.0) || data.heavy_set_empty == Some(false) {
                    return None;
                }
                let samples = ((2.0 / delta).ln() / (2.0 * epsilon * epsilon)).ceil();
                if !samples.is_finite() || samples > u32::MAX as f64 {
                    return None;
                }
                let samples = (samples as usize).max(1);
                data.n
                    .is_none_or(|n| n >= samples as u64)
                    .then(|| UnivMonQConfig {
                        ordered_samples: samples,
                        ..UnivMonQConfig::default()
                    })
            }
            _ => None,
        }
    }
}
