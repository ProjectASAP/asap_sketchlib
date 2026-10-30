//! Analytics of Hydra, a grid of cells over every subpopulation of a key
//! schema.

use super::cardinality::HllParams;
use super::count_matrix::{MatrixParams, collision_probability};
use super::frameworks::{Cell, CellData, split_delta};
use super::quantiles::KllParams;
use super::univmon::UnivMonParams;
use super::{
    ErrorBound, ErrorMetric, SketchAnalytics, StatisticKind, is_positive, p_med, sort_ops,
};
use crate::Hydra;
use crate::input::HydraCounter;

/// A Hydra cell's kind and parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HydraCell {
    CountMin(MatrixParams),
    CountSketch(MatrixParams),
    /// An `Ertl` HyperLogLog.
    Hll(HllParams),
    Kll(KllParams),
    UnivMon(UnivMonParams),
}

impl HydraCell {
    fn cell(&self) -> Cell<'_> {
        match self {
            Self::CountMin(p) => Cell::CountMin(p),
            Self::CountSketch(p) => Cell::CountSketch(p),
            Self::Hll(p) => Cell::HllErtl(p),
            Self::Kll(p) => Cell::Kll(p),
            Self::UnivMon(p) => Cell::UnivMon(p),
        }
    }
}

/// `d` rows, `w` columns and schema arity `a` of a Hydra, and its cells.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HydraParams {
    pub rows: usize,
    pub cols: usize,
    pub arity: usize,
    pub cell: HydraCell,
}

impl Default for HydraParams {
    fn default() -> Self {
        Self {
            rows: 3,
            cols: 1024,
            arity: 1,
            cell: HydraCell::CountMin(MatrixParams::default()),
        }
    }
}

/// The data inputs of Hydra.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct HydraData {
    /// `n`, the updates.
    pub n: Option<u64>,
    /// `N`, their total weight.
    pub total_weight: Option<u64>,
    /// The schema labels' total length in bytes, escapes included.
    pub label_bytes: Option<usize>,
    /// A lower bound on `N_Q`, the queried subpopulation's weight.
    pub group_weight: Option<u64>,
    /// A lower bound on `D_Q`, its distinct values.
    pub group_distinct: Option<u64>,
    /// A lower bound on `‖f_Q‖₂`.
    pub group_l2: Option<f64>,
    /// The schema arity [`SketchAnalytics::params_for`] sizes for; 1 when
    /// unknown.
    pub arity: Option<usize>,
    /// Each cell's inner data.
    pub cell: CellData,
}

const CM_CONTRACT: &str = "hydra_cm_cell_markov_median_binomial_v1";
const CM_TOTAL_CONTRACT: &str = "hydra_cm_group_total_markov_min_v1";
const CS_CONTRACT: &str = "hydra_cs_cell_markov_median_binomial_v1";
const KLL_CONTRACT: &str = "hydra_kll_cell_mixture_rank_median_binomial_v1";
const HLL_CONTRACT: &str = "hydra_hll_cell_collision_median_binomial_v1";
const UM_TOTAL_CONTRACT: &str = "hydra_univmon_total_markov_median_binomial_v1";
const UM_L2_CONTRACT: &str = "hydra_univmon_l2_collision_median_binomial_v1";
const UM_DISTINCT_CONTRACT: &str = "hydra_univmon_distinct_collision_median_binomial_v1";

/// `2^a − 1`, the subkeys one update writes.
fn subkeys(arity: usize) -> f64 {
    ((1u64 << arity.min(63)) - 1) as f64
}

/// `4(2^a − 1)/w`: the colliding weight, per unit of `N`, one row's cell
/// holds with probability at least `1 − q·w/4`.
fn collision_share(params: &HydraParams) -> f64 {
    4.0 * subkeys(params.arity) / params.cols as f64
}

/// `q·w/4`, one row's collision failure probability.
fn row_failure(params: &HydraParams) -> f64 {
    collision_probability(params.cols, true) * params.cols as f64 / 4.0
}

/// A median over rows of one cell each: `P_med(d, q·w/4) + d·δ_in`, with
/// `ε` built from the inner bound by `epsilon`.
fn median_bound(
    statistic: StatisticKind,
    params: &HydraParams,
    data: &HydraData,
    delta: Option<f64>,
    metric: ErrorMetric,
    contract: &'static str,
    epsilon: impl Fn(f64) -> Option<f64>,
) -> Option<ErrorBound> {
    let cell = params.cell.cell();
    let inner = |delta| cell.error(statistic, &data.cell, delta);
    let own = p_med(params.rows, row_failure(params));
    let (bound, total) = split_delta(own, params.rows as f64, delta, inner)?;
    Some(ErrorBound {
        metric,
        epsilon: epsilon(bound.epsilon)?,
        delta: Some(total),
        contract,
    })
}

/// The HyperLogLog Distinct bound, which has no `δ`.
fn hll_distinct(params: &HydraParams, data: &HydraData) -> Option<ErrorBound> {
    let inner = params
        .cell
        .cell()
        .error(StatisticKind::Distinct, &data.cell, None)?;
    let collided = collision_share(params) * data.n? as f64 / data.group_distinct?.max(1) as f64;
    Some(ErrorBound {
        metric: ErrorMetric::Relative,
        epsilon: inner.epsilon + (1.0 + inner.epsilon) * collided,
        delta: None,
        contract: HLL_CONTRACT,
    })
}

/// `C_total`: the most ops one cell's group total costs, a Count-Min row-0
/// sum or a KLL's per-level count.
fn total_mass_ops(cell: &HydraCell, data: &HydraData) -> Option<f64> {
    match cell {
        HydraCell::CountMin(p) => Some(p.cols as f64),
        HydraCell::Kll(p) => {
            let levels = super::quantiles::kll_levels(p, data.cell.stream.n);
            Some(2.0 * levels as f64)
        }
        _ => None,
    }
}

impl SketchAnalytics for Hydra {
    type Params = HydraParams;
    type Data = HydraData;

    fn theoretical_memory_bytes(params: &HydraParams, data: &HydraData) -> Option<usize> {
        let cells = params.rows * params.cols;
        let heap = params.cell.cell().heap(&data.cell)?;
        let labels = (2 * params.arity * size_of::<String>())
            .saturating_add(data.label_bytes?.saturating_mul(2));
        Some(
            size_of::<Self>()
                .saturating_add(cells.saturating_mul(size_of::<HydraCounter>()))
                .saturating_add((cells + 1).saturating_mul(heap))
                .saturating_add(labels),
        )
    }

    fn theoretical_update_ops(params: &HydraParams, data: &HydraData) -> f64 {
        let inner = params.cell.cell().update(&data.cell);
        subkeys(params.arity) * params.rows as f64 * (1.0 + inner)
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &HydraParams,
        data: &HydraData,
    ) -> Option<f64> {
        let d = params.rows as f64;
        let cell = params.cell.cell();
        let readout = match (&params.cell, statistic) {
            (HydraCell::CountMin(_) | HydraCell::Kll(_), StatisticKind::Total) => {
                let mass = total_mass_ops(&params.cell, data)?;
                return Some(d + (d + 1.0) * mass);
            }
            (HydraCell::Kll(_), StatisticKind::Rank) => {
                cell.query(StatisticKind::Quantile, &data.cell)?
            }
            (HydraCell::UnivMon(_), StatisticKind::FrequencyEntropy)
            | (HydraCell::UnivMon(_), StatisticKind::Total)
            | (HydraCell::UnivMon(_), StatisticKind::FrequencyL2)
            | (HydraCell::UnivMon(_), StatisticKind::Distinct)
            | (HydraCell::CountMin(_) | HydraCell::CountSketch(_), StatisticKind::ItemFrequency)
            | (HydraCell::Hll(_), StatisticKind::Distinct)
            | (HydraCell::Kll(_), StatisticKind::Quantile) => cell.query(statistic, &data.cell)?,
            _ => return None,
        };
        Some((d + 1.0) * readout + 2.0 * d + sort_ops(d))
    }

    fn theoretical_merge_ops(params: &HydraParams, data: &HydraData) -> Option<f64> {
        let merge = params.cell.cell().merge(&data.cell)?;
        Some((params.rows * params.cols) as f64 * (2.0 + merge))
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &HydraParams,
        data: &HydraData,
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        let share = collision_share(params);
        match (&params.cell, statistic) {
            (HydraCell::CountMin(_), StatisticKind::ItemFrequency) => median_bound(
                statistic,
                params,
                data,
                delta,
                ErrorMetric::L1,
                CM_CONTRACT,
                |inner| Some(share * (1.0 + inner) + inner),
            ),
            (HydraCell::CountSketch(_), StatisticKind::ItemFrequency) => median_bound(
                statistic,
                params,
                data,
                delta,
                ErrorMetric::L1,
                CS_CONTRACT,
                |inner| Some(share * (1.0 + inner) + inner),
            ),
            (HydraCell::CountMin(_), StatisticKind::Total) => Some(ErrorBound {
                metric: ErrorMetric::L1,
                epsilon: share,
                delta: Some(super::fixed_delta(
                    row_failure(params).powi(params.rows as i32),
                    delta,
                )?),
                contract: CM_TOTAL_CONTRACT,
            }),
            (HydraCell::Kll(_), StatisticKind::Quantile | StatisticKind::Rank) => {
                let mixed = share * data.total_weight? as f64 / data.group_weight?.max(1) as f64;
                let metric = if statistic == StatisticKind::Quantile {
                    ErrorMetric::Rank
                } else {
                    ErrorMetric::ABSOLUTE
                };
                median_bound(
                    statistic,
                    params,
                    data,
                    delta,
                    metric,
                    KLL_CONTRACT,
                    |inner| Some(mixed + inner),
                )
            }
            (HydraCell::Hll(_), StatisticKind::Distinct) => hll_distinct(params, data),
            (HydraCell::UnivMon(_), StatisticKind::Total) => Some(ErrorBound {
                metric: ErrorMetric::L1,
                epsilon: share,
                delta: Some(super::fixed_delta(
                    p_med(params.rows, row_failure(params)),
                    delta,
                )?),
                contract: UM_TOTAL_CONTRACT,
            }),
            (HydraCell::UnivMon(_), StatisticKind::FrequencyL2) => {
                let collided = share * data.total_weight? as f64 / data.group_l2?;
                median_bound(
                    statistic,
                    params,
                    data,
                    delta,
                    ErrorMetric::Relative,
                    UM_L2_CONTRACT,
                    |inner| Some(inner + (1.0 + inner) * collided),
                )
            }
            (HydraCell::UnivMon(_), StatisticKind::Distinct) => {
                let collided = share * data.n? as f64 / data.group_distinct?.max(1) as f64;
                median_bound(
                    statistic,
                    params,
                    data,
                    delta,
                    ErrorMetric::Relative,
                    UM_DISTINCT_CONTRACT,
                    |inner| Some(inner + (1.0 + inner) * collided),
                )
            }
            _ => None,
        }
    }

    fn params_for(
        statistic: StatisticKind,
        epsilon: f64,
        delta: f64,
        data: &HydraData,
    ) -> Option<HydraParams> {
        if !is_positive(epsilon) || !is_positive(delta) {
            return None;
        }
        let half = epsilon / 2.0;
        let inner_epsilon = half / (1.0 + half);
        let rows = (1..=99)
            .step_by(2)
            .find(|&d| p_med(d, 0.25) <= delta / 2.0)?;
        let inner_delta = delta / (2.0 * rows as f64);
        let arity = data.arity.unwrap_or(1);
        let cell = match statistic {
            StatisticKind::ItemFrequency => {
                HydraCell::CountMin(<super::frameworks::CmCell as SketchAnalytics>::params_for(
                    statistic,
                    inner_epsilon,
                    inner_delta,
                    &(),
                )?)
            }
            StatisticKind::Quantile | StatisticKind::Rank => {
                HydraCell::Kll(<crate::Kll as SketchAnalytics>::params_for(
                    statistic,
                    inner_epsilon,
                    inner_delta,
                    &data.cell.stream,
                )?)
            }
            StatisticKind::Distinct => HydraCell::Hll(
                <crate::HyperLogLog<crate::Ertl> as SketchAnalytics>::params_for(
                    statistic,
                    inner_epsilon,
                    inner_delta,
                    &data.cell.hll,
                )?,
            ),
            StatisticKind::FrequencyL2 => {
                HydraCell::UnivMon(<crate::UnivMon as SketchAnalytics>::params_for(
                    statistic,
                    inner_epsilon,
                    inner_delta,
                    &data.cell.flow,
                )?)
            }
            _ => return None,
        };
        let realized = cell
            .cell()
            .error(statistic, &data.cell, Some(inner_delta))?
            .epsilon;
        // The collision term at `w = 1`, which halves with each doubling.
        let at_one = 4.0
            * subkeys(arity)
            * match statistic {
                StatisticKind::ItemFrequency => 1.0 + realized,
                StatisticKind::Quantile | StatisticKind::Rank => {
                    data.total_weight? as f64 / data.group_weight?.max(1) as f64
                }
                StatisticKind::Distinct => {
                    (1.0 + realized) * data.n? as f64 / data.group_distinct?.max(1) as f64
                }
                StatisticKind::FrequencyL2 => {
                    (1.0 + realized) * data.total_weight? as f64 / data.group_l2?
                }
                _ => return None,
            };
        let cols = (at_one / half).ceil().max(1.0);
        (cols <= (1u64 << 32) as f64).then(|| HydraParams {
            rows,
            cols: (cols as usize).next_power_of_two(),
            arity,
            cell,
        })
    }
}
