//! Analytics of the exponential histograms: ExponentialHistogram, its
//! EhBucketSketch, and the experimental ExponentialHistogramUnivMon.

use super::cardinality::HllParams;
use super::count_matrix::{L2_NORM_CONTRACT, MatrixParams, l2_norm_error};
use super::frameworks::{Cell, CellData};
use super::heavy_hitters::{CocoParams, ElasticParams};
use super::quantiles::{DdParams, KllParams};
use super::univmon::UnivMonParams;
use super::{ErrorBound, ErrorMetric, SketchAnalytics, StatisticKind, fixed_delta};
use crate::sketch_framework::eh::EhBucket;
use crate::{EhBucketSketch, ExponentialHistogram};

/// An exponential histogram bucket's sketch kind and parameters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EhCell {
    CountMin(MatrixParams),
    Coco(CocoParams),
    CountSketchWithL2Norm(MatrixParams),
    CountSketch(MatrixParams),
    DdSketch(DdParams),
    Elastic(ElasticParams),
    /// An `Ertl` HyperLogLog.
    Hll(HllParams),
    Kll(KllParams),
    #[cfg(feature = "experimental")]
    Uniform(super::UniformParams),
    UnivMon(UnivMonParams),
}

impl EhCell {
    fn cell(&self) -> Cell<'_> {
        match self {
            Self::CountMin(p) => Cell::CountMin(p),
            Self::Coco(p) => Cell::Coco(p),
            Self::CountSketchWithL2Norm(p) => Cell::CountSketchWithL2Norm(p),
            Self::CountSketch(p) => Cell::CountSketch(p),
            Self::DdSketch(p) => Cell::DdSketch(p),
            Self::Elastic(p) => Cell::Elastic(p),
            Self::Hll(p) => Cell::HllErtl(p),
            Self::Kll(p) => Cell::Kll(p),
            #[cfg(feature = "experimental")]
            Self::Uniform(p) => Cell::Uniform(p),
            Self::UnivMon(p) => Cell::UnivMon(p),
        }
    }

    /// Whether buckets merge by L2 mass rather than by arrival count.
    fn by_mass(&self) -> bool {
        matches!(self, Self::CountSketchWithL2Norm(_) | Self::UnivMon(_))
    }
}

impl Default for EhCell {
    fn default() -> Self {
        Self::CountMin(MatrixParams::default())
    }
}

impl SketchAnalytics for EhBucketSketch {
    type Params = EhCell;
    type Data = CellData;

    fn theoretical_memory_bytes(params: &EhCell, data: &CellData) -> usize {
        size_of::<Self>().saturating_add(params.cell().heap(data))
    }

    fn theoretical_update_ops(params: &EhCell, data: &CellData) -> f64 {
        params.cell().update(data)
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &EhCell,
        data: &CellData,
    ) -> Option<f64> {
        params.cell().query(statistic, data)
    }

    fn theoretical_merge_ops(params: &EhCell, data: &CellData) -> Option<f64> {
        params.cell().merge(data)
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &EhCell,
        data: &CellData,
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        params.cell().error(statistic, data, delta)
    }

    fn params_for(_: StatisticKind, _: f64, _: f64, _: &CellData) -> Option<EhCell> {
        None
    }
}

/// `k`, the window length and the bucket sketch of an ExponentialHistogram.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EhParams {
    pub k: usize,
    pub window: u64,
    pub cell: EhCell,
}

impl Default for EhParams {
    fn default() -> Self {
        Self {
            k: 2,
            window: 1000,
            cell: EhCell::default(),
        }
    }
}

/// The data inputs of ExponentialHistogram.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct EhData {
    /// `N_w`, the arrivals in the window.
    pub window_arrivals: Option<u64>,
    /// `D_w`, a lower bound on the distinct keys in the window.
    pub window_distinct: Option<u64>,
    /// `G`, the most buckets the histogram keeps; from `N_w` when unknown
    /// and merging by count.
    pub buckets: Option<usize>,
    /// Each bucket's inner data.
    pub cell: CellData,
}

const EH_COUNT_CONTRACT: &str = "eh_l1_boundary_bucket_v1";
const EH_MASS_CONTRACT: &str = "eh_l2_boundary_bucket_mass_v1";

/// `ε_EH = 2/(⌈k/2⌉ + 1)`, the most of the window the oldest bucket holds
/// when merging by count.
pub fn eh_count_epsilon(k: usize) -> f64 {
    2.0 / (k.max(1).div_ceil(2) + 1) as f64
}

/// `ε_EH2 = ((1 + ε₂)/(1 − ε₂))·√(2/k)`, the boundary error when merging by
/// mass with an inner L2 estimate off by `ε₂`.
pub fn eh_mass_epsilon(k: usize, l2_epsilon: f64) -> f64 {
    (1.0 + l2_epsilon) / (1.0 - l2_epsilon) * (2.0 / k.max(1) as f64).sqrt()
}

/// `G`: the given bucket count, or `(⌊k/2⌋ + 2)(⌊log2(2N_w)⌋ + 1)` when
/// merging by count; `None` when neither is known.
fn bucket_bound(params: &EhParams, data: &EhData) -> Option<usize> {
    data.buckets.or_else(|| {
        if params.cell.by_mass() {
            return None;
        }
        let arrivals = data.window_arrivals?.max(1);
        let levels = (2 * arrivals).ilog2() as usize + 1;
        Some((params.k / 2 + 2) * levels)
    })
}

/// The slots a bucket list reaches holding at most `G + 1` buckets.
fn bucket_slots(buckets: usize) -> usize {
    super::frameworks::vec_slots(0, buckets + 1)
}

/// FrequencyL2 merging by mass: `ε = ε_EH2 + ε₂(1 + ε_EH2)`,
/// `δ = (G + 2)·δ_mass + δ_in` with `δ_in = δ_mass` from layer 0.
fn mass_error(
    params: &EhParams,
    data: &EhData,
    layer0: &MatrixParams,
    delta: Option<f64>,
) -> Option<ErrorBound> {
    let mass = l2_norm_error(layer0, None, L2_NORM_CONTRACT)?;
    let boundary = eh_mass_epsilon(params.k, mass.epsilon);
    let buckets = bucket_bound(params, data)? as f64;
    Some(ErrorBound {
        metric: ErrorMetric::Relative,
        epsilon: boundary + mass.epsilon * (1.0 + boundary),
        delta: Some(fixed_delta((buckets + 3.0) * mass.delta?, delta)?),
        contract: EH_MASS_CONTRACT,
    })
}

impl SketchAnalytics for ExponentialHistogram {
    type Params = EhParams;
    type Data = EhData;

    fn theoretical_memory_bytes(params: &EhParams, data: &EhData) -> usize {
        let Some(buckets) = bucket_bound(params, data) else {
            return usize::MAX;
        };
        let heap = params.cell.cell().heap(&data.cell);
        size_of::<Self>()
            .saturating_add(bucket_slots(buckets).saturating_mul(size_of::<EhBucket>()))
            .saturating_add((buckets + 1).saturating_mul(heap))
    }

    /// Average per update.
    fn theoretical_update_ops(params: &EhParams, data: &EhData) -> f64 {
        let cell = params.cell.cell();
        let g = bucket_bound(params, data).map_or(f64::INFINITY, |g| g as f64);
        let copy = cell.cells(&data.cell);
        let merge = cell.merge(&data.cell).unwrap_or(f64::INFINITY);
        let base = 2.0 * copy + cell.update(&data.cell) + merge;
        if params.cell.by_mass() {
            let estimate = cell
                .query(StatisticKind::FrequencyL2, &data.cell)
                .unwrap_or(f64::INFINITY);
            base + 2.0 * estimate + 10.0 * g + 4.0
        } else {
            base + 8.0 * g + 4.0
        }
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &EhParams,
        data: &EhData,
    ) -> Option<f64> {
        let cell = params.cell.cell();
        let readout = cell.query(statistic, &data.cell)?;
        let g = bucket_bound(params, data)? as f64;
        let merge = cell.merge(&data.cell)?;
        Some(2.0 * g + 4.0 + cell.cells(&data.cell) + g * merge + readout)
    }

    fn theoretical_merge_ops(_: &EhParams, _: &EhData) -> Option<f64> {
        None
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &EhParams,
        data: &EhData,
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        let cell = params.cell.cell();
        match (&params.cell, statistic) {
            (EhCell::CountMin(_) | EhCell::CountSketch(_), StatisticKind::ItemFrequency) => {
                let boundary = eh_count_epsilon(params.k);
                let inner = cell.error(statistic, &data.cell, delta)?;
                Some(ErrorBound {
                    metric: ErrorMetric::L1,
                    epsilon: boundary + inner.epsilon * (1.0 + boundary),
                    delta: inner.delta,
                    contract: EH_COUNT_CONTRACT,
                })
            }
            (EhCell::Hll(_), StatisticKind::Distinct) => {
                let boundary = eh_count_epsilon(params.k);
                let inner = cell.error(statistic, &data.cell, None)?;
                let share = data.window_arrivals? as f64 / data.window_distinct?.max(1) as f64;
                Some(ErrorBound {
                    metric: ErrorMetric::Relative,
                    epsilon: inner.epsilon + (1.0 + inner.epsilon) * boundary * share,
                    delta: None,
                    contract: EH_COUNT_CONTRACT,
                })
            }
            (EhCell::CountSketchWithL2Norm(layer0), StatisticKind::FrequencyL2) => {
                mass_error(params, data, layer0, delta)
            }
            (EhCell::UnivMon(p), StatisticKind::FrequencyL2) => {
                let layer0 = MatrixParams {
                    rows: p.rows,
                    cols: p.cols,
                };
                mass_error(params, data, &layer0, delta)
            }
            _ => None,
        }
    }

    fn params_for(_: StatisticKind, _: f64, _: f64, _: &EhData) -> Option<EhParams> {
        None
    }
}

#[cfg(feature = "experimental")]
pub use eh_univmon::*;

#[cfg(feature = "experimental")]
mod eh_univmon {
    use super::*;
    use crate::common::heap::hash_map_bytes_after_inserts;
    use crate::sketch_framework::eh_univmon::{
        DEFAULT_POOL_INITIAL_SIZE, EhUnivMonExactBucket, EhUnivMonSketchBucket,
    };
    use crate::{ExponentialHistogramUnivMon, OwnedDataInput, UnivMon};

    /// `k`, the window length, the UnivMon shape and the pool's initial
    /// size of an ExponentialHistogramUnivMon.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct EhUnivMonParams {
        pub k: usize,
        pub window: u64,
        pub univmon: UnivMonParams,
        pub pool_initial_size: usize,
    }

    impl Default for EhUnivMonParams {
        fn default() -> Self {
            Self {
                k: 2,
                window: 1000,
                univmon: UnivMonParams {
                    rows: 5,
                    cols: 2048,
                    layers: 8,
                    ..UnivMonParams::default()
                },
                pool_initial_size: DEFAULT_POOL_INITIAL_SIZE,
            }
        }
    }

    /// The data inputs of ExponentialHistogramUnivMon.
    #[derive(Debug, Clone, Copy, Default, PartialEq)]
    pub struct EhUnivMonData {
        /// `G_s`, the most UnivMon buckets the histogram holds.
        pub sketch_buckets: Option<usize>,
        /// `G_e`, the most exact buckets it holds.
        pub exact_buckets: Option<usize>,
        /// `K`, the most keys its exact buckets hold together.
        pub exact_keys: Option<usize>,
        /// Bytes each key owns.
        pub key_bytes: Option<usize>,
        /// Whether a query reads only exact buckets.
        pub exact_query: Option<bool>,
        /// Each UnivMon bucket's inner data.
        pub flow: crate::analytics::FlowData,
    }

    const EXACT_CONTRACT: &str = "eh_univmon_exact_boundary_bucket_v1";
    const SKETCH_CONTRACT: &str = "eh_univmon_l2_boundary_bucket_v1";

    fn inner(data: &EhUnivMonData) -> CellData {
        CellData {
            flow: data.flow,
            ..CellData::default()
        }
    }

    /// `(G_s, G_e, K)`, infinite where unknown.
    fn shape(data: &EhUnivMonData) -> (f64, f64, f64) {
        let known = |v: Option<usize>| v.map_or(f64::INFINITY, |v| v as f64);
        (
            known(data.sketch_buckets),
            known(data.exact_buckets),
            known(data.exact_keys),
        )
    }

    impl SketchAnalytics for ExponentialHistogramUnivMon {
        type Params = EhUnivMonParams;
        type Data = EhUnivMonData;

        fn theoretical_memory_bytes(params: &EhUnivMonParams, data: &EhUnivMonData) -> usize {
            let (Some(sketches), Some(exact), Some(keys)) =
                (data.sketch_buckets, data.exact_buckets, data.exact_keys)
            else {
                return usize::MAX;
            };
            let cell = Cell::UnivMon(&params.univmon);
            let heap = cell.heap(&inner(data));
            let pooled = params.pool_initial_size.max(sketches + 1);
            let free_slots = super::super::frameworks::vec_slots(params.pool_initial_size, pooled);
            let map = hash_map_bytes_after_inserts::<(OwnedDataInput, i64)>(keys)
                .saturating_add(keys.saturating_mul(data.key_bytes.unwrap_or(0)));
            size_of::<Self>()
                .saturating_add(
                    bucket_slots(sketches).saturating_mul(size_of::<EhUnivMonSketchBucket>()),
                )
                .saturating_add(
                    bucket_slots(exact).saturating_mul(size_of::<EhUnivMonExactBucket>()),
                )
                .saturating_add(free_slots.saturating_mul(size_of::<UnivMon>()))
                .saturating_add(pooled.saturating_mul(heap))
                .saturating_add(exact.saturating_mul(map))
        }

        /// Average per update over the hashes.
        fn theoretical_update_ops(params: &EhUnivMonParams, data: &EhUnivMonData) -> f64 {
            let cells = inner(data);
            let cell = Cell::UnivMon(&params.univmon);
            let (gs, ge, k) = shape(data);
            let copy = cell.cells(&cells);
            let update = cell.update(&cells);
            let merge = cell.merge(&cells).unwrap_or(f64::INFINITY);
            let e = crate::analytics::est_count_sketch(params.univmon.rows);
            k * (1.0 + update + 2.0 * ge)
                + 2.0 * ge * ge
                + 10.0 * ge
                + 2.0 * gs * gs
                + gs * (2.0 * copy + merge + 3.0 * e + 7.0)
                + 3.0 * e
                + 6.0
        }

        fn theoretical_query_ops(
            statistic: StatisticKind,
            params: &EhUnivMonParams,
            data: &EhUnivMonData,
        ) -> Option<f64> {
            let cells = inner(data);
            let cell = Cell::UnivMon(&params.univmon);
            let readout = cell.query(statistic, &cells)?;
            let (gs, ge, k) = shape(data);
            let merge = cell.merge(&cells)?;
            let copy = cell.cells(&cells);
            Some(
                2.0 * (gs + ge)
                    + 6.0
                    + copy
                    + gs * (1.0 + merge)
                    + ge
                    + k * (3.0 + cell.update(&cells))
                    + readout,
            )
        }

        fn theoretical_merge_ops(_: &EhUnivMonParams, _: &EhUnivMonData) -> Option<f64> {
            None
        }

        fn theoretical_error(
            statistic: StatisticKind,
            params: &EhUnivMonParams,
            data: &EhUnivMonData,
            delta: Option<f64>,
        ) -> Option<ErrorBound> {
            (statistic == StatisticKind::FrequencyL2).then_some(())?;
            if data.exact_query? {
                return Some(ErrorBound {
                    metric: ErrorMetric::Relative,
                    epsilon: (2.0 / params.k.max(1) as f64).sqrt(),
                    delta: Some(fixed_delta(0.0, delta)?),
                    contract: EXACT_CONTRACT,
                });
            }
            let layer0 = MatrixParams {
                rows: params.univmon.rows,
                cols: params.univmon.cols,
            };
            let mass = l2_norm_error(&layer0, None, L2_NORM_CONTRACT)?;
            let boundary = eh_mass_epsilon(params.k, mass.epsilon);
            let buckets = data.sketch_buckets? as f64;
            Some(ErrorBound {
                metric: ErrorMetric::Relative,
                epsilon: boundary + mass.epsilon * (1.0 + boundary),
                delta: Some(fixed_delta((buckets + 3.0) * mass.delta?, delta)?),
                contract: SKETCH_CONTRACT,
            })
        }

        fn params_for(
            _: StatisticKind,
            _: f64,
            _: f64,
            _: &EhUnivMonData,
        ) -> Option<EhUnivMonParams> {
            None
        }
    }
}
