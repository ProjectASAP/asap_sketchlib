//! Analytics of the heavy-hitter sketches: SpaceSaving, ElasticSketch and
//! CocoSketch.

use super::count_matrix::{MatrixParams, count_min_error, count_min_params_for};
use super::{ErrorBound, ErrorMetric, SketchAnalytics, StatisticKind, fixed_delta, sort_ops};
use crate::sketches::coco::{CocoBucket, DEFAULT_DEPTH, DEFAULT_WIDTH};
use crate::sketches::elastic::{DEFAULT_LIGHT_COLS, DEFAULT_LIGHT_ROWS, HeavyBucket};
use crate::{
    CocoSketch, DefaultXxHasher, ElasticSketch, SPACE_SAVING_DEFAULT_CAPACITY, SketchHasher,
    SpaceSaving,
};

/// `m`, the counters of a SpaceSaving summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpaceSavingParams {
    pub capacity: usize,
}

impl Default for SpaceSavingParams {
    fn default() -> Self {
        Self {
            capacity: SPACE_SAVING_DEFAULT_CAPACITY,
        }
    }
}

/// SpaceSaving's data inputs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SpaceSavingData {
    /// Bytes each key owns: a string's or byte slice's length, 0 for a
    /// number.
    pub key_bytes: Option<usize>,
    /// `k`, the TopK size. Unknown counts as `m`.
    pub top_k: Option<usize>,
}

const SPACE_SAVING_CONTRACT: &str = "space_saving_min_counter_v1";

impl<H: SketchHasher> SketchAnalytics for SpaceSaving<H> {
    type Params = SpaceSavingParams;
    type Data = SpaceSavingData;

    fn theoretical_memory_bytes(
        params: &SpaceSavingParams,
        data: &SpaceSavingData,
    ) -> Option<usize> {
        Some(SpaceSaving::<DefaultXxHasher>::theoretical_memory_bytes(
            params.capacity,
            data.key_bytes?,
        ))
    }

    fn theoretical_update_ops(_: &SpaceSavingParams, _: &SpaceSavingData) -> f64 {
        47.0
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &SpaceSavingParams,
        data: &SpaceSavingData,
    ) -> Option<f64> {
        let m = params.capacity.max(1);
        match statistic {
            StatisticKind::ItemFrequency => Some(4.0),
            StatisticKind::TopK => Some(1.0 + 7.0 * data.top_k.unwrap_or(m).min(m) as f64),
            _ => None,
        }
    }

    fn theoretical_merge_ops(params: &SpaceSavingParams, _: &SpaceSavingData) -> Option<f64> {
        let m = params.capacity.max(1) as f64;
        Some(44.0 * m + sort_ops(2.0 * m) + 13.0)
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &SpaceSavingParams,
        _: &SpaceSavingData,
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        matches!(
            statistic,
            StatisticKind::ItemFrequency | StatisticKind::TopK
        )
        .then_some(())?;
        Some(ErrorBound {
            metric: ErrorMetric::L1,
            epsilon: 1.0 / params.capacity.max(1) as f64,
            delta: Some(fixed_delta(0.0, delta)?),
            contract: SPACE_SAVING_CONTRACT,
        })
    }

    fn params_for(
        statistic: StatisticKind,
        epsilon: f64,
        _: f64,
        _: &SpaceSavingData,
    ) -> Option<SpaceSavingParams> {
        matches!(
            statistic,
            StatisticKind::ItemFrequency | StatisticKind::TopK
        )
        .then_some(())?;
        let capacity = (1.0 / epsilon).ceil();
        (epsilon > 0.0 && capacity <= u32::MAX as f64).then_some(SpaceSavingParams {
            capacity: capacity.max(1.0) as usize,
        })
    }
}

/// `H` heavy buckets over a light Count-Min of `d` rows and `w` columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ElasticParams {
    pub bucket_count: usize,
    pub light_rows: usize,
    pub light_cols: usize,
}

impl Default for ElasticParams {
    fn default() -> Self {
        Self {
            bucket_count: 8,
            light_rows: DEFAULT_LIGHT_ROWS,
            light_cols: DEFAULT_LIGHT_COLS,
        }
    }
}

/// `D` and the key bytes, the data inputs of ElasticSketch, CocoSketch and
/// the matrices with a heap or a Space-Saving summary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FlowData {
    /// `D`, the distinct keys.
    pub distinct: Option<u64>,
    /// Bytes each key owns: a string's or byte slice's length, 0 for a
    /// number, and for ElasticSketch the capacity of the `String` passed in.
    pub key_bytes: Option<usize>,
}

/// Light rows that bound an Elastic estimate: light row 5 hashes with the
/// heavy part's seed, so rows past the fifth add nothing to `δ`.
const ELASTIC_INDEPENDENT_ROWS: usize = 5;

const ELASTIC_CONTRACT: &str = "elastic_one_sided_light_cms_v1";

fn elastic_light(params: &ElasticParams) -> MatrixParams {
    MatrixParams {
        rows: params.light_rows.min(ELASTIC_INDEPENDENT_ROWS),
        cols: params.light_cols,
    }
}

fn elastic_sized(light: MatrixParams) -> ElasticParams {
    ElasticParams {
        light_rows: light.rows,
        light_cols: light.cols,
        ..ElasticParams::default()
    }
}

impl<H: SketchHasher> SketchAnalytics for ElasticSketch<H> {
    type Params = ElasticParams;
    type Data = FlowData;

    fn theoretical_memory_bytes(params: &ElasticParams, data: &FlowData) -> Option<usize> {
        let bucket = size_of::<HeavyBucket>().saturating_add(data.key_bytes?);
        Some(
            size_of::<Self>()
                .saturating_add(params.bucket_count.saturating_mul(bucket))
                .saturating_add(
                    (params.light_rows.saturating_mul(params.light_cols))
                        .saturating_mul(size_of::<i32>()),
                ),
        )
    }

    fn theoretical_update_ops(params: &ElasticParams, _: &FlowData) -> f64 {
        2.0 + params.light_rows as f64
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &ElasticParams,
        _: &FlowData,
    ) -> Option<f64> {
        let d = params.light_rows as f64;
        let h = params.bucket_count as f64;
        match statistic {
            StatisticKind::ItemFrequency => Some(1.0 + d),
            StatisticKind::TopK => Some(h * (5.0 + d) + 2.0 * sort_ops(h)),
            _ => None,
        }
    }

    fn theoretical_merge_ops(params: &ElasticParams, _: &FlowData) -> Option<f64> {
        let d = params.light_rows as f64;
        let h = params.bucket_count as f64;
        Some(h * (13.0 + 3.0 * d) + 2.0 * d * params.light_cols as f64)
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &ElasticParams,
        data: &FlowData,
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        let light = elastic_light(params);
        match statistic {
            StatisticKind::ItemFrequency => count_min_error(&light, false, delta, ELASTIC_CONTRACT),
            StatisticKind::TopK => {
                let keys = data.distinct? as f64;
                let per_key = count_min_error(&light, false, None, ELASTIC_CONTRACT)?;
                let fixed = keys * per_key.delta?;
                (fixed < 1.0).then_some(())?;
                Some(ErrorBound {
                    delta: Some(fixed_delta(fixed, delta)?),
                    ..per_key
                })
            }
            _ => None,
        }
    }

    fn params_for(
        statistic: StatisticKind,
        epsilon: f64,
        delta: f64,
        data: &FlowData,
    ) -> Option<ElasticParams> {
        let per_key = match statistic {
            StatisticKind::ItemFrequency => delta,
            StatisticKind::TopK => delta / data.distinct?.max(1) as f64,
            _ => return None,
        };
        count_min_params_for(epsilon, per_key, false, ELASTIC_INDEPENDENT_ROWS).map(elastic_sized)
    }
}

/// `d` rows and `w` columns of a CocoSketch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CocoParams {
    pub rows: usize,
    pub cols: usize,
}

impl Default for CocoParams {
    fn default() -> Self {
        Self {
            rows: DEFAULT_DEPTH,
            cols: DEFAULT_WIDTH,
        }
    }
}

impl<H: SketchHasher> SketchAnalytics for CocoSketch<H> {
    type Params = CocoParams;
    type Data = FlowData;

    fn theoretical_memory_bytes(params: &CocoParams, data: &FlowData) -> Option<usize> {
        let cells = params.rows.saturating_mul(params.cols);
        let held = data.distinct.map_or(cells, |d| {
            cells.min(usize::try_from(d).unwrap_or(usize::MAX))
        });
        Some(
            size_of::<Self>()
                .saturating_add(cells.saturating_mul(size_of::<CocoBucket>()))
                .saturating_add(held.saturating_mul(data.key_bytes?)),
        )
    }

    fn theoretical_update_ops(params: &CocoParams, _: &FlowData) -> f64 {
        params.rows as f64 + 1.0
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &CocoParams,
        _: &FlowData,
    ) -> Option<f64> {
        match statistic {
            StatisticKind::ItemFrequency => Some(params.rows as f64),
            StatisticKind::PartialKeySum => Some((params.rows * params.cols) as f64),
            _ => None,
        }
    }

    fn theoretical_merge_ops(params: &CocoParams, _: &FlowData) -> Option<f64> {
        Some((params.rows * params.cols * (params.rows + 2)) as f64)
    }

    fn theoretical_error(
        _: StatisticKind,
        _: &CocoParams,
        _: &FlowData,
        _: Option<f64>,
    ) -> Option<ErrorBound> {
        None
    }

    fn params_for(_: StatisticKind, _: f64, _: f64, _: &FlowData) -> Option<CocoParams> {
        None
    }
}
