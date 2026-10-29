//! Analytics of the counter matrices with top-k: Count-Min and Count Sketch
//! with a heap, with a Space-Saving summary, and folded.

use super::count_matrix::{
    COUNT_MIN_CONTRACT, COUNT_SKETCH_CONTRACT, MatrixParams, TWO_POW_MINUS_32,
    collision_probability, count_min_error, count_min_params_for, count_sketch_error,
    count_sketch_params_for,
};
use super::heavy_hitters::FlowData;
use super::{
    DEFAULT_DELTA, ErrorBound, HashPath, SketchAnalytics, StatisticKind, est_count_min,
    est_count_sketch, fixed_delta, heap_update_ops, max_row_failure, sort_ops,
};
use crate::common::heap::DEFAULT_HEAP_SIZE;
use crate::common::structures::fixed_structure::{DEFAULT_COLS, DEFAULT_ROWS};
use crate::count_min_fold::MAX_FULL_COLS;
use crate::{
    CountMinSketchFold, CountMinSketchWithHeap, CountMinSketchWithSpaceSaving, CountSketchFold,
    CountSketchWithHeap, CountSketchWithSpaceSaving, DataInput, FoldCell, FoldEntry, HeapCount,
    HeapCounter, MATRIX_MAX_ROWS, MatrixStorage, SPACE_SAVING_DEFAULT_CAPACITY, SketchHasher,
    SpaceSaving, TopKHeap,
};

/// `d` rows, `w` columns and heap capacity `k`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MatrixHeapParams {
    pub rows: usize,
    pub cols: usize,
    pub heap_size: usize,
}

impl Default for MatrixHeapParams {
    fn default() -> Self {
        Self {
            rows: DEFAULT_ROWS,
            cols: DEFAULT_COLS,
            heap_size: DEFAULT_HEAP_SIZE,
        }
    }
}

/// `d` rows, `w` columns and `m` Space-Saving counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MatrixSpaceSavingParams {
    pub rows: usize,
    pub cols: usize,
    pub capacity: usize,
}

impl Default for MatrixSpaceSavingParams {
    fn default() -> Self {
        Self {
            rows: DEFAULT_ROWS,
            cols: DEFAULT_COLS,
            capacity: SPACE_SAVING_DEFAULT_CAPACITY,
        }
    }
}

/// `d` rows, full width `W`, fold level `F` and heap capacity `k`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FoldParams {
    pub rows: usize,
    pub full_cols: usize,
    pub fold_level: u32,
    pub heap_size: usize,
}

impl Default for FoldParams {
    fn default() -> Self {
        Self {
            rows: DEFAULT_ROWS,
            full_cols: DEFAULT_COLS,
            fold_level: 3,
            heap_size: DEFAULT_HEAP_SIZE,
        }
    }
}

/// The data inputs of CountMinSketchFold and CountSketchFold.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FoldData {
    /// `D`, the distinct keys.
    pub distinct: Option<u64>,
    /// `E`, the most entries one cell holds. Unknown counts as
    /// `min(2^F, D)`.
    pub max_cell_entries: Option<usize>,
    /// `S`, the entries in cells holding more than one. Unknown counts as
    /// `d·min(W, D)`.
    pub collided_entries: Option<usize>,
    /// Bytes each key owns: a string's or byte slice's length, 0 for a
    /// number. Unknown counts as 0.
    pub key_bytes: Option<usize>,
}

/// The base sketch of a top-k matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Base {
    CountMin,
    CountSketch,
}

impl Base {
    fn est(self, rows: usize) -> f64 {
        match self {
            Base::CountMin => est_count_min(rows),
            Base::CountSketch => est_count_sketch(rows),
        }
    }
}

/// The base sketch's ItemFrequency bound.
fn item_error(
    base: Base,
    shape: MatrixParams,
    fast_path: bool,
    delta: Option<f64>,
) -> Option<ErrorBound> {
    match base {
        Base::CountMin => count_min_error(&shape, fast_path, delta, COUNT_MIN_CONTRACT),
        Base::CountSketch => count_sketch_error(
            shape.rows,
            collision_probability(shape.cols, fast_path),
            delta,
            COUNT_SKETCH_CONTRACT,
        ),
    }
}

/// Each reported count as ItemFrequency, with the base sketch's `δ` times
/// the `keys` that could be reported; `None` without `keys` or when that
/// product is at least 1.
fn top_k_error(
    base: Base,
    shape: MatrixParams,
    fast_path: bool,
    keys: Option<f64>,
    delta: Option<f64>,
) -> Option<ErrorBound> {
    let keys = keys?.max(1.0);
    match base {
        Base::CountMin => {
            let per_key = item_error(base, shape, fast_path, None)?;
            let total = keys * per_key.delta?;
            (total < 1.0).then_some(())?;
            Some(ErrorBound {
                delta: Some(fixed_delta(total, delta)?),
                ..per_key
            })
        }
        Base::CountSketch => {
            let total = delta.unwrap_or(DEFAULT_DELTA);
            let per_key = item_error(base, shape, fast_path, Some(total / keys))?;
            Some(ErrorBound {
                delta: Some(total),
                ..per_key
            })
        }
    }
}

/// The base sketch's ItemFrequency sizing.
fn item_params(base: Base, epsilon: f64, delta: f64, fast_path: bool) -> Option<MatrixParams> {
    match base {
        Base::CountMin => count_min_params_for(epsilon, delta, fast_path, MATRIX_MAX_ROWS),
        Base::CountSketch => count_sketch_params_for(epsilon, delta, fast_path),
    }
}

/// `params_for` of a top-k matrix: ItemFrequency as the base sketch, TopK as
/// ItemFrequency at `δ/keys`.
fn top_k_params(
    base: Base,
    statistic: StatisticKind,
    epsilon: f64,
    delta: f64,
    fast_path: bool,
    keys: Option<f64>,
) -> Option<MatrixParams> {
    match statistic {
        StatisticKind::ItemFrequency => item_params(base, epsilon, delta, fast_path),
        StatisticKind::TopK => item_params(base, epsilon, delta / keys?.max(1.0), fast_path),
        _ => None,
    }
}

/// [`TopKHeap::theoretical_memory_bytes`] for keys owning `key_bytes` bytes.
fn heap_memory<C: HeapCount>(heap_size: usize, key_bytes: Option<usize>) -> usize {
    let key = vec![0u8; key_bytes.unwrap_or(0)];
    TopKHeap::<C>::theoretical_memory_bytes(heap_size, &DataInput::Bytes(&key))
}

fn distinct(data: Option<u64>) -> Option<f64> {
    data.map(|d| d as f64)
}

/// Keys a structure of `capacity` holds: `min(capacity, D)`.
fn held(capacity: usize, distinct: Option<u64>) -> f64 {
    distinct.map_or(capacity, |d| capacity.min(d as usize)) as f64
}

type HeapCountOf<S> = <<S as MatrixStorage>::Counter as HeapCounter>::HeapCount;

macro_rules! matrix_with_heap {
    ($sketch:ident, $base:expr) => {
        impl<S: MatrixStorage, Mode: HashPath, H: SketchHasher> SketchAnalytics
            for $sketch<S, Mode, H>
        where
            S::Counter: HeapCounter,
        {
            type Params = MatrixHeapParams;
            type Data = FlowData;

            fn theoretical_memory_bytes(params: &MatrixHeapParams, data: &FlowData) -> usize {
                (size_of::<Self>() - size_of::<TopKHeap<HeapCountOf<S>>>())
                    .saturating_add(
                        params
                            .rows
                            .saturating_mul(params.cols)
                            .saturating_mul(size_of::<S::Counter>()),
                    )
                    .saturating_add(heap_memory::<HeapCountOf<S>>(
                        params.heap_size,
                        data.key_bytes,
                    ))
            }

            fn theoretical_update_ops(params: &MatrixHeapParams, _: &FlowData) -> f64 {
                params.rows as f64 + $base.est(params.rows) + heap_update_ops(params.heap_size)
            }

            fn theoretical_query_ops(
                statistic: StatisticKind,
                params: &MatrixHeapParams,
                _: &FlowData,
            ) -> Option<f64> {
                match statistic {
                    StatisticKind::ItemFrequency => Some($base.est(params.rows)),
                    StatisticKind::TopK => Some(params.heap_size as f64),
                    _ => None,
                }
            }

            fn theoretical_merge_ops(params: &MatrixHeapParams, data: &FlowData) -> Option<f64> {
                let keys = 2.0 * held(params.heap_size, data.distinct);
                let per_key = 3.0 + $base.est(params.rows) + heap_update_ops(params.heap_size);
                Some(2.0 * (params.rows * params.cols) as f64 + 3.0 + keys * per_key)
            }

            fn theoretical_error(
                statistic: StatisticKind,
                params: &MatrixHeapParams,
                data: &FlowData,
                delta: Option<f64>,
            ) -> Option<ErrorBound> {
                let shape = MatrixParams {
                    rows: params.rows,
                    cols: params.cols,
                };
                match statistic {
                    StatisticKind::ItemFrequency => item_error($base, shape, Mode::FAST, delta),
                    StatisticKind::TopK => {
                        top_k_error($base, shape, Mode::FAST, distinct(data.distinct), delta)
                    }
                    _ => None,
                }
            }

            fn params_for(
                statistic: StatisticKind,
                epsilon: f64,
                delta: f64,
                data: &FlowData,
            ) -> Option<MatrixHeapParams> {
                let keys = distinct(data.distinct);
                let shape = top_k_params($base, statistic, epsilon, delta, Mode::FAST, keys)?;
                Some(MatrixHeapParams {
                    rows: shape.rows,
                    cols: shape.cols,
                    ..MatrixHeapParams::default()
                })
            }
        }
    };
}

matrix_with_heap!(CountMinSketchWithHeap, Base::CountMin);
matrix_with_heap!(CountSketchWithHeap, Base::CountSketch);

/// SpaceSaving's most ops for one unit update of an unmerged summary.
const SPACE_SAVING_UPDATE_OPS: f64 = 47.0;

macro_rules! matrix_with_space_saving {
    ($sketch:ident, $base:expr) => {
        impl<S: MatrixStorage, Mode: HashPath, H: SketchHasher> SketchAnalytics
            for $sketch<S, Mode, H>
        {
            type Params = MatrixSpaceSavingParams;
            type Data = FlowData;

            fn theoretical_memory_bytes(
                params: &MatrixSpaceSavingParams,
                data: &FlowData,
            ) -> usize {
                (size_of::<Self>() - size_of::<SpaceSaving<H>>())
                    .saturating_add(
                        params
                            .rows
                            .saturating_mul(params.cols)
                            .saturating_mul(size_of::<S::Counter>()),
                    )
                    .saturating_add(SpaceSaving::<H>::theoretical_memory_bytes(
                        params.capacity,
                        data.key_bytes.unwrap_or(0),
                    ))
            }

            fn theoretical_update_ops(params: &MatrixSpaceSavingParams, _: &FlowData) -> f64 {
                params.rows as f64 + SPACE_SAVING_UPDATE_OPS
            }

            fn theoretical_query_ops(
                statistic: StatisticKind,
                params: &MatrixSpaceSavingParams,
                data: &FlowData,
            ) -> Option<f64> {
                let est = $base.est(params.rows);
                match statistic {
                    StatisticKind::ItemFrequency => Some(est),
                    StatisticKind::TopK => {
                        let keys = held(params.capacity, data.distinct);
                        Some(keys * (5.0 + est) + sort_ops(keys))
                    }
                    _ => None,
                }
            }

            fn theoretical_merge_ops(
                params: &MatrixSpaceSavingParams,
                _: &FlowData,
            ) -> Option<f64> {
                let m = params.capacity.max(1) as f64;
                let matrix = 2.0 * (params.rows * params.cols) as f64;
                Some(matrix + 44.0 * m + sort_ops(2.0 * m) + 13.0)
            }

            fn theoretical_error(
                statistic: StatisticKind,
                params: &MatrixSpaceSavingParams,
                data: &FlowData,
                delta: Option<f64>,
            ) -> Option<ErrorBound> {
                let shape = MatrixParams {
                    rows: params.rows,
                    cols: params.cols,
                };
                match statistic {
                    StatisticKind::ItemFrequency => item_error($base, shape, Mode::FAST, delta),
                    StatisticKind::TopK => {
                        let keys = data.distinct.map(|_| held(params.capacity, data.distinct));
                        top_k_error($base, shape, Mode::FAST, keys, delta)
                    }
                    _ => None,
                }
            }

            fn params_for(
                statistic: StatisticKind,
                epsilon: f64,
                delta: f64,
                data: &FlowData,
            ) -> Option<MatrixSpaceSavingParams> {
                let sized = MatrixSpaceSavingParams::default();
                let keys = data.distinct.map(|_| held(sized.capacity, data.distinct));
                let shape = top_k_params($base, statistic, epsilon, delta, Mode::FAST, keys)?;
                Some(MatrixSpaceSavingParams {
                    rows: shape.rows,
                    cols: shape.cols,
                    ..sized
                })
            }
        }
    };
}

matrix_with_space_saving!(CountMinSketchWithSpaceSaving, Base::CountMin);
matrix_with_space_saving!(CountSketchWithSpaceSaving, Base::CountSketch);

/// `E` and `S` of a fold state, from `data` or their upper bounds.
fn fold_entries(params: &FoldParams, data: &FoldData) -> (f64, usize) {
    let keys = data.distinct.map_or(usize::MAX, |d| d as usize);
    let most = data
        .max_cell_entries
        .unwrap_or((1usize << params.fold_level).min(keys));
    let collided = data
        .collided_entries
        .unwrap_or(params.rows.saturating_mul(params.full_cols.min(keys)));
    (most as f64, collided)
}

fn fold_shape(params: &FoldParams) -> MatrixParams {
    MatrixParams {
        rows: params.rows,
        cols: params.full_cols,
    }
}

/// Fold sizing: the base sketch's on `RegularPath` with `W` a power of two
/// at most 65536.
fn fold_params(
    base: Base,
    statistic: StatisticKind,
    epsilon: f64,
    delta: f64,
    keys: Option<f64>,
) -> Option<FoldParams> {
    let delta = match statistic {
        StatisticKind::ItemFrequency => delta,
        StatisticKind::TopK => delta / keys?.max(1.0),
        _ => return None,
    };
    let shape = match base {
        Base::CountMin => {
            let shape = count_min_params_for(epsilon, delta, false, usize::MAX)?;
            MatrixParams {
                cols: shape.cols.next_power_of_two(),
                ..shape
            }
        }
        Base::CountSketch => fold_count_sketch_params_for(epsilon, delta)?,
    };
    (shape.cols <= MAX_FULL_COLS).then_some(FoldParams {
        rows: shape.rows,
        full_cols: shape.cols,
        ..FoldParams::default()
    })
}

/// The odd `d ≤ 19` and power-of-two `W` with `q ≤ β·ε²` on `RegularPath`
/// that minimize `d·W`.
fn fold_count_sketch_params_for(epsilon: f64, delta: f64) -> Option<MatrixParams> {
    if !(epsilon > 0.0) || !(delta > 0.0) {
        return None;
    }
    let mut best: Option<MatrixParams> = None;
    for rows in (1..=MATRIX_MAX_ROWS).step_by(2) {
        let room = max_row_failure(rows, delta) * epsilon * epsilon - TWO_POW_MINUS_32;
        if !(room > 0.0) || 1.0 / room > MAX_FULL_COLS as f64 {
            continue;
        }
        let cols = ((1.0 / room).ceil() as usize).next_power_of_two();
        if best.is_none_or(|b| rows * cols < b.rows * b.cols) {
            best = Some(MatrixParams { rows, cols });
        }
    }
    best
}

macro_rules! fold {
    ($sketch:ident, $base:expr) => {
        impl<H: SketchHasher> SketchAnalytics for $sketch<H> {
            type Params = FoldParams;
            type Data = FoldData;

            fn theoretical_memory_bytes(params: &FoldParams, data: &FoldData) -> usize {
                let (_, collided) = fold_entries(params, data);
                let cells = params
                    .rows
                    .saturating_mul(params.full_cols >> params.fold_level);
                (size_of::<Self>() - size_of::<TopKHeap>())
                    .saturating_add(cells.saturating_mul(size_of::<FoldCell>()))
                    .saturating_add(collided.saturating_mul(2 * size_of::<FoldEntry>()))
                    .saturating_add(heap_memory::<i64>(params.heap_size, data.key_bytes))
            }

            fn theoretical_update_ops(params: &FoldParams, data: &FoldData) -> f64 {
                let (most, _) = fold_entries(params, data);
                let d = params.rows as f64;
                2.0 * d * most + d + $base.est(params.rows) + heap_update_ops(params.heap_size)
            }

            fn theoretical_query_ops(
                statistic: StatisticKind,
                params: &FoldParams,
                data: &FoldData,
            ) -> Option<f64> {
                let (most, _) = fold_entries(params, data);
                match statistic {
                    StatisticKind::ItemFrequency => {
                        Some(params.rows as f64 * most + $base.est(params.rows))
                    }
                    StatisticKind::TopK => Some(params.heap_size as f64),
                    _ => None,
                }
            }

            fn theoretical_merge_ops(params: &FoldParams, data: &FoldData) -> Option<f64> {
                let (most, _) = fold_entries(params, data);
                let d = params.rows as f64;
                let cells = d * (params.full_cols >> params.fold_level) as f64;
                let entries = d * held(params.full_cols, data.distinct);
                let keys = held(params.heap_size, data.distinct);
                let per_key =
                    1.0 + d * most + $base.est(params.rows) + heap_update_ops(params.heap_size);
                Some(cells + entries * (2.0 + most) + keys * per_key)
            }

            fn theoretical_error(
                statistic: StatisticKind,
                params: &FoldParams,
                data: &FoldData,
                delta: Option<f64>,
            ) -> Option<ErrorBound> {
                let shape = fold_shape(params);
                match statistic {
                    StatisticKind::ItemFrequency => item_error($base, shape, false, delta),
                    StatisticKind::TopK => {
                        top_k_error($base, shape, false, distinct(data.distinct), delta)
                    }
                    _ => None,
                }
            }

            fn params_for(
                statistic: StatisticKind,
                epsilon: f64,
                delta: f64,
                data: &FoldData,
            ) -> Option<FoldParams> {
                fold_params($base, statistic, epsilon, delta, distinct(data.distinct))
            }
        }
    };
}

fold!(CountMinSketchFold, Base::CountMin);
fold!(CountSketchFold, Base::CountSketch);
