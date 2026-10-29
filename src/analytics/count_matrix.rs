//! Analytics of the counter matrices: Count-Min, Count Sketch, Count Sketch
//! with row L2 norms, and the experimental CountMinHll and MicroCm.

use super::{
    DEFAULT_DELTA, ErrorBound, ErrorMetric, HashPath, SketchAnalytics, StatisticKind,
    est_count_min, est_count_sketch, fixed_delta, is_positive, max_row_failure, p_med,
};
use crate::common::structures::fixed_structure::{DEFAULT_COLS, DEFAULT_ROWS};
use crate::{
    CountMinSketch, CountSketch, CountSketchWithL2Norm, MATRIX_MAX_ROWS, MatrixStorage,
    SketchHasher,
};
use std::f64::consts::E;

/// `d` rows and `w` columns of a counter matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MatrixParams {
    pub rows: usize,
    pub cols: usize,
}

impl Default for MatrixParams {
    fn default() -> Self {
        Self {
            rows: DEFAULT_ROWS,
            cols: DEFAULT_COLS,
        }
    }
}

pub(crate) const TWO_POW_MINUS_32: f64 = 1.0 / 4_294_967_296.0;

/// `q`, the most probability two keys share a column of a `w`-column row.
pub fn collision_probability(cols: usize, fast_path: bool) -> f64 {
    let w = cols as f64;
    if !fast_path {
        1.0 / w + TWO_POW_MINUS_32
    } else if cols.is_power_of_two() {
        1.0 / w
    } else {
        9.0 / (8.0 * w)
    }
}

/// Count-Min's `ε` at width `w`, widened on `FastPath` for a width that is
/// not a power of two.
fn count_min_epsilon(cols: usize, fast_path: bool) -> f64 {
    let w = cols as f64;
    if fast_path && !cols.is_power_of_two() {
        let r = (cols.next_power_of_two() - cols) as f64;
        E * (w + 3.0 * r) / ((w + r) * (w + r))
    } else {
        E / w
    }
}

/// Count-Min sizing: `w = ⌈e/ε⌉`, rounded up to a power of two on `FastPath`,
/// and `d = max(1, ⌈ln(1/δ)⌉)`; `None` past `max_rows`.
pub(crate) fn count_min_params_for(
    epsilon: f64,
    delta: f64,
    fast_path: bool,
    max_rows: usize,
) -> Option<MatrixParams> {
    if !is_positive(epsilon) || !is_positive(delta) {
        return None;
    }
    let mut cols = (E / epsilon).ceil().max(1.0);
    if !cols.is_finite() || cols > u32::MAX as f64 {
        return None;
    }
    if fast_path {
        cols = (cols as usize).next_power_of_two() as f64;
    }
    let rows = (1.0 / delta).ln().ceil().max(1.0) as usize;
    (rows <= max_rows).then_some(MatrixParams {
        rows,
        cols: cols as usize,
    })
}

/// Count-Min's ItemFrequency bound.
pub(crate) fn count_min_error(
    params: &MatrixParams,
    fast_path: bool,
    delta: Option<f64>,
    contract: &'static str,
) -> Option<ErrorBound> {
    Some(ErrorBound {
        metric: ErrorMetric::L1,
        epsilon: count_min_epsilon(params.cols, fast_path),
        delta: Some(fixed_delta((-(params.rows as f64)).exp(), delta)?),
        contract,
    })
}

/// Count Sketch's ItemFrequency bound at a chosen `δ`: the largest per-row
/// failure `β` with `P_med(d, β) ≤ δ`, and `ε = √(q/β)` by Chebyshev.
pub(crate) fn count_sketch_error(
    rows: usize,
    q: f64,
    delta: Option<f64>,
    contract: &'static str,
) -> Option<ErrorBound> {
    let delta = delta.unwrap_or(DEFAULT_DELTA);
    let beta = max_row_failure(rows, delta);
    (beta > 0.0).then(|| ErrorBound {
        metric: ErrorMetric::L2,
        epsilon: (q / beta).sqrt(),
        delta: Some(delta),
        contract,
    })
}

/// Count Sketch sizing: the odd `d ≤ 19` and width that meet `(ε, δ)` with
/// the fewest counters `d·w`.
pub(crate) fn count_sketch_params_for(
    epsilon: f64,
    delta: f64,
    fast_path: bool,
) -> Option<MatrixParams> {
    if !is_positive(epsilon) || !is_positive(delta) {
        return None;
    }
    let mut best: Option<MatrixParams> = None;
    for rows in (1..=MATRIX_MAX_ROWS).step_by(2) {
        let target = max_row_failure(rows, delta) * epsilon * epsilon;
        let cols = if fast_path {
            if !is_positive(target) || 1.0 / target > (1u64 << 32) as f64 {
                continue;
            }
            ((1.0 / target).ceil() as usize).next_power_of_two()
        } else {
            let room = target - TWO_POW_MINUS_32;
            if !is_positive(room) || 1.0 / room > u32::MAX as f64 {
                continue;
            }
            (1.0 / room).ceil() as usize
        };
        if best.is_none_or(|b| rows * cols < b.rows * b.cols) {
            best = Some(MatrixParams { rows, cols });
        }
    }
    best
}

pub(crate) const COUNT_MIN_CONTRACT: &str = "count_min_l1_markov_v1";
pub(crate) const COUNT_SKETCH_CONTRACT: &str = "count_sketch_l2_median_chosen_row_rate_v1";

impl<S: MatrixStorage, Mode: HashPath, H: SketchHasher> SketchAnalytics
    for CountMinSketch<S, Mode, H>
{
    type Params = MatrixParams;
    type Data = ();

    fn theoretical_memory_bytes(params: &MatrixParams, _: &()) -> usize {
        CountMinSketch::<S, Mode>::theoretical_memory_bytes(params.rows, params.cols)
    }

    fn theoretical_update_ops(params: &MatrixParams, _: &()) -> f64 {
        params.rows as f64
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &MatrixParams,
        _: &(),
    ) -> Option<f64> {
        match statistic {
            StatisticKind::ItemFrequency => Some(est_count_min(params.rows)),
            StatisticKind::Total => Some(params.cols as f64),
            _ => None,
        }
    }

    fn theoretical_merge_ops(params: &MatrixParams, _: &()) -> Option<f64> {
        Some(2.0 * (params.rows * params.cols) as f64)
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &MatrixParams,
        _: &(),
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        match statistic {
            StatisticKind::ItemFrequency => {
                count_min_error(params, Mode::FAST, delta, COUNT_MIN_CONTRACT)
            }
            StatisticKind::Total => Some(ErrorBound::EXACT),
            _ => None,
        }
    }

    fn params_for(
        statistic: StatisticKind,
        epsilon: f64,
        delta: f64,
        _: &(),
    ) -> Option<MatrixParams> {
        match statistic {
            StatisticKind::ItemFrequency => {
                count_min_params_for(epsilon, delta, Mode::FAST, MATRIX_MAX_ROWS)
            }
            StatisticKind::Total => Some(MatrixParams::default()),
            _ => None,
        }
    }
}

impl<S: MatrixStorage, Mode: HashPath, H: SketchHasher> SketchAnalytics
    for CountSketch<S, Mode, H>
{
    type Params = MatrixParams;
    type Data = ();

    fn theoretical_memory_bytes(params: &MatrixParams, _: &()) -> usize {
        CountSketch::<S, Mode>::theoretical_memory_bytes(params.rows, params.cols)
    }

    fn theoretical_update_ops(params: &MatrixParams, _: &()) -> f64 {
        params.rows as f64
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &MatrixParams,
        _: &(),
    ) -> Option<f64> {
        (statistic == StatisticKind::ItemFrequency).then(|| est_count_sketch(params.rows))
    }

    fn theoretical_merge_ops(params: &MatrixParams, _: &()) -> Option<f64> {
        Some(2.0 * (params.rows * params.cols) as f64)
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &MatrixParams,
        _: &(),
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        (statistic == StatisticKind::ItemFrequency).then_some(())?;
        let q = collision_probability(params.cols, Mode::FAST);
        count_sketch_error(params.rows, q, delta, COUNT_SKETCH_CONTRACT)
    }

    fn params_for(
        statistic: StatisticKind,
        epsilon: f64,
        delta: f64,
        _: &(),
    ) -> Option<MatrixParams> {
        (statistic == StatisticKind::ItemFrequency).then_some(())?;
        count_sketch_params_for(epsilon, delta, Mode::FAST)
    }
}

/// The FrequencyL2 per-row failure probability, by Chebyshev's inequality.
const L2_ROW_FAILURE: f64 = 0.1;

/// The most rows ASAPPlanner sizes a FrequencyL2 Count Sketch with.
pub(crate) const L2_MAX_ROWS: usize = 19;

/// FrequencyL2 bound of a Count Sketch with row L2 norms on `FastPath`:
/// `Relative`, `ε = 1 − √(1 − √(20q))`, `δ = P_med(d, 0.1)`, when `20q < 1`.
pub(crate) fn l2_norm_error(
    params: &MatrixParams,
    delta: Option<f64>,
    contract: &'static str,
) -> Option<ErrorBound> {
    let spread = (20.0 * collision_probability(params.cols, true)).sqrt();
    (spread < 1.0).then_some(())?;
    Some(ErrorBound {
        metric: ErrorMetric::Relative,
        epsilon: 1.0 - (1.0 - spread).sqrt(),
        delta: Some(fixed_delta(p_med(params.rows, L2_ROW_FAILURE), delta)?),
        contract,
    })
}

/// FrequencyL2 sizing: the narrowest power-of-two width with
/// `20q ≤ (1 − (1 − ε)²)²` and the fewest rows with `P_med(d, 0.1) ≤ δ`.
pub(crate) fn l2_norm_params_for(epsilon: f64, delta: f64) -> Option<MatrixParams> {
    if !is_positive(epsilon) || !is_positive(delta) {
        return None;
    }
    let spread = 1.0 - (1.0 - epsilon.min(1.0)).powi(2);
    let cols = (20.0 / (spread * spread)).ceil();
    if !cols.is_finite() || cols > (1u64 << 32) as f64 {
        return None;
    }
    let cols = (cols as usize).next_power_of_two();
    let rows = (1..=L2_MAX_ROWS).find(|&d| p_med(d, L2_ROW_FAILURE) <= delta)?;
    (rows * (cols.ilog2() as usize + 1) <= 128).then_some(MatrixParams { rows, cols })
}

pub(crate) const L2_NORM_CONTRACT: &str = "univmon_layer0_f2_median_chebyshev_binomial_v1";

impl<H: SketchHasher> SketchAnalytics for CountSketchWithL2Norm<H> {
    type Params = MatrixParams;
    type Data = ();

    fn theoretical_memory_bytes(params: &MatrixParams, _: &()) -> usize {
        CountSketchWithL2Norm::<crate::DefaultXxHasher>::theoretical_memory_bytes(
            params.rows,
            params.cols,
        )
    }

    fn theoretical_update_ops(params: &MatrixParams, _: &()) -> f64 {
        3.0 * params.rows as f64
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &MatrixParams,
        _: &(),
    ) -> Option<f64> {
        matches!(
            statistic,
            StatisticKind::ItemFrequency | StatisticKind::FrequencyL2
        )
        .then(|| est_count_sketch(params.rows))
    }

    fn theoretical_merge_ops(params: &MatrixParams, _: &()) -> Option<f64> {
        Some((3 * params.rows * params.cols + params.rows) as f64)
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &MatrixParams,
        _: &(),
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        match statistic {
            StatisticKind::ItemFrequency => count_sketch_error(
                params.rows,
                collision_probability(params.cols, true),
                delta,
                COUNT_SKETCH_CONTRACT,
            ),
            StatisticKind::FrequencyL2 => l2_norm_error(params, delta, L2_NORM_CONTRACT),
            _ => None,
        }
    }

    fn params_for(
        statistic: StatisticKind,
        epsilon: f64,
        delta: f64,
        _: &(),
    ) -> Option<MatrixParams> {
        let params = match statistic {
            StatisticKind::ItemFrequency => count_sketch_params_for(epsilon, delta, true)?,
            StatisticKind::FrequencyL2 => l2_norm_params_for(epsilon, delta)?,
            _ => return None,
        };
        CountSketchWithL2Norm::<crate::DefaultXxHasher>::check_hash_bits(params.rows, params.cols)
            .ok()
            .map(|()| params)
    }
}

#[cfg(feature = "experimental")]
pub use experimental::*;

#[cfg(feature = "experimental")]
mod experimental {
    use super::*;
    use crate::{CountMinHll, MicroCm};

    /// `d` rows, `w` columns and HyperLogLog precision `p` of a CountMinHll.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct CountMinHllParams {
        pub rows: usize,
        pub cols: usize,
        pub precision: u32,
    }

    impl Default for CountMinHllParams {
        fn default() -> Self {
            Self {
                rows: DEFAULT_ROWS,
                cols: DEFAULT_COLS,
                precision: 8,
            }
        }
    }

    impl<H: SketchHasher> SketchAnalytics for CountMinHll<H> {
        type Params = CountMinHllParams;
        type Data = ();

        fn theoretical_memory_bytes(params: &CountMinHllParams, _: &()) -> usize {
            size_of::<Self>() + ((params.rows * params.cols) << params.precision)
        }

        fn theoretical_update_ops(params: &CountMinHllParams, _: &()) -> f64 {
            params.rows as f64
        }

        fn theoretical_query_ops(
            statistic: StatisticKind,
            params: &CountMinHllParams,
            _: &(),
        ) -> Option<f64> {
            (statistic == StatisticKind::KeyDistinct)
                .then(|| (params.rows << params.precision) as f64)
        }

        fn theoretical_merge_ops(params: &CountMinHllParams, _: &()) -> Option<f64> {
            Some(((2 * params.rows * params.cols) << params.precision) as f64)
        }

        fn theoretical_error(
            _: StatisticKind,
            _: &CountMinHllParams,
            _: &(),
            _: Option<f64>,
        ) -> Option<ErrorBound> {
            None
        }

        fn params_for(_: StatisticKind, _: f64, _: f64, _: &()) -> Option<CountMinHllParams> {
            None
        }
    }

    /// `d` rows, `w` columns, `T` sub-windows and zoom base `c` of a MicroCm.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct MicroCmParams {
        pub rows: usize,
        pub cols: usize,
        pub t: usize,
        pub c: u32,
    }

    impl Default for MicroCmParams {
        fn default() -> Self {
            Self {
                rows: 4,
                cols: 1024,
                t: 12,
                c: 2,
            }
        }
    }

    /// MicroCm's data input.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct MicroCmData {
        /// `I`, the updates per sub-window of a count-based clock.
        pub items_per_sub_window: Option<u64>,
    }

    impl<H: SketchHasher> SketchAnalytics for MicroCm<H> {
        type Params = MicroCmParams;
        type Data = MicroCmData;

        fn theoretical_memory_bytes(params: &MicroCmParams, _: &MicroCmData) -> usize {
            size_of::<Self>() + 4 * params.rows * params.cols * (params.t + 7).div_ceil(4)
        }

        fn theoretical_update_ops(params: &MicroCmParams, data: &MicroCmData) -> f64 {
            let cells = (params.rows * params.cols) as f64;
            let per_item = data.items_per_sub_window.unwrap_or(1).max(1) as f64;
            5.0 * params.rows as f64 + 5.0 * cells / per_item
        }

        fn theoretical_query_ops(
            statistic: StatisticKind,
            params: &MicroCmParams,
            _: &MicroCmData,
        ) -> Option<f64> {
            (statistic == StatisticKind::ItemFrequency)
                .then(|| (params.rows * (2 * params.t + 3)) as f64)
        }

        fn theoretical_merge_ops(params: &MicroCmParams, _: &MicroCmData) -> Option<f64> {
            let rounds = if params.c == 2 { 2 } else { 1 };
            let pixels = params.t + 2;
            Some((params.rows * params.cols * ((6 + 2 * rounds) * pixels + 7)) as f64)
        }

        fn theoretical_error(
            _: StatisticKind,
            _: &MicroCmParams,
            _: &MicroCmData,
            _: Option<f64>,
        ) -> Option<ErrorBound> {
            None
        }

        fn params_for(_: StatisticKind, _: f64, _: f64, _: &MicroCmData) -> Option<MicroCmParams> {
            None
        }
    }
}
