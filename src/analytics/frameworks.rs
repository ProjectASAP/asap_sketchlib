//! Analytics of the frameworks that wrap other sketches: NitroBatch,
//! TumblingWindow, HashSketchEnsemble and Octo, and what Hydra and the
//! exponential histograms share with them.

use super::cardinality::{HllData, HllParams};
use super::count_matrix::{MatrixParams, collision_probability};
use super::heavy_hitters::{CocoParams, ElasticParams, FlowData};
use super::quantiles::{DdData, DdParams, KllParams, StreamData};
use super::univmon::UnivMonParams;
use super::{
    DEFAULT_DELTA, ErrorBound, ErrorMetric, HashPath, SketchAnalytics, StatisticKind,
    est_count_sketch, is_positive, p_med,
};
use crate::common::numerical::NumericalValue;
use crate::sketches::hll::{HyperLogLogHipImpl, HyperLogLogImpl};
use crate::{
    Classic, CocoSketch, CountMinSketch, CountMinSketchFold, CountSketch, CountSketchFold,
    CountSketchWithL2Norm, DdSketch, ElasticSketch, Ertl, FastPath, HashSketchEnsemble,
    HllRegisterStorage, HyperLogLog, HyperLogLogHip, Kll, MatrixStorage, NitroBatch, NitroTarget,
    SketchHasher, TumblingWindow, TumblingWindowSketch, UnivMon, UnivMonQ, Vector2D,
};
use std::f64::consts::E;

/// What a framework reads of its inner sketch beyond [`SketchAnalytics`].
pub trait InnerSketch: SketchAnalytics + Sized {
    /// Bytes of one counter, register or item of a state with `params`.
    fn cell_bytes(params: &Self::Params) -> usize;

    /// `C_in`: the cells a clone or clear of one state touches, its heap
    /// bytes over [`Self::cell_bytes`].
    fn theoretical_cells(params: &Self::Params, data: &Self::Data) -> f64 {
        match Self::theoretical_memory_bytes(params, data) {
            Some(bytes) if bytes < usize::MAX => {
                bytes.saturating_sub(size_of::<Self>()) as f64 / Self::cell_bytes(params) as f64
            }
            _ => f64::INFINITY,
        }
    }

    /// Whether merging states cloned from one prototype or built from one
    /// seed keeps the inner bound; Kll's does not.
    const SHARED_SEED_MERGE_BOUND: bool = true;
}

/// `C_in` of a live state: its heap bytes over its cell bytes.
pub(crate) fn heap_cells(memory_bytes: usize, inline_bytes: usize, cell_bytes: usize) -> usize {
    memory_bytes.saturating_sub(inline_bytes) / cell_bytes.max(1)
}

impl<S: MatrixStorage, Mode: HashPath, H: SketchHasher> InnerSketch for CountMinSketch<S, Mode, H> {
    fn cell_bytes(_: &MatrixParams) -> usize {
        size_of::<S::Counter>()
    }
}

impl<S: MatrixStorage, Mode: HashPath, H: SketchHasher> InnerSketch for CountSketch<S, Mode, H> {
    fn cell_bytes(_: &MatrixParams) -> usize {
        size_of::<S::Counter>()
    }
}

impl<H: SketchHasher> InnerSketch for CountSketchWithL2Norm<H> {
    fn cell_bytes(_: &MatrixParams) -> usize {
        size_of::<i64>()
    }
}

impl<R: HllRegisterStorage, H: SketchHasher> InnerSketch for HyperLogLogImpl<Ertl, R, H> {
    fn cell_bytes(_: &HllParams) -> usize {
        1
    }
}

impl<R: HllRegisterStorage, H: SketchHasher> InnerSketch for HyperLogLogImpl<Classic, R, H> {
    fn cell_bytes(_: &HllParams) -> usize {
        1
    }
}

impl<R: HllRegisterStorage> InnerSketch for HyperLogLogHipImpl<R> {
    fn cell_bytes(_: &HllParams) -> usize {
        1
    }
}

impl<T: NumericalValue> InnerSketch for Kll<T> {
    fn cell_bytes(_: &KllParams) -> usize {
        size_of::<T>()
    }

    const SHARED_SEED_MERGE_BOUND: bool = false;
}

impl InnerSketch for DdSketch {
    fn cell_bytes(_: &DdParams) -> usize {
        size_of::<u64>()
    }
}

impl<H: SketchHasher> InnerSketch for CocoSketch<H> {
    fn cell_bytes(_: &CocoParams) -> usize {
        size_of::<crate::sketches::coco::CocoBucket>()
    }
}

impl<H: SketchHasher> InnerSketch for ElasticSketch<H> {
    fn cell_bytes(_: &ElasticParams) -> usize {
        size_of::<i32>()
    }
}

impl InnerSketch for UnivMon {
    fn cell_bytes(_: &UnivMonParams) -> usize {
        size_of::<i64>()
    }
}

impl<H: SketchHasher> InnerSketch for CountMinSketchFold<H> {
    fn cell_bytes(_: &super::FoldParams) -> usize {
        size_of::<crate::sketches::count_min_fold::FoldCell>()
    }
}

impl<H: SketchHasher> InnerSketch for CountSketchFold<H> {
    fn cell_bytes(_: &super::FoldParams) -> usize {
        size_of::<crate::sketches::count_min_fold::FoldCell>()
    }
}

impl<H: SketchHasher> InnerSketch for UnivMonQ<H> {
    fn cell_bytes(config: &crate::UnivMonQConfig) -> usize {
        usize::from(config.counter_bits / 8).max(1)
    }
}

#[cfg(feature = "experimental")]
impl InnerSketch for crate::UniformSampling {
    fn cell_bytes(_: &super::UniformParams) -> usize {
        crate::sketches::uniform::SAMPLE_ENTRY_BYTES
    }
}

/// The inner data inputs of a framework whose cells can be several kinds;
/// each kind reads its own field.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct CellData {
    /// Kll and UniformSampling cells.
    pub stream: StreamData,
    /// Coco, Elastic and UnivMon cells.
    pub flow: FlowData,
    /// Classic and Ertl HyperLogLog cells.
    pub hll: HllData,
    /// DdSketch cells.
    pub dd: DdData,
}

pub(crate) type CmCell = CountMinSketch<Vector2D<i32>, FastPath>;
pub(crate) type CsCell = CountSketch<Vector2D<i32>, FastPath>;

/// One cell kind and its parameters, for dispatching to its analytics.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Cell<'a> {
    CountMin(&'a MatrixParams),
    CountSketch(&'a MatrixParams),
    CountSketchWithL2Norm(&'a MatrixParams),
    HllErtl(&'a HllParams),
    HllClassic(&'a HllParams),
    HllHip(&'a HllParams),
    Kll(&'a KllParams),
    DdSketch(&'a DdParams),
    Coco(&'a CocoParams),
    Elastic(&'a ElasticParams),
    #[cfg(feature = "experimental")]
    Uniform(&'a super::UniformParams),
    UnivMon(&'a UnivMonParams),
}

/// Runs `$body` with `$T` the cell's sketch type, `$p` its params and `$d`
/// its data.
macro_rules! on_cell {
    ($cell:expr, $data:expr, |$T:ident, $p:ident, $d:ident| $body:expr) => {{
        let data: &CellData = $data;
        match $cell {
            Cell::CountMin($p) => {
                type $T = CmCell;
                let $d = &();
                $body
            }
            Cell::CountSketch($p) => {
                type $T = CsCell;
                let $d = &();
                $body
            }
            Cell::CountSketchWithL2Norm($p) => {
                type $T = CountSketchWithL2Norm;
                let $d = &();
                $body
            }
            Cell::HllErtl($p) => {
                type $T = HyperLogLog<Ertl>;
                let $d = &data.hll;
                $body
            }
            Cell::HllClassic($p) => {
                type $T = HyperLogLog<Classic>;
                let $d = &data.hll;
                $body
            }
            Cell::HllHip($p) => {
                type $T = HyperLogLogHip;
                let $d = &();
                $body
            }
            Cell::Kll($p) => {
                type $T = Kll;
                let $d = &data.stream;
                $body
            }
            Cell::DdSketch($p) => {
                type $T = DdSketch;
                let $d = &data.dd;
                $body
            }
            Cell::Coco($p) => {
                type $T = CocoSketch;
                let $d = &data.flow;
                $body
            }
            Cell::Elastic($p) => {
                type $T = ElasticSketch;
                let $d = &data.flow;
                $body
            }
            #[cfg(feature = "experimental")]
            Cell::Uniform($p) => {
                type $T = crate::UniformSampling;
                let $d = &data.stream;
                $body
            }
            Cell::UnivMon($p) => {
                type $T = UnivMon;
                let $d = &data.flow;
                $body
            }
        }
    }};
}

impl Cell<'_> {
    /// `mem_in − size_of::<Inner>()`, the heap bytes of one state.
    pub(crate) fn heap(self, data: &CellData) -> Option<usize> {
        on_cell!(self, data, |T, p, d| {
            <T as SketchAnalytics>::theoretical_memory_bytes(p, d)
                .map(|bytes| bytes.saturating_sub(size_of::<T>()))
        })
    }

    /// `U_in`.
    pub(crate) fn update(self, data: &CellData) -> f64 {
        on_cell!(self, data, |T, p, d| {
            <T as SketchAnalytics>::theoretical_update_ops(p, d)
        })
    }

    /// `Q_in` of `statistic`.
    pub(crate) fn query(self, statistic: StatisticKind, data: &CellData) -> Option<f64> {
        on_cell!(self, data, |T, p, d| {
            <T as SketchAnalytics>::theoretical_query_ops(statistic, p, d)
        })
    }

    /// `M_in`.
    pub(crate) fn merge(self, data: &CellData) -> Option<f64> {
        on_cell!(self, data, |T, p, d| {
            <T as SketchAnalytics>::theoretical_merge_ops(p, d)
        })
    }

    /// `C_in`.
    pub(crate) fn cells(self, data: &CellData) -> f64 {
        on_cell!(self, data, |T, p, d| <T as InnerSketch>::theoretical_cells(
            p, d
        ))
    }

    /// `ε_in`, `δ_in` of `statistic`.
    pub(crate) fn error(
        self,
        statistic: StatisticKind,
        data: &CellData,
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        on_cell!(self, data, |T, p, d| {
            <T as SketchAnalytics>::theoretical_error(statistic, p, d, delta)
        })
    }
}

/// The inner bound and total `δ` of a framework that fails with `own_delta`
/// plus `copies` inner failures: fixed at `own_delta + copies·δ_in` for a
/// fixed `δ_in`, else the inner gets `(δ − own_delta)/copies`.
pub(crate) fn split_delta(
    own_delta: f64,
    copies: f64,
    delta: Option<f64>,
    inner: impl Fn(Option<f64>) -> Option<ErrorBound>,
) -> Option<(ErrorBound, f64)> {
    let at_default = inner(None)?;
    // A fixed-δ contract refuses a `delta` below its own.
    if inner(Some(at_default.delta? * 0.5)).is_none() {
        let total = own_delta + copies * at_default.delta?;
        return super::fixed_delta(total, delta).map(|total| (at_default, total));
    }
    let total = delta.unwrap_or(DEFAULT_DELTA);
    let room = (total - own_delta) / copies;
    if !is_positive(room) {
        return None;
    }
    inner(Some(room)).map(|bound| (bound, total))
}

// ---------------------------------------------------------------------------
// NitroBatch
// ---------------------------------------------------------------------------

/// A Count-Min or Count Sketch a [`NitroBatch`] samples into.
pub trait NitroInner: InnerSketch<Params = MatrixParams, Data = ()> + NitroTarget {
    /// Whether the estimate is Count-Min's.
    const COUNT_MIN: bool;
}

impl<H: SketchHasher> NitroInner for CountMinSketch<Vector2D<i32>, FastPath, H> {
    const COUNT_MIN: bool = true;
}

impl<H: SketchHasher> NitroInner for CountSketch<Vector2D<i32>, FastPath, H> {
    const COUNT_MIN: bool = false;
}

/// Admission rate `ρ` and the inner matrix of a NitroBatch.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NitroParams {
    pub rate: f64,
    pub inner: MatrixParams,
}

impl Default for NitroParams {
    fn default() -> Self {
        Self {
            rate: 0.1,
            inner: MatrixParams::default(),
        }
    }
}

/// `N`, the data input of NitroBatch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NitroData {
    /// `N`, the updates in the batches.
    pub total_weight: Option<u64>,
}

const NITRO_CM_CONTRACT: &str = "nitro_batch_item_sampling_chebyshev_v1";

/// `V = (1 − ρ)/ρ + ρ·u(1 − u)`, `u` the fractional part of `1/ρ`.
pub fn nitro_sampling_variance(rate: f64) -> f64 {
    let inverse = 1.0 / rate;
    let u = inverse - inverse.floor();
    (1.0 - rate) / rate + rate * u * (1.0 - u)
}

impl<S: NitroInner> SketchAnalytics for NitroBatch<S> {
    type Params = NitroParams;
    type Data = NitroData;

    fn theoretical_memory_bytes(params: &NitroParams, _: &NitroData) -> Option<usize> {
        let inner = S::theoretical_memory_bytes(&params.inner, &())?;
        Some(size_of::<Self>() + inner - size_of::<S>())
    }

    /// Average per update of a batch.
    fn theoretical_update_ops(params: &NitroParams, _: &NitroData) -> f64 {
        params.rate * params.inner.rows as f64
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &NitroParams,
        _: &NitroData,
    ) -> Option<f64> {
        (statistic == StatisticKind::ItemFrequency).then(|| est_count_sketch(params.inner.rows))
    }

    fn theoretical_merge_ops(params: &NitroParams, _: &NitroData) -> Option<f64> {
        S::theoretical_merge_ops(&params.inner, &())
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &NitroParams,
        data: &NitroData,
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        (statistic == StatisticKind::ItemFrequency && S::COUNT_MIN).then_some(())?;
        let n = data.total_weight? as f64;
        let rows = params.inner.rows;
        let w = params.inner.cols as f64;
        let delta = delta.unwrap_or(DEFAULT_DELTA);
        let collisions = p_med(rows, collision_probability(params.inner.cols, true) * w / E);
        let room = delta - collisions;
        (room > 0.0 && n > 0.0).then_some(())?;
        let sampling = (2.0 * nitro_sampling_variance(params.rate) / (n * room)).sqrt();
        let inner = E / w;
        Some(ErrorBound {
            metric: ErrorMetric::L1,
            epsilon: sampling + inner * (1.0 + sampling),
            delta: Some(delta),
            contract: NITRO_CM_CONTRACT,
        })
    }

    fn params_for(_: StatisticKind, _: f64, _: f64, _: &NitroData) -> Option<NitroParams> {
        None
    }
}

// ---------------------------------------------------------------------------
// TumblingWindow
// ---------------------------------------------------------------------------

/// The window length, retained windows, pool size and inner parameters of a
/// TumblingWindow.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TumblingParams<P> {
    pub window_size: u64,
    pub max_windows: usize,
    pub pool_initial_size: usize,
    pub inner: P,
}

impl<P> TumblingParams<P> {
    /// Window length 1, 8 retained windows and a pool of 10 around `inner`.
    pub fn around(inner: P) -> Self {
        Self {
            window_size: 1,
            max_windows: 8,
            pool_initial_size: 10,
            inner,
        }
    }
}

impl<P: Default> Default for TumblingParams<P> {
    fn default() -> Self {
        Self::around(P::default())
    }
}

/// The data inputs of TumblingWindow.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct TumblingData<D> {
    /// `t`, the closed windows a query merges; `max_windows` when unknown.
    pub windows: Option<usize>,
    /// `b`, the window boundaries one update crosses; unbounded when unknown.
    pub boundaries: Option<u64>,
    /// The inner data of each window's state.
    pub inner: D,
}

/// `A = max(pool_initial_size, max_windows + 2)`, the most states alive.
fn tumbling_states<P>(params: &TumblingParams<P>) -> usize {
    params.pool_initial_size.max(params.max_windows + 2)
}

/// The most slots a `Vec` built with capacity `initial` reaches holding at
/// most `most` items: `initial`, or `max(2·initial, 2(most − 1), 4)` once
/// it grows.
pub(crate) fn vec_slots(initial: usize, most: usize) -> usize {
    if most <= initial {
        initial
    } else {
        (2 * initial).max(2 * (most - 1)).max(4)
    }
}

impl<S> SketchAnalytics for TumblingWindow<S>
where
    S: TumblingWindowSketch + InnerSketch,
{
    type Params = TumblingParams<S::Params>;
    type Data = TumblingData<S::Data>;

    fn theoretical_memory_bytes(params: &Self::Params, data: &Self::Data) -> Option<usize> {
        let states = tumbling_states(params);
        let heap =
            S::theoretical_memory_bytes(&params.inner, &data.inner)?.saturating_sub(size_of::<S>());
        let closed_slots = vec_slots(params.max_windows, params.max_windows + 1);
        let free_slots = vec_slots(params.pool_initial_size, states);
        Some(
            size_of::<Self>()
                .saturating_add(
                    closed_slots.saturating_mul(crate::tumbling::closed_window_bytes::<S>()),
                )
                .saturating_add(free_slots.saturating_mul(size_of::<S>()))
                .saturating_add(states.saturating_mul(heap)),
        )
    }

    fn theoretical_update_ops(params: &Self::Params, data: &Self::Data) -> f64 {
        let boundaries = data.boundaries.map_or(f64::INFINITY, |b| b as f64);
        let boundary = 4.0
            + 2.0 * params.max_windows as f64
            + S::theoretical_cells(&params.inner, &data.inner);
        S::theoretical_update_ops(&params.inner, &data.inner) + boundaries * boundary
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &Self::Params,
        data: &Self::Data,
    ) -> Option<f64> {
        S::theoretical_query_ops(statistic, &params.inner, &data.inner)?;
        let windows = data
            .windows
            .unwrap_or(params.max_windows)
            .min(params.max_windows) as f64;
        let merge = S::theoretical_merge_ops(&params.inner, &data.inner)?;
        Some(S::theoretical_cells(&params.inner, &data.inner) + windows * (1.0 + merge))
    }

    fn theoretical_merge_ops(_: &Self::Params, _: &Self::Data) -> Option<f64> {
        None
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &Self::Params,
        data: &Self::Data,
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        S::SHARED_SEED_MERGE_BOUND.then_some(())?;
        S::theoretical_error(statistic, &params.inner, &data.inner, delta)
    }

    fn params_for(
        statistic: StatisticKind,
        epsilon: f64,
        delta: f64,
        data: &Self::Data,
    ) -> Option<Self::Params> {
        S::SHARED_SEED_MERGE_BOUND.then_some(())?;
        S::params_for(statistic, epsilon, delta, &data.inner).map(TumblingParams::around)
    }
}

// ---------------------------------------------------------------------------
// HashSketchEnsemble
// ---------------------------------------------------------------------------

/// One sketch of a HashSketchEnsemble and its parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnsembleCell {
    /// A Count-Min on `Vector2D` storage of `counter_bytes`-byte counters.
    CountMin {
        shape: MatrixParams,
        counter_bytes: usize,
    },
    /// A Count Sketch on `Vector2D` storage of `counter_bytes`-byte counters.
    CountSketch {
        shape: MatrixParams,
        counter_bytes: usize,
    },
    HllErtl(HllParams),
    HllClassic(HllParams),
    HllHip(HllParams),
}

impl EnsembleCell {
    fn cell(&self) -> Cell<'_> {
        match self {
            Self::CountMin { shape, .. } => Cell::CountMin(shape),
            Self::CountSketch { shape, .. } => Cell::CountSketch(shape),
            Self::HllErtl(p) => Cell::HllErtl(p),
            Self::HllClassic(p) => Cell::HllClassic(p),
            Self::HllHip(p) => Cell::HllHip(p),
        }
    }

    /// Bytes this sketch holds beyond its `EnsembleSketch` slot: a boxed
    /// matrix's struct and counters, or an inline HyperLogLog's heap.
    fn bytes(&self, data: &CellData) -> Option<usize> {
        let matrix = |inline: usize, shape: &MatrixParams, counter_bytes: usize| {
            let counters = shape.rows.saturating_mul(shape.cols);
            Some(inline.saturating_add(counters.saturating_mul(counter_bytes)))
        };
        match self {
            Self::CountMin {
                shape,
                counter_bytes,
            } => matrix(size_of::<CmCell>(), shape, *counter_bytes),
            Self::CountSketch {
                shape,
                counter_bytes,
            } => matrix(size_of::<CsCell>(), shape, *counter_bytes),
            _ => self.cell().heap(data),
        }
    }
}

/// The sketches of a HashSketchEnsemble, and `index`, the one the query and
/// error functions read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnsembleParams {
    pub sketches: Vec<EnsembleCell>,
    pub index: usize,
}

impl Default for EnsembleParams {
    fn default() -> Self {
        Self {
            sketches: vec![EnsembleCell::CountMin {
                shape: MatrixParams::default(),
                counter_bytes: size_of::<i32>(),
            }],
            index: 0,
        }
    }
}

impl<H: SketchHasher<HashType = crate::MatrixHash> + 'static> SketchAnalytics
    for HashSketchEnsemble<H>
{
    type Params = EnsembleParams;
    type Data = CellData;

    fn theoretical_memory_bytes(params: &EnsembleParams, data: &CellData) -> Option<usize> {
        params.sketches.iter().try_fold(
            size_of::<Self>() + params.sketches.len() * size_of::<crate::EnsembleSketch>(),
            |bytes, sketch| Some(bytes.saturating_add(sketch.bytes(data)?)),
        )
    }

    fn theoretical_update_ops(params: &EnsembleParams, data: &CellData) -> f64 {
        params
            .sketches
            .iter()
            .map(|sketch| 1.0 + sketch.cell().update(data))
            .sum()
    }

    fn theoretical_query_ops(
        statistic: StatisticKind,
        params: &EnsembleParams,
        data: &CellData,
    ) -> Option<f64> {
        let sketch = params.sketches.get(params.index)?;
        Some(1.0 + sketch.cell().query(statistic, data)?)
    }

    fn theoretical_merge_ops(_: &EnsembleParams, _: &CellData) -> Option<f64> {
        None
    }

    fn theoretical_error(
        statistic: StatisticKind,
        params: &EnsembleParams,
        data: &CellData,
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        params
            .sketches
            .get(params.index)?
            .cell()
            .error(statistic, data, delta)
    }

    fn params_for(_: StatisticKind, _: f64, _: f64, _: &CellData) -> Option<EnsembleParams> {
        None
    }
}

// ---------------------------------------------------------------------------
// Octo
// ---------------------------------------------------------------------------

#[cfg(feature = "octo-runtime")]
pub use octo::*;

#[cfg(feature = "octo-runtime")]
mod octo {
    use super::super::count_matrix::{
        COUNT_MIN_CONTRACT, COUNT_SKETCH_CONTRACT, count_min_error, count_sketch_error,
    };
    use super::*;
    use crate::{OCTO_MAX_PROMOTION_THRESHOLD, OctoConfig, OctoPartition, OctoPlan};

    /// The parent sketch of an Octo runtime and its parameters; Count-Min and
    /// Count Sketch parents hash on `RegularPath`.
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub enum OctoParent {
        CountMin(MatrixParams),
        CountSketch(MatrixParams),
        CountMinWithHeap(MatrixParams),
        CountSketchWithHeap(MatrixParams),
        /// A HyperLogLog parent; `threshold_zero` when its workers promote
        /// every register change.
        Hll {
            params: HllParams,
            threshold_zero: bool,
        },
        DdSketch(DdParams),
        /// UnivMon, Coco and Elastic parents.
        Other,
    }

    /// The data inputs of an Octo error bound.
    #[derive(Debug, Clone, Copy, Default, PartialEq)]
    pub struct OctoData {
        /// `N`, the total update weight.
        pub total_weight: Option<u64>,
        /// `‖f‖₂`.
        pub l2: Option<f64>,
        /// Whether the parent is read after a flush.
        pub flushed: bool,
        /// The parent's inner data.
        pub inner: CellData,
    }

    const OCTO_CM_CONTRACT: &str = "octo_cm_residual_additive_v1";
    const OCTO_CS_CONTRACT: &str = "octo_cs_residual_additive_v1";

    /// `τ_max`: the largest threshold a worker holds, the initial one or the
    /// adaptive ceiling after the controller clamps it into
    /// `[max(min_threshold, 1), 127]`.
    pub fn octo_max_threshold(config: &OctoConfig) -> u32 {
        let initial = config.threshold.get();
        config.adaptive.as_ref().map_or(initial, |adaptive| {
            let floor = adaptive
                .min_threshold
                .clamp(1, OCTO_MAX_PROMOTION_THRESHOLD);
            let ceiling = adaptive
                .max_threshold
                .clamp(floor, OCTO_MAX_PROMOTION_THRESHOLD);
            initial.max(ceiling)
        })
    }

    /// `K'`: 1 under `HashByKey`, `K` under `RoundRobin`.
    pub fn octo_workers_per_key(config: &OctoConfig) -> usize {
        match config.partition {
            OctoPartition::HashByKey => 1,
            OctoPartition::RoundRobin => config.num_workers.max(1),
        }
    }

    /// The residual error `ε_in·scale + K·(τ_max − 1)` around an inner bound.
    fn residual(
        inner: ErrorBound,
        scale: f64,
        workers: usize,
        config: &OctoConfig,
        contract: &'static str,
    ) -> ErrorBound {
        let held = workers as f64 * (octo_max_threshold(config).saturating_sub(1)) as f64;
        ErrorBound {
            metric: ErrorMetric::ABSOLUTE,
            epsilon: inner.epsilon * scale + held,
            delta: inner.delta,
            contract,
        }
    }

    /// The ItemFrequency or Distinct error of an Octo parent read after the
    /// queues drain, before or after a flush, as `docs/sketch_analytics.md`
    /// gives it; `None` where the section gives none.
    pub fn octo_error(
        statistic: StatisticKind,
        parent: &OctoParent,
        config: &OctoConfig,
        data: &OctoData,
        delta: Option<f64>,
    ) -> Option<ErrorBound> {
        let k = config.num_workers.max(1);
        let item = statistic == StatisticKind::ItemFrequency;
        match parent {
            OctoParent::CountMin(p) | OctoParent::CountMinWithHeap(p) if item => {
                let inner = count_min_error(p, false, delta, COUNT_MIN_CONTRACT)?;
                if data.flushed && matches!(parent, OctoParent::CountMin(_)) {
                    return Some(inner);
                }
                let n = data.total_weight? as f64;
                Some(residual(
                    inner,
                    n,
                    octo_workers_per_key(config),
                    config,
                    OCTO_CM_CONTRACT,
                ))
            }
            OctoParent::CountSketch(p) | OctoParent::CountSketchWithHeap(p) if item => {
                let q = collision_probability(p.cols, false);
                let inner = count_sketch_error(p.rows, q, delta, COUNT_SKETCH_CONTRACT)?;
                if data.flushed && matches!(parent, OctoParent::CountSketch(_)) {
                    return Some(inner);
                }
                Some(residual(inner, data.l2?, k, config, OCTO_CS_CONTRACT))
            }
            OctoParent::Hll {
                params,
                threshold_zero,
            } if statistic == StatisticKind::Distinct => (data.flushed || *threshold_zero)
                .then(|| Cell::HllClassic(params).error(statistic, &data.inner, delta))?,
            OctoParent::DdSketch(p) if data.flushed => {
                Cell::DdSketch(p).error(statistic, &data.inner, delta)
            }
            _ => None,
        }
    }

    /// Octo's memory:
    /// `mem_in + K·(size_of::<Worker>() + worker counter bytes)`
    /// `+ K·queue_capacity·(size_of::<WorkerMsg>() + size_of::<Delta>() + 16)`.
    pub fn octo_memory_bytes<L: OctoPlan>(
        config: &OctoConfig,
        parent_bytes: usize,
        worker_counter_bytes: usize,
    ) -> usize {
        let k = config.num_workers.max(1);
        let slots = config.queue_capacity.max(1);
        parent_bytes
            .saturating_add(k.saturating_mul(size_of::<L::Worker>() + worker_counter_bytes))
            .saturating_add(
                k.saturating_mul(slots)
                    .saturating_mul(crate::octo::queue_slot_bytes::<L>()),
            )
    }

    /// `τ_min`: the smallest threshold a worker holds, the initial one or the
    /// adaptive floor, within `[1, 127]`.
    pub fn octo_min_threshold(config: &OctoConfig) -> u32 {
        let initial = config.threshold.get();
        config
            .adaptive
            .as_ref()
            .map_or(initial, |adaptive| {
                initial.min(
                    adaptive
                        .min_threshold
                        .clamp(1, OCTO_MAX_PROMOTION_THRESHOLD),
                )
            })
            .clamp(1, OCTO_MAX_PROMOTION_THRESHOLD)
    }

    /// Worker ops of one update: `d` counter writes; the parent receives at
    /// most `d/τ_min` writes per update on average.
    pub fn octo_update_ops(rows: usize, config: &OctoConfig) -> (f64, f64) {
        (rows as f64, rows as f64 / octo_min_threshold(config) as f64)
    }
}
