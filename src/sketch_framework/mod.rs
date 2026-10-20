//! Higher-level frameworks built on top of the core sketches.
//!
//! These APIs compose, extend, or orchestrate sketches for more advanced
//! workflows:
//!
//! - windowed analytics: [`ExponentialHistogram`], [`TumblingWindow`]
//! - subpopulation queries: [`Hydra`]
//! - universal multi-metric monitoring: [`UnivMon`] and experimental [`UnivMonQ`]
//! - batch update acceleration: [`NitroBatch`]
//! - shared-hash or multi-sketch coordination: [`HashSketchEnsemble`]
//! - runtime and parallel execution helpers: the `octo` family
//!
//! Most users can begin with the core algorithms in [`crate::sketches`]. Reach
//! for this module when you need to add windows, grouped queries, coordinated
//! execution, or higher-level sketch composition.
//!
//! Some parts of this module are feature-gated:
//!
//! - `eh_univmon` requires `experimental`
//! - the `OctoRuntime` execution runtime requires `octo-runtime`

pub mod eh;
pub use eh::EhBucket;
pub use eh::ExponentialHistogram;

/// Sketch-type adapters used by exponential histograms.
pub mod eh_bucket_sketch;
pub use eh_bucket_sketch::EhBucketSketch;
pub use eh_bucket_sketch::SketchNorm;

pub mod hashlayer;
pub use hashlayer::{EnsembleSketch, HashSketchEnsemble};

pub mod sketch_catalog;

pub mod hydra;
pub use hydra::Hydra;

pub mod univmon;
pub use univmon::{UnivMon, UnivMonDeltaFidelity};

pub mod univmon_q;
pub use univmon_q::{
    UnivMonQ, UnivMonQCdfEntry, UnivMonQConfig, UnivMonQError, UnivMonQOrderedQueryDiagnostics,
    UnivMonQQuery,
};

pub mod univmon_two_tier;
pub use univmon_two_tier::{UnivMonPool, UnivMonTwoTier};

pub mod nitro;
pub use nitro::{
    NitroBatch, NitroEstimate, NitroTarget, nitro_delta_saturated_i32, nitro_delta_saturated_u32,
};

#[cfg(feature = "experimental")]
pub mod eh_univmon;
#[cfg(feature = "experimental")]
pub use eh_univmon::{
    EhUnivMonExactBucket, EhUnivMonQueryResult, EhUnivMonSketchBucket, ExponentialHistogramUnivMon,
};

pub mod octo;
pub use octo::{
    CocoOctoAggregator, CocoOctoPlan, CocoOctoWorker, CocoSketchWorker,
    CountMinSketchOctoAggregator, CountMinSketchOctoPlan, CountMinSketchOctoWorker,
    CountMinSketchWithHeapOctoAggregator, CountMinSketchWithHeapOctoPlan,
    CountMinSketchWithHeapOctoWorker, CountMinSketchWorker, CountSketchOctoAggregator,
    CountSketchOctoPlan, CountSketchOctoWorker, CountSketchWithHeapOctoAggregator,
    CountSketchWithHeapOctoPlan, CountSketchWithHeapOctoWorker, CountSketchWithL2NormWorker,
    CountSketchWorker, DdOctoAggregator, DdOctoPlan, DdOctoWorker, DdSketchWorker,
    ElasticOctoAggregator, ElasticOctoPlan, ElasticOctoWorker, ElasticSketchWorker,
    HllOctoAggregator, HllOctoPlan, HllOctoWorker, KeyedHashes, KeyedLayerHashes,
    OCTO_DEFAULT_HEAP_SIZE, OctoAggregator, OctoPlan, OctoWorker, RowHashes, UnivMonOctoAggregator,
    UnivMonOctoPlan, UnivMonOctoWorker, flow_key_string, max_hll_threshold, threshold_for_error,
    univmon_layer_threshold,
};
#[cfg(feature = "octo-runtime")]
pub use octo::{
    OctoAdaptiveThreshold, OctoConfig, OctoPartition, OctoReadHandle, OctoResult, OctoRuntime,
    run_octo,
};

pub mod tumbling;
pub use tumbling::{
    CountMinSketchFoldConfig, CountSketchFoldConfig, KllConfig, SketchPool, TumblingWindow,
    TumblingWindowSketch,
};
