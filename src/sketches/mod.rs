//! Core sketch implementations.
//!
//! This module is the main entry point for the library's sketch algorithms.
//! Broadly, the sketches fall into a few common use cases:
//!
//! - frequency estimation: [`CountMinSketch`], [`CountSketch`]
//! - cardinality estimation: [`HyperLogLog`]
//! - quantiles and distributions: [`Kll`], [`DdSketch`]
//! - specialized or composite structures: heap-backed, folded, and
//!   runtime-oriented variants such as [`CountMinSketchWithHeap`], [`CountSketchWithHeap`],
//!   [`CountMinSketchWithSpaceSaving`], [`CountSketchWithSpaceSaving`], [`CountMinSketchFold`], and [`CountSketchFold`]
//!
//! For most users:
//!
//! - choose [`CountMinSketch`] for fast approximate non-negative frequency queries
//! - choose [`CountSketch`] for Count Sketch style frequency estimation
//! - choose [`HyperLogLog`] for distinct counts
//! - choose [`Kll`] for general quantile estimation
//! - choose [`DdSketch`] when relative-error quantiles are important
//!
//! [`FastPath`] and [`RegularPath`] control how some matrix-backed frequency
//! sketches map values to rows and columns. Several specialized sketches in
//! this module are feature-gated behind `experimental`.
//!
//! ## `*sketch` vs `*sketch_heap` files
//!
//! For each Count-Min / Count Sketch style algorithm we ship two files:
//!
//! - [`countminsketch`] / [`countsketch`] — the core sketch (matrix of
//!   counters only). Answers point-frequency queries: "how often did key `k`
//!   appear?" These do not track which keys are heavy hitters.
//! - [`countminsketch_heap`] / [`countsketch_heap`] — the same sketch paired
//!   with a min-heap that tracks the top-`k` heavy hitters as items stream in
//!   ([`CountMinSketchWithHeap`], [`CountSketchWithHeap`]). Use these when the question is
//!   "which keys are the most frequent?" rather than "how often did this
//!   specific key appear?"
//!
//! The split keeps the core sketch lean for callers that only need point
//! queries, while the `_heap` variants compose the sketch with heap
//! bookkeeping for heavy-hitter workloads.

pub mod coco;
pub use coco::CocoBucket;
pub use coco::CocoSketch;

/// Fixed-counter heavy-hitter tracking over a Stream-Summary.
pub mod space_saving;
pub use space_saving::{SPACE_SAVING_DEFAULT_CAPACITY, SpaceSaving};

/// Approximate set membership over a packed bit grid.
pub mod bloom;
pub use bloom::{
    BLOOM_DEFAULT_COLS, BLOOM_DEFAULT_ROWS, BLOOM_MAX_BITS, BLOOM_MAX_SLICES, Bloom, BloomMode,
};

pub mod countsketch;
pub use countsketch::{CountSketch, CountSketchDelta};

/// Count-Min-shaped grid with a per-bucket HyperLogLog (distinct-count) sketch.
#[cfg(feature = "experimental")]
pub mod countminsketch_hll;
#[cfg(feature = "experimental")]
pub use countminsketch_hll::CountMinHll;

/// Sliding-window frequency estimation with adaptive zoom (MicroscopeSketch).
#[cfg(feature = "experimental")]
pub mod microscope;
#[cfg(feature = "experimental")]
pub use microscope::MicroCm;

/// Hashing path markers for matrix-backed sketches.
pub mod mode;
pub use mode::{FastPath, RegularPath};

pub mod countminsketch;
pub use crate::MatrixStorage;
pub use countminsketch::{CountMinSketch, CountMinSketchDelta, QUICKSTART_COLS, QUICKSTART_ROWS};

pub mod elastic;
pub use elastic::ElasticSketch;

/// An exact set of string keys and its delta.
pub mod set_aggregator;
pub use elastic::HeavyBucket;
pub use set_aggregator::{SetAggregator, SetAggregatorDelta};

/// HyperLogLog implementations and aliases.
pub mod hll;
pub use hll::{
    Classic, Ertl, HyperLogLog, HyperLogLogDelta, HyperLogLogHip, HyperLogLogHipP12,
    HyperLogLogHipP14, HyperLogLogHipP16, HyperLogLogP12, HyperLogLogP14, HyperLogLogP16,
};

pub mod kll;
pub use kll::Kll;

pub mod kll_dynamic;
pub use kll_dynamic::KllDynamic;

#[cfg(feature = "experimental")]
pub mod kmv;
#[cfg(feature = "experimental")]
pub use kmv::Kmv;

#[cfg(feature = "experimental")]
pub mod uniform;
#[cfg(feature = "experimental")]
pub use uniform::UniformSampling;

pub mod ddsketch;
pub use ddsketch::{DdSketch, DdSketchDelta};

pub mod countminsketch_heap;
pub use countminsketch_heap::{CountMinSketchWithHeap, CountMinSketchWithHeapDelta};

pub mod countsketch_heap;
pub use countsketch_heap::{CountSketchHeapCounter, CountSketchWithHeap, CountSketchWithHeapDelta};

pub mod countsketch_l2_norm;
pub use countsketch_l2_norm::CountSketchWithL2Norm;

pub mod countminsketch_space_saving;
pub use countminsketch_space_saving::{CandidateCounter, CountMinSketchWithSpaceSaving};

pub mod countsketch_space_saving;
pub use countsketch_space_saving::CountSketchWithSpaceSaving;

pub mod octo_delta;
pub use octo_delta::{
    CocoDelta, CountMinSketchCellDelta, CountSketchCellDelta, DdSketchBucketDelta, ElasticDelta,
    HllRegisterDelta, KeyedCountMinSketchCellDelta, KeyedCountSketchCellDelta,
    OCTO_COCO_DEFAULT_THRESHOLD, OCTO_COUNT_MIN_DEFAULT_THRESHOLD,
    OCTO_COUNT_SKETCH_DEFAULT_THRESHOLD, OCTO_DDSKETCH_DEFAULT_THRESHOLD,
    OCTO_ELASTIC_DEFAULT_THRESHOLD, OCTO_HLL_DEFAULT_THRESHOLD, OCTO_MAX_PROMOTION_THRESHOLD,
    OCTO_UNIVMON_DEFAULT_THRESHOLD, OctoThreshold, UnivMonCellDelta,
};

pub mod count_min_fold;
pub use count_min_fold::{CountMinSketchFold, FoldCell, FoldEntry};

pub mod count_sketch_fold;
pub use count_sketch_fold::CountSketchFold;
