//! Core sketch implementations.
//!
//! This module is the main entry point for the library's sketch algorithms.
//! Broadly, the sketches fall into a few common use cases:
//!
//! - frequency estimation: [`CountMin`], [`Count`]
//! - cardinality estimation: [`HyperLogLog`]
//! - quantiles and distributions: [`KLL`], [`DDSketch`]
//! - specialized or composite structures: heap-backed, folded, and
//!   runtime-oriented variants such as [`CMSHeap`], [`CSHeap`], [`FoldCMS`], and
//!   [`FoldCS`]
//!
//! For most users:
//!
//! - choose [`CountMin`] for fast approximate non-negative frequency queries
//! - choose [`Count`] for count-sketch style frequency estimation
//! - choose [`HyperLogLog`] for distinct counts
//! - choose [`KLL`] for general quantile estimation
//! - choose [`DDSketch`] when relative-error quantiles are important
//!
//! [`FastPath`] and [`RegularPath`] control how some matrix-backed frequency
//! sketches map values to rows and columns. Several specialized sketches in
//! this module are feature-gated behind `experimental`.

#[cfg(feature = "experimental")]
pub mod coco;
#[cfg(feature = "experimental")]
pub use coco::Coco;
#[cfg(feature = "experimental")]
pub use coco::CocoBucket;

pub mod count;
pub use count::Count;
pub use count::CountL2HH;

pub mod mode;
pub use mode::{FastPath, RegularPath};

pub mod countmin;
pub use crate::MatrixStorage;
pub use countmin::{CountMin, QUICKSTART_COL_NUM, QUICKSTART_ROW_NUM};

#[cfg(feature = "experimental")]
pub mod elastic;
#[cfg(feature = "experimental")]
pub use elastic::Elastic;
#[cfg(feature = "experimental")]
pub use elastic::HeavyBucket;

pub mod hll;
pub use hll::{
    Classic, ErtlMLE, HyperLogLog, HyperLogLogHIP, HyperLogLogHIPP12, HyperLogLogHIPP14,
    HyperLogLogHIPP16, HyperLogLogP12, HyperLogLogP14, HyperLogLogP16,
};

pub mod kll;
pub use kll::KLL;

pub mod kll_dynamic;
pub use kll_dynamic::KLLDynamic;

#[cfg(feature = "experimental")]
pub mod kmv;
#[cfg(feature = "experimental")]
pub use kmv::KMV;

#[cfg(feature = "experimental")]
pub mod uniform;
#[cfg(feature = "experimental")]
pub use uniform::UniformSampling;

pub mod ddsketch;
pub use ddsketch::DDSketch;

pub mod cms_heap;
pub use cms_heap::CMSHeap;

pub mod cs_heap;
pub use cs_heap::CSHeap;

pub mod octo_delta;
pub use octo_delta::{CM_PROMASK, COUNT_PROMASK, CmDelta, CountDelta, HLL_PROMASK, HllDelta};

pub mod fold_cms;
pub use fold_cms::{FoldCMS, FoldCell, FoldEntry};

pub mod fold_cs;
pub use fold_cs::FoldCS;
