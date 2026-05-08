#![doc = include_str!("../README.md")]
//! `asap_sketchlib` is organized into the following layers:
//!
//! - [`sketches`]: core sketch data structures such as Count-Min, HyperLogLog,
//!   KLL, and DDSketch.
//! - [`common`]: shared input types, hashing abstractions, storage backends, and
//!   reusable utilities used across sketches.
//! - [`proto`]: portable protobuf message types for sketch interchange.
//! - [`sketch_framework`]: higher-level composition layers such as Hydra,
//!   UnivMon, tumbling windows, and batch/parallel execution helpers.
//! - [`wrapper`]: wire-format-aligned variants of the sketches consumed by the
//!   ASAP query engine.
//! - [`message_pack_format`]: description of the MessagePack envelope shared
//!   with `sketchlib-go`.
//!
//! Most users can start with the crate-root re-exports such as [`DataInput`],
//! [`CountMin`], [`HyperLogLog`], [`KLL`], and [`DDSketch`]. Reach for the
//! submodules directly when you need lower-level storage, hashing, or
//! framework-specific APIs.

/// Shared primitives used across sketches, including input wrappers, hashers,
/// storage backends, and reusable utilities.
pub mod common;
/// Portable protobuf message types for sketch interchange.
pub mod proto;
/// Higher-level composition layers such as Hydra, UnivMon, tumbling windows,
/// and batch/parallel execution helpers.
pub mod sketch_framework;
/// Core sketch implementations such as Count-Min, HyperLogLog, KLL, and DDSketch.
pub mod sketches;
/// Wire-format-aligned variants of the sketches, byte-compatible with
/// `sketchlib-go` and consumed by the ASAP query engine.
pub mod wrapper;
/// MessagePack format description shared with `sketchlib-go`.
pub mod message_pack_format;
#[cfg(test)]
pub mod test_utils;

#[doc(hidden)]
pub mod __private {
    pub use serde;
    pub use serde_big_array;
}

pub use common::*;
pub use sketch_framework::*;
pub use sketches::*;
pub use wrapper::{
    COUNT_SKETCH_TOPK_CAPACITY, CmsHeapItem, CountMinSketch, CountMinSketchDelta,
    CountMinSketchWithHeap, CountSketch, CountSketchDelta, DDSKETCH_GROW_CHUNK, DdSketch,
    DdSketchDelta, DeltaResult, HllSketch, HllSketchDelta, HllVariant, HydraKllSketch, KllSketch,
    KllSketchData, SetAggregator,
};
