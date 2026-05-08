//! Wire-format-aligned sketch types consumed by the ASAP query engine.
//!
//! These mirror the in-process sketches in [`crate::sketches`] but expose
//! public-field, proto-decode-friendly shapes byte-compatible with
//! `sketchlib-go`. See [`crate::message_pack_format`] for a description of
//! the on-the-wire MessagePack envelope.

pub mod ddsketch;
pub use ddsketch::{DDSKETCH_GROW_CHUNK, DdSketch, DdSketchDelta};

pub mod countminsketch;
pub use countminsketch::{CountMinSketch, CountMinSketchDelta};

pub mod countsketch;
pub use countsketch::{COUNT_SKETCH_TOPK_CAPACITY, CountSketch, CountSketchDelta};

pub mod hll;
pub use hll::{HllSketch, HllSketchDelta, HllVariant};

pub mod kll;
pub use kll::{KllSketch, KllSketchData};

pub mod countminsketch_topk;
pub use countminsketch_topk::{CmsHeapItem, CountMinSketchWithHeap};

pub mod hydra_kll;
pub use hydra_kll::HydraKllSketch;

pub mod set_aggregator;
pub use set_aggregator::SetAggregator;

pub mod delta_set_aggregator;
pub use delta_set_aggregator::DeltaResult;
