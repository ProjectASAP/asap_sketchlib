//! Wire-level Data Transfer Objects for MessagePack serialization.
//!
//! These types describe the over-the-wire shapes shared with
//! `sketchlib-go`. They are the canonical schema; runtime types in
//! [`crate::wrapper`] convert to/from them via
//! [`crate::message_pack_format::MessagePackCodec`].
//!
//! Three wrapper types — `CountSketch`, `DdSketch`, and `HllSketch` —
//! act as their own DTOs (their public fields ARE the wire fields).
//! Those are not redefined here; the `MessagePackCodec` impls in
//! `wrapper/{countsketch,ddsketch,hll}.rs` serialize the wrapper
//! struct directly.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::wrapper::countminsketch_topk::CmsHeapItem;

// ---------- CountMinSketch ----------

/// Wire DTO for [`crate::wrapper::CountMinSketch`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountMinSketchWire {
    pub sketch: Vec<Vec<f64>>,
    #[serde(rename = "row_num")]
    pub rows: usize,
    #[serde(rename = "col_num")]
    pub cols: usize,
}

// ---------- CountMinSketchWithHeap ----------

/// Inner CMS payload nested inside [`CountMinSketchWithHeapWire`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountMinSketchInnerWire {
    pub sketch: Vec<Vec<f64>>,
    #[serde(rename = "row_num")]
    pub rows: usize,
    #[serde(rename = "col_num")]
    pub cols: usize,
}

/// Wire DTO for [`crate::wrapper::CountMinSketchWithHeap`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountMinSketchWithHeapWire {
    pub sketch: CountMinSketchInnerWire,
    pub topk_heap: Vec<CmsHeapItem>,
    pub heap_size: usize,
}

// ---------- KLL ----------

/// Wire DTO for [`crate::wrapper::KllSketch`]. Public — referenced as a
/// nested field by [`HydraKllSketchWire`] and re-exported through
/// [`crate::wrapper`] for backwards compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KllSketchData {
    pub k: u16,
    pub sketch_bytes: Vec<u8>,
}

// ---------- HydraKLL ----------

/// Wire DTO for [`crate::wrapper::HydraKllSketch`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HydraKllSketchWire {
    #[serde(rename = "row_num")]
    pub rows: usize,
    #[serde(rename = "col_num")]
    pub cols: usize,
    pub sketches: Vec<Vec<KllSketchData>>,
}

// ---------- SetAggregator ----------

/// Borrowed serialize-side wire DTO for [`crate::wrapper::SetAggregator`].
/// Used to avoid cloning the underlying set on the encode path.
#[derive(Serialize)]
pub(crate) struct StringSetRef<'a> {
    pub values: &'a HashSet<String>,
}

/// Owned deserialize-side wire DTO for [`crate::wrapper::SetAggregator`].
#[derive(Deserialize)]
pub(crate) struct StringSetOwned {
    pub values: HashSet<String>,
}

// ---------- DeltaSetAggregator ----------

/// Wire DTO for the delta set aggregator. Public — re-exported through
/// [`crate::wrapper::delta_set_aggregator`] for backwards compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaResult {
    pub added: HashSet<String>,
    pub removed: HashSet<String>,
}
