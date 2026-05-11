//! MessagePack wire-format for [`crate::wrapper::CountMinSketchWithHeap`].
//!
//! Owns the wire DTOs and the [`MessagePackCodec`] impl for the
//! wire-format-aligned `CountMinSketchWithHeap` wrapper.

use serde::{Deserialize, Serialize};

use super::{Error as MsgPackError, MessagePackCodec};
use crate::wrapper::countminsketch_topk::{
    CmsHeapItem, CountMinSketchWithHeap, WireHeapItem, sketchlib_cms_heap_from_matrix_and_heap,
};

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

impl MessagePackCodec for CountMinSketchWithHeap {
    fn to_msgpack(&self) -> Result<Vec<u8>, MsgPackError> {
        let wire = CountMinSketchWithHeapWire {
            sketch: CountMinSketchInnerWire {
                sketch: self.sketch_matrix(),
                rows: self.rows,
                cols: self.cols,
            },
            topk_heap: self.topk_heap_items(),
            heap_size: self.heap_size,
        };
        Ok(rmp_serde::to_vec(&wire)?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, MsgPackError> {
        let wire: CountMinSketchWithHeapWire = rmp_serde::from_slice(bytes)?;

        let mut sorted_topk_heap = wire.topk_heap;
        sorted_topk_heap.sort_by(|a, b| b.value.partial_cmp(&a.value).unwrap());

        let wire_heap: Vec<WireHeapItem> = sorted_topk_heap
            .iter()
            .map(|h| WireHeapItem {
                key: h.key.clone(),
                value: h.value,
            })
            .collect();
        let backend = sketchlib_cms_heap_from_matrix_and_heap(
            wire.sketch.rows,
            wire.sketch.cols,
            wire.heap_size,
            &wire.sketch.sketch,
            &wire_heap,
        );

        Ok(Self {
            rows: wire.sketch.rows,
            cols: wire.sketch.cols,
            heap_size: wire.heap_size,
            backend,
        })
    }
}
