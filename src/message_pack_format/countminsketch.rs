//! MessagePack wire-format for [`crate::wrapper::CountMinSketch`].
//!
//! Owns the wire DTO and the [`MessagePackCodec`] impl for the
//! wire-format-aligned `CountMinSketch` wrapper.

use serde::{Deserialize, Serialize};

use super::{Error as MsgPackError, MessagePackCodec};
use crate::wrapper::countminsketch::{CountMinSketch, sketchlib_cms_from_matrix};

/// Wire DTO for [`crate::wrapper::CountMinSketch`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountMinSketchWire {
    pub sketch: Vec<Vec<f64>>,
    #[serde(rename = "row_num")]
    pub rows: usize,
    #[serde(rename = "col_num")]
    pub cols: usize,
}

impl MessagePackCodec for CountMinSketch {
    fn to_msgpack(&self) -> Result<Vec<u8>, MsgPackError> {
        let wire = CountMinSketchWire {
            sketch: self.sketch(),
            rows: self.rows,
            cols: self.cols,
        };
        Ok(rmp_serde::to_vec(&wire)?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, MsgPackError> {
        let wire: CountMinSketchWire = rmp_serde::from_slice(bytes)?;
        let backend = sketchlib_cms_from_matrix(wire.rows, wire.cols, &wire.sketch);
        Ok(Self {
            rows: wire.rows,
            cols: wire.cols,
            backend,
        })
    }
}
