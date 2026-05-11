//! MessagePack wire-format for [`crate::wrapper::HydraKllSketch`].
//!
//! Owns the wire DTO and the [`MessagePackCodec`] impl for the
//! wire-format-aligned `HydraKllSketch` wrapper.

use serde::{Deserialize, Serialize};

use crate::message_pack_format::portable::kll::KllSketchData;
use crate::message_pack_format::{Error as MsgPackError, MessagePackCodec};
use crate::wrapper::hydra_kll::HydraKllSketch;
use crate::wrapper::kll::KllSketch;

/// Wire DTO for [`crate::wrapper::HydraKllSketch`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HydraKllSketchWire {
    #[serde(rename = "row_num")]
    pub rows: usize,
    #[serde(rename = "col_num")]
    pub cols: usize,
    pub sketches: Vec<Vec<KllSketchData>>,
}

impl MessagePackCodec for HydraKllSketch {
    fn to_msgpack(&self) -> Result<Vec<u8>, MsgPackError> {
        let mut sketches = Vec::with_capacity(self.rows);
        for row in &self.sketch {
            let mut row_data = Vec::with_capacity(self.cols);
            for cell in row {
                row_data.push(KllSketchData {
                    k: cell.k,
                    sketch_bytes: cell.sketch_bytes(),
                });
            }
            sketches.push(row_data);
        }
        let wire = HydraKllSketchWire {
            rows: self.rows,
            cols: self.cols,
            sketches,
        };
        Ok(rmp_serde::to_vec(&wire)?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, MsgPackError> {
        use crate::sketches::kll::KLL;
        use rmp_serde::decode::Error as RmpDecodeError;

        let wire: HydraKllSketchWire = rmp_serde::from_slice(bytes)?;

        if wire.sketches.len() != wire.rows {
            return Err(MsgPackError::Decode(RmpDecodeError::Uncategorized(
                format!(
                    "HydraKLL row count mismatch: expected {}, got {}",
                    wire.rows,
                    wire.sketches.len()
                ),
            )));
        }

        let mut sketch: Vec<Vec<KllSketch>> = Vec::with_capacity(wire.rows);
        for (row_idx, row) in wire.sketches.into_iter().enumerate() {
            if row.len() != wire.cols {
                return Err(MsgPackError::Decode(RmpDecodeError::Uncategorized(
                    format!(
                        "HydraKLL column count mismatch in row {}: expected {}, got {}",
                        row_idx,
                        wire.cols,
                        row.len()
                    ),
                )));
            }
            let mut accum_row: Vec<KllSketch> = Vec::with_capacity(wire.cols);
            for cell in row {
                let backend = KLL::deserialize_from_bytes(&cell.sketch_bytes)?;
                accum_row.push(KllSketch { k: cell.k, backend });
            }
            sketch.push(accum_row);
        }

        Ok(Self {
            sketch,
            rows: wire.rows,
            cols: wire.cols,
        })
    }
}
