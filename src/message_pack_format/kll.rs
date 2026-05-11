//! MessagePack wire-format for [`crate::wrapper::KllSketch`].
//!
//! Owns the wire DTO ([`KllSketchData`]) and the [`MessagePackCodec`]
//! impl for the wire-format-aligned `KllSketch` wrapper. The DTO is
//! also referenced as a nested field by
//! [`crate::message_pack_format::hydra_kll::HydraKllSketchWire`].

use serde::{Deserialize, Serialize};

use super::{Error as MsgPackError, MessagePackCodec};
use crate::sketches::kll::KLL;
use crate::wrapper::kll::KllSketch;

/// Wire DTO for [`crate::wrapper::KllSketch`]. Public — referenced as a
/// nested field by [`crate::message_pack_format::hydra_kll::HydraKllSketchWire`]
/// and re-exported through [`crate::wrapper::kll`] for backwards
/// compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KllSketchData {
    pub k: u16,
    pub sketch_bytes: Vec<u8>,
}

impl MessagePackCodec for KllSketch {
    fn to_msgpack(&self) -> Result<Vec<u8>, MsgPackError> {
        let wire = KllSketchData {
            k: self.k,
            sketch_bytes: self.sketch_bytes(),
        };
        Ok(rmp_serde::to_vec(&wire)?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, MsgPackError> {
        let wire: KllSketchData = rmp_serde::from_slice(bytes)?;
        // Decode the nested KLL payload via the typed `rmp_serde::decode::Error`
        // path so that the surfaced `MsgPackError::Decode` carries the real
        // underlying error rather than a stringified box.
        let backend = KLL::deserialize_from_bytes(&wire.sketch_bytes)?;
        Ok(Self { k: wire.k, backend })
    }
}
