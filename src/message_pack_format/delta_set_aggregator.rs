//! MessagePack wire-format for the delta set aggregator (see
//! [`crate::wrapper::delta_set_aggregator`]).
//!
//! Owns the [`DeltaResult`] wire DTO and its [`MessagePackCodec`] impl.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use super::{Error as MsgPackError, MessagePackCodec};

/// Wire DTO for the delta set aggregator. Public — re-exported through
/// [`crate::wrapper::delta_set_aggregator`] for backwards compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaResult {
    pub added: HashSet<String>,
    pub removed: HashSet<String>,
}

impl MessagePackCodec for DeltaResult {
    fn to_msgpack(&self) -> Result<Vec<u8>, MsgPackError> {
        Ok(rmp_serde::to_vec(self)?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, MsgPackError> {
        Ok(rmp_serde::from_slice(bytes)?)
    }
}
