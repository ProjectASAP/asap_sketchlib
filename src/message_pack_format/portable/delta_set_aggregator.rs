//! Wire-format DTO for the delta set aggregator.
//!
//! Owns the [`DeltaResult`] wire DTO and its [`MessagePackCodec`] impl.
//! The streaming/state-tracking logic lives in the downstream consumer
//! (ASAPQuery's accumulators) — only the over-the-wire shape lives here.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::message_pack_format::{Error as MsgPackError, MessagePackCodec};

/// Wire DTO for the delta set aggregator: a snapshot of added/removed
/// string keys between two consecutive observations.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_msgpack_round_trip() {
        let mut added = HashSet::new();
        added.insert("web".to_string());
        added.insert("api".to_string());

        let mut removed = HashSet::new();
        removed.insert("db".to_string());

        let bytes = DeltaResult { added, removed }.to_msgpack().unwrap();
        let result = DeltaResult::from_msgpack(&bytes).unwrap();

        assert_eq!(result.added.len(), 2);
        assert!(result.added.contains("web"));
        assert!(result.added.contains("api"));
        assert_eq!(result.removed.len(), 1);
        assert!(result.removed.contains("db"));
    }

    #[test]
    fn test_empty_sets() {
        let dr = DeltaResult {
            added: HashSet::new(),
            removed: HashSet::new(),
        };
        let bytes = dr.to_msgpack().unwrap();
        let result = DeltaResult::from_msgpack(&bytes).unwrap();
        assert!(result.added.is_empty());
        assert!(result.removed.is_empty());
    }
}
