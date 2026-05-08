use std::collections::HashSet;

use crate::message_pack_format::dto::{StringSetOwned, StringSetRef};
use crate::message_pack_format::{Error as MsgPackError, MessagePackCodec};

/// Set aggregator for tracking a set of unique string keys.
/// Wire format: `StringSet { values: HashSet<String> }` in MessagePack.
#[derive(Debug, Clone)]
pub struct SetAggregator {
    pub values: HashSet<String>,
}

impl SetAggregator {
    pub fn new() -> Self {
        Self {
            values: HashSet::new(),
        }
    }

    /// Insert a key into the set.
    pub fn update(&mut self, key: &str) {
        self.values.insert(key.to_string());
    }

    /// Merge another SetAggregator into self in place (set union).
    pub fn merge(
        &mut self,
        other: &SetAggregator,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for v in &other.values {
            self.values.insert(v.clone());
        }
        Ok(())
    }

    /// Merge from references, returning a new aggregator.
    pub fn merge_refs(
        inputs: &[&SetAggregator],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if inputs.is_empty() {
            return Err("SetAggregator::merge_refs called with empty input".into());
        }
        let mut merged = SetAggregator::new();
        for s in inputs {
            merged.merge(s)?;
        }
        Ok(merged)
    }

    /// Serialize to MessagePack: `StringSet { values: HashSet<String> }` as a msgpack map.
    /// Thin shim over [`MessagePackCodec::to_msgpack`].
    pub fn serialize_msgpack(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        self.to_msgpack().map_err(MsgPackError::into_encode)
    }

    /// Thin shim over [`MessagePackCodec::from_msgpack`].
    pub fn deserialize_msgpack(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::from_msgpack(buffer).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("Failed to deserialize SetAggregator from MessagePack: {e}").into()
        })
    }
}

impl MessagePackCodec for SetAggregator {
    fn to_msgpack(&self) -> Result<Vec<u8>, MsgPackError> {
        let wrapper = StringSetRef {
            values: &self.values,
        };
        Ok(rmp_serde::to_vec(&wrapper)?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, MsgPackError> {
        let wrapper: StringSetOwned = rmp_serde::from_slice(bytes)?;
        Ok(Self {
            values: wrapper.values,
        })
    }
}

impl Default for SetAggregator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[test]
    fn test_creation() {
        let sa = SetAggregator::new();
        assert!(sa.values.is_empty());
    }

    #[test]
    fn test_insert() {
        let mut sa = SetAggregator::new();
        sa.update("web");
        sa.update("api");
        sa.update("web"); // duplicate
        assert_eq!(sa.values.len(), 2);
        assert!(sa.values.contains("web"));
        assert!(sa.values.contains("api"));
    }

    #[test]
    fn test_merge() {
        let mut sa1 = SetAggregator::new();
        let mut sa2 = SetAggregator::new();

        sa1.update("web");
        sa1.update("api");
        sa2.update("api"); // duplicate
        sa2.update("db");

        sa1.merge(&sa2).unwrap();
        assert_eq!(sa1.values.len(), 3);
        assert!(sa1.values.contains("web"));
        assert!(sa1.values.contains("api"));
        assert!(sa1.values.contains("db"));
    }

    #[test]
    fn test_msgpack_round_trip() {
        let mut sa = SetAggregator::new();
        sa.update("web");
        sa.update("api");

        let bytes = sa.serialize_msgpack().unwrap();
        let deserialized = SetAggregator::deserialize_msgpack(&bytes).unwrap();

        assert_eq!(deserialized.values.len(), 2);
        assert!(deserialized.values.contains("web"));
        assert!(deserialized.values.contains("api"));
    }

    #[test]
    fn test_msgpack_matches_wire_format() {
        // Verify wire format is StringSet { values: [...] } not a plain array.
        #[derive(Deserialize)]
        struct StringSet {
            values: HashSet<String>,
        }
        let mut sa = SetAggregator::new();
        sa.update("a");
        let bytes = sa.serialize_msgpack().unwrap();
        let decoded: StringSet =
            rmp_serde::from_slice(&bytes).expect("should decode as StringSet { values: ... }");
        assert!(decoded.values.contains("a"));
    }
}
