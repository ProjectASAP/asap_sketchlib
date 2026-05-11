// Wire format for the delta set aggregator: `DeltaResult` plus module-level
// `serialize_msgpack` / `deserialize_msgpack` free functions. Streaming
// logic (window tracking, stateful accumulation) stays upstream — only the
// over-the-wire shape lives here.

use std::collections::HashSet;

use crate::message_pack_format::{Error as MsgPackError, MessagePackCodec};

/// Re-export of the wire DTO — canonical definition lives in
/// [`crate::message_pack_format::portable::delta_set_aggregator::DeltaResult`].
/// Preserved here for backwards compatibility.
pub use crate::message_pack_format::portable::delta_set_aggregator::DeltaResult;

/// Serialize a delta result to MessagePack. Thin shim over
/// [`MessagePackCodec::to_msgpack`] preserved for backwards compatibility.
pub fn serialize_msgpack(
    added: &HashSet<String>,
    removed: &HashSet<String>,
) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    let result = DeltaResult {
        added: added.clone(),
        removed: removed.clone(),
    };
    result.to_msgpack().map_err(MsgPackError::into_encode)
}

/// Deserialize a delta result from MessagePack. Thin shim over
/// [`MessagePackCodec::from_msgpack`].
pub fn deserialize_msgpack(
    buffer: &[u8],
) -> Result<DeltaResult, Box<dyn std::error::Error + Send + Sync>> {
    DeltaResult::from_msgpack(buffer).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
        format!("Failed to deserialize DeltaResult from MessagePack: {e}").into()
    })
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

        let bytes = serialize_msgpack(&added, &removed).unwrap();
        let result = deserialize_msgpack(&bytes).unwrap();

        assert_eq!(result.added.len(), 2);
        assert!(result.added.contains("web"));
        assert!(result.added.contains("api"));
        assert_eq!(result.removed.len(), 1);
        assert!(result.removed.contains("db"));
    }

    #[test]
    fn test_empty_sets() {
        let added = HashSet::new();
        let removed = HashSet::new();
        let bytes = serialize_msgpack(&added, &removed).unwrap();
        let result = deserialize_msgpack(&bytes).unwrap();
        assert!(result.added.is_empty());
        assert!(result.removed.is_empty());
    }
}
