//! ASAPv1 wire serialization for [`SetAggregator`] (`0x08 0x00`, §3.21) and
//! its delta [`SetAggregatorDelta`] (`0x08 0x80`, §3.22).
//!
//! Neither type hashes or takes a construction parameter, so the metadata map
//! is exactly `{metadata_version: 1}`. Every member array is strictly ascending
//! by UTF-8 bytes, so a set has one encoding; members are msgpack `str` only.

use std::collections::HashSet;

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError};
use serde::{Deserialize, Serialize};

use crate::message_pack_format::envelope;
use crate::message_pack_format::kind;
use crate::message_pack_format::portable::delta_set_aggregator::SetAggregatorDelta;
use crate::message_pack_format::wire_key::WireString;

use super::SetAggregator;

/// The metadata map both kinds carry, a msgpack **map** (`to_vec_named`).
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SetMetadata {
    metadata_version: u8,
}

const SET_METADATA: SetMetadata = SetMetadata {
    metadata_version: 1,
};

/// SetAggregator payload (§3.21), positional: `[values]`.
#[derive(Debug, Serialize, Deserialize)]
struct SetPayload<S> {
    values: Vec<S>,
}

/// SetAggregatorDelta payload (§3.22), positional: `[added, removed]`.
#[derive(Debug, Serialize, Deserialize)]
struct DeltaPayload<S> {
    added: Vec<S>,
    removed: Vec<S>,
}

/// The set's members in ascending UTF-8 byte order.
fn ascending(set: &HashSet<String>) -> Vec<&str> {
    let mut members: Vec<&str> = set.iter().map(String::as_str).collect();
    members.sort_unstable();
    members
}

/// Rebuilds a set from a member array, rejecting one that is not strictly
/// ascending by UTF-8 bytes.
fn read_ascending(
    field: &str,
    members: Vec<WireString>,
) -> Result<HashSet<String>, RmpDecodeError> {
    if let Some(idx) = members.windows(2).position(|pair| pair[0].0 >= pair[1].0) {
        return Err(RmpDecodeError::Uncategorized(format!(
            "ASAPv1 {field} must be strictly ascending by UTF-8 bytes, broken at index {}",
            idx + 1
        )));
    }
    Ok(members.into_iter().map(WireString::into_string).collect())
}

/// Rejects a member that is in both `added` and `removed`.
fn check_disjoint(added: &HashSet<String>, removed: &HashSet<String>) -> Result<(), String> {
    match added.intersection(removed).next() {
        Some(member) => Err(format!(
            "ASAPv1 DeltaResult: member {member:?} is both added and removed"
        )),
        None => Ok(()),
    }
}

/// Splits the envelope, checks the kind_id and the metadata, and returns the
/// payload block.
fn open<'a>(bytes: &'a [u8], kind: &[u8], name: &str) -> Result<&'a [u8], RmpDecodeError> {
    let (kind_id, metadata, payload) =
        envelope::split(bytes).map_err(RmpDecodeError::Uncategorized)?;
    if kind_id != kind {
        return Err(RmpDecodeError::Uncategorized(format!(
            "{name} kind_id mismatch: stored {kind_id:?}, expected {kind:?}"
        )));
    }
    let meta: SetMetadata = envelope::decode_block(metadata)?;
    if meta != SET_METADATA {
        return Err(RmpDecodeError::Uncategorized(format!(
            "ASAPv1 {name} envelope: metadata mismatch"
        )));
    }
    Ok(payload)
}

impl SetAggregator {
    /// Serializes the set into an ASAPv1 MessagePack envelope, members in
    /// ascending UTF-8 byte order.
    pub fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError> {
        let metadata = rmp_serde::to_vec_named(&SET_METADATA)?;
        let payload = rmp_serde::to_vec(&SetPayload {
            values: ascending(&self.values),
        })?;
        Ok(envelope::encode(kind::SET_AGGREGATOR, &metadata, &payload))
    }

    /// Deserializes a set from an ASAPv1 MessagePack envelope.
    pub fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let payload = open(bytes, kind::SET_AGGREGATOR, "SetAggregator")?;
        let p: SetPayload<WireString> = envelope::decode_block(payload)?;
        Ok(SetAggregator {
            values: read_ascending("SetAggregator values", p.values)?,
        })
    }
}

impl SetAggregatorDelta {
    /// Serializes the delta into an ASAPv1 MessagePack envelope under the
    /// SetAggregator delta kind_id, each side in ascending UTF-8 byte order.
    /// A member in both `added` and `removed` is an error.
    pub fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError> {
        check_disjoint(&self.added, &self.removed).map_err(RmpEncodeError::Syntax)?;
        let metadata = rmp_serde::to_vec_named(&SET_METADATA)?;
        let payload = rmp_serde::to_vec(&DeltaPayload {
            added: ascending(&self.added),
            removed: ascending(&self.removed),
        })?;
        Ok(envelope::encode(
            kind::SET_AGGREGATOR_DELTA,
            &metadata,
            &payload,
        ))
    }

    /// Deserializes a delta from an ASAPv1 MessagePack envelope.
    pub fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let payload = open(bytes, kind::SET_AGGREGATOR_DELTA, "DeltaResult")?;
        let p: DeltaPayload<WireString> = envelope::decode_block(payload)?;
        let added = read_ascending("DeltaResult added", p.added)?;
        let removed = read_ascending("DeltaResult removed", p.removed)?;
        check_disjoint(&added, &removed).map_err(RmpDecodeError::Uncategorized)?;
        Ok(SetAggregatorDelta { added, removed })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set_of(members: &[&str]) -> HashSet<String> {
        members.iter().map(|m| m.to_string()).collect()
    }

    fn metadata_bytes() -> Vec<u8> {
        rmp_serde::to_vec_named(&SET_METADATA).unwrap()
    }

    fn crafted_set(values: &[&str]) -> Vec<u8> {
        let payload = rmp_serde::to_vec(&SetPayload {
            values: values.to_vec(),
        })
        .unwrap();
        envelope::encode(kind::SET_AGGREGATOR, &metadata_bytes(), &payload)
    }

    fn crafted_delta(added: &[&str], removed: &[&str]) -> Vec<u8> {
        let payload = rmp_serde::to_vec(&DeltaPayload {
            added: added.to_vec(),
            removed: removed.to_vec(),
        })
        .unwrap();
        envelope::encode(kind::SET_AGGREGATOR_DELTA, &metadata_bytes(), &payload)
    }

    #[test]
    fn set_aggregator_rejects_unordered_and_duplicate_members() {
        assert!(SetAggregator::from_asapv1(&crafted_set(&["b", "a"])).is_err());
        assert!(SetAggregator::from_asapv1(&crafted_set(&["a", "a"])).is_err());
    }

    /// A `bin` member is rejected even when it holds valid UTF-8.
    #[test]
    fn a_bin_member_is_rejected() {
        let bin = || vec![serde_bytes::Bytes::new(b"a")];
        let set = rmp_serde::to_vec(&SetPayload { values: bin() }).unwrap();
        let bytes = envelope::encode(kind::SET_AGGREGATOR, &metadata_bytes(), &set);
        assert!(SetAggregator::from_asapv1(&bytes).is_err());

        let delta = rmp_serde::to_vec(&DeltaPayload {
            added: vec![],
            removed: bin(),
        })
        .unwrap();
        let bytes = envelope::encode(kind::SET_AGGREGATOR_DELTA, &metadata_bytes(), &delta);
        assert!(SetAggregatorDelta::from_asapv1(&bytes).is_err());
    }

    #[test]
    fn metadata_must_be_exactly_metadata_version_1() {
        #[derive(Serialize)]
        struct WithExtra {
            metadata_version: u8,
            bogus_field: u8,
        }
        let payload = rmp_serde::to_vec(&SetPayload::<&str> { values: vec![] }).unwrap();
        let metas = [
            rmp_serde::to_vec_named(&WithExtra {
                metadata_version: 1,
                bogus_field: 0,
            })
            .unwrap(),
            rmp_serde::to_vec_named(&SetMetadata {
                metadata_version: 2,
            })
            .unwrap(),
        ];
        for meta in metas {
            let bytes = envelope::encode(kind::SET_AGGREGATOR, &meta, &payload);
            assert!(SetAggregator::from_asapv1(&bytes).is_err());
        }
    }

    /// A well-formed payload under another kind_id is rejected: the set under
    /// the delta's, the delta under the set's or the retired `0x09 0x00`.
    #[test]
    fn each_decoder_rejects_a_foreign_kind_id() {
        let set = crafted_set(&["a"]);
        let delta = crafted_delta(&["a"], &[]);
        let (_, metadata, payload) = envelope::split(&set).unwrap();
        let bytes = envelope::encode(kind::SET_AGGREGATOR_DELTA, metadata, payload);
        assert!(SetAggregator::from_asapv1(&bytes).is_err());
        for kind in [kind::SET_AGGREGATOR, &[0x09, 0x00]] {
            let (_, metadata, payload) = envelope::split(&delta).unwrap();
            let bytes = envelope::encode(kind, metadata, payload);
            assert!(SetAggregatorDelta::from_asapv1(&bytes).is_err());
        }
    }

    #[test]
    fn delta_result_rejects_a_member_both_added_and_removed() {
        let both = crafted_delta(&["a", "b"], &["b"]);
        assert!(SetAggregatorDelta::from_asapv1(&both).is_err());
        let delta = SetAggregatorDelta {
            added: set_of(&["a"]),
            removed: set_of(&["a"]),
        };
        assert!(delta.to_asapv1().is_err());
    }

    #[test]
    fn delta_result_rejects_unordered_sides() {
        assert!(SetAggregatorDelta::from_asapv1(&crafted_delta(&["b", "a"], &[])).is_err());
        assert!(SetAggregatorDelta::from_asapv1(&crafted_delta(&[], &["c", "c"])).is_err());
    }
}
