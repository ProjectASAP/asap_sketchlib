//! ASAPv1 wire serialization for [`SetAggregator`] (`0x08 0x00`, §3.21) and
//! [`DeltaResult`] (`0x09 0x00`, §3.22).
//!
//! ## No hash spec, no structural params
//!
//! Neither type hashes a key onto the wire or takes a construction parameter,
//! so the metadata map is `metadata_version` alone.
//!
//! ## Canonical order
//!
//! Every string array is written in strictly ascending byte order of its UTF-8
//! encoding, so a set has exactly one encoding. The decoder rejects any other
//! order, which also rejects a duplicate key.

use std::collections::HashSet;

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError, from_slice};
use serde::{Deserialize, Serialize};

use crate::asapv1::envelope;
use crate::asapv1::wire_key::WireString;

use super::{DeltaResult, SetAggregator};

/// SetAggregator kind_id: family `0x08`, single variant `0x00`.
const SET_AGGREGATOR_KIND: &[u8] = &[0x08, 0x00];

/// DeltaResult kind_id: family `0x09`, single variant `0x00`.
const DELTA_RESULT_KIND: &[u8] = &[0x09, 0x00];

/// The metadata map both kinds carry (ASAPv1 §2), written with
/// `to_vec_named`. `deny_unknown_fields` makes decode fail closed.
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
struct SetAggregatorPayload<S> {
    values: Vec<S>,
}

/// DeltaResult payload (§3.22), positional: `[added, removed]`.
#[derive(Debug, Serialize, Deserialize)]
struct DeltaResultPayload<S> {
    added: Vec<S>,
    removed: Vec<S>,
}

/// The set's members in canonical (ascending byte) order.
fn canonical(set: &HashSet<String>) -> Vec<&str> {
    let mut keys: Vec<&str> = set.iter().map(String::as_str).collect();
    keys.sort_unstable();
    keys
}

/// Rebuilds a set from a wire array, rejecting any array that is not strictly
/// ascending. The set reserves from the array actually shipped.
fn read_canonical(field: &str, keys: Vec<WireString>) -> Result<HashSet<String>, RmpDecodeError> {
    if let Some(idx) = keys.windows(2).position(|pair| pair[0].0 >= pair[1].0) {
        return Err(RmpDecodeError::Uncategorized(format!(
            "ASAPv1 {field} must be strictly ascending in byte order, broken at index {}",
            idx + 1
        )));
    }
    Ok(keys.into_iter().map(WireString::into_string).collect())
}

/// Splits the envelope, checks the kind_id and the metadata, and returns the
/// payload slice.
fn open<'a>(bytes: &'a [u8], kind: &[u8], name: &str) -> Result<&'a [u8], RmpDecodeError> {
    let (kind_id, metadata, payload) =
        envelope::split(bytes).map_err(RmpDecodeError::Uncategorized)?;
    if kind_id != kind {
        return Err(RmpDecodeError::Uncategorized(format!(
            "{name} kind_id mismatch: stored {kind_id:?}, expected {kind:?}"
        )));
    }
    let meta: SetMetadata = from_slice(metadata)?;
    if meta != SET_METADATA {
        return Err(RmpDecodeError::Uncategorized(format!(
            "ASAPv1 {name} envelope: metadata mismatch"
        )));
    }
    Ok(payload)
}

fn metadata_bytes() -> Result<Vec<u8>, RmpEncodeError> {
    rmp_serde::to_vec_named(&SET_METADATA)
}

impl SetAggregator {
    /// Serializes the set into an ASAPv1 MessagePack envelope, members in
    /// ascending byte order.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        let payload = rmp_serde::to_vec(&SetAggregatorPayload {
            values: canonical(&self.values),
        })?;
        Ok(envelope::encode(
            SET_AGGREGATOR_KIND,
            &metadata_bytes()?,
            &payload,
        ))
    }

    /// Deserializes a set from an ASAPv1 MessagePack envelope.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let payload = open(bytes, SET_AGGREGATOR_KIND, "SetAggregator")?;
        let p: SetAggregatorPayload<WireString> = from_slice(payload)?;
        Ok(SetAggregator {
            values: read_canonical("SetAggregator values", p.values)?,
        })
    }
}

impl DeltaResult {
    /// Serializes the delta into an ASAPv1 MessagePack envelope, each set in
    /// ascending byte order. A key in both `added` and `removed` is an error.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        if let Some(key) = self.added.intersection(&self.removed).next() {
            return Err(RmpEncodeError::Syntax(format!(
                "ASAPv1 DeltaResult: key {key:?} is both added and removed"
            )));
        }
        let payload = rmp_serde::to_vec(&DeltaResultPayload {
            added: canonical(&self.added),
            removed: canonical(&self.removed),
        })?;
        Ok(envelope::encode(
            DELTA_RESULT_KIND,
            &metadata_bytes()?,
            &payload,
        ))
    }

    /// Deserializes a delta from an ASAPv1 MessagePack envelope.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let payload = open(bytes, DELTA_RESULT_KIND, "DeltaResult")?;
        let p: DeltaResultPayload<WireString> = from_slice(payload)?;
        let added = read_canonical("DeltaResult added", p.added)?;
        let removed = read_canonical("DeltaResult removed", p.removed)?;
        if let Some(key) = added.intersection(&removed).next() {
            return Err(RmpDecodeError::Uncategorized(format!(
                "ASAPv1 DeltaResult: key {key:?} is both added and removed"
            )));
        }
        Ok(DeltaResult { added, removed })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set_of(keys: &[&str]) -> HashSet<String> {
        keys.iter().map(|k| k.to_string()).collect()
    }

    fn crafted_set(values: &[&str]) -> Vec<u8> {
        envelope::encode(
            SET_AGGREGATOR_KIND,
            &metadata_bytes().unwrap(),
            &rmp_serde::to_vec(&SetAggregatorPayload {
                values: values.to_vec(),
            })
            .unwrap(),
        )
    }

    fn crafted_delta(added: &[&str], removed: &[&str]) -> Vec<u8> {
        envelope::encode(
            DELTA_RESULT_KIND,
            &metadata_bytes().unwrap(),
            &rmp_serde::to_vec(&DeltaResultPayload {
                added: added.to_vec(),
                removed: removed.to_vec(),
            })
            .unwrap(),
        )
    }

    const AWKWARD: [&str; 6] = ["", "web", "api", "\u{e9}", "\u{1f600}", "\u{ff5e}"];

    #[test]
    fn set_aggregator_round_trips_and_re_encodes_byte_identically() {
        let agg = SetAggregator {
            values: set_of(&AWKWARD),
        };
        let bytes = agg.serialize_to_bytes().expect("serialize");
        assert!(bytes.starts_with(b"ASAPv1"));
        assert_eq!(&bytes[7..10], &[2u8, 0x08, 0x00]);

        let decoded = SetAggregator::deserialize_from_bytes(&bytes).expect("decode");
        assert_eq!(decoded.values, agg.values);
        assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), bytes);
    }

    /// Byte order, not UTF-16 order: U+FF5E precedes U+1F600.
    #[test]
    fn set_aggregator_emits_members_in_ascending_byte_order() {
        let agg = SetAggregator {
            values: set_of(&AWKWARD),
        };
        let want = crafted_set(&["", "api", "web", "\u{e9}", "\u{ff5e}", "\u{1f600}"]);
        assert_eq!(agg.serialize_to_bytes().expect("serialize"), want);
    }

    #[test]
    fn set_aggregator_empty_has_exactly_one_encoding() {
        let bytes = SetAggregator::new()
            .serialize_to_bytes()
            .expect("serialize");
        assert_eq!(bytes, crafted_set(&[]));
        let decoded = SetAggregator::deserialize_from_bytes(&bytes).expect("decode");
        assert!(decoded.values.is_empty());
    }

    #[test]
    fn set_aggregator_rejects_unordered_and_duplicate_members() {
        assert!(SetAggregator::deserialize_from_bytes(&crafted_set(&["b", "a"])).is_err());
        assert!(SetAggregator::deserialize_from_bytes(&crafted_set(&["a", "a"])).is_err());
        assert!(
            SetAggregator::deserialize_from_bytes(&crafted_set(&["\u{1f600}", "\u{ff5e}"]))
                .is_err()
        );
    }

    /// A member must be msgpack `str`; a `bin` holding valid UTF-8 is rejected.
    #[test]
    fn set_aggregator_rejects_a_bin_member() {
        let payload = rmp_serde::to_vec(&SetAggregatorPayload {
            values: vec![serde_bytes::Bytes::new(b"a")],
        })
        .unwrap();
        let bytes = envelope::encode(SET_AGGREGATOR_KIND, &metadata_bytes().unwrap(), &payload);
        assert!(SetAggregator::deserialize_from_bytes(&bytes).is_err());
    }

    #[test]
    fn metadata_rejects_unknown_missing_and_wrong_version() {
        #[derive(Serialize)]
        struct WithExtra {
            metadata_version: u8,
            bogus_field: u8,
        }
        #[derive(Serialize)]
        struct Empty {}
        let payload = rmp_serde::to_vec(&SetAggregatorPayload::<&str> { values: vec![] }).unwrap();
        let metas = [
            rmp_serde::to_vec_named(&WithExtra {
                metadata_version: 1,
                bogus_field: 0,
            })
            .unwrap(),
            rmp_serde::to_vec_named(&Empty {}).unwrap(),
            rmp_serde::to_vec_named(&SetMetadata {
                metadata_version: 2,
            })
            .unwrap(),
        ];
        for meta in metas {
            let bytes = envelope::encode(SET_AGGREGATOR_KIND, &meta, &payload);
            assert!(SetAggregator::deserialize_from_bytes(&bytes).is_err());
        }
    }

    #[test]
    fn each_kind_rejects_the_other() {
        let set = SetAggregator::new().serialize_to_bytes().unwrap();
        let delta = DeltaResult {
            added: HashSet::new(),
            removed: HashSet::new(),
        }
        .serialize_to_bytes()
        .unwrap();
        assert!(DeltaResult::deserialize_from_bytes(&set).is_err());
        assert!(SetAggregator::deserialize_from_bytes(&delta).is_err());
    }

    #[test]
    fn delta_result_round_trips_and_re_encodes_byte_identically() {
        let delta = DeltaResult {
            added: set_of(&["queue", "\u{e9}t\u{e9}", "\u{1f600}"]),
            removed: set_of(&["db", "cache", ""]),
        };
        let bytes = delta.serialize_to_bytes().expect("serialize");
        assert_eq!(&bytes[7..10], &[2u8, 0x09, 0x00]);
        assert_eq!(
            bytes,
            crafted_delta(
                &["queue", "\u{e9}t\u{e9}", "\u{1f600}"],
                &["", "cache", "db"]
            )
        );

        let decoded = DeltaResult::deserialize_from_bytes(&bytes).expect("decode");
        assert_eq!(decoded.added, delta.added);
        assert_eq!(decoded.removed, delta.removed);
        assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), bytes);
    }

    #[test]
    fn delta_result_empty_round_trips() {
        let bytes = crafted_delta(&[], &[]);
        let decoded = DeltaResult::deserialize_from_bytes(&bytes).expect("decode");
        assert!(decoded.added.is_empty() && decoded.removed.is_empty());
        assert_eq!(decoded.serialize_to_bytes().unwrap(), bytes);
    }

    #[test]
    fn delta_result_rejects_a_key_both_added_and_removed() {
        assert!(DeltaResult::deserialize_from_bytes(&crafted_delta(&["a", "b"], &["b"])).is_err());
        let delta = DeltaResult {
            added: set_of(&["a"]),
            removed: set_of(&["a"]),
        };
        assert!(delta.serialize_to_bytes().is_err());
    }

    #[test]
    fn delta_result_rejects_unordered_sides() {
        assert!(DeltaResult::deserialize_from_bytes(&crafted_delta(&["b", "a"], &[])).is_err());
        assert!(DeltaResult::deserialize_from_bytes(&crafted_delta(&[], &["c", "c"])).is_err());
    }

    #[test]
    fn crafted_bytes_are_errors_not_panics() {
        let set = SetAggregator {
            values: set_of(&AWKWARD),
        }
        .serialize_to_bytes()
        .unwrap();
        for cut in [0, 1, 6, 9, 14, set.len() - 1] {
            assert!(SetAggregator::deserialize_from_bytes(&set[..cut]).is_err());
        }
        assert!(SetAggregator::deserialize_from_bytes(&[0xff; 64]).is_err());
        assert!(DeltaResult::deserialize_from_bytes(&[0xff; 64]).is_err());
        let garbage = envelope::encode(DELTA_RESULT_KIND, &metadata_bytes().unwrap(), &[0xc1]);
        assert!(DeltaResult::deserialize_from_bytes(&garbage).is_err());
    }
}
