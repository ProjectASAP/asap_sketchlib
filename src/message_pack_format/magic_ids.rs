//! Magic-ID constants and wrapper encoding for the ASAP sketch wire format.
//!
//! Every serialized binary produced by this library is wrapped in the **ASAPv1
//! envelope**, a self-describing header that identifies the sketch type and
//! reserves space for future metadata without a fixed-size ceiling:
//!
//! ```text
//! [ b"ASAPv1" | version: u8 | kind_id_len: u8 | kind_id: [u8; kind_id_len] | metadata_len: u32 | metadata: bytes | <msgpack payload> ]
//! ```
//!
//! * `b"ASAPv1"` — 6-byte ASCII sentinel; unambiguously not a msgpack value.
//! * `version` — envelope layout version; currently `0x01`.
//! * `kind_id_len + kind_id` — variable-length sketch discriminant encoded as
//!   canonical unsigned big-endian with no leading zero bytes.  For current
//!   sketch types this is 1–2 bytes; future additions can use more without a
//!   protocol change.
//! * `metadata_len + metadata` — MessagePack metadata describing the hash
//!   profile needed for cross-process updates and queries.
//!
//! **Portable** sketches (cross-language, shared with `sketchlib-go`) use a
//! 1-byte `kind_id` drawn from the `0x01–0x09` range.
//!
//! **Native** Rust sketches use a 2-byte `kind_id`: first byte is the family /
//! mode discriminant (`0x81–0x8e`); second byte is the hasher ID (`HASHER_*`).
//!
//! Magic IDs are **stable** — once assigned, a value is never reused.  Adding
//! a new sketch type requires a new constant here; removing or repurposing an
//! existing constant is a **breaking protocol change**.  The Go mirror of this
//! table lives in `sketchlib-go/wire/asapmsgpack/magic_ids.go`.

// ── Envelope constants ────────────────────────────────────────────────────────

/// 6-byte ASCII sentinel that opens every ASAP sketch binary.
pub const WRAPPER_MAGIC: &[u8; 6] = b"ASAPv1";

/// Envelope layout version stored immediately after `WRAPPER_MAGIC`. Increment
/// if the header structure (field order, field semantics) ever changes.
pub const WRAPPER_VERSION: u8 = 0x02;

use serde::{Deserialize, Serialize};

pub const HASH_PROFILE_PROJECTASAP_XXH3_V1: &str = "projectasap.xxh3.seedlist.v1";
pub const HASH_ALGORITHM_XXH3_64_128: &str = "xxh3_64_128";
pub const HASH_SEED_DERIVATION_INDEX_WRAP: &str = "seed_list_index_wrap";
pub const HASH_INPUT_ENCODING_PROJECTASAP_V1: &str = "projectasap.input.v1";
pub const MATRIX_BASE_SEED_INDEX: u32 = 0;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WrapperMetadata {
    pub metadata_version: u8,
    pub hash_spec_present: bool,
    pub hash_profile_id: String,
    pub hash_algorithm: String,
    pub seed_list: Vec<u64>,
    pub canonical_seed_index: u32,
    pub matrix_seed_index: u32,
    pub hydra_seed_index: u32,
    pub univmon_bottom_layer_seed_index: u32,
    pub seed_derivation: String,
    pub input_encoding: String,
}

impl WrapperMetadata {
    pub fn standard_hash() -> Self {
        Self {
            metadata_version: 1,
            hash_spec_present: true,
            hash_profile_id: HASH_PROFILE_PROJECTASAP_XXH3_V1.to_string(),
            hash_algorithm: HASH_ALGORITHM_XXH3_64_128.to_string(),
            seed_list: crate::SEEDLIST.to_vec(),
            canonical_seed_index: crate::CANONICAL_HASH_SEED as u32,
            matrix_seed_index: MATRIX_BASE_SEED_INDEX,
            hydra_seed_index: crate::HYDRA_SEED as u32,
            univmon_bottom_layer_seed_index: crate::BOTTOM_LAYER_FINDER as u32,
            seed_derivation: HASH_SEED_DERIVATION_INDEX_WRAP.to_string(),
            input_encoding: HASH_INPUT_ENCODING_PROJECTASAP_V1.to_string(),
        }
    }

    pub fn no_hash_spec() -> Self {
        Self {
            metadata_version: 1,
            hash_spec_present: false,
            hash_profile_id: String::new(),
            hash_algorithm: String::new(),
            seed_list: Vec::new(),
            canonical_seed_index: 0,
            matrix_seed_index: 0,
            hydra_seed_index: 0,
            univmon_bottom_layer_seed_index: 0,
            seed_derivation: String::new(),
            input_encoding: String::new(),
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.metadata_version != 1 {
            return Err(format!(
                "ASAPv1 wrapper: unsupported metadata_version {}",
                self.metadata_version
            ));
        }
        if !self.hash_spec_present {
            let expected = Self::no_hash_spec();
            if self != &expected {
                return Err("ASAPv1 wrapper: no-hash metadata mismatch".to_string());
            }
            return Ok(());
        }
        let expected = Self::standard_hash();
        if self != &expected {
            return Err("ASAPv1 wrapper: hash metadata mismatch".to_string());
        }
        Ok(())
    }
}

fn metadata_for_kind_id(kind_id: &[u8]) -> WrapperMetadata {
    match kind_id {
        // Portable wire formats are cross-language and always use the standard
        // ProjectASAP hash profile when they need key hashing. Keep the profile
        // present for every portable kind so consumers can validate uniformly.
        [0x01..=0x09] => WrapperMetadata::standard_hash(),
        // Native Rust sketches with an explicit DefaultXxHasher byte.
        [_, HASHER_DEFAULT_XX] => WrapperMetadata::standard_hash(),
        // Native framework sketches with no generic hasher but standard hash use.
        [NATIVE_HYDRA | NATIVE_UNIVMON, HASHER_UNKNOWN] => WrapperMetadata::standard_hash(),
        // DDSketch/KLL and custom/unknown hashers do not have a complete,
        // registered hash profile to record.
        _ => WrapperMetadata::no_hash_spec(),
    }
}

/// Prepend the ASAPv1 envelope to `payload` and return the complete binary.
///
/// `kind_id` identifies the sketch type (1–2 bytes for all current types).
pub fn encode_wrapper(kind_id: &[u8], payload: &[u8]) -> Vec<u8> {
    let metadata = metadata_for_kind_id(kind_id);
    let metadata_bytes =
        rmp_serde::to_vec(&metadata).expect("WrapperMetadata serialization cannot fail");
    let metadata_len = u32::try_from(metadata_bytes.len()).expect("wrapper metadata too large");
    let mut out = Vec::with_capacity(
        WRAPPER_MAGIC.len() + 1 + 1 + kind_id.len() + 4 + metadata_bytes.len() + payload.len(),
    );
    out.extend_from_slice(WRAPPER_MAGIC);
    out.push(WRAPPER_VERSION);
    out.push(kind_id.len() as u8);
    out.extend_from_slice(kind_id);
    out.extend_from_slice(&metadata_len.to_be_bytes());
    out.extend_from_slice(&metadata_bytes);
    out.extend_from_slice(payload);
    out
}

/// Strip the ASAPv1 envelope from `bytes` and return `(kind_id, payload)`.
///
/// Returns `Err(String)` on any structural mismatch so callers can convert to
/// their own error type.
pub fn decode_wrapper(bytes: &[u8]) -> Result<(&[u8], &[u8]), String> {
    let (kind_id, _metadata, payload) = decode_wrapper_with_metadata(bytes)?;
    Ok((kind_id, payload))
}

/// Strip the ASAPv1 envelope from `bytes` and return `(kind_id, metadata, payload)`.
///
/// Returns `Err(String)` on any structural mismatch or unsupported metadata.
pub fn decode_wrapper_with_metadata(
    bytes: &[u8],
) -> Result<(&[u8], WrapperMetadata, &[u8]), String> {
    let magic_len = WRAPPER_MAGIC.len();
    let version_offset = magic_len;
    let kind_id_len_offset = magic_len + 1;
    let kind_id_offset = magic_len + 2;
    let min_len = kind_id_offset + 1 + 4;

    if bytes.len() < min_len {
        return Err(format!(
            "ASAPv1 wrapper: too short ({} bytes, need ≥{min_len})",
            bytes.len()
        ));
    }
    if &bytes[..magic_len] != WRAPPER_MAGIC {
        return Err(format!(
            "ASAPv1 wrapper: bad magic {:?}, expected b\"ASAPv1\"",
            &bytes[..magic_len]
        ));
    }
    let version = bytes[version_offset];
    if version != WRAPPER_VERSION {
        return Err(format!(
            "ASAPv1 wrapper: unsupported version 0x{version:02x}"
        ));
    }
    let kind_id_len = bytes[kind_id_len_offset] as usize;
    let metadata_len_offset = kind_id_offset + kind_id_len;
    if bytes.len() < metadata_len_offset + 4 {
        return Err(format!(
            "ASAPv1 wrapper: kind_id_len={kind_id_len} but only {} bytes available after offset {kind_id_offset}",
            bytes.len().saturating_sub(kind_id_offset)
        ));
    }
    let metadata_len = u32::from_be_bytes(
        bytes[metadata_len_offset..metadata_len_offset + 4]
            .try_into()
            .expect("metadata length slice has four bytes"),
    ) as usize;
    let metadata_start = metadata_len_offset + 4;
    let metadata_end = metadata_start + metadata_len;
    if bytes.len() < metadata_end {
        return Err(format!(
            "ASAPv1 wrapper: metadata_len={metadata_len} but only {} bytes available after offset {metadata_start}",
            bytes.len().saturating_sub(metadata_start)
        ));
    }
    let metadata: WrapperMetadata = rmp_serde::from_slice(&bytes[metadata_start..metadata_end])
        .map_err(|err| format!("ASAPv1 wrapper: invalid metadata: {err}"))?;
    metadata.validate()?;
    let kind_id = &bytes[kind_id_offset..metadata_len_offset];
    let expected_metadata = metadata_for_kind_id(kind_id);
    if metadata != expected_metadata {
        return Err("ASAPv1 wrapper: metadata does not match kind_id".to_string());
    }
    Ok((kind_id, metadata, &bytes[metadata_end..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_wrapper_with_metadata(
        kind_id: &[u8],
        metadata: &WrapperMetadata,
        payload: &[u8],
    ) -> Vec<u8> {
        let metadata_bytes = rmp_serde::to_vec(metadata).expect("metadata serialization");
        let metadata_len = u32::try_from(metadata_bytes.len()).expect("metadata too large");
        let mut out = Vec::new();
        out.extend_from_slice(WRAPPER_MAGIC);
        out.push(WRAPPER_VERSION);
        out.push(kind_id.len() as u8);
        out.extend_from_slice(kind_id);
        out.extend_from_slice(&metadata_len.to_be_bytes());
        out.extend_from_slice(&metadata_bytes);
        out.extend_from_slice(payload);
        out
    }

    #[test]
    fn portable_wrapper_records_standard_hash_metadata() {
        let payload = [0x91, 0x2a];
        let encoded = encode_wrapper(&[COUNT_MIN_SKETCH], &payload);

        let (kind_id, metadata, decoded_payload) =
            decode_wrapper_with_metadata(&encoded).expect("decode wrapper");

        assert_eq!(kind_id, &[COUNT_MIN_SKETCH]);
        assert_eq!(decoded_payload, payload);
        assert_eq!(metadata, WrapperMetadata::standard_hash());
        assert_eq!(metadata.seed_list, crate::SEEDLIST);
        assert_eq!(
            metadata.canonical_seed_index,
            crate::CANONICAL_HASH_SEED as u32
        );
        assert_eq!(metadata.hydra_seed_index, crate::HYDRA_SEED as u32);
        assert_eq!(
            metadata.univmon_bottom_layer_seed_index,
            crate::BOTTOM_LAYER_FINDER as u32
        );
    }

    #[test]
    fn unknown_native_hasher_does_not_claim_hash_metadata() {
        let payload = [0x90];
        let kind_id = [NATIVE_COUNT_MIN_REGULAR, HASHER_UNKNOWN];
        let encoded = encode_wrapper(&kind_id, &payload);

        let (decoded_kind_id, metadata, decoded_payload) =
            decode_wrapper_with_metadata(&encoded).expect("decode wrapper");

        assert_eq!(decoded_kind_id, kind_id);
        assert_eq!(decoded_payload, payload);
        assert_eq!(metadata, WrapperMetadata::no_hash_spec());
    }

    #[test]
    fn wrapper_rejects_hash_metadata_mismatch() {
        let payload = [0x90];
        let mut metadata = WrapperMetadata::standard_hash();
        metadata.canonical_seed_index += 1;
        let encoded = encode_wrapper_with_metadata(&[COUNT_SKETCH], &metadata, &payload);

        let err = decode_wrapper(&encoded).expect_err("metadata mismatch should fail");

        assert!(err.contains("hash metadata mismatch"), "{err}");
    }

    #[test]
    fn wrapper_rejects_metadata_kind_id_mismatch() {
        let payload = [0x90];
        let metadata = WrapperMetadata::no_hash_spec();
        let encoded = encode_wrapper_with_metadata(&[COUNT_SKETCH], &metadata, &payload);

        let err = decode_wrapper(&encoded).expect_err("metadata kind mismatch should fail");

        assert!(err.contains("metadata does not match kind_id"), "{err}");
    }
}

/// HLL sketch (all variants: Regular, Datafusion, Hip).
pub const HLL: u8 = 0x01;

/// Count-Min sketch (f64 counters, no top-k heap).
pub const COUNT_MIN_SKETCH: u8 = 0x02;

/// Count-Min sketch with top-k heap (f64 counters + heap).
pub const COUNT_MIN_SKETCH_WITH_HEAP: u8 = 0x03;

/// Count Sketch (signed f64 counters).
pub const COUNT_SKETCH: u8 = 0x04;

/// DDSketch (quantile sketch, alpha-parameterised bucket array).
pub const DD_SKETCH: u8 = 0x05;

/// KLL quantile sketch.
pub const KLL_SKETCH: u8 = 0x06;

/// Hydra-KLL sketch (grid of KLL cells).
pub const HYDRA_KLL_SKETCH: u8 = 0x07;

/// Set aggregator (distinct string set).
pub const SET_AGGREGATOR: u8 = 0x08;

/// Delta-set aggregator result (added / removed string sets).
pub const DELTA_RESULT: u8 = 0x09;

// ── Hasher discriminants ─────────────────────────────────────────────────────
//
// Embedded as the second header byte in every native sketch binary so the
// reader can verify that it uses the same hash function as the writer.
// Custom hashers that do not register an ID are stored as HASHER_UNKNOWN,
// which suppresses the mismatch check on both sides.

/// Default XxHash3-64 hasher (`DefaultXxHasher`).
pub const HASHER_DEFAULT_XX: u8 = 0x01;

/// Sentinel for custom hashers with no registered ID — mismatch check is
/// skipped when either the stored value or the expected value is this byte.
pub const HASHER_UNKNOWN: u8 = 0xff;

/// Validates the stored hasher byte against the expected hasher `H`.
///
/// The check is skipped (returns `Ok`) when either side is `HASHER_UNKNOWN`,
/// allowing custom hashers to interoperate without requiring a registered ID.
pub(crate) fn check_hasher_id<H: crate::SketchHasher>(
    stored: u8,
) -> Result<(), rmp_serde::decode::Error> {
    let expected = H::hasher_magic_id();
    if stored == HASHER_UNKNOWN || expected == HASHER_UNKNOWN || stored == expected {
        return Ok(());
    }
    Err(rmp_serde::decode::Error::Uncategorized(format!(
        "hasher mismatch: stored 0x{stored:02x}, expected 0x{expected:02x}"
    )))
}

// ── Native (Rust-internal) sketch types ─────────────────────────────────────
//
// These are the generic sketch types in `crate::sketches`. Their wire format
// is produced by `serialize_to_bytes` / `deserialize_from_bytes` and is
// internal to Rust — Go (`sketchlib-go`) never reads these bytes directly.
//
// The first header byte encodes both the sketch family AND the phantom-type
// parameters (Mode, Variant) that are invisible in the msgpack payload.
// The second header byte encodes the hasher (see HASHER_* above).
//
// Layout: [ family+mode byte | hasher byte | <rmp_serde named payload> ]
//
// ID range 0x81–0x8f is reserved for native types.

/// Count-Min sketch with `RegularPath` hashing mode.
pub const NATIVE_COUNT_MIN_REGULAR: u8 = 0x81;

/// Count-Min sketch with `FastPath` hashing mode.
pub const NATIVE_COUNT_MIN_FAST: u8 = 0x82;

/// Count Sketch with `RegularPath` hashing mode.
pub const NATIVE_COUNT_SKETCH_REGULAR: u8 = 0x83;

/// Count Sketch with `FastPath` hashing mode.
pub const NATIVE_COUNT_SKETCH_FAST: u8 = 0x84;

/// Count-Min + heavy-hitter heap (`CountL2HH`).
pub const NATIVE_CMS_HEAP: u8 = 0x85;

/// HyperLogLog Classic (HLL++) estimator (`HyperLogLogImpl<Classic, _, _>`).
pub const NATIVE_HLL_CLASSIC: u8 = 0x86;

/// HyperLogLog ErtlMLE estimator (`HyperLogLogImpl<ErtlMLE, _, _>`).
pub const NATIVE_HLL_ERTL_MLE: u8 = 0x87;

/// HyperLogLog HIP variant (`HyperLogLogHIPImpl<_>`).
pub const NATIVE_HLL_HIP: u8 = 0x88;

/// DDSketch (`sketches::DDSketch`).
pub const NATIVE_DD_SKETCH: u8 = 0x89;

/// KLL quantile sketch (`sketches::kll::KLL<T>`).
pub const NATIVE_KLL: u8 = 0x8a;

/// Dynamic KLL quantile sketch (`sketches::kll_dynamic::KLLDynamic<T>`).
pub const NATIVE_KLL_DYNAMIC: u8 = 0x8b;

/// KMV (K-Minimum Values) sketch (`sketches::KMV`).
pub const NATIVE_KMV: u8 = 0x8c;

/// Hydra composite sketch (`sketch_framework::hydra`).
pub const NATIVE_HYDRA: u8 = 0x8d;

/// UnivMon sketch (`sketch_framework::univmon`).
pub const NATIVE_UNIVMON: u8 = 0x8e;
