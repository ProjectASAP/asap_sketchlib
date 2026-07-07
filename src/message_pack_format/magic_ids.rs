//! Magic-ID constants and wrapper encoding for the ASAP sketch wire format.
//!
//! Every serialized binary produced by this library is wrapped in the **ASAPv1
//! envelope**, a self-describing header that identifies the sketch type and
//! reserves space for future metadata without a fixed-size ceiling:
//!
//! ```text
//! [ b"ASAPv1" | version: u8 | kind_id_len: u8 | kind_id: [u8; kind_id_len] | <msgpack payload> ]
//! ```
//!
//! * `b"ASAPv1"` — 6-byte ASCII sentinel; unambiguously not a msgpack value.
//! * `version` — envelope layout version; currently `0x01`.
//! * `kind_id_len + kind_id` — variable-length sketch discriminant encoded as
//!   canonical unsigned big-endian with no leading zero bytes.  For current
//!   sketch types this is 1–2 bytes; future additions can use more without a
//!   protocol change.
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
pub const WRAPPER_VERSION: u8 = 0x01;

/// Prepend the ASAPv1 envelope to `payload` and return the complete binary.
///
/// `kind_id` identifies the sketch type (1–2 bytes for all current types).
pub fn encode_wrapper(kind_id: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(WRAPPER_MAGIC.len() + 1 + 1 + kind_id.len() + payload.len());
    out.extend_from_slice(WRAPPER_MAGIC);
    out.push(WRAPPER_VERSION);
    out.push(kind_id.len() as u8);
    out.extend_from_slice(kind_id);
    out.extend_from_slice(payload);
    out
}

/// Strip the ASAPv1 envelope from `bytes` and return `(kind_id, payload)`.
///
/// Returns `Err(String)` on any structural mismatch so callers can convert to
/// their own error type.
pub fn decode_wrapper(bytes: &[u8]) -> Result<(&[u8], &[u8]), String> {
    let magic_len = WRAPPER_MAGIC.len();
    let version_offset = magic_len;
    let kind_id_len_offset = magic_len + 1;
    let kind_id_offset = magic_len + 2;
    let min_len = kind_id_offset + 1;

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
    let header_end = kind_id_offset + kind_id_len;
    if bytes.len() < header_end {
        return Err(format!(
            "ASAPv1 wrapper: kind_id_len={kind_id_len} but only {} bytes available after offset {kind_id_offset}",
            bytes.len().saturating_sub(kind_id_offset)
        ));
    }
    Ok((&bytes[kind_id_offset..header_end], &bytes[header_end..]))
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
