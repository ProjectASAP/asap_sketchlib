//! Magic-ID constants for the portable MessagePack wire format.
//!
//! Every serialized binary produced by [`crate::message_pack_format::MessagePackCodec`]
//! is prefixed with a single byte that identifies the sketch type, analogous to
//! how Prometheus uses magic bytes to discriminate metric types.
//!
//! The prefix layout is:
//!
//! ```text
//! [ magic_id: u8 | <rmp_serde msgpack payload> ]
//! ```
//!
//! Magic IDs are stable across versions. Adding a new sketch type requires a
//! new constant here; removing or repurposing an existing constant is a
//! breaking protocol change. The Go mirror of this table lives in
//! `sketchlib-go/wire/asapmsgpack/magic_ids.go`.

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
