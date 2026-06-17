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

// ── Native (Rust-internal) sketch types ─────────────────────────────────────
//
// These are the generic sketch types in `crate::sketches`. Their wire format
// is produced by `serialize_to_bytes` / `deserialize_from_bytes` and is
// internal to Rust — Go (`sketchlib-go`) never reads these bytes directly.
//
// They use a separate range (0x81+) to make it visually clear that the byte
// refers to an internal format distinct from the portable cross-language ones.

/// Generic Count-Min sketch (`sketches::CountMin<_, _>`).
pub const NATIVE_COUNT_MIN: u8 = 0x81;

/// Generic Count Sketch (`sketches::Count<_, _, _>`).
pub const NATIVE_COUNT_SKETCH: u8 = 0x82;

/// Count-Min + heavy-hitter heap (`sketches::CMSHeap` / `CountL2HH`).
pub const NATIVE_CMS_HEAP: u8 = 0x83;

/// Generic HyperLogLog (`sketches::HyperLogLogImpl<_, _, _>` — Classic and ErtlMLE variants).
pub const NATIVE_HLL: u8 = 0x84;

/// HyperLogLog HIP variant (`sketches::HyperLogLogHIPImpl<_>`).
pub const NATIVE_HLL_HIP: u8 = 0x85;

/// DDSketch (`sketches::DDSketch`).
pub const NATIVE_DD_SKETCH: u8 = 0x86;

/// KLL quantile sketch (`sketches::kll::KLL<T>`).
pub const NATIVE_KLL: u8 = 0x87;

/// Dynamic KLL quantile sketch (`sketches::kll_dynamic::KLLDynamic<T>`).
pub const NATIVE_KLL_DYNAMIC: u8 = 0x88;

/// KMV (K-Minimum Values) sketch (`sketches::KMV`).
pub const NATIVE_KMV: u8 = 0x89;

/// Hydra composite sketch (`sketch_framework::hydra`).
pub const NATIVE_HYDRA: u8 = 0x8a;

/// UnivMon sketch (`sketch_framework::univmon`).
pub const NATIVE_UNIVMON: u8 = 0x8b;
