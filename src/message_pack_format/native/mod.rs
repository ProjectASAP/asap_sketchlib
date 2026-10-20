//! Older `MessagePackCodec` shims over the pure-Rust sketch types in
//! [`crate::sketches`], being retired.
//!
//! Each impl forwards to that sketch's own `to_asapv1` /
//! `from_asapv1`, so the bytes are ASAPv1 — the framing in
//! `envelope.rs` plus the per-sketch `wire.rs` — and the shim adds nothing
//! to them. Call those methods directly; `docs/asapv1_wire_format.md`
//! specifies the format `sketchlib-go` mirrors.

pub mod countminsketch;
pub mod countsketch;
pub mod countsketch_l2_norm;
pub mod ddsketch;
pub mod hll;
pub mod kll;
pub mod kll_dynamic;
#[cfg(feature = "experimental")]
pub mod kmv;
