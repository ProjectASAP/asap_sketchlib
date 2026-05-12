//! Native MessagePack codec impls for the pure-Rust sketch types in
//! [`crate::sketches`].
//!
//! These impls are thin shims over each sketch's existing
//! `serialize_to_bytes` / `deserialize_from_bytes` methods, exposing
//! them through the unified [`crate::message_pack_format::MessagePackCodec`]
//! trait. The byte format is internal to Rust — Go (`sketchlib-go`)
//! never reads it. For the cross-language wire format, see
//! [`crate::message_pack_format::portable`].

pub mod countminsketch;
pub mod countsketch;
pub mod countsketch_topk;
pub mod ddsketch;
pub mod hll;
pub mod kll;
pub mod kll_dynamic;
#[cfg(feature = "experimental")]
pub mod kmv;
