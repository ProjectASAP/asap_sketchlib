//! MessagePack codecs — split into two sub-modules by audience.
//!
//! - [`portable`]: cross-language wire format shared with `sketchlib-go`.
//!   Every type that crosses the wire lives here in a per-algorithm
//!   submodule whose filename mirrors the corresponding file in
//!   [`crate::wrapper`] and in `sketchlib-go`. Touching this module is
//!   a protocol change and requires the Go side to be kept in lock-step
//!   (golden-byte tests catch drift).
//!
//! - [`native`]: thin trait shims over the existing `serialize_to_bytes`
//!   / `deserialize_from_bytes` methods on the pure-Rust generic sketch
//!   types in [`crate::sketches`]. The byte format is internal to Rust
//!   — Go never reads it. Free to evolve without cross-language
//!   coordination.
//!
//! The [`MessagePackCodec`] trait and unified [`Error`] type live at
//! this top level so both worlds share the same encode/decode contract.

pub mod codec;
pub mod error;
pub mod native;
pub mod portable;

pub use codec::MessagePackCodec;
pub use error::Error;
