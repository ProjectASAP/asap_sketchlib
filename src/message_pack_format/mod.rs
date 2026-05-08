//! MessagePack wire-format definitions shared with `sketchlib-go`.
//!
//! `asap_sketchlib` and the Go counterpart `sketchlib-go` each maintain
//! their own MessagePack representation. This module is the Rust-side
//! source of truth: every type that crosses the wire is described here
//! (as a DTO in [`dto`]) or, for the three wrappers whose public fields
//! ARE the wire fields, called out explicitly below. Both
//! representations are kept byte-compatible at the envelope level even
//! though the in-language struct shapes differ.
//!
//! # Layout
//!
//! - [`Error`]: unified encode/decode error type
//! - [`MessagePackCodec`]: trait implemented by every wire type — the
//!   canonical encode/decode entry point
//! - [`dto`]: wire-level DTOs for wrappers that need a separate
//!   over-the-wire shape (CountMinSketch, CountMinSketchWithHeap,
//!   HydraKllSketch, KllSketch, SetAggregator, DeltaResult)
//!
//! # Wrappers that act as their own DTO
//!
//! [`crate::wrapper::CountSketch`], [`crate::wrapper::DdSketch`], and
//! [`crate::wrapper::HllSketch`] derive `Serialize` / `Deserialize`
//! directly — their public field layout matches the wire shape exactly,
//! so no separate DTO is required. Their [`MessagePackCodec`] impls
//! serialize the wrapper struct verbatim.

pub mod codec;
pub mod dto;
pub mod error;

pub use codec::MessagePackCodec;
pub use error::Error;
