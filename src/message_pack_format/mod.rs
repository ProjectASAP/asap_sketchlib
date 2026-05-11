//! MessagePack wire-format definitions shared with `sketchlib-go`.
//!
//! `asap_sketchlib` and the Go counterpart `sketchlib-go` each maintain
//! their own MessagePack representation. This module is the Rust-side
//! source of truth: every type that crosses the wire is described here
//! in a per-algorithm submodule whose filename mirrors the corresponding
//! file in [`crate::wrapper`]. Both representations are kept byte-
//! compatible at the envelope level even though the in-language struct
//! shapes differ.
//!
//! # Layout
//!
//! - `Error`: unified encode/decode error type
//! - `MessagePackCodec`: trait implemented by every wire type — the
//!   canonical encode/decode entry point
//! - One submodule per wrapper file under [`crate::wrapper`]
//!   (`countminsketch`, `countminsketch_topk`, `countsketch`,
//!   `ddsketch`, `hll`, `kll`, `hydra_kll`, `set_aggregator`,
//!   `delta_set_aggregator`). Each owns its wire DTO struct(s) (when
//!   the wrapper needs a separate over-the-wire shape) and the
//!   `MessagePackCodec` impl for the matching wrapper type.
//!
//! # Wrappers that act as their own DTO
//!
//! [`crate::wrapper::CountSketch`], [`crate::wrapper::DdSketch`], and
//! [`crate::wrapper::HllSketch`] derive `Serialize` / `Deserialize`
//! directly — their public field layout matches the wire shape exactly,
//! so no separate DTO is required. Their `MessagePackCodec` impls
//! serialize the wrapper struct verbatim.

pub mod codec;
pub mod countminsketch;
pub mod countminsketch_topk;
pub mod countsketch;
pub mod ddsketch;
pub mod delta_set_aggregator;
pub mod error;
pub mod hll;
pub mod hydra_kll;
pub mod kll;
pub mod set_aggregator;

pub use codec::MessagePackCodec;
pub use error::Error;
