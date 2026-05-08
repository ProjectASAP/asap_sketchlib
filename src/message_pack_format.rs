//! MessagePack format definitions for the wire types in [`crate::wrapper`].
//!
//! Note: `asap_sketchlib` and the Go counterpart `sketchlib-go` each maintain
//! their own MessagePack representation. This module describes the Rust side;
//! the Go side lives in `sketchlib-go`. Both representations are kept
//! byte-compatible at the envelope level, but their in-language struct
//! shapes differ.
//!
//! This module is currently a placeholder — concrete schema definitions
//! will land in a follow-up PR.
