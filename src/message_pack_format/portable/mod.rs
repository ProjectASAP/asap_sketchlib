//! Portable MessagePack wire format shared with `sketchlib-go`.
//!
//! Every type that crosses the wire is described here in a per-algorithm
//! submodule whose filename mirrors the corresponding file in
//! `sketchlib-go`. Both representations are kept byte-compatible at the
//! envelope level even though the in-language struct shapes differ.
//!
//! Touching anything in this module is a protocol change: the Go side
//! must be kept in lock-step, and the cross-language golden-byte tests
//! must continue to pass.

pub mod countminsketch;
pub mod countminsketch_topk;
pub mod countsketch;
pub mod ddsketch;
pub mod delta_set_aggregator;
pub mod hll;
pub mod hydra_kll;
pub mod kll;
pub mod set_aggregator;
