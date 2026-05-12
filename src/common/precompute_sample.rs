//! Generated at build time by `build.rs` into `$OUT_DIR/precompute_sample.rs`.
//!
//! Temporary fix to reduce committed SLoC: this 64K-entry table is regenerated
//! deterministically on every build instead of being checked in. See `build.rs`
//! for the fixed seed used.

include!(concat!(env!("OUT_DIR"), "/precompute_sample.rs"));
