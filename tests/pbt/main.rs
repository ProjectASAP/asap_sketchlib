//! Property tests: the laws each sketch family claims, checked with
//! `proptest` against oracles derived independently of the implementation.
//!
//! One target rather than one per file, so `cargo test --test pbt` runs
//! the whole suite and the library is linked once. One file per sketch, so a
//! law lives next to the family it constrains; what more than one sketch
//! needs lives in `support`.

#[macro_use]
mod support;

mod count_min;
mod count_sketch;
mod ensemble;
mod hydra;
mod set_aggregator;
mod univmon;
mod univmon_q;
