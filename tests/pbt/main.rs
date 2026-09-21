//! Property tests: the laws each sketch family claims, checked with
//! `proptest` against oracles derived independently of the implementation.
//!
//! One target rather than one per file, so `cargo test --test pbt` runs
//! the whole suite and the library is linked once. One file per sketch, so a
//! law lives next to the family it constrains; what more than one sketch
//! needs lives in `support`.

#[macro_use]
mod support;

mod coco;
mod count_min;
mod count_sketch;
mod exponential_histogram;
mod fold;
mod heaps;
mod topk_wrappers;
mod elastic;
mod space_saving;
mod hll;

#[cfg(feature = "experimental")]
mod count_min_hll;
#[cfg(feature = "experimental")]
mod kmv;
mod ddsketch;
mod kll;
mod kll_dynamic;
