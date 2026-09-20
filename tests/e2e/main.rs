//! End-to-end suites: every sketch against exact ground truth from seeded
//! synthetic streams, each approximate answer judged by its own family's
//! error bound.
//!
//! One target rather than one per file, so the shared harness in
//! `tests/common/` is compiled once.

#[path = "../common/mod.rs"]
mod common;

mod cardinality;
mod composition;
mod data_input;
mod frameworks;
mod frequency;
mod heavy_hitters;
mod matrix_instances;
mod membership;
mod nitro;
mod numeric_types;
mod octo;
mod quantiles;
mod topk;
mod windows;
mod wire;

#[cfg(feature = "experimental")]
mod experimental;
