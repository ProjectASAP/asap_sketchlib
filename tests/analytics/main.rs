//! The analytics in `docs/sketch_analytics.md` against the sketches: memory
//! against `memory_bytes()`, op formulas against counted ops (`op-count`
//! feature), error bounds against failure rates over independent trials, and
//! sizing against its own error bound.
//!
//! One target, one file per family; what more than one family needs lives in
//! `support`.

mod support;

mod cardinality;
mod count_matrix;
mod heavy_hitters;
mod quantiles;
mod top_k;
mod univmon;
