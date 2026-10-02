//! Older per-sketch MessagePack wire types, being retired.
//!
//! Each per-algorithm submodule holds one sketch's pre-envelope wire type.
//! New code uses ASAPv1: prefer a sketch's own `serialize_to_bytes`; do not
//! add callers here.

pub mod delta_set_aggregator;
pub mod hydra_kll;
pub(crate) mod kll;
pub mod sampling;
pub mod set_aggregator;
