#![doc = include_str!("../README.md")]

/// Shared primitives used across sketches, including input wrappers, hashers,
/// storage backends, and reusable utilities.
pub mod common;
/// Portable protobuf message types for sketch interchange.
pub mod proto;
/// Higher-level composition layers such as Hydra, UnivMon, tumbling windows,
/// and batch/parallel execution helpers.
pub mod sketch_framework;
/// Core sketch implementations such as Count-Min, HyperLogLog, KLL, and DDSketch.
pub mod sketches;
#[cfg(test)]
pub mod test_utils;

#[doc(hidden)]
pub mod __private {
    pub use serde;
    pub use serde_big_array;
}

pub use common::*;
pub use sketch_framework::*;
pub use sketches::*;
