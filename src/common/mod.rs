//! Shared building blocks used across the library.
//!
//! This module groups the reusable primitives that power multiple sketch
//! implementations:
//!
//! - input and key types such as [`DataInput`], [`OwnedDataInput`], and [`HeapEntry`]
//! - hashing abstractions such as [`SketchHasher`] and [`DefaultXxHasher`]
//! - storage backends such as [`Vector2D`] and [`MatrixStorage`]
//! - shared utilities such as heaps, numerical traits, and sampling helpers
//!
//! Most users only need [`DataInput`] and, for matrix-backed frequency
//! sketches, [`Vector2D`]. Advanced users can use this module to customize
//! hashing and storage layout or to integrate directly with lower-level shared
//! infrastructure.
//!
//! The precomputed tables and low-level helpers exposed here are public for
//! transparency and advanced use cases, but they are not required for ordinary
//! sketch usage.

/// Hashing utilities and seed definitions shared across sketches.
pub mod hash;
pub mod heap;
pub mod input;
pub mod numerical;
pub mod precompute_hash;
pub mod precompute_sample;
pub mod structure_utils;
/// Reusable storage backends and low-level containers.
pub mod structures;
pub mod tuple_key;

pub use hash::{
    DEFAULT_CANONICAL_SEED_INDEX, DefaultXxHasher, DigestBuildHasher, DigestHasher,
    HYDRA_SEED_INDEX, HashProfile, MATRIX_MAX_ROWS, MatrixHashLayout, SEED_LIST, SketchHasher,
    UNIVMON_BOTTOM_LAYER_SEED_INDEX, hash_for_matrix, hash_for_matrix_generic,
    hash_for_matrix_seeded, hash_for_matrix_seeded_generic, hash_for_matrix_seeded_with_layout,
    hash_for_matrix_seeded_with_layout_generic, hash_layout_for_matrix, hash64_seeded,
    hash64_seeded_owned, hash128_seeded, hash128_seeded_owned, seed_for_index,
};
pub use heap::{HeapCount, HeapCounter, TopKHeap};
pub use input::{DataInput, HeapEntry, OwnedDataInput};
pub use numerical::NumericalValue;
pub use precompute_hash::PRECOMPUTED_HASH;
pub use precompute_sample::LN_ONE_MINUS_U_TABLE;
pub use structure_utils::{Nitro, NitroContext, compute_median_inline_f64};
pub use structures::{
    BitMatrix, CommonHeap, CommonHeapOrder, DefaultMatrixI32, DefaultMatrixI64, DefaultMatrixI128,
    FastPathHasher, FixedMatrix, HllRegisterStorage, HllRegisters, HllRegistersP12,
    HllRegistersP14, HllRegistersP16, MatrixFastHash, MatrixHash, MatrixStorage, QuickMatrixI32,
    QuickMatrixI64, QuickMatrixI128, RetainLargest, RetainSmallest, Vector1D, Vector2D, Vector3D,
};
pub use tuple_key::{KeyColumn, TupleKey};
