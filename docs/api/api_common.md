# API: Common Module (Canonical)

Status: `Shared`

## Purpose

Canonical reference for the `src/common` module, which provides shared input types,
hashing utilities, data structures, and heavy-hitter helpers used across sketches
and frameworks.

## Module Organization

**File:** [src/common/mod.rs](../../src/common/mod.rs)

```text
src/common/
├── mod.rs                  # Public exports
├── input.rs                # DataInput/OwnedDataInput/HeapEntry + Hydra enums
├── hash.rs                 # Hash traits, constants, and matrix hash helpers
├── heap.rs                 # TopKHeap
├── structure_utils.rs      # Nitro and median utility
├── precompute_hash.rs      # PRECOMPUTED_HASH
├── precompute_sample.rs    # LN_ONE_MINUS_U_TABLE
└── structures/
    ├── vector1d.rs
    ├── vector2d.rs
    ├── vector3d.rs
    ├── bit_matrix.rs
    ├── matrix_storage.rs
    ├── fixed_structure.rs
    └── heap.rs
```

## Public Exports

```rust
use asap_sketchlib::common::{
    // Input layer
    DataInput, OwnedDataInput, HeapEntry,

    // Hash layer
    SketchHasher, DefaultXxHasher,
    DigestHasher, DigestBuildHasher,
    SEED_LIST, DEFAULT_CANONICAL_SEED_INDEX, UNIVMON_BOTTOM_LAYER_SEED_INDEX, HYDRA_SEED_INDEX,
    MatrixHashLayout,
    hash64_seeded, hash128_seeded,
    hash64_seeded_owned, hash128_seeded_owned,
    hash_layout_for_matrix,
    hash_for_matrix, hash_for_matrix_seeded,
    hash_for_matrix_seeded_with_layout,
    hash_for_matrix_seeded_with_layout_generic,
    hash_for_matrix_generic, hash_for_matrix_seeded_generic,

    // Structure layer
    MatrixStorage, FastPathHasher, MatrixHash,
    Vector1D, Vector2D, Vector3D,
    BitMatrix,
    FixedMatrix, HllRegisters,
    DefaultMatrixI32, DefaultMatrixI64, DefaultMatrixI128,
    QuickMatrixI32, QuickMatrixI64, QuickMatrixI128,
    CommonHeap, CommonHeapOrder, RetainLargest, RetainSmallest,

    // HH helper + utilities
    TopKHeap, HeapCount, HeapCounter,
    Nitro, compute_median_inline_f64,

    // Precomputed assets
    PRECOMPUTED_HASH,
    LN_ONE_MINUS_U_TABLE,
};
```

## Canonical Subpages

- [Common Input Types](./api_common_input.md)
- [Common Hash Utilities](./api_common_hash.md)
- [Common Heap Utilities](./api_common_heap.md)
- [Common Structures](./api_common_structures.md)

## Cross-Cutting Notes

- `DataInput`/`OwnedDataInput` define borrowed-vs-owned key semantics used across all
  sketches and heaps.
- `SketchHasher` enables custom hash injection for sketches that support hasher
  generics.
- Matrix fast paths choose hash packing mode automatically through
  `hash_layout_for_matrix(rows, cols)`.
- `DigestHasher` replaces the full byte-wise hash with a single avalanche step,
  for maps whose key is already a digest.
- `BitMatrix` implements the same `MatrixStorage` interface as the counter
  matrices over one bit per cell, so membership sketches share the matrix
  hashing and fast paths.
- `TopKHeap` is built for top-k heavy hitter maintenance and key lookup.

## Quick Example

```rust
use asap_sketchlib::{hash64_seeded, DataInput, Vector2D, DEFAULT_CANONICAL_SEED_INDEX};

let _h = hash64_seeded(DEFAULT_CANONICAL_SEED_INDEX, &DataInput::U64(42));
let matrix = Vector2D::<i32>::unfilled(3, 16);
assert_eq!(matrix.rows(), 3);
```

## Status

Primary common-module entry point.
