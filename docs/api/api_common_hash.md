# API: Common Hash Utilities

Status: `Shared`

## Purpose

Shared hashing traits, constants, and helpers used by sketches/frameworks.

## Type/Struct

- `SketchHasher`
- `DefaultXxHasher`
- `MatrixHashMode`
- `MatrixHashType` (see matrix storage)

## Constructors

Not applicable.

## Insert/Update

Not applicable.

## Query

```rust
pub const SEEDLIST: [u64; 20]
pub const CANONICAL_HASH_SEED: usize
pub const BOTTOM_LAYER_FINDER: usize
pub const HYDRA_SEED: usize

fn hash64_seeded(d: usize, key: &SketchInput) -> u64
fn hash128_seeded(d: usize, key: &SketchInput) -> u128
fn hash_item64_seeded(d: usize, key: &HeapItem) -> u64
fn hash_item128_seeded(d: usize, key: &HeapItem) -> u128
fn hash_mode_for_matrix(rows: usize, cols: usize) -> MatrixHashMode
fn hash_for_matrix(rows: usize, cols: usize, key: &SketchInput) -> MatrixHashType
fn hash_for_matrix_seeded(seed_idx: usize, rows: usize, cols: usize, key: &SketchInput) -> MatrixHashType
fn hash_for_matrix_seeded_with_mode(seed_idx: usize, mode: MatrixHashMode, rows: usize, key: &SketchInput) -> MatrixHashType
fn hash_for_matrix_seeded_with_mode_generic<H: SketchHasher>(
    seed_idx: usize,
    mode: MatrixHashMode,
    rows: usize,
    key: &SketchInput,
) -> MatrixHashType
fn hash_for_matrix_generic<H: SketchHasher>(rows: usize, cols: usize, key: &SketchInput) -> MatrixHashType
fn hash_for_matrix_seeded_generic<H: SketchHasher>(
    seed_idx: usize,
    rows: usize,
    cols: usize,
    key: &SketchInput,
) -> MatrixHashType
```

## Merge

Not applicable.

## Serialization

Not applicable.

## Examples

```rust
use sketchlib_rust::{hash64_seeded, SketchInput, CANONICAL_HASH_SEED};

let h = hash64_seeded(CANONICAL_HASH_SEED, &SketchInput::U64(42));
assert!(h > 0 || h == 0);
```

## Caveats

- Matrix hash helper selection depends on row/column shape.

## See Also

- [Common Module (Canonical)](./api_common.md)
- [Common Input Types](./api_common_input.md)

## Status

Canonical shared hash utility layer.
