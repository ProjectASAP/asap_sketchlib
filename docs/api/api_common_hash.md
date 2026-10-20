# API: Common Hash Utilities

Status: `Shared`

## Purpose

Shared hashing traits, constants, and helpers used by sketches/frameworks.

## Type/Struct

- `SketchHasher`
- `DefaultXxHasher`
- `DigestHasher` — `std::hash::Hasher` for `u64` keys that are already a digest; one avalanche step, no full hash
- `DigestBuildHasher` — `BuildHasherDefault<DigestHasher>`, the `BuildHasher` to hand a `HashMap`
- `MatrixHashLayout`
- `MatrixHash` (see matrix storage)

## Constructors

Not applicable.

## Insert/Update

Not applicable.

## Query

```rust
pub const SEED_LIST: [u64; 20]
pub const MATRIX_MAX_ROWS: usize
pub const DEFAULT_CANONICAL_SEED_INDEX: usize
pub const UNIVMON_BOTTOM_LAYER_SEED_INDEX: usize
pub const HYDRA_SEED_INDEX: usize

fn hash64_seeded(seed_index: usize, key: &DataInput) -> u64
fn hash128_seeded(seed_index: usize, key: &DataInput) -> u128
fn hash64_seeded_owned(seed_index: usize, key: &OwnedDataInput) -> u64
fn hash128_seeded_owned(seed_index: usize, key: &OwnedDataInput) -> u128
fn hash_layout_for_matrix(rows: usize, cols: usize) -> MatrixHashLayout
fn hash_for_matrix(rows: usize, cols: usize, key: &DataInput) -> MatrixHash
fn hash_for_matrix_seeded(seed_index: usize, rows: usize, cols: usize, key: &DataInput) -> MatrixHash
fn hash_for_matrix_seeded_with_layout(seed_index: usize, layout: MatrixHashLayout, rows: usize, key: &DataInput) -> MatrixHash
fn hash_for_matrix_seeded_with_layout_generic<H: SketchHasher>(
    seed_index: usize,
    layout: MatrixHashLayout,
    rows: usize,
    key: &DataInput,
) -> MatrixHash
fn hash_for_matrix_generic<H: SketchHasher>(rows: usize, cols: usize, key: &DataInput) -> MatrixHash
fn hash_for_matrix_seeded_generic<H: SketchHasher>(
    seed_index: usize,
    rows: usize,
    cols: usize,
    key: &DataInput,
) -> MatrixHash
```

## Merge

Not applicable.

## Serialization

Not applicable.

## Examples

```rust
use asap_sketchlib::{hash64_seeded, DataInput, DEFAULT_CANONICAL_SEED_INDEX};

let h = hash64_seeded(DEFAULT_CANONICAL_SEED_INDEX, &DataInput::U64(42));
assert!(h > 0 || h == 0);
```

## Caveats

- Matrix hash helper selection depends on row/column shape.
- Seed indices wrap at `SEED_LIST.len()`, so a matrix row `r` and row
  `r + MATRIX_MAX_ROWS` draw the same seed and place every key identically.

## See Also

- [Common Module (Canonical)](./api_common.md)
- [Common Input Types](./api_common_input.md)

## Status

Canonical shared hash utility layer.
