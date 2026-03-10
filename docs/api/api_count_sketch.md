# API: Count Sketch

Status: `Recommended`

## Purpose

Approximate frequency estimation with signed counters and median aggregation.

## Type/Struct

- `Count<S = Vector2D<i32>, Mode = RegularPath, H = DefaultXxHasher>`
- `CountL2HH<H = DefaultXxHasher>`

## Constructors

```rust
fn default() -> Self
fn with_dimensions(rows: usize, cols: usize) -> Self
fn from_storage(counts: S) -> Self

// CountL2HH
fn default() -> Self
fn with_dimensions(rows: usize, cols: usize) -> Self
fn with_dimensions_and_seed(rows: usize, cols: usize, seed_idx: usize) -> Self
```

## Insert/Update

```rust
fn insert(&mut self, value: &SketchInput)
fn insert_many(&mut self, value: &SketchInput, many: S::Counter)
fn fast_insert_with_hash_value(&mut self, hashed_val: &S::HashValueType)

// CountL2HH
fn fast_insert_with_count(&mut self, val: &SketchInput, c: i64)
fn fast_insert_with_count_and_hash(&mut self, hashed_val: u128, c: i64)
fn fast_insert_with_count_without_l2_and_hash(&mut self, hashed_val: u128, c: i64)
```

## Query

```rust
fn estimate(&self, value: &SketchInput) -> f64
fn fast_estimate_with_hash(&self, hashed_val: &S::HashValueType) -> f64

// CountL2HH
fn fast_get_est(&self, val: &SketchInput) -> f64
fn fast_get_est_with_hash(&self, hashed_val: u128) -> f64
fn fast_update_and_est(&mut self, val: &SketchInput, c: i64) -> f64
fn fast_update_and_est_without_l2(&mut self, val: &SketchInput, c: i64) -> f64
fn get_l2(&self) -> f64
fn get_l2_sqr(&self) -> f64
```

## Merge

```rust
fn merge(&mut self, other: &Self)
```

## Serialization

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

## Examples

```rust
use sketchlib_rust::{Count, SketchInput};

let mut cs = Count::with_dimensions(5, 2048);
cs.insert(&SketchInput::Str("alpha"));
let est = cs.estimate(&SketchInput::Str("alpha"));
assert!(est >= 1.0);
```

## Caveats

- `merge` requires matching dimensions.
- `CountL2HH` is the L2-heavy-hitter variant used by `UnivMon` internals.

## Status

Core frequency primitive; widely used by framework layers.
