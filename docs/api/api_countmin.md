# API: CountMin

Status: `Recommended`

## Purpose

Approximate frequency estimation with sub-linear memory.

## Type/Struct

- `CountMin<S = Vector2D<i32>, Mode = RegularPath, H = DefaultXxHasher>`

## Constructors

```rust
fn default() -> Self
fn with_dimensions(rows: usize, cols: usize) -> Self
fn from_storage(counts: S) -> Self
```

## Insert/Update

```rust
fn insert(&mut self, value: &SketchInput)
fn insert_many(&mut self, value: &SketchInput, many: S::Counter)
fn bulk_insert(&mut self, values: &[SketchInput])
fn bulk_insert_many(&mut self, values: &[(SketchInput, S::Counter)])
fn fast_insert_with_hash_value(&mut self, hashed_val: &S::HashValueType)
fn fast_insert_many_with_hash_value(&mut self, hashed_val: &S::HashValueType, many: S::Counter)
```

## Query

```rust
fn estimate(&self, value: &SketchInput) -> S::Counter
fn fast_estimate_with_hash(&self, hashed_val: &S::HashValueType) -> S::Counter
fn rows(&self) -> usize
fn cols(&self) -> usize
fn as_storage(&self) -> &S
fn as_storage_mut(&mut self) -> &mut S
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
use sketchlib_rust::{CountMin, SketchInput};

let mut cm = CountMin::with_dimensions(3, 1024);
cm.insert(&SketchInput::U64(42));
let est = cm.estimate(&SketchInput::U64(42));
assert!(est >= 1);
```

## Caveats

- `merge` requires matching dimensions.
- `FastPath` and regular modes must be paired consistently.

## Status

Used as a primary building block in frameworks and orchestrated paths.
