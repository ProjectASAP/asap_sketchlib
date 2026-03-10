# API: CSHeap

Status: `Additional`

## Purpose

Count Sketch with integrated heavy-hitter heap (`HHHeap`) for top-k tracking.

## Type/Struct

- `CSHeap<S = Vector2D<i64>, M = RegularPath, H = DefaultXxHasher>`

## Constructors

```rust
fn new(rows: usize, cols: usize, top_k: usize) -> Self
fn from_storage(storage: S, top_k: usize) -> Self
fn default() -> Self
```

## Insert/Update

```rust
fn insert(&mut self, key: &SketchInput)
fn insert_many(&mut self, key: &SketchInput, many: S::Counter)
fn bulk_insert(&mut self, values: &[SketchInput])
fn clear_heap(&mut self)
```

## Query

```rust
fn estimate(&self, key: &SketchInput) -> f64
fn rows(&self) -> usize
fn cols(&self) -> usize
fn cs(&self) -> &Count<S, M, H>
fn heap(&self) -> &HHHeap
```

## Merge

```rust
fn merge(&mut self, other: &Self)
```

## Serialization

Not currently provided as a dedicated public API.

## Examples

```rust
use sketchlib_rust::{CSHeap, SketchInput, Vector2D, RegularPath};

let mut sk = CSHeap::<Vector2D<i64>, RegularPath>::new(5, 256, 8);
sk.insert(&SketchInput::Str("flow"));
assert!(sk.estimate(&SketchInput::Str("flow")) >= 1.0);
```

## Caveats

- Estimate semantics follow Count Sketch and may be non-integer.
- Merge requires matching dimensions and compatible type parameters.

## Status

Useful helper wrapper; tested but less central than base sketches.
