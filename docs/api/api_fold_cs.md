# API: FoldCS

Status: `Recommended`

## Purpose

Folding Count Sketch for memory-efficient signed-frequency sub-window aggregation.

## Type/Struct

- `FoldCS<H = DefaultXxHasher>`

## Constructors

```rust
fn new(rows: usize, full_cols: usize, fold_level: u32, top_k: usize) -> Self
fn new_full(rows: usize, full_cols: usize, top_k: usize) -> Self
```

## Insert/Update

```rust
fn insert(&mut self, key: &SketchInput, delta: i64)
fn insert_one(&mut self, key: &SketchInput)
fn clear(&mut self)
```

## Query

```rust
fn query(&self, key: &SketchInput) -> i64
fn rows(&self) -> usize
fn fold_cols(&self) -> usize
fn full_cols(&self) -> usize
fn fold_level(&self) -> u32
fn to_flat_counters(&self) -> Vec<i64>
```

## Merge

```rust
fn merge_same_level(&mut self, other: &FoldCS<H>)
fn unfold_merge(a: &FoldCS<H>, b: &FoldCS<H>) -> FoldCS<H>
fn hierarchical_merge(sketches: &[FoldCS<H>]) -> FoldCS<H>
```

## Serialization

Not currently provided as a dedicated public API.

## Examples

```rust
use sketchlib_rust::{FoldCS, SketchInput};

let mut sk = FoldCS::new(3, 4096, 4, 16);
sk.insert(&SketchInput::Str("k"), 5);
let est = sk.query(&SketchInput::Str("k"));
assert!(est >= 0);
```

## Caveats

- `full_cols` must be a power of two.
- Estimate behavior follows Count Sketch (signed updates + median logic).

## Status

Actively maintained folded Count Sketch path.
