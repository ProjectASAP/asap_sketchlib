# API: FoldCMS

Status: `Recommended`

## Purpose

Folding Count-Min Sketch for memory-efficient sub-window aggregation.

## Type/Struct

- `FoldCMS<H = DefaultXxHasher>`
- `FoldCell`, `FoldEntry`

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
fn merge_same_level(&mut self, other: &FoldCMS<H>)
fn unfold_merge(a: &FoldCMS<H>, b: &FoldCMS<H>) -> FoldCMS<H>
fn hierarchical_merge(sketches: &[FoldCMS<H>]) -> FoldCMS<H>
```

## Serialization

Not currently provided as a dedicated public API.

## Examples

```rust
use sketchlib_rust::{FoldCMS, SketchInput};

let mut sk = FoldCMS::new(3, 4096, 4, 16);
sk.insert(&SketchInput::Str("k"), 3);
assert!(sk.query(&SketchInput::Str("k")) >= 3);
```

## Caveats

- `full_cols` must be a power of two.
- Fold-level merge paths must follow level compatibility rules.

## Status

Actively maintained and benchmarked against standard Count-Min behavior.
