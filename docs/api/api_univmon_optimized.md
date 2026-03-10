# API: UnivMon Optimized

Status: `Recommended`

## Purpose

Optimized two-tier UnivMon stack with sketch pooling.

## Type/Struct

- `UnivSketchPool`
- `UnivMonPyramid`

## Constructors

```rust
// UnivSketchPool
fn new(heap_size: usize, sketch_row: usize, sketch_col: usize, layer_size: usize, cap: usize) -> Self

// UnivMonPyramid
fn new(
    top_heap_size: usize,
    top_rows: usize,
    top_cols: usize,
    bottom_heap_size: usize,
    bottom_rows: usize,
    bottom_cols: usize,
    layer_size: usize,
    pool_cap: usize,
) -> Self
fn with_defaults() -> Self
```

## Insert/Update

```rust
fn insert(&mut self, key: &SketchInput, value: i64)
fn fast_insert(&mut self, key: &SketchInput, value: i64)
fn free(&mut self)
```

## Query

```rust
fn calc_l1(&self) -> f64
fn calc_l2(&self) -> f64
fn calc_entropy(&self) -> f64
fn calc_card(&self) -> f64
fn calc_g_sum<F>(&self, g: F, is_card: bool) -> f64

// Pool introspection
fn available(&self) -> usize
fn total_allocated(&self) -> usize
```

## Merge

```rust
fn merge(&mut self, other: &UnivMonPyramid)
```

## Serialization

No dedicated serialization API.

## Examples

```rust
use sketchlib_rust::{SketchInput, UnivMonPyramid};

let mut um = UnivMonPyramid::with_defaults();
um.insert(&SketchInput::U64(1), 1);
assert!(um.calc_l1() >= 1.0);
```

## Caveats

- Merge expects compatible layout/configuration.

## Status

Recommended optimized path for pooled UnivMon deployments.
