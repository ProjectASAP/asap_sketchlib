# API: EHUnivOptimized

Status: `Unstable`

## Purpose

Hybrid ExponentialHistogram + UnivMon path with map/sketch tiers and pooling.

## Type/Struct

- `EHMapBucket`
- `EHUnivMonBucket`
- `EHUnivOptimized`
- `EHUnivQueryResult`

## Constructors

```rust
fn new(
    k: usize,
    window: u64,
    heap_size: usize,
    sketch_row: usize,
    sketch_col: usize,
    layer_size: usize,
) -> Self
fn with_pool_cap(
    k: usize,
    window: u64,
    heap_size: usize,
    sketch_row: usize,
    sketch_col: usize,
    layer_size: usize,
    pool_cap: usize,
) -> Self
fn with_defaults(k: usize, window: u64) -> Self
```

## Insert/Update

```rust
fn update(&mut self, time: u64, key: &SketchInput, value: i64)
fn update_window(&mut self, window: u64)
```

## Query

```rust
fn query_interval(&self, t1: u64, t2: u64) -> Option<EHUnivQueryResult>
fn cover(&self, mint: u64, maxt: u64) -> bool
fn get_min_time(&self) -> Option<u64>
fn get_max_time(&self) -> Option<u64>
fn bucket_count(&self) -> usize
fn pool(&self) -> &UnivSketchPool
fn get_memory_info(&self) -> (usize, usize, Vec<usize>, Vec<usize>)

// Result helpers
fn calc_l1(&self) -> f64
fn calc_l2(&self) -> f64
fn calc_entropy(&self) -> f64
fn calc_card(&self) -> f64
```

## Merge

Managed internally by compaction/promotion logic.

## Serialization

No dedicated serialization API.

## Examples

```rust
use sketchlib_rust::{EHUnivOptimized, SketchInput};

let mut eh = EHUnivOptimized::with_defaults(2, 120);
eh.update(1, &SketchInput::U64(10), 1);
let _ = eh.query_interval(0, 120);
```

## Caveats

- Result may be exact map-tier or sketch-tier depending on interval composition.

## Status

Unstable (update soon); optimized sliding-window universal monitoring path.
