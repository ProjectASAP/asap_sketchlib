# API: ExponentialHistogramUnivMon

Status: `Unstable`

## Purpose

Hybrid ExponentialHistogram + UnivMon path with map/sketch tiers and pooling.

## Type/Struct

- `EhUnivMonExactBucket`
- `EhUnivMonSketchBucket`
- `ExponentialHistogramUnivMon`
- `EhUnivMonQueryResult`

## Constructors

```rust
fn new(
    k: usize,
    window: u64,
    heap_size: usize,
    rows: usize,
    cols: usize,
    layers: usize,
) -> Self
fn with_pool_initial_size(
    k: usize,
    window: u64,
    heap_size: usize,
    rows: usize,
    cols: usize,
    layers: usize,
    pool_initial_size: usize,
) -> Self
fn with_defaults(k: usize, window: u64) -> Self
```

## Insert/Update

```rust
fn update(&mut self, time: u64, key: &DataInput, value: i64)
fn update_window(&mut self, window: u64)
```

## Query

```rust
fn query_interval(&self, t1: u64, t2: u64) -> Option<EhUnivMonQueryResult>
fn cover(&self, mint: u64, maxt: u64) -> bool
fn min_time(&self) -> Option<u64>
fn max_time(&self) -> Option<u64>
fn bucket_count(&self) -> usize
fn pool(&self) -> &UnivMonPool
fn memory_info(&self) -> (usize, usize, Vec<usize>, Vec<usize>)

// Result helpers
fn estimate_l1(&self) -> f64
fn estimate_l2(&self) -> f64
fn estimate_entropy(&self) -> f64
fn estimate_cardinality(&self) -> f64
```

## Merge

Managed internally by compaction/promotion logic.

## Serialization

No dedicated serialization API.

## Examples

```rust
use asap_sketchlib::{ExponentialHistogramUnivMon, DataInput};

let mut eh = ExponentialHistogramUnivMon::with_defaults(2, 120);
eh.update(1, &DataInput::U64(10), 1);
let _ = eh.query_interval(0, 120);
```

## Caveats

- Result may be exact map-tier or sketch-tier depending on interval composition.

## Status

Unstable; API may change in upcoming minor releases as the optimized sliding-window universal monitoring path evolves.
