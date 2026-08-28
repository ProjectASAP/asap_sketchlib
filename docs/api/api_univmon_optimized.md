# API: UnivMon Optimized

Status: `Ready`

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
fn insert(&mut self, key: &DataInput, value: i64)
fn fast_insert(&mut self, key: &DataInput, value: i64)
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

For the supported non-negative update stream, `calc_l1()` returns the exact
tracked total weight rather than estimating the linear sum through the
hierarchy.

## Merge

```rust
fn merge(&mut self, other: &UnivMonPyramid)
```

## Serialization

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

These produce/consume the **ASAPv1** wire envelope (kind `0x11 0x00`) — see the
[ASAPv1 wire format spec](../asapv1_wire_format.md). `UnivMonPyramid` shares
[`UnivMon`](./api_univmon.md)'s payload and differs only in its metadata: the
two-tier layout (`layer_size`, `elephant_layers`, `elephant_row`,
`elephant_col`, `mouse_row`, `mouse_col`, `heap_size`) plus the heaps'
`key_type`. Layer `i` takes the elephant dimensions while `i < elephant_layers`
and the mouse dimensions after, so every per-layer geometry is derived and none
is stored.

`UnivSketchPool` is a free-list of scratch `UnivMon`s rather than a sketch, and
has no wire kind.

## Examples

```rust
use asap_sketchlib::{DataInput, UnivMonPyramid};

let mut um = UnivMonPyramid::with_defaults();
um.insert(&DataInput::U64(1), 1);
assert!(um.calc_l1() >= 1.0);
```

## Caveats

- Updates must have non-negative weights.
- `insert` and `fast_insert` use different physical layouts and cannot be mixed
  within one sketch.
- Merge expects compatible layout/configuration and update strategy.

## Status

Ready optimized path for pooled UnivMon deployments.
