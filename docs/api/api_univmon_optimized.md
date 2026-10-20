# API: UnivMon Optimized

Status: `Ready`

## Purpose

Two-tier UnivMon pyramid, and a free-list pool of scratch `UnivMon`s.

## Type/Struct

- `UnivMonPool`
- `UnivMonTwoTier`

## Constructors

```rust
// UnivMonPool
fn new(initial_size: usize, heap_size: usize, rows: usize, cols: usize, layers: usize) -> Self

// UnivMonTwoTier
fn new(
    heap_size: usize,
    elephant_layers: usize,
    elephant_rows: usize,
    elephant_cols: usize,
    mouse_rows: usize,
    mouse_cols: usize,
    layers: usize,
) -> Self
fn with_defaults() -> Self
```

## Insert/Update

```rust
fn update_weighted(&mut self, key: &DataInput, weight: i64)
fn update_weighted_terminal(&mut self, key: &DataInput, weight: i64)
fn clear(&mut self)
```

## Query

```rust
fn estimate_l1(&self) -> f64
fn estimate_l2(&self) -> f64
fn estimate_entropy(&self) -> f64
fn estimate_cardinality(&self) -> f64
fn estimate_g_sum<F>(&self, g: F, heavy_only: bool) -> f64

// UnivMonPool
fn available(&self) -> usize
fn total_allocated(&self) -> usize
```

For the supported non-negative update stream, `estimate_l1()` returns the exact
tracked total weight rather than estimating the linear sum through the
hierarchy.

## Merge

```rust
fn merge(&mut self, other: &UnivMonTwoTier)
```

## Serialization

```rust
fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError>
fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

These produce/consume the **ASAPv1** wire envelope (kind `0x11 0x00`) — see the
[ASAPv1 wire format spec](../asapv1_wire_format.md). `UnivMonTwoTier` shares
[`UnivMon`](./api_univmon.md)'s payload and differs only in its metadata: the
two-tier layout (`layers`, `elephant_layers`, `elephant_rows`,
`elephant_cols`, `mouse_rows`, `mouse_cols`, `heap_size`) plus the heaps'
`key_type`. Layer `i` takes the elephant dimensions while `i < elephant_layers`
and the mouse dimensions after, so every per-layer geometry is derived and none
is stored. `elephant_rows` and `mouse_rows` carry `UnivMon`'s per-layer row bound,
`1 <= rows <= 20` (`MATRIX_MAX_ROWS`, the seed list length).

`UnivMonPool` is a free-list of scratch `UnivMon`s rather than a sketch, and
has no wire kind.

## Examples

```rust
use asap_sketchlib::{DataInput, UnivMonTwoTier};

let mut um = UnivMonTwoTier::with_defaults();
um.update_weighted(&DataInput::U64(1), 1);
assert!(um.estimate_l1() >= 1.0);
```

## Caveats

- Updates must have non-negative weights.
- `update_weighted` and `update_weighted_terminal` use different physical layouts and cannot be mixed
  within one sketch.
- Merge expects compatible layout/configuration and update strategy.

## Status

Ready optimized path for pooled UnivMon deployments.
