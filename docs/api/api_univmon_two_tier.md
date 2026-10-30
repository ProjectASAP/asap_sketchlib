# API: Two-tier UnivMon

Status: `Ready`

## Purpose

Two-tier UnivMon pyramid.

## Type/Struct

- `UnivMonTwoTier`

## Constructors

```rust
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
fn estimate_g_sum<F>(&self, g: F, count_every_key: bool) -> f64
fn top_layer_complete(&self) -> bool
```

For the supported non-negative update stream, `estimate_l1()` returns the exact
tracked total weight rather than estimating the linear sum through the
hierarchy. `estimate_entropy()` is Shannon entropy in bits (log2).

The estimators are [`UnivMon`](./api_univmon.md)'s, with the same
[error bounds](./api_univmon.md#error-bounds). `estimate_l2()` uses layer 0's
Count Sketch F2 estimate in standard mode, so its `(ε, δ)` bound uses layer 0's
dimensions (the elephant ones whenever `elephant_layers >= 1`); in terminal mode
it sums the strata, each held to its own layer's dimensions.
`estimate_cardinality()` counts every heap key; it misses by more than `ε F0`
jointly with `top_layer_complete()` with probability at most `1.35/(kε²)` for
`k = heap_size >= 8`, independent of the elephant/mouse split.

## Merge

```rust
fn merge(&mut self, other: &UnivMonTwoTier)
```

## Serialization

```rust
fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError>
fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

These produce/consume the **ASAPv1** wire envelope (kind `0x11 0x01`; the retired `0x11 0x00`, which also carried the layers' `l2`, still decodes) — see the
[ASAPv1 wire format spec](../asapv1_wire_format.md). `UnivMonTwoTier` shares
[`UnivMon`](./api_univmon.md)'s payload and differs only in its metadata: the
two-tier layout (`layers`, `elephant_layers`, `elephant_rows`,
`elephant_cols`, `mouse_rows`, `mouse_cols`, `heap_size`) plus the heaps'
`key_type`. Layer `i` takes the elephant dimensions while `i < elephant_layers`
and the mouse dimensions after, so every per-layer geometry is derived and none
is stored. `elephant_rows` and `mouse_rows` carry `UnivMon`'s per-layer row bound,
`1 <= rows <= 20` (`MATRIX_MAX_ROWS`, the seed list length).

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
