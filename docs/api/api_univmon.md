# API: UnivMon

Status: `Ready`

## Purpose

Universal stream-monitoring sketch for L1/L2/cardinality/entropy.

## Type/Struct

- `UnivMon`

## Constructors

```rust
fn default() -> Self
fn init_univmon(heap_size: usize, sketch_row: usize, sketch_col: usize, layer_size: usize) -> Self
```

## Insert/Update

```rust
fn insert(&mut self, key: &DataInput, value: i64)
fn fast_insert(&mut self, key: &DataInput, value: i64)
fn free(&mut self)
```

`insert` maintains the cumulative sampled substream at every selected layer.
`fast_insert` uses the Joltik update-last-layer layout and reconstructs logical
layers during queries. Choose one update method for the lifetime of a sketch;
mixing the two physical layouts is rejected.

## Query

```rust
fn calc_l1(&self) -> f64
fn calc_l2(&self) -> f64
fn calc_entropy(&self) -> f64
fn calc_card(&self) -> f64
fn calc_g_sum<F>(&self, g: F, is_card: bool) -> f64
```

Because UnivMon accepts only non-negative updates, `calc_l1()` returns the
exact tracked total weight. The generic recurrence remains available through
`calc_g_sum` for compatible frequency functions.

## Merge

```rust
fn merge(&mut self, other: &UnivMon)
```

## Serialization

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

## Examples

```rust
use asap_sketchlib::{DataInput, UnivMon};

let mut um = UnivMon::init_univmon(32, 3, 1024, 4);
um.insert(&DataInput::Str("flow"), 1);
assert!(um.calc_l1() >= 1.0);
```

## Caveats

- Updates must have non-negative weights.
- Structure parameters and update strategies must match before merge.
- Merge combines total weight, recomputes cached L2 state, and re-estimates the
  union of bounded heavy-hitter candidates from merged counters.

## Status

Primary multi-metric framework.
