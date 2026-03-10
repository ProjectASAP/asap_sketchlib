# API: UnivMon

Status: `Recommended`

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
```

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
use sketchlib_rust::{SketchInput, UnivMon};

let mut um = UnivMon::init_univmon(32, 3, 1024, 4);
um.insert(&SketchInput::Str("flow"), 1);
assert!(um.calc_l1() >= 1.0);
```

## Caveats

- Structure parameters should match before merge.

## Status

Primary multi-metric framework.
