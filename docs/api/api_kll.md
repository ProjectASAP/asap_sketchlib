# API: KLL

Status: `Ready`

## Purpose

Approximate quantile estimation with rank-error guarantees.

## Type/Struct

- `KLL`
- `Cdf`

## Constructors

```rust
fn default() -> Self
fn init_kll(k: i32) -> Self
fn init(k: usize, m: usize) -> Self
```

## Insert/Update

```rust
fn update(&mut self, val: &SketchInput) -> Result<(), &'static str>
fn clear(&mut self)
```

## Query

```rust
fn quantile(&self, q: f64) -> f64
fn rank(&self, x: f64) -> usize
fn count(&self) -> usize
fn cdf(&self) -> Cdf

// Cdf
fn quantile(&self, x: f64) -> f64
fn query(&self, p: f64) -> f64
fn quantile_li(&self, x: f64) -> f64
fn query_li(&self, p: f64) -> f64
```

## Merge

```rust
fn merge(&mut self, other: &KLL)
```

## Serialization

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

## Examples

```rust
use sketchlib_rust::{KLL, SketchInput};

let mut kll = KLL::init_kll(200);
kll.update(&SketchInput::F64(10.0)).unwrap();
kll.update(&SketchInput::F64(20.0)).unwrap();
let q50 = kll.quantile(0.5);
assert!(q50 >= 10.0);
```

## Caveats

- Numeric inputs only through `SketchInput`.

## Status

Production-usable quantile sketch with comprehensive tests.
