# API: DDSketch

Status: `Ready`

## Purpose

Approximate quantile estimation with relative error guarantees.

## Type/Struct

- `DDSketch`

## Constructors

```rust
fn new(alpha: f64) -> Self
```

## Insert/Update

```rust
fn add(&mut self, v: f64)
fn add_input(&mut self, v: &SketchInput) -> Result<(), &'static str>
```

## Query

```rust
fn get_value_at_quantile(&self, q: f64) -> Option<f64>
fn get_count(&self) -> u64
fn min(&self) -> Option<f64>
fn max(&self) -> Option<f64>
```

## Merge

```rust
fn merge(&mut self, other: &DDSketch)
```

## Serialization

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

## Examples

```rust
use sketchlib_rust::DDSketch;

let mut dds = DDSketch::new(0.01);
dds.add(1.0);
dds.add(2.0);
let p50 = dds.get_value_at_quantile(0.5).unwrap();
assert!(p50 >= 1.0);
```

## Caveats

- Inputs must be positive values.
- Merge requires compatible configuration (`alpha`).

## Status

Supported and tested for multiple distributions.
