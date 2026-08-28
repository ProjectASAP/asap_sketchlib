# API: Hydra

Status: `Ready`

## Purpose

Hierarchical subpopulation sketching over semicolon-separated keys.

## Type/Struct

- `Hydra`

## Constructors

```rust
fn default() -> Self
fn with_dimensions(r: usize, c: usize, sketch_type: HydraCounter) -> Self
```

## Insert/Update

```rust
fn update(&mut self, key: &str, value: &DataInput, count: Option<i32>)
```

## Query

```rust
fn query_key(&self, key: Vec<&str>, query: &HydraQuery) -> f64
fn query_frequency(&self, key: Vec<&str>, value: &DataInput) -> f64
fn query_quantile(&self, key: Vec<&str>, threshold: f64) -> f64
```

## Merge

```rust
fn merge(&mut self, other: &Hydra) -> Result<(), String>
```

## Serialization

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

## Examples

```rust
use asap_sketchlib::{Hydra, DataInput};

let mut hydra = Hydra::default();
hydra.update("region=us;service=api", &DataInput::Str("err"), None);
let est = hydra.query_frequency(vec!["region=us"], &DataInput::Str("err"));
assert!(est >= 1.0);
```

## Caveats

- Canonical enum/query/input definitions are in [Common Input Types](./api_common_input.md).
- Query compatibility depends on `HydraCounter` variant.

## Status

Primary subpopulation framework with broad test coverage.
