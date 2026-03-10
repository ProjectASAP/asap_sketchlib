# API: Hydra

Status: `Recommended`

## Purpose

Hierarchical subpopulation sketching over semicolon-separated keys.

## Type/Struct

- `Hydra`
- `MultiHeadHydra`

## Constructors

```rust
fn default() -> Self
fn with_dimensions(r: usize, c: usize, sketch_type: HydraCounter) -> Self

// MultiHeadHydra
fn with_dimensions(r: usize, c: usize, dimensions: Vec<(String, HydraCounter)>) -> Self
```

## Insert/Update

```rust
fn update(&mut self, key: &str, value: &SketchInput, count: Option<i32>)

// MultiHeadHydra
fn update(&mut self, key: &str, values: &[(&SketchInput, &[&str])], count: Option<i32>)
```

## Query

```rust
fn query_key(&self, key: Vec<&str>, query: &HydraQuery) -> f64
fn query_frequency(&self, key: Vec<&str>, value: &SketchInput) -> f64
fn query_quantile(&self, key: Vec<&str>, threshold: f64) -> f64

// MultiHeadHydra
fn query_key(&self, key: Vec<&str>, dimension: &str, query: &HydraQuery) -> f64
fn dimension_index(&self, dimension: &str) -> Option<usize>
```

## Merge

```rust
fn merge(&mut self, other: &Hydra) -> Result<(), String>
fn merge(&mut self, other: &MultiHeadHydra) -> Result<(), String>
```

## Serialization

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

## Examples

```rust
use sketchlib_rust::{Hydra, SketchInput};

let mut hydra = Hydra::default();
hydra.update("region=us;service=api", &SketchInput::Str("err"), None);
let est = hydra.query_frequency(vec!["region=us"], &SketchInput::Str("err"));
assert!(est >= 1.0);
```

## Caveats

- Canonical enum/query/input definitions are in [Common Input Types](./api_common_input.md).
- Query compatibility depends on `HydraCounter` variant.

## Status

Primary subpopulation framework with broad test coverage.
