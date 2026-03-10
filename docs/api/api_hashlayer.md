# API: HashLayer

Status: `Recommended`

## Purpose

Coordinate multiple compatible sketches and reuse hashes across them.

## Type/Struct

- `HashLayer`

## Constructors

```rust
fn default() -> Self
fn new(lst: Vec<OrchestratedSketch>) -> Result<Self, &'static str>
fn push(&mut self, sketch: OrchestratedSketch) -> Result<(), &'static str>
```

## Insert/Update

```rust
fn insert_all(&mut self, val: &SketchInput)
fn insert_at(&mut self, indices: &[usize], val: &SketchInput)
fn insert_all_with_hash(&mut self, hash_value: &HashValue)
fn insert_at_with_hash(&mut self, indices: &[usize], hash_value: &HashValue)
```

## Query

```rust
fn query_at(&self, index: usize, val: &SketchInput) -> Result<f64, &'static str>
fn query_all(&self, val: &SketchInput) -> Vec<Result<f64, &'static str>>
fn query_at_with_hash(&self, index: usize, hash_value: &HashValue) -> Result<f64, &'static str>
fn query_all_with_hash(&self, hash_value: &HashValue) -> Vec<Result<f64, &'static str>>
fn len(&self) -> usize
fn is_empty(&self) -> bool
fn get(&self, index: usize) -> Option<&OrchestratedSketch>
```

## Merge

No layer-level merge API.

## Serialization

No dedicated serialization API.

## Examples

```rust
use sketchlib_rust::HashLayer;

let mut layer = HashLayer::default();
layer.insert_all(&sketchlib_rust::SketchInput::U64(7));
let _ = layer.query_at(0, &sketchlib_rust::SketchInput::U64(7));
```

## Caveats

- Accepts only `OrchestratedSketch` values that support hash reuse.
- Node-orchestrator adapter query is still TODO in `HashLayerNode`.

## Status

Core optimization layer; actively used and tested.
