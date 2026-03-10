# API: KMV

Status: `Unstable`

> Warning: This API is available and tested, but not yet integrated into the primary structured framework surfaces.

## Purpose

K-minimum values cardinality estimator.

## Type/Struct

- `KMV<H = DefaultXxHasher>`

## Constructors

```rust
fn default() -> Self
fn new(k: usize) -> Self
```

## Insert/Update

```rust
fn insert(&mut self, item: &SketchInput)
fn insert_by_hash(&mut self, hash_value: u64)
```

## Query

```rust
fn estimate(&mut self) -> f64
```

## Merge

```rust
fn merge(&mut self, other: &mut KMV<H>)
```

## Serialization

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

## Examples

```rust
use sketchlib_rust::{KMV, SketchInput};

let mut kmv = KMV::new(1024);
kmv.insert(&SketchInput::U64(1));
let _ = kmv.estimate();
```

## Caveats

- Not currently part of primary framework wrappers.

## Status

Unstable; retained for compatibility visibility.
