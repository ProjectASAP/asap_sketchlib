# API: HyperLogLog

Status: `Ready`

## Purpose

Approximate cardinality estimation.

## Type/Struct

- `HyperLogLog<Regular, H = DefaultXxHasher>`
- `HyperLogLog<DataFusion, H = DefaultXxHasher>`
- `HyperLogLogHIP`

## Constructors

```rust
fn new() -> Self
fn default() -> Self
```

## Insert/Update

```rust
fn insert(&mut self, obj: &SketchInput)
fn insert_many(&mut self, items: &[SketchInput])
fn insert_with_hash(&mut self, hashed: u64)
fn insert_many_with_hashes(&mut self, hashes: &[u64])
```

## Query

```rust
fn estimate(&self) -> usize
```

## Merge

```rust
fn merge(&mut self, other: &Self)
```

## Serialization

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

## Examples

```rust
use sketchlib_rust::{DataFusion, HyperLogLog, SketchInput};

let mut hll = HyperLogLog::<DataFusion>::new();
for i in 0..1000u64 {
    hll.insert(&SketchInput::U64(i));
}
let card = hll.estimate();
assert!(card > 900);
```

## Caveats

- `HyperLogLogHIP` is not mergeable.

## Status

Canonical cardinality implementation in this library.
