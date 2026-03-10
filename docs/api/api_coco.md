# API: Coco

Status: `Unstable`

> Warning: This API is kept for backward compatibility. It uses specialized substring semantics and does not match structured sketch interfaces.

## Purpose

Substring-based aggregation sketch for arbitrary string keys.

## Type/Struct

- `Coco<H = DefaultXxHasher>`
- `CocoBucket`

## Constructors

```rust
fn new() -> Self
fn init_with_size(w: usize, d: usize) -> Self
```

## Insert/Update

```rust
fn insert(&mut self, key: &str, v: u64)
```

## Query

```rust
fn estimate(&mut self, partial_key: &str) -> u64
fn estimate_with_udf<F>(&mut self, partial_key: &str, udf: F) -> u64
```

## Merge

```rust
fn merge(&mut self, other: &Coco<H>)
```

## Serialization

Derives serde; no dedicated byte API helpers.

## Examples

```rust
use sketchlib_rust::Coco;

let mut sk = Coco::init_with_size(64, 4);
sk.insert("region=us|id=1", 3);
let _ = sk.estimate("region=us");
```

## Caveats

- Query semantics are substring/UDF based (not exact-key frequency).
- Replacement behavior is probabilistic.

## Status

Unstable.
