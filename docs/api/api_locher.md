# API: Locher

Status: `Unstable`

> Warning: This API is not top-level re-exported and remains an experimental module path.

## Purpose

Heavy-hitter sampling with per-row heaps.

## Type/Struct

- `LocherSketch<H = DefaultXxHasher>`

## Constructors

```rust
fn new(r: usize, l: usize, k: usize) -> Self
```

## Insert/Update

```rust
fn insert(&mut self, e: &str, v: u64)
```

## Query

```rust
fn estimate(&self, e: &str) -> f64
```

## Merge

No public merge API.

## Serialization

Derives serde; no dedicated byte API helpers.

## Examples

```rust
use sketchlib_rust::sketches::locher::LocherSketch;

let mut sk = LocherSketch::new(3, 64, 5);
sk.insert("flow", 1);
let _ = sk.estimate("flow");
```

## Caveats

- Access path is `sketchlib_rust::sketches::locher::LocherSketch`.
- Not fully integrated with framework enums.

## Status

Unstable.
