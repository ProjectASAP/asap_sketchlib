# API: Elastic

Status: `Unstable`

> Warning: This API is kept for backward compatibility. It does not follow the full structured API parity used by Ready sketches.

## Purpose

Heavy/light split frequency estimator with a heavy bucket plus Count-Min backing sketch.

## Type/Struct

- `Elastic<H = DefaultXxHasher>`
- `HeavyBucket`

## Constructors

```rust
fn new() -> Self
fn init_with_length(l: i32) -> Self
```

## Insert/Update

```rust
fn insert(&mut self, id: String)
```

## Query

```rust
fn query(&mut self, id: String) -> i32
```

## Merge

No public merge API.

## Serialization

Derives serde; no dedicated byte API helpers.

## Examples

```rust
use sketchlib_rust::Elastic;

let mut sk = Elastic::init_with_length(8);
sk.insert("flow".to_string());
let _ = sk.query("flow".to_string());
```

## Caveats

- String-centric API (`String` in insert/query).
- Lifecycle and parity differ from structured sketches.

## Status

Unstable; migration work is tracked in `features.md`.
