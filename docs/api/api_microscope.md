# API: MicroScope

Status: `Legacy/Experimental`

> Warning: `merge` is explicitly marked as placeholder in source and should be treated as experimental.

## Purpose

Compact time-window frequency approximation with zoom-in/zoom-out counters.

## Type/Struct

- `MicroScope`

## Constructors

```rust
fn init_microscope(w: usize, t: usize) -> Self
```

## Insert/Update

```rust
fn insert(&mut self, timestamp: u64)
fn delete(&mut self, timestamp: u64)
```

## Query

```rust
fn query(&self, timestamp: u64) -> f64
fn debug(&self)
```

## Merge

```rust
fn merge(&mut self, other: &MicroScope, ts: u64)
```

## Serialization

Derives serde; no dedicated byte API helpers.

## Examples

```rust
use sketchlib_rust::MicroScope;

let mut sk = MicroScope::init_microscope(128, 8);
sk.insert(1);
let _ = sk.query(1);
```

## Caveats

- Placeholder merge semantics.
- Specialized timestamp-based API.

## Status

Legacy/Experimental.
