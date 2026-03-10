# API: NitroBatch

Status: `Ready`

## Purpose

Batch-mode geometric sampling wrapper that updates a sketch target.

## Type/Struct

- `NitroBatch<S: NitroTarget>`
- Traits: `NitroTarget`, `NitroMerge`, `NitroEstimate`

## Constructors

```rust
fn init_nitro(rate: f64) -> Self
fn with_target(rate: f64, sk: S) -> Self
```

## Insert/Update

```rust
fn insert(&mut self, data: &[i64])
fn insert_cached_step(&mut self, data: &[i64])
fn draw_geometric(&mut self)
fn reduce_to_skip(&mut self)
```

## Query

```rust
fn target(&self) -> &S
fn target_mut(&mut self) -> &mut S
fn into_target(self) -> S
fn get_sampling_rate(&self) -> f64
fn get_ctx(&self) -> (usize, f64, usize, usize)
fn estimate_median(&self, value: &SketchInput) -> f64
```

## Merge

```rust
fn merge(&mut self, other: &Self)
```

## Serialization

No dedicated serialization API.

## Examples

```rust
use sketchlib_rust::{CountMin, FastPath, NitroBatch, Vector2D};

let base = CountMin::<Vector2D<i32>, FastPath>::default();
let mut nitro = NitroBatch::with_target(0.1, base);
nitro.insert(&[1, 2, 3, 4]);
```

## Caveats

- Works with targets implementing `NitroTarget`.
- Sampling introduces intentional approximation.

## Status

Ready for batch sampling workflows.
