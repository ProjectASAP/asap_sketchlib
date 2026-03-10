# API: UniformSampling

Status: `Unstable`

> Warning: Useful and tested, but currently documented as Unstable until broader API alignment work completes.

## Purpose

Reservoir-like uniform sampler with merge support.

## Type/Struct

- `UniformSampling`

## Constructors

```rust
fn new(sample_rate: f64) -> Self
fn with_seed(sample_rate: f64, seed: u64) -> Self
```

## Insert/Update

```rust
fn update(&mut self, value: f64)
fn update_input(&mut self, value: &SketchInput) -> Result<(), &'static str>
```

## Query

```rust
fn sample_rate(&self) -> f64
fn len(&self) -> usize
fn is_empty(&self) -> bool
fn total_seen(&self) -> u64
fn samples(&self) -> Vec<f64>
fn sample_at(&self, idx: usize) -> Option<f64>
```

## Merge

```rust
fn merge(&mut self, other: &UniformSampling) -> Result<(), &'static str>
```

## Serialization

Derives serde; no dedicated byte API helpers.

## Examples

```rust
use sketchlib_rust::UniformSampling;

let mut sk = UniformSampling::new(0.2);
sk.update(1.0);
let _ = sk.samples();
```

## Caveats

- Supports numeric inputs only in `update_input`.
- Merge requires matching sampling rates.

## Status

Unstable.
