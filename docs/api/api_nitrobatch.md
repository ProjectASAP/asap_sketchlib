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
fn init_nitro_with_seed(rate: f64, seed: u64) -> Self
fn with_target_and_seed(rate: f64, sk: S, seed: u64) -> Self
```

Sampling is where all of Nitro's randomness lives: which updates reach the
target sketch is drawn from the geometric skip distribution. `init_nitro` and
`with_target` seed that RNG from the OS, so the admitted subset — and therefore
every estimate — differs between runs. The `*_with_seed` forms make it a
deterministic function of the input, which is what lets an accuracy bound be
asserted reproducibly.

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
fn estimate_median(&self, value: &DataInput) -> f64
```

## Merge

```rust
fn merge(&mut self, other: &Self)
```

## Serialization

No dedicated serialization API.

## Examples

```rust
use asap_sketchlib::{CountMin, FastPath, NitroBatch, Vector2D};

let base = CountMin::<Vector2D<i32>, FastPath>::default();
let mut nitro = NitroBatch::with_target(0.1, base);
nitro.insert(&[1, 2, 3, 4]);
```

## Caveats

- Works with targets implementing `NitroTarget`.
- Sampling introduces intentional approximation.

## Status

Ready for batch sampling workflows.
