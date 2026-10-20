# API: KLL

Status: `Ready`

## Purpose

Approximate quantile estimation with rank-error guarantees.

## Type/Struct

- `Kll<T = f64>`
- `Cdf`

## Constructors

```rust
fn default() -> Self
fn with_k(k: i32) -> Self
fn init(k: usize, m: usize) -> Self
fn with_k_and_seed(k: i32, seed: u64) -> Self
fn init_with_seed(k: usize, m: usize, seed: u64) -> Self
```

The unseeded constructors draw the compaction coin from the wall clock, so two
runs over the same input produce different sketches. The `*_with_seed` forms
fix the coin and store the seed, so `clear()` re-seeds from it too — use them
for reproducible replay, cross-process parity, and any test asserting an
accuracy bound. `KllDynamic` exposes the same pair
(`init_with_seed` / `with_k_and_seed`).

## Insert/Update

```rust
fn update(&mut self, val: &T)
fn update_batch(&mut self, values: &[T])
fn update_data_input(&mut self, val: &DataInput) -> Result<(), &'static str> // Kll<f64> only
fn update_batch_data_input(&mut self, values: &[DataInput]) -> Result<(), &'static str> // Kll<f64> only
fn clear(&mut self)
```

`update_batch` / `update_batch_data_input` are exactly equivalent to looping `update` / `update_data_input` (including coin-state and byte-identical `to_asapv1` for same seed), with empty slices as a no-op. `KllDynamic` and `HydraCounter::Kll` expose the same `update_batch` / `update_batch` via delegation.

## Query

```rust
fn quantile(&self, q: f64) -> f64
fn rank(&self, x: f64) -> usize
fn count(&self) -> usize
fn cdf(&self) -> Cdf

// Cdf
fn quantile(&self, x: f64) -> f64
fn query(&self, p: f64) -> f64
fn normalized_rank_interpolated(&self, x: f64) -> f64
fn value_at_interpolated(&self, p: f64) -> f64
```

## Merge

```rust
fn merge(&mut self, other: &Kll<T>)
```

## Serialization

```rust
fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError>
fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

## Examples

```rust
use asap_sketchlib::{Kll, DataInput};

let mut kll = Kll::<i64>::with_k(200);
kll.update(&10);
kll.update(&20);
let q50 = kll.quantile(0.5);
assert!(q50 >= 10.0);
```

```rust
use asap_sketchlib::{Kll, DataInput};

let mut kll = Kll::<f64>::with_k(200);
kll.update_data_input(&DataInput::F64(10.0)).unwrap();
kll.update_data_input(&DataInput::F64(20.0)).unwrap();
let q50 = kll.quantile(0.5);
assert!(q50 >= 10.0);
```

## Caveats

- `Kll<T>` is generic over numeric types implementing `NumericalValue`.
- `update_data_input` exists only on `Kll<f64>` for type-erased `DataInput` call paths.
- Query-side APIs still return `f64`.

## Status

Production-usable quantile sketch with comprehensive tests.
