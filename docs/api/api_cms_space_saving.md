# API: CountMinSketchWithSpaceSaving

Status: `Unstable`

## Purpose

Count-Min sketch with a Space-Saving summary that nominates top-k candidates.
Every update reaches both; a top-k query ranks the summary's monitored keys by
their Count-Min estimate. The summary picks the candidates and the matrix gives
each its frequency. The summary's `capacity` is independent of `k`.

[`CountMinSketchWithHeap`](./api_cms_heap.md) keeps an estimate heap instead: a key's count is
its matrix estimate and a newcomer enters only by beating the heap minimum.

## Type/Struct

- `CountMinSketchWithSpaceSaving<S = Vector2D<i64>, M = RegularPath, H = DefaultXxHasher>`
- `CandidateCounter`: the matrix counter types the summary sits beside
  (`i32`, `i64`, `i128`, `f64`).

## Constructors

```rust
fn new(rows: usize, cols: usize, capacity: usize) -> Self
fn from_parts(cms: CountMinSketch<S, M, H>, space_saving: SpaceSaving<H>) -> Self
```

## Insert/Update

```rust
fn update(&mut self, key: &DataInput)
fn update_weighted(&mut self, key: &DataInput, weight: S::Counter)
fn update_batch(&mut self, values: &[DataInput])
```

`update_weighted` adds `many` to the matrix and `many.summary_weight()` to the
summary: the weight rounded up, saturating at `u64::MAX`, and nothing when the
weight is not positive.

## Query

```rust
fn estimate(&self, key: &DataInput) -> S::Counter
fn top_k(&self, k: usize) -> Vec<(OwnedDataInput, S::Counter)>
fn rows(&self) -> usize
fn cols(&self) -> usize
fn capacity(&self) -> usize
fn cms(&self) -> &CountMinSketch<S, M, H>
fn space_saving(&self) -> &SpaceSaving<H>
```

`top_k` returns at most `min(k, capacity)` keys, highest estimate first, ties
broken by heap key order.

## Merge

```rust
fn merge(&mut self, other: &Self)
```

Adds the matrices and merges the summaries with `SpaceSaving::merge`. Panics
when the dimensions, the admission rates or the capacities differ.

## Serialization

```rust
fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError>
fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

The **ASAPv1** envelope, kind `0x1e 0x00` (spec §3.24), for
`CountMinSketchWithSpaceSaving<Vector2D<T>, Mode, H>` with `T` `i32`, `i64` or `f64`, `Mode`
`FastPath` or `RegularPath`, and `H: HashProfile`. The metadata is Count-Min's
plus `capacity` and `key_type`; the payload is the matrix followed by the
summary's triples and running scalars. A matrix holding `p != 1` does not
serialize. There is no delta kind.

## Examples

```rust
use asap_sketchlib::{CountMinSketchWithSpaceSaving, DataInput, RegularPath, Vector2D};

let mut sk = CountMinSketchWithSpaceSaving::<Vector2D<i64>, RegularPath>::new(4, 1024, 64);
sk.update_weighted(&DataInput::Str("flow"), 3);
assert_eq!(sk.top_k(1)[0].1, 3);
```
