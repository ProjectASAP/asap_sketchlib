# API: CSSpaceSaving

Status: `Ready`

## Purpose

Count Sketch with a Space-Saving summary that nominates top-k candidates.
Every update reaches both; a top-k query ranks the summary's monitored keys by
their Count Sketch estimate (row median). The summary picks the candidates and
the matrix gives each its frequency. The summary's `capacity` is independent
of `k`.

[`CSHeap`](./api_cs_heap.md) keeps an estimate heap instead.

## Type/Struct

- `CSSpaceSaving<S = Vector2D<i64>, M = RegularPath, H = DefaultXxHasher>`

## Constructors

```rust
fn new(rows: usize, cols: usize, capacity: usize) -> Self
fn from_parts(cs: Count<S, M, H>, space_saving: SpaceSaving<H>) -> Self
```

## Insert/Update

```rust
fn insert(&mut self, key: &DataInput)
fn insert_many(&mut self, key: &DataInput, many: S::Counter)
fn bulk_insert(&mut self, values: &[DataInput])
```

`insert_many` adds `±many` to the matrix and `many.summary_weight()`
(`SpaceSavingCounter`) to the summary, so a negative weight moves the matrix
alone.

## Query

```rust
fn estimate(&self, key: &DataInput) -> f64
fn top_k(&self, k: usize) -> Vec<(HeapItem, f64)>
fn rows(&self) -> usize
fn cols(&self) -> usize
fn capacity(&self) -> usize
fn cs(&self) -> &Count<S, M, H>
fn space_saving(&self) -> &SpaceSaving<H>
```

`top_k` returns at most `min(k, capacity)` keys, highest estimate first by
`f64::total_cmp`, ties broken by heap key order.

## Merge

```rust
fn merge(&mut self, other: &Self)
```

Adds the matrices and merges the summaries with `SpaceSaving::merge`. Panics
when the dimensions, the admission rates or the capacities differ.

## Serialization

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

The **ASAPv1** envelope, kind `0x1f 0x00` (spec §3.25), for
`CSSpaceSaving<Vector2D<T>, Mode, H>` with `T` `i32`, `i64` or `f64`, `Mode`
`FastPath` or `RegularPath`, and `H: HashProfile`. Same layout as
[CMSSpaceSaving](./api_cms_space_saving.md). There is no delta kind.

## Examples

```rust
use asap_sketchlib::{CSSpaceSaving, DataInput, FastPath, Vector2D};

let mut sk = CSSpaceSaving::<Vector2D<f64>, FastPath>::new(5, 1024, 64);
sk.insert_many(&DataInput::Str("flow"), 2.5);
assert_eq!(sk.top_k(1)[0].1, 2.5);
```
