# API: CountSketch

Status: `Ready`

## Purpose

`CountSketch` is this crate's Count Sketch implementation for approximate frequency estimation with signed counters and median aggregation.

## Type/Struct

- `CountSketch<S = Vector2D<i32>, Mode = RegularPath, H = DefaultXxHasher>`
- `CountSketchWithL2Norm<H = DefaultXxHasher>`

## Constructors

```rust
fn default() -> Self
fn with_dimensions(rows: usize, cols: usize) -> Self
fn from_storage(counts: S) -> Self
fn check_shape(rows: usize, cols: usize) -> Result<(), String>

// CountSketchWithL2Norm
fn default() -> Self
fn with_dimensions(rows: usize, cols: usize) -> Self
fn with_dimensions_and_seed_index(rows: usize, cols: usize, seed_index: usize) -> Self
```

`check_shape` is defined once for the type (`CountSketch::check_shape(3, 64)`
needs no type parameters) and accepts any shape with both dimensions non-zero and
`rows * cols` within `usize`; `with_dimensions` and `from_storage` panic on any
other shape. Every row draws its own seed (`seed_for_index`), so any row count
hashes independently.

## Insert/Update

```rust
fn update(&mut self, value: &DataInput)
fn update_weighted(&mut self, value: &DataInput, weight: S::Counter)
fn update_with_hash(&mut self, hashed_val: &H::HashType)
fn clear(&mut self)

// Vector2D<f64>, RegularPath: weights finite (signed); an error (a bad weight
// or a counter overflow) leaves the sketch unchanged
fn check_weight(weight: f64) -> Result<(), String>
fn try_update_weighted(&mut self, value: &DataInput, weight: f64) -> Result<(), String>

// CountSketchWithL2Norm
fn update_weighted(&mut self, val: &DataInput, weight: i64)
fn update_weighted_with_hash(&mut self, hashed_val: u128, weight: i64)
```

`clear` zeroes every counter and returns the Nitro sampler to its start, keeping
the shape, allocation and admission rate.

## Query

```rust
fn estimate(&self, value: &DataInput) -> f64
fn estimate_with_hash(&self, hashed_val: &H::HashType) -> f64
fn memory_bytes(&self) -> usize

// CountSketchWithL2Norm
fn estimate(&self, val: &DataInput) -> f64
fn estimate_with_hash(&self, hashed_val: u128) -> f64
fn update_weighted_and_estimate(&mut self, val: &DataInput, weight: i64) -> f64
fn l2(&self) -> f64
fn l2_squared(&self) -> f64
```

`memory_bytes` is the struct plus its `rows * cols` counters, exact for
`Vector2D` storage.

## Merge

```rust
fn merge(&mut self, other: &Self)
```

## Serialization

```rust
fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError>
fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

These produce/consume the **ASAPv1** wire envelope (kind `0x04 0x00`) — see the
[ASAPv1 wire format spec](../asapv1_wire_format.md). They are **not** available
on every `CountSketch`: the impl exists only for wire-eligible configs
`CountSketch<Vector2D<T>, Mode, H>` where `T` is `i32`, `i64` or `f64`
(`CsWireCounter`), `Mode` is `FastPath` or `RegularPath` (`CsWireMode`), and
`H: HashProfile`. An `i128` or non-`Vector2D` sketch must be converted first
(only you know if the mapping is lossless).

`i32` is **not** widened to `i64` on the wire. The counter type is carried in
the metadata and pinned on decode, so `i32` bytes do not decode into an `i64`
sketch, or the reverse — which is what lets a nested `Vector2D<i32>` Count
Sketch (the variant `HydraCounter` and `EhBucketSketch` hold) round-trip back into
its own type. `rows`/`cols` and the `mode` are carried in the metadata too; the
payload is just `[counts]`, packed row-major with signed cells. The wire covers `1 <= rows <= 20` (`MATRIX_MAX_ROWS`) and
`cols <= u32::MAX`; `to_asapv1` and `from_asapv1` refuse a wider matrix in
either mode.

`CountSketchWithL2Norm` has its own ASAPv1 kind (`0x19 0x01`) and the same two methods,
available for any `H: HashProfile`. Its counters are always `i64` and its
column derivation is fixed by the algorithm, so the metadata carries no
`counter_type` and no `mode` — only `seed_index`, `rows` and `cols`. It
carries the same `1 <= rows <= 20` wire bound. The
payload is `[counts]`, the matrix packed row-major. Each row's L2 accumulator
always equals that row's sum of squared counters (saturated at `i64::MAX`), so
it stays in memory as a cache and the decoder recomputes it. The retired kind
`0x19 0x00` also carried it as `[counts, l2]`; `from_asapv1` still reads it,
dropping `l2`, and `to_asapv1` never writes it. The same `counts` run is what a
`UnivMon` layer inlines into its own payload.

## Deltas

```rust
CountSketchDelta<T = i32, Mode = RegularPath, H = DefaultXxHasher>

fn new(rows: usize, cols: usize, indices: Vec<u64>, deltas: Vec<T>) -> Result<Self, String>
fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError>
fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError>

// on CountSketch<Vector2D<T>, Mode, H>
fn merge_delta(&mut self, delta: &CountSketchDelta<T, Mode, H>) -> Result<(), String>
```

A `CountSketchDelta` is the ASAPv1 delta kind `0x04 0x80`, for the same configs
as the state serialization: the changed cells as strictly ascending row-major
indices `row * cols + col`, below `rows * cols`, and the non-zero signed value
added to each. `new` and the decoder refuse anything else. `merge_delta` adds
the delta to the counters; a delta of other dimensions or admission rate, or an
integer add that overflows, fails and leaves the sketch unchanged.

## Admission rate

```rust
fn with_admission_rate(self, p: f64) -> Self
fn admission_rate(&self) -> f64

// on CountSketchDelta
fn with_admission_rate(self, p: f64) -> Self
```

`p` is the probability with which each update was admitted, `1` unless set.
Estimates count admitted updates alone; a consumer divides a key's frequency by
`p`. The ASAPv1 metadata carries `p` as its last key, `admission_rate`, when `p
< 1`; a `p` outside `(0, 1]` fails to serialize. `merge` panics, and
`merge_delta` fails, when the two rates differ. A sketch nested in another kind
(`EhBucketSketch`, `Hydra`, CountMinSketchWithHeap, CountSketchWithHeap, Elastic) must hold `p = 1` to
serialize.

## Examples

```rust
use asap_sketchlib::{Count, DataInput};

let mut cs = CountSketch::with_dimensions(5, 2048);
cs.update(&DataInput::Str("alpha"));
let est = cs.estimate(&DataInput::Str("alpha"));
assert!(est >= 1.0);
```

## Caveats

- `merge` requires matching dimensions.
- `CountSketchWithL2Norm` is the L2-heavy-hitter variant used by `UnivMon` internals.

## Status

Core frequency primitive; this crate exposes it as `CountSketch` and uses it widely in framework layers.
