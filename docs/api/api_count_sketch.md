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

`check_shape` accepts the ASAPv1 geometry, `1 <= rows <= MATRIX_MAX_ROWS` (20)
and `1 <= cols <= u32::MAX`, on every hashing path; `with_dimensions` and
`from_storage` panic on any other shape. Past 20 rows the regular path and the
fast path's per-row (`Rows`) layout would hash row `r + 20` with row `r`'s
seed; the packed layouts carve one hash and reuse no seed.

## Insert/Update

```rust
fn update(&mut self, value: &DataInput)
fn update_weighted(&mut self, value: &DataInput, weight: S::Counter)
fn update_with_hash(&mut self, hashed_val: &H::HashType)
fn clear(&mut self)

// CountSketchWithL2Norm
fn update_weighted(&mut self, val: &DataInput, weight: i64)
fn update_weighted_with_hash(&mut self, hashed_val: u128, weight: i64)
fn update_weighted_with_hash_without_l2(&mut self, hashed_val: u128, c: i64)
```

`clear` zeroes every counter and returns the Nitro sampler to its start, keeping
the shape, allocation and admission rate.

## Query

```rust
fn estimate(&self, value: &DataInput) -> f64
fn estimate_with_hash(&self, hashed_val: &H::HashType) -> f64

// CountSketchWithL2Norm
fn estimate(&self, val: &DataInput) -> f64
fn estimate_with_hash(&self, hashed_val: u128) -> f64
fn update_weighted_and_estimate(&mut self, val: &DataInput, weight: i64) -> f64
fn l2(&self) -> f64
fn l2_squared(&self) -> f64
```

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
payload is just `[counts]`, packed row-major with signed cells. The wire covers `1 <= rows <= 20` (`MATRIX_MAX_ROWS`, the seed
list length): past that, the regular path gives row `r` and row `r + 20` the
same seed and identical counters, so a wider matrix is refused on both sides in
either mode.

`CountSketchWithL2Norm` has its own ASAPv1 kind (`0x19 0x00`) and the same two methods,
available for any `H: HashProfile`. Its counters are always `i64` and its
column derivation is fixed by the algorithm, so the metadata carries no
`counter_type` and no `mode` — only `seed_index`, `rows` and `cols`. Its rows
are seeded per row too, so it carries the same `1 <= rows <= 20` bound. The
payload is `[counts, l2]`: the matrix packed row-major, then one L2
accumulator per row. `l2` is carried rather than recomputed, because
`update_weighted_with_hash_without_l2` moves counters without it and the
accumulator saturates one way. The same `(counts, l2)` pair is what a `UnivMon`
layer inlines into its own payload.

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
