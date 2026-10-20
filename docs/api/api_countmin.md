# API: CountMinSketch

Status: `Ready`

## Purpose

Approximate frequency estimation with sub-linear memory.

## Type/Struct

- `CountMinSketch<S = Vector2D<i32>, Mode = RegularPath, H = DefaultXxHasher>`

## Constructors

```rust
fn default() -> Self
fn with_dimensions(rows: usize, cols: usize) -> Self
fn from_storage(counts: S) -> Self
```

## Insert/Update

```rust
fn update(&mut self, value: &DataInput)
fn update_weighted(&mut self, value: &DataInput, weight: S::Counter)
fn update_batch(&mut self, values: &[DataInput])
fn update_weighted_batch(&mut self, values: &[(DataInput, S::Counter)])
fn update_with_hash(&mut self, hashed_val: &H::HashType)
fn update_weighted_with_hash(&mut self, hashed_val: &H::HashType, many: S::Counter)
```

## Query

```rust
fn estimate(&self, value: &DataInput) -> S::Counter
fn estimate_with_hash(&self, hashed_val: &H::HashType) -> S::Counter
fn rows(&self) -> usize
fn cols(&self) -> usize
fn as_storage(&self) -> &S
fn as_storage_mut(&mut self) -> &mut S
```

## Merge

```rust
fn merge(&mut self, other: &Self)
fn merge_max(&mut self, other: &Self)
```

`merge` adds each counter pair. `merge_max` keeps the larger of each pair,
which is tighter but correct only when the two sketches observed disjoint key
sets — a key both sides counted reads back as the larger side, not the sum.
Both assert matching dimensions.

## Serialization

```rust
fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError>
fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

These produce/consume the **ASAPv1** wire envelope (kind `0x02 0x00`) — see the
[ASAPv1 wire format spec](../asapv1_wire_format.md). They are **not** available
on every `CountMinSketch`: the impl exists only for wire-eligible configs
`CountMinSketch<Vector2D<T>, Mode, H>` where `T` is `i32`, `i64` or `f64`
(`CmsWireCounter`), `Mode` is `FastPath` or `RegularPath` (`CmsWireMode`), and
`H: HashProfile`. `i32` is carried at its own width and pinned on decode. An
`i128` / other exotic-counter or non-`Vector2D` sketch must be converted to a
wire-eligible storage first (only you know if the mapping is lossless).
`rows`/`cols` are carried in the envelope metadata; the payload is just
`[counts]`. The wire covers `1 <= rows <= 20` (`MATRIX_MAX_ROWS`, the seed
list length): past that, the regular path gives row `r` and row `r + 20` the
same seed and identical counters, so a wider matrix is refused on both sides in
either mode.

## Deltas

```rust
CountMinSketchDelta<T = i32, Mode = RegularPath, H = DefaultXxHasher>

fn new(rows: usize, cols: usize, indices: Vec<u64>, deltas: Vec<T>) -> Result<Self, String>
fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError>
fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError>

// on CountMinSketch<Vector2D<T>, Mode, H>
fn merge_delta(&mut self, delta: &CountMinSketchDelta<T, Mode, H>) -> Result<(), String>
```

A `CountMinSketchDelta` is the ASAPv1 delta kind `0x02 0x80`, for the same configs
as the state serialization: the changed cells as strictly ascending row-major
indices `row * cols + col`, below `rows * cols`, and the non-zero value added
to each. `new` and the decoder refuse anything else. `merge_delta` adds the
delta to the counters; a delta of other dimensions or admission rate, or an
integer add that overflows, fails and leaves the sketch unchanged.

## Admission rate

```rust
fn with_admission_rate(self, p: f64) -> Self
fn admission_rate(&self) -> f64

// on CountMinSketchDelta
fn with_admission_rate(self, p: f64) -> Self
```

`p` is the probability with which each update was admitted, `1` unless set.
Estimates count admitted updates alone; a consumer divides a key's frequency and
a row's total by `p`. The ASAPv1 metadata carries `p` as its last key,
`admission_rate`, when `p < 1`; a `p` outside `(0, 1]` fails to serialize.
`merge` and `merge_max` panic, and `merge_delta` fails, when the two rates
differ. A sketch nested in another kind (`EhBucketSketch`, `Hydra`, CountMinSketchWithHeap,
CountSketchWithHeap, Elastic) must hold `p = 1` to serialize.

## Examples

```rust
use asap_sketchlib::{CountMinSketch, DataInput};

let mut cm = CountMinSketch::with_dimensions(3, 1024);
cm.update(&DataInput::U64(42));
let est = cm.estimate(&DataInput::U64(42));
assert!(est >= 1);
```

## Caveats

- `merge` requires matching dimensions.
- `FastPath` and regular modes must be paired consistently.

## Status

Used as a primary building block in frameworks and orchestrated paths.
