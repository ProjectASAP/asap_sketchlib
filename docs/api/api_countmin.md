# API: CountMin

Status: `Ready`

## Purpose

Approximate frequency estimation with sub-linear memory.

## Type/Struct

- `CountMin<S = Vector2D<i32>, Mode = RegularPath, H = DefaultXxHasher>`

## Constructors

```rust
fn default() -> Self
fn with_dimensions(rows: usize, cols: usize) -> Self
fn from_storage(counts: S) -> Self
```

## Insert/Update

```rust
fn insert(&mut self, value: &DataInput)
fn insert_many(&mut self, value: &DataInput, many: S::Counter)
fn bulk_insert(&mut self, values: &[DataInput])
fn bulk_insert_many(&mut self, values: &[(DataInput, S::Counter)])
fn fast_insert_with_hash_value(&mut self, hashed_val: &S::HashValueType)
fn fast_insert_many_with_hash_value(&mut self, hashed_val: &S::HashValueType, many: S::Counter)
```

## Query

```rust
fn estimate(&self, value: &DataInput) -> S::Counter
fn fast_estimate_with_hash(&self, hashed_val: &S::HashValueType) -> S::Counter
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

## Compression

```rust
fn compress_sum(&mut self, ratio: usize)
fn compress_max(&mut self, ratio: usize)   // Counter: PartialOrd
```

Folds every row down to `cols / ratio` columns in place. New column `j` absorbs
old columns `j`, `j + cols/ratio`, `j + 2*cols/ratio`, and so on, which is the
equal-division grouping of Elastic Sketch section 3.2.1. `ratio` must divide
`cols`, or the grouping does not line up with the query and the call asserts.

Nothing else changes at query time. A regular-path column is
`(hash & 0xFFFF_FFFF) % cols` per row, and Lemma 3.2 gives
`(h % w) % w' == h % w'` when `w'` divides `w`, so a compressed sketch is just a
narrower sketch. There is no decompression step and no separate query path.

`compress_sum` leaves the error bound unchanged and conserves each row's total.
`compress_max` tightens the bound and stays one-sided — it can only overestimate.
Measured at ratio 8 over 40 keys totalling 190, the max fold overshoots by 107
and the sum fold by 550.

Both are **regular-path only**, and deliberately so. The fast path derives a
row's column by shifting the packed hash by `row * mask_bits`, and `mask_bits`
comes from `cols`; changing the width moves every row past the first onto a
different bit field, so Lemma 3.2 does not survive there. Compression is also
one-way: the original counters are gone.


## Serialization

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

These produce/consume the **ASAPv1** wire envelope (kind `0x02 0x00`) — see the
[ASAPv1 wire format spec](../asapv1_wire_format.md). They are **not** available
on every `CountMin`: the impl exists only for wire-eligible configs
`CountMin<Vector2D<T>, Mode, H>` where `T` is `i64` or `f64` (`CmsWireCounter`),
`Mode` is `FastPath` or `RegularPath` (`CmsWireMode`), and `H: HashProfile`. The
default storage is `Vector2D<i32>`, which is **not** wire-eligible — an `i32` /
`i128` / other exotic-counter or non-`Vector2D` sketch must be converted to a
`Vector2D<i64>` / `Vector2D<f64>` first (only you know if the mapping is
lossless). `rows`/`cols` are carried in the envelope metadata; the payload is
just `[counts]`.

## Examples

```rust
use asap_sketchlib::{CountMin, DataInput};

let mut cm = CountMin::with_dimensions(3, 1024);
cm.insert(&DataInput::U64(42));
let est = cm.estimate(&DataInput::U64(42));
assert!(est >= 1);
```

## Caveats

- `merge` requires matching dimensions.
- `FastPath` and regular modes must be paired consistently.

## Status

Used as a primary building block in frameworks and orchestrated paths.
