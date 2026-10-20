# API: UnivMon

Status: `Ready`

## Purpose

Universal stream-monitoring sketch for L1/L2/cardinality/entropy.

## Type/Struct

- `UnivMon`

## Constructors

```rust
fn default() -> Self
fn new(heap_size: usize, rows: usize, cols: usize, layers: usize) -> Self
```

`new` panics on a shape whose hashes would not be independent:

- `layers > UNIVMON_MAX_LAYERS` (19). Layer `i` hashes at seed index `i`, and
  the layers a key reaches are read from the hash at `UNIVMON_BOTTOM_LAYER_SEED_INDEX`
  (19). A 20th layer would hash at the finder's seed, and later ones would wrap
  onto layer 0's.
- `rows * (ceil_log2(cols) + 1) > 128`. Every row of a layer reads
  its column and its sign from one 128-bit hash; past this budget a deep row's
  column reuses a shallow row's sign bit.

## Insert/Update

```rust
fn update_weighted(&mut self, key: &DataInput, weight: i64)
fn fast_insert(&mut self, key: &DataInput, value: i64)
fn free(&mut self)
```

`update_weighted` maintains the cumulative sampled substream at every selected layer.
`fast_insert` uses the Joltik update-last-layer layout and reconstructs logical
layers during queries. Choose one update method for the lifetime of a sketch;
mixing the two physical layouts is rejected.

## Query

```rust
fn estimate_l1(&self) -> f64
fn estimate_l2(&self) -> f64
fn estimate_entropy(&self) -> f64
fn estimate_cardinality(&self) -> f64
fn estimate_g_sum<F>(&self, g: F, heavy_only: bool) -> f64
```

Because UnivMon accepts only non-negative updates, `estimate_l1()` returns the
exact tracked total weight. The generic recurrence remains available through
`estimate_g_sum` for compatible frequency functions.

## Merge

```rust
fn merge(&mut self, other: &UnivMon)
```

## Serialization

```rust
fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError>
fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

These produce/consume the **ASAPv1** wire envelope (kind `0x10 0x00`) — see the
[ASAPv1 wire format spec](../asapv1_wire_format.md). UnivMon hashes through the
crate's default hasher, so its metadata carries that profile and no seed index:
the bottom-layer finder's seed is fixed by the algorithm, and layer `i`'s
counter hashes at seed index `i`.

The pyramid shape (`layers`, `rows`, `cols`, `heap_size`) and
the heaps' `key_type` live in the metadata; the payload is
`[counts, l2, heap_lens, keys, heap_counts, candidate_complete, bucket_size,
update_mode]`. Each layer's `CountSketchWithL2Norm` state is inlined rather than nested in
an envelope of its own, and the heap indexes are rebuilt on load, so no index
reaches the wire. Heap entries are emitted layer by layer, each layer in
descending count with ties broken by a total order over the key, so a decoded
pyramid re-serializes byte-identically. `update_mode` and `candidate_complete`
are carried: they are acquired from the stream, and a decoder that guessed them
would pick the wrong query recurrence or report a widened threshold as zero.

Every layer is a `CountSketchWithL2Norm` matrix hashed per row, so `rows` carries that
sketch's bound: `1 <= rows <= 20` (`MATRIX_MAX_ROWS`, the seed list
length), refused on both sides past it.

## Examples

```rust
use asap_sketchlib::{DataInput, UnivMon};

let mut um = UnivMon::new(32, 3, 1024, 4);
um.update_weighted(&DataInput::Str("flow"), 1);
assert!(um.estimate_l1() >= 1.0);
```

## Caveats

- Updates must have non-negative weights.
- Structure parameters and update strategies must match before merge.
- Merge combines total weight, recomputes cached L2 state, and re-estimates the
  union of bounded heavy-hitter candidates from merged counters.

## Status

Primary multi-metric framework.
