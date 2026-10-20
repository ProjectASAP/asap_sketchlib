# API: CountSketchWithHeap

Status: `Ready`

## Purpose

Count Sketch with integrated heavy-hitter heap (`TopKHeap`) for top-k tracking.

## Type/Struct

- `CountSketchWithHeap<S = Vector2D<i64>, M = RegularPath, H = DefaultXxHasher>`

The heap is an `TopKHeap<C>` whose count type `C` is `S::Counter`'s
`HeapCounter::HeapCount`: `f64` over `f64` counters, `i64` over `i32`, `i64`
and `i128`. Each entry's count is the sketch's median estimate, through
`CountSketchHeapCounter::heap_count`.

## Constructors

```rust
fn new(rows: usize, cols: usize, top_k: usize) -> Self
fn from_storage(storage: S, top_k: usize) -> Self
fn default() -> Self
```

## Insert/Update

```rust
fn update(&mut self, key: &DataInput)
fn update_weighted(&mut self, key: &DataInput, weight: S::Counter)
fn update_batch(&mut self, values: &[DataInput])
fn clear_heap(&mut self)
```

## Query

```rust
fn estimate(&self, key: &DataInput) -> f64
fn rows(&self) -> usize
fn cols(&self) -> usize
fn cs(&self) -> &CountSketch<S, M, H>
fn heap(&self) -> &TopKHeap<C>
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

These produce/consume the **ASAPv1** wire envelope (kind `0x0a 0x00`) — see the
[ASAPv1 wire format spec](../asapv1_wire_format.md). They are **not** available
on every `CountSketchWithHeap`: the impl exists only for wire-eligible configs
`CountSketchWithHeap<Vector2D<T>, Mode, H>` where `T` is `i32`, `i64` or `f64`
(`CsWireCounter`), `Mode` is `FastPath` or `RegularPath` (`CsWireMode`), and
`H: HashProfile`. An `i128` or non-`Vector2D` sketch must be converted first
(only you know if the mapping is lossless).

A sketch travels as the base matrix plus the heap's entries: the metadata
carries `rows`, `cols`, `counter_type`, `mode`, the heap capacity `heap_size`
and the heap's `key_type`, and the payload is `[counts, keys, heap_counts]`,
with `heap_counts` `f64` under `counter_type` `"f64"` and `i64` otherwise. The
heap's digest index is rebuilt on load, so no index reaches the wire, and
`heap_size` never sizes an allocation on decode. The base matrix is bound by
the same `1 <= rows <= 20` (`MATRIX_MAX_ROWS`, the seed list length) the
stand-alone sketch is: past that, the regular path gives row `r` and row
`r + 20` the same seed and identical counters, so a wider matrix is refused on
both sides in either mode.

Heap keys are `OwnedDataInput`s, so the key type is a runtime property: `key_type`
names the **exact** variant (`"i32"` stays `"i32"`, never widened to `"i64"`)
and the `keys` array is homogeneous in it. A heap whose keys mix variants, or
holds an `I128` / `U128` key, does not serialize. An empty heap emits
`key_type = "u64"`.

A key of several typed columns, such as one GROUP BY row, is a
[`TupleKey`](./api_common_input.md#tuple-keys): insert
`DataInput::Bytes(&key.to_bytes()?)` and the heap carries `key_type`
`"bytes"`; `TupleKey::from_bytes` reads a heap key back into its columns.

Entries are emitted in descending count (`f64::total_cmp` for `f64` counts),
ties broken by a total order over the key, so a decoded sketch re-serializes
byte-identically. The decoder rejects entries in any other order.

`CountSketchWithL2Norm` is a different algorithm with its own ASAPv1 kind (`0x19 0x00`);
see [the Count Sketch API doc](./api_count_sketch.md).

## Deltas

```rust
CountSketchWithHeapDelta<T = i64, Mode = RegularPath, H = DefaultXxHasher>

fn new(rows: usize, cols: usize, indices: Vec<u64>, deltas: Vec<T>,
       heap: TopKHeap<T::HeapCount>) -> Result<Self, String>
fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError>
fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError>

// on CountSketchWithHeap<Vector2D<T>, Mode, H>
fn merge_delta(&mut self, delta: &CountSketchWithHeapDelta<T, Mode, H>)
    -> Result<(), String>
```

A `CountSketchWithHeapDelta` is the ASAPv1 delta kind `0x0a 0x80`, for the same configs as
the state serialization: the changed cells as `CountSketchDelta`'s (strictly
ascending row-major indices below `rows * cols`, each with a non-zero value)
and the producer's whole heap, encoded as the state's heap is. `new` and the
decoder refuse anything else. `merge_delta` adds the cells to the counters and
replaces the heap with the delta's, its key type included; a delta of other
dimensions or heap capacity, or an integer add that overflows, fails and leaves
the sketch unchanged.

## Examples

```rust
use asap_sketchlib::{CountSketchWithHeap, DataInput, Vector2D, RegularPath};

let mut sk = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 256, 8);
sk.update(&DataInput::Str("flow"));
assert!(sk.estimate(&DataInput::Str("flow")) >= 1.0);
```

## Caveats

- Estimate semantics follow Count Sketch and may be non-integer.
- Merge requires matching dimensions and compatible type parameters.
- **Over integer counters, heap counts are a saturating, truncating view of the
  estimate.** The insert path stores `heap_count_from_estimate(estimate)`, where the
  estimate is the row median as `f64` and the heap holds `i64`. The conversion
  saturates at `i64::MAX` / `i64::MIN` (and maps `NaN` to `0`) rather than
  wrapping, because a wrapped negative count would corrupt the heap's ordering.
  An `i128`-backed sketch can genuinely hold counts past `i64::MAX` —
  `update_weighted(key, i128)` accepts them, and the sketch keeps them — but its
  heap entry clamps. Above `2^53` the estimate has already lost precision
  inside `CountSketch::estimate`.
- Every storage backend is insertable, `i128` included: the bound is
  `S::Counter: CountSketchHeapCounter`, which `i32`, `i64`, `i128` and `f64` all satisfy.
  This is *not* true of `CountMinSketchWithHeap`; see its page.

## Status

Useful helper wrapper; tested but less central than base sketches.
