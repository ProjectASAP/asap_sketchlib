# API: CountMinSketchWithHeap

Status: `Ready`

## Purpose

Count-Min with integrated heavy-hitter heap (`TopKHeap`) for top-k tracking.

## Type/Struct

- `CountMinSketchWithHeap<S = Vector2D<i64>, M = RegularPath, H = DefaultXxHasher>`

The heap is an `TopKHeap<C>` whose count type `C` is `S::Counter`'s
`HeapCounter::HeapCount`: `f64` over `f64` counters, `i64` over `i32`, `i64`
and `i128`. Each entry's count is the sketch's estimate, unrounded.

## Constructors

```rust
fn new(rows: usize, cols: usize, heap_size: usize) -> Self
fn from_storage(storage: S, heap_size: usize) -> Self
fn default() -> Self
// Vector2D storage: bytes `new(rows, cols, heap_size)` holds once its heap is
// full of keys shaped like `key`, priced before building
fn estimated_memory_bytes(rows: usize, cols: usize, heap_size: usize, key: &DataInput) -> usize
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
fn estimate(&self, key: &DataInput) -> S::Counter
fn rows(&self) -> usize
fn cols(&self) -> usize
fn count_min_sketch(&self) -> &CountMinSketch<S, M, H>
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

These produce/consume the **ASAPv1** wire envelope (kind `0x03 0x00`) — see the
[ASAPv1 wire format spec](../asapv1_wire_format.md). They are **not** available
on every `CountMinSketchWithHeap`: the impl exists only for wire-eligible configs
`CountMinSketchWithHeap<Vector2D<T>, Mode, H>` where `T` is `i32`, `i64` or `f64`
(`CmsWireCounter`), `Mode` is `FastPath` or `RegularPath` (`CmsWireMode`), and
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

## Deltas

```rust
CountMinSketchWithHeapDelta<T = i64, Mode = RegularPath, H = DefaultXxHasher>

fn new(rows: usize, cols: usize, indices: Vec<u64>, deltas: Vec<T>,
       heap: TopKHeap<T::HeapCount>) -> Result<Self, String>
fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError>
fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError>

// on CountMinSketchWithHeap<Vector2D<T>, Mode, H>
fn merge_delta(&mut self, delta: &CountMinSketchWithHeapDelta<T, Mode, H>)
    -> Result<(), String>
```

A `CountMinSketchWithHeapDelta` is the ASAPv1 delta kind `0x03 0x80`, for the same configs as
the state serialization: the changed cells as `CountMinSketchDelta`'s (strictly
ascending row-major indices below `rows * cols`, each with a non-zero value)
and the producer's whole heap, encoded as the state's heap is. `new` and the
decoder refuse anything else. `merge_delta` adds the cells to the counters and
replaces the heap with the delta's, its key type included; a delta of other
dimensions or heap capacity, or an integer add that overflows, fails and leaves
the sketch unchanged.

## Examples

```rust
use asap_sketchlib::{CountMinSketchWithHeap, DataInput, Vector2D, RegularPath};

let mut sk = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 256, 8);
sk.update(&DataInput::Str("flow"));
assert!(sk.estimate(&DataInput::Str("flow")) >= 1);
```

## Caveats

- Heap can be cleared independently of sketch counters.
- Merge requires matching dimensions and compatible type parameters.
- **Six built-in instances are constructible but inert.** `update`,
  `update_weighted`, `estimate` and `merge` need `S::Counter` to convert into its
  heap count. `i128` has no `Into<i64>`, so `CountMinSketchWithHeap<Vector2D<i128>>`,
  `CountMinSketchWithHeap<QuickMatrixI128>` and `CountMinSketchWithHeap<DefaultMatrixI128>` — on both hashing
  paths — allocate a sketch and a heap and can then do nothing but report their
  dimensions. Use `CountSketchWithHeap` when you need a 128-bit counter beside a top-k heap;
  it supports every backend.

## Status

Useful helper wrapper; tested but less central than base sketches.
