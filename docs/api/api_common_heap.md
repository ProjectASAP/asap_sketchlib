# API: Common Heap Utilities

Status: `Shared`

## Purpose

Common heavy-hitter heap helper used by multiple sketches/frameworks.

## Type/Struct

- `TopKHeap<C: HeapCount = i64>`

`C` is the count type, `i64` or `f64`; `f64` counts are ranked by
`f64::total_cmp`. `HeapCounter` maps a matrix counter type to the heap count
type a top-k sketch keeps beside it.

A capacity-bounded min-heap of `HeapEntry<C>` beside an index from key digest to
heap position, so a keyed update is a lookup and a sift rather than a scan. The
index is patched through each sift: `update` costs one hash, one map probe and
`O(log k)` index writes, and its rate does not fall as `k` grows. See
[TopKHeap acceleration](../topk_heap_acceleration.md) for the measurements.

## Constructors

```rust
fn new(heap_size: usize) -> Self
fn from_heap(other: &TopKHeap<C>) -> Self
```

## Insert/Update

```rust
fn update(&mut self, key: &DataInput, count: C) -> bool
fn update_owned(&mut self, key: &OwnedDataInput, count: C) -> bool
fn clear(&mut self)
```

## Query

```rust
fn find(&self, key: &DataInput) -> Option<usize>
fn find_owned(&self, key: &OwnedDataInput) -> Option<usize>
fn heap(&self) -> &[HeapEntry<C>]
fn len(&self) -> usize
fn is_empty(&self) -> bool
fn capacity(&self) -> usize
fn estimated_memory_bytes(heap_size: usize, key: &DataInput) -> usize
```

`estimated_memory_bytes` is the bytes a heap of capacity `heap_size` holds in
steady state once keys shaped like `key` (owned string or byte length; 0 for a
number) churn through it, for pricing a heap before building one. A heap holds
at most this, and exactly this once evictions have resized its index.

## Merge

No dedicated merge method on `TopKHeap`; reconciliation is sketch-specific.

## Serialization

None of its own; a sketch holding a `TopKHeap` carries its entries in its
ASAPv1 payload.

## Examples

```rust
use asap_sketchlib::{TopKHeap, DataInput};

let mut hh = TopKHeap::new(8);
hh.update(&DataInput::Str("u1"), 10);
assert!(hh.find(&DataInput::Str("u1")).is_some());
```

## Caveats

- Key ownership conversion follows `DataInput`/`OwnedDataInput` behavior from [Common Input Types](./api_common_input.md).
- `find` returns a heap position, which the next `update` may move. Read it
  before mutating, not after.
- Counts come from the caller, so `update` accepts a count that falls as well as
  one that rises; the resident re-sifts either way.

## See Also

- [Common Module (Canonical)](./api_common.md)
- [Common Input Types](./api_common_input.md)

## Status

Canonical shared heap utility.
