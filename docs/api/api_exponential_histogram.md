# API: ExponentialHistogram

Status: `Ready`

## Purpose

Sliding-window coordinator over `EHSketchList` payload buckets.

## Type/Struct

- `EHBucket`
- `ExponentialHistogram`

## Constructors

```rust
fn new(k: usize, window: u64, eh_type: EHSketchList) -> Self
```

## Insert/Update

```rust
fn update(&mut self, time: u64, val: &SketchInput)
fn update_with<F>(&mut self, time: u64, update_fn: F) where F: FnOnce(&mut EHSketchList)
fn update_window(&mut self, window: u64)
```

## Query

```rust
fn query_interval_merge(&self, t1: u64, t2: u64) -> Option<EHSketchList>
fn cover(&self, mint: u64, maxt: u64) -> bool
fn get_min_time(&self) -> Option<u64>
fn get_max_time(&self) -> Option<u64>
fn bucket_count(&self) -> usize
fn get_memory_info(&self) -> (usize, Vec<usize>)
```

## Merge

Managed internally during bucket compaction.

## Serialization

No dedicated serialization API.

## Examples

```rust
use sketchlib_rust::{CountMin, EHSketchList, ExponentialHistogram, FastPath, SketchInput, Vector2D};

let template = EHSketchList::CM(CountMin::<Vector2D<i32>, FastPath>::default());
let mut eh = ExponentialHistogram::new(3, 120, template);
eh.update(10, &SketchInput::Str("flow"));
let _ = eh.query_interval_merge(0, 120);
```

## Caveats

- Payload behavior depends on selected `EHSketchList` variant.

## Status

Ready sliding-window coordinator.
