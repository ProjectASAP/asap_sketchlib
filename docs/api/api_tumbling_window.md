# API: TumblingWindow

Status: `Recommended`

## Purpose

Fixed-size tumbling-window manager with sketch pooling.

## Type/Struct

- `TumblingWindow<S: TumblingWindowSketch>`
- `SketchPool<S>`
- Configs: `FoldCMSConfig`, `FoldCSConfig`, `KLLConfig`
- Trait: `TumblingWindowSketch`

## Constructors

```rust
fn new(window_size: u64, max_windows: usize, config: S::Config, pool_cap: usize) -> Self

// Pool
fn new(cap: usize, config: S::Config) -> Self
```

## Insert/Update

```rust
fn insert(&mut self, time: u64, key: &SketchInput, value: i64)
fn flush(&mut self, current_time: u64)
```

## Query

```rust
fn query_all(&self) -> S
fn query_recent(&self, n: usize) -> S
fn active_sketch(&self) -> &S
fn closed_count(&self) -> usize
fn pool_available(&self) -> usize
fn pool_total_allocated(&self) -> usize

// Specialized helpers
fn query_all_hierarchical(&self) -> FoldCMS
fn query_all_hierarchical(&self) -> FoldCS
```

## Merge

Window-level results are merged through sketch-specific logic.

## Serialization

No dedicated serialization API.

## Examples

```rust
use sketchlib_rust::{FoldCMSConfig, SketchInput, TumblingWindow};

let cfg = FoldCMSConfig { rows: 3, full_cols: 4096, fold_level: 4, top_k: 16 };
let mut tw = TumblingWindow::new(60, 8, cfg, 4);
tw.insert(1, &SketchInput::Str("k"), 1);
let _ = tw.query_recent(1);
```

## Caveats

- Generic type `S` determines query/merge semantics.
- `window_size` must be > 0.

## Status

Recommended window manager for folded sketches and KLL.
