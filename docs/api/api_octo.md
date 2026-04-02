# API: OctoSketch

Status: `Ready`

## Purpose

Delta-promotion primitives for multi-threaded sketch updates, based on
the OctoSketch architecture (NSDI 2024). Child sketches maintain small
counters and emit compact deltas when a promotion threshold is reached;
a parent sketch absorbs those deltas at full precision.

### Two Usage Levels

- **Low-level (this page)**: Call `insert_emit_delta` / `apply_delta`
  directly on sketch structs. You manage threads, channels, and
  scheduling yourself.
- **Turnkey runtime**: Use `OctoRuntime` or `run_octo` for a ready-made
  multi-threaded pipeline. See [Runtime API](#runtime-api) at the bottom.

## Delta Types

Defined in `src/sketches/octo_delta.rs`.

```rust
pub struct CmDelta {
    pub row: u16,
    pub col: u16,
    pub value: u8,
}

pub struct CountDelta {
    pub row: u16,
    pub col: u16,
    pub value: i8,
}

pub struct HllDelta {
    pub pos: u16,
    pub value: u8,
}
```

### Promotion Thresholds

| Sketch | Constant | Value | Trigger |
| --- | --- | --- | --- |
| CountMin | `CM_PROMASK` | `0x1f` (31) | Emit when counter reaches a multiple of `CM_PROMASK` |
| Count | `COUNT_PROMASK` | `0x1f` (31) | Emit when `\|counter\| >= COUNT_PROMASK` |
| HyperLogLog | `HLL_PROMASK` | `0` | Emit on every register improvement |

## CountMin Delta API

Available on `CountMin<S, RegularPath, H>` and `CountMin<S, FastPath, H>`
where `S::Counter = i32`.

```rust
fn insert_emit_delta(&mut self, value: &SketchInput, emit: &mut impl FnMut(CmDelta))
fn apply_delta(&mut self, delta: CmDelta)
```

`insert_emit_delta` inserts a key and calls `emit` with one `CmDelta` per
row when the row counter reaches a multiple of `CM_PROMASK`. The child
counter keeps running; the delta carries the threshold value.

`apply_delta` increments the parent counter at `(row, col)` by
`delta.value`.

### CountMin Delta Example

```rust
use sketchlib_rust::{CountMin, RegularPath, Vector2D, SketchInput};
use sketchlib_rust::octo_delta::CmDelta;

let mut child = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(3, 4096);
let mut parent = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(3, 4096);

let key = SketchInput::U64(42);
child.insert_emit_delta(&key, &mut |delta: CmDelta| {
    parent.apply_delta(delta);
});
```

## Count Sketch Delta API

Available on `Count<S, RegularPath, H>` and `Count<S, FastPath, H>`
where `S::Counter = i32`.

```rust
fn insert_emit_delta(&mut self, value: &SketchInput, emit: &mut impl FnMut(CountDelta))
fn apply_delta(&mut self, delta: CountDelta)
```

`insert_emit_delta` inserts a key with its per-row sign and calls `emit`
when `|counter| >= COUNT_PROMASK`. The delta carries the signed counter
value. After emission, the child counter resets to zero.

`apply_delta` increments the parent counter at `(row, col)` by
`delta.value` (signed).

### Count Sketch Delta Example

```rust
use sketchlib_rust::{Count, RegularPath, Vector2D, SketchInput};
use sketchlib_rust::octo_delta::CountDelta;

let mut child = Count::<Vector2D<i32>, RegularPath>::with_dimensions(3, 4096);
let mut parent = Count::<Vector2D<i32>, RegularPath>::with_dimensions(3, 4096);

let key = SketchInput::U64(99);
child.insert_emit_delta(&key, &mut |delta: CountDelta| {
    parent.apply_delta(delta);
});
```

## HyperLogLog Delta API

Available on all `HyperLogLogImpl<Variant, Registers, H>` variants
(Regular, DataFusion, and any precision level).

```rust
fn insert_emit_delta(&mut self, obj: &SketchInput, emit: &mut impl FnMut(HllDelta))
fn insert_emit_delta_with_hash(&mut self, hashed_val: u64, emit: &mut impl FnMut(HllDelta))
fn apply_delta(&mut self, delta: HllDelta)
```

`insert_emit_delta` calls `emit` only when a register improves (the new
leading-zero count exceeds the stored value). Since `HLL_PROMASK = 0`,
every improvement is emitted immediately.

`apply_delta` applies a max-update: the parent register is set to
`max(current, delta.value)`.

### HyperLogLog Example

```rust
use sketchlib_rust::{HyperLogLog, Regular, SketchInput};
use sketchlib_rust::octo_delta::HllDelta;

let mut child = HyperLogLog::<Regular>::default();
let mut parent = HyperLogLog::<Regular>::default();

child.insert_emit_delta(&SketchInput::U64(1), &mut |delta: HllDelta| {
    parent.apply_delta(delta);
});
```

---

## Runtime API

For users who want a turnkey multi-threaded pipeline without managing
threads directly.

### OctoConfig

```rust
pub struct OctoConfig {
    pub num_workers: usize,      // default: 4
    pub pin_cores: bool,         // default: true
    pub queue_capacity: usize,   // default: 65536
}
```

### OctoRuntime (Streaming)

```rust
fn new<F, PF>(config: &OctoConfig, worker_factory: F, parent_factory: PF) -> Self
fn insert(&mut self, input: SketchInput<'_>)
fn insert_batch(&mut self, inputs: &[SketchInput<'_>])
fn read_handle(&self) -> OctoReadHandle<P>
fn close(&self)
fn finish(self) -> OctoResult<P>
```

### run_octo (Batch)

```rust
pub fn run_octo<W, P>(
    inputs: &[SketchInput<'_>],
    config: &OctoConfig,
    worker_factory: impl Fn(usize) -> W,
    parent_factory: impl FnOnce() -> P,
) -> OctoResult<P>
```

### Concrete Worker / Aggregator Pairs

| Sketch | Worker | Aggregator | Delta Type |
| --- | --- | --- | --- |
| CountMin | `CmOctoWorker` | `CmOctoAggregator` | `CmDelta` |
| Count | `CountOctoWorker` | `CountOctoAggregator` | `CountDelta` |
| HyperLogLog | `HllOctoWorker` | `HllOctoAggregator` | `HllDelta` |

## Caveats

- Child counters below the promotion threshold are lost when the child
  is dropped (slight under-count for CMS/Count; no loss for HLL since
  every improvement is emitted).
- Core pinning silently falls back if the platform has fewer cores than
  `num_workers + 1`.
- `insert` after `close` panics.

## Status

Core multi-threaded insertion framework; actively used and tested.
