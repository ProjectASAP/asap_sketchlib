# Message Pack Format

`src/message_pack_format/` is the Rust-side source of truth for the
MessagePack encode/decode contract. It is split into two sub-modules
by audience:

- **`portable/`** — cross-language wire format shared with the Go
  counterpart `sketchlib-go`. Touching anything here is a protocol
  change and requires the Go side to be kept in lock-step (golden-byte
  tests catch drift).
- **`native/`** — thin trait shims over the existing
  `serialize_to_bytes` / `deserialize_from_bytes` methods on the
  pure-Rust generic sketch types in [`src/sketches/`](./api/). The byte
  format is internal to Rust — Go never reads it, and the format is
  free to evolve without cross-language coordination.

The [`MessagePackCodec`](#core-types) trait and unified `Error` type
live at the module root so both worlds share the same encode/decode
contract.

## Core Types

Both live at the top of `src/message_pack_format/` and are re-exported
through the module root:

- `MessagePackCodec` (in [`codec.rs`](../src/message_pack_format/codec.rs)) —
  the trait every codec-enabled type implements. Two methods:
  `to_msgpack`, `from_msgpack`. This is the canonical encode/decode
  entry point.
- `Error` (in [`error.rs`](../src/message_pack_format/error.rs)) — the
  unified encode/decode error type returned by both `native` and
  `portable` impls.

## `portable/` — Cross-Language Wire Format

One submodule per algorithm, with the filenames mirrored on the Go
side:

- `countminsketch.rs`, `countminsketch_topk.rs`, `countsketch.rs`,
  `ddsketch.rs`, `hll.rs`, `kll.rs`, `hydra_kll.rs`,
  `set_aggregator.rs`, `delta_set_aggregator.rs`

Each submodule owns:

1. The wire-format-aligned runtime type and its delta companion (e.g.
   `CountMinSketch`, `CountMinSketchDelta`) — these are the types
   re-exported at the crate root.
2. The wire DTO struct(s), when the runtime type needs a separate
   over-the-wire shape (e.g. borrow / owned pairs, byte-compatible
   field reordering with `sketchlib-go`).
3. The `MessagePackCodec` impl for the runtime type.

The full list of re-exported wire-format-aligned types:

- `countminsketch.rs` — `CountMinSketch`, `CountMinSketchDelta`
- `countminsketch_topk.rs` — `CountMinSketchWithHeap`, `CmsHeapItem`
- `countsketch.rs` — `CountSketch`, `CountSketchDelta`
- `ddsketch.rs` — `DdSketch`, `DdSketchDelta`
- `hll.rs` — `HllSketch`, `HllSketchDelta`, `HllVariant`
- `kll.rs` — `KllSketch`, `KllSketchData`
- `hydra_kll.rs` — `HydraKllSketch`
- `set_aggregator.rs` — `SetAggregator`
- `delta_set_aggregator.rs` — `DeltaResult`

Use these for sketches that must cross a process / language boundary.
For high-throughput local ingest, custom hashers, and framework
composition, reach for the generic sketches in
[`src/sketches/`](./apis.md) instead:

| Need | Use |
| --- | --- |
| Local high-throughput ingest, custom hashers, framework composition | [`src/sketches/`](./apis.md) |
| Cross-process / cross-language transfer matching `sketchlib-go` bytes | `src/message_pack_format/portable/` |

### Types that act as their own DTO

`CountSketch`, `DdSketch`, and `HllSketch` derive `Serialize` /
`Deserialize` directly because their public field layout already
matches the wire shape. Their `MessagePackCodec` impls serialize the
struct verbatim — no separate DTO is required.

### Protocol invariants

- The wire envelope must remain byte-compatible with `sketchlib-go`.
- Adding, reordering, renaming, or retyping a field counts as a
  protocol change; bump the format version on both sides and add a
  golden-byte test before shipping.
- DTOs that appear as nested fields in another wire type (e.g.
  `KllSketchData` inside `HydraKllSketchWire`) are part of the same
  protocol surface — treat them with the same care.

## `native/` — Rust-Internal Codec Shims

One submodule per generic sketch type in [`src/sketches/`](./api/)
whose serialization is exposed through `MessagePackCodec`:

- `countminsketch.rs`, `countsketch.rs`, `countsketch_topk.rs`,
  `ddsketch.rs`, `hll.rs`, `kll.rs`, `kll_dynamic.rs`
- `kmv.rs` (gated behind the `experimental` feature flag)

Each impl forwards `to_msgpack` / `from_msgpack` to the sketch's
existing `serialize_to_bytes` / `deserialize_from_bytes` methods. The
byte format is **not** part of the cross-language protocol — it is an
internal Rust serialization that can evolve freely.

Use the native codecs when you want a single unified trait-based
encode/decode entry point for the generic in-process sketch types,
without going through a wire-format-aligned wrapper.

## Choosing Between `portable` and `native`

| Need | Use |
|------|-----|
| Send a sketch to Go (`sketchlib-go`) or any non-Rust consumer | `portable` (the wire-format-aligned types re-exported at the crate root) |
| Persist or transport a sketch within an all-Rust pipeline | `native` (works directly on the generic [`sketches`](./api/) types) |
| New sketch crossing the wire | Add a `portable/<name>.rs`, mirror the filename in `sketchlib-go`, and add a golden-byte test |
| New internal-only sketch serialization | Add a `native/<name>.rs` shim and you are done |

## Cross-Reference

- Generated rustdoc for the trait and per-algorithm wire DTOs is the
  most up-to-date reference; build it with
  `cargo doc --no-deps --all-features --open`.
