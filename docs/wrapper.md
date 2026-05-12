# Wire-Format-Aligned Sketches

> **Note:** The standalone `src/wrapper/` module has been removed. The
> wire-format-aligned sketch types it used to expose now live alongside
> their wire DTOs in
> [`src/message_pack_format/portable/`](./message_pack_format.md). The
> top-level re-exports (`CountMinSketch`, `CountSketch`, `DdSketch`,
> `HllSketch`, `KllSketch`, `HydraKllSketch`, `CountMinSketchWithHeap`,
> `SetAggregator`, `DeltaResult`, …) are unchanged — only the source
> location moved.

These types are byte-compatible with the Go counterpart `sketchlib-go`
and are intended for sketches that must cross a process / language
boundary. For high-throughput local ingest, custom hashers, and
framework composition, use the generic sketches in
[`src/sketches/`](./apis.md) instead.

## Layout

One file per algorithm, under `src/message_pack_format/portable/`. Each
file owns the runtime type, its delta companion (where applicable), the
wire DTO (when a separate over-the-wire shape is needed), and the
`MessagePackCodec` impl:

- `countminsketch.rs` — `CountMinSketch`, `CountMinSketchDelta`
- `countminsketch_topk.rs` — `CountMinSketchWithHeap`, `CmsHeapItem`
- `countsketch.rs` — `CountSketch`, `CountSketchDelta`
- `ddsketch.rs` — `DdSketch`, `DdSketchDelta`
- `hll.rs` — `HllSketch`, `HllSketchDelta`, `HllVariant`
- `kll.rs` — `KllSketch`, `KllSketchData`
- `hydra_kll.rs` — `HydraKllSketch`
- `set_aggregator.rs` — `SetAggregator`
- `delta_set_aggregator.rs` — `DeltaResult`

## Serialization

Each type implements
[`MessagePackCodec`](./message_pack_format.md), the single entry point
for encode/decode (`to_msgpack` / `from_msgpack`). The on-the-wire
shape is described per-algorithm in
[`src/message_pack_format/portable/`](./message_pack_format.md).

## Generic In-Process vs Wire-Aligned

| Need | Use |
| --- | --- |
| Local high-throughput ingest, custom hashers, framework composition | [`src/sketches/`](./apis.md) |
| Cross-process / cross-language transfer matching `sketchlib-go` bytes | `src/message_pack_format/portable/` |

For the underlying algorithms and per-sketch APIs, see the dedicated
pages under [APIs Index](./apis.md). For the wire envelope itself, see
[Message Pack Format](./message_pack_format.md).
