# Wrapper Module

`src/wrapper/` holds wire-format-aligned sketch types consumed by the
ASAP query engine. They mirror the in-process sketches in
[`src/sketches/`](./library_map.md) but expose public-field,
proto-decode-friendly shapes that are byte-compatible with the Go
counterpart `sketchlib-go`.

Use the in-process sketches under `src/sketches/` for high-throughput
local ingest, and the wrapper variants when a sketch must cross a
process / language boundary.

## Layout

One file per algorithm. Each file owns the wrapper struct, its delta
companion (where applicable), and any helpers needed to bridge to the
in-process implementation:

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

Each wrapper type implements
[`MessagePackCodec`](./message_pack_format.md) and exposes thin
inherent shims (`serialize_msgpack` / `deserialize_msgpack`) for
backwards compatibility. The on-the-wire shape is described
per-algorithm in [`src/message_pack_format/`](./message_pack_format.md).

## In-Process vs Wrapper

| Need | Use |
| --- | --- |
| Local high-throughput ingest, custom hashers, framework composition | [`src/sketches/`](./apis.md) |
| Cross-process / cross-language transfer matching `sketchlib-go` bytes | `src/wrapper/` |

For the underlying algorithms and per-sketch APIs, see the dedicated
pages under [APIs Index](./apis.md). For the wire envelope itself,
see [Message Pack Format](./message_pack_format.md).
