# Message Pack Format

`src/message_pack_format/` is the Rust-side source of truth for the
MessagePack wire format shared with the Go counterpart `sketchlib-go`.
Both implementations are kept byte-compatible at the envelope level
even though the in-language struct shapes differ.

This module describes the wire types and the encode/decode contract.
The actual sketch types live in [`src/wrapper/`](./wrapper.md).

## Core Types

- `MessagePackCodec` (in `codec.rs`) — the trait every wrapper sketch
  implements. Two methods: `to_msgpack`, `from_msgpack`. This is the
  canonical encode/decode entry point.
- `Error` (in `error.rs`) — the unified encode/decode error type.

## Layout

One submodule per wrapper file, named to mirror `src/wrapper/`:

- `countminsketch.rs`, `countminsketch_topk.rs`, `countsketch.rs`,
  `ddsketch.rs`, `hll.rs`, `kll.rs`, `hydra_kll.rs`,
  `set_aggregator.rs`, `delta_set_aggregator.rs`

Each submodule owns:

1. The wire DTO struct(s), when the wrapper needs a separate
   over-the-wire shape (e.g. borrow / owned pairs, byte-compatible
   field reordering with `sketchlib-go`).
2. The `MessagePackCodec` impl for the matching wrapper type.

### Wrappers that act as their own DTO

`CountSketch`, `DdSketch`, and `HllSketch` derive `Serialize` /
`Deserialize` directly because their public field layout already
matches the wire shape. Their `MessagePackCodec` impls serialize the
wrapper verbatim — no separate DTO is required.

## Cross-Reference

- Wrapper types and how to choose between in-process vs wire variants:
  [Wrapper Module](./wrapper.md)
- Generated rustdoc for the trait and per-algorithm wire DTOs is the
  most up-to-date reference; build it with
  `cargo doc --no-deps --all-features --open`.
