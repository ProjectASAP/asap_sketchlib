# Library Map

## Core Modules

- **`src/common/`** - Foundation for all sketches ([api_common.md](./api/api_common.md))
  - `input.rs` - `DataInput` enum, `HeapItem`, `HHItem`, framework enums (`HydraCounter`, `L2HH`, `HydraQuery`)
  - `structures/` - High-performance data structures (`Vector1D`, `Vector2D`, `Vector3D`, `CommonHeap`, `MatrixStorage`, `FixedMatrix`)
  - `heap.rs` - `HHHeap` convenience wrapper for heavy hitter tracking
  - `hash.rs` - Hashing utilities (`hash_for_matrix`, `hash64_seeded`, `SEEDLIST`, `BOTTOM_LAYER_FINDER`) plus `SketchHasher` for custom hasher injection
  - `mode.rs` is under `src/sketches/` and provides `RegularPath` / `FastPath` type-level insert/estimate path selection

- **`src/sketches/`** - Sketch implementations (status source: [apis.md](./apis.md))
  - `Ready` in API index: `countminsketch.rs`, `countsketch.rs`, `hll.rs`, `kll.rs`, `ddsketch.rs`, `countminsketch_topk.rs`, `countsketch_topk.rs`
  - `Unstable` in API index: `coco.rs`, `elastic.rs`, `uniform.rs`, `kmv.rs`

- **`src/sketch_framework/`** - Orchestration and serving layers (status source: [apis.md](./apis.md))
  - `Ready` in API index: `hydra.rs`, `hashlayer.rs`, `univmon.rs`, `univmon_optimized.rs`, `nitro.rs`, `eh.rs`, `eh_sketch_list.rs`
  - `Unstable` in API index: `eh_univ_optimized.rs`
  - Infrastructure module: `orchestrator/` (node-level manager used by framework APIs)

- **`src/message_pack_format/`** - MessagePack wire-format definitions, the `MessagePackCodec` trait, and the wire-format-aligned sketch types shared with `sketchlib-go` ([message_pack_format.md](./message_pack_format.md))
  - `portable/` — wire-format-aligned sketch types (`CountMinSketch`, `CountSketch`, `DdSketch`, `HllSketch`, `KllSketch`, `HydraKllSketch`, `CountMinSketchWithHeap`, `SetAggregator`, `DeltaResult`) consumed by the ASAP query engine, byte-compatible with `sketchlib-go`
  - `native/` — Rust-internal codec shims over `src/sketches/` byte serialization

## Documentation

- **`docs/`** - API and feature documentation
  - [apis.md](./apis.md) - Canonical API index with one page per API surface
  - [api_common.md](./api/api_common.md) - Common module canonical reference
  - [features.md](./features.md) - Feature status and roadmap

## Utilities

- The large precomputed hash/sample tables are no longer checked-in arrays;
  they are built lazily at runtime via `std::sync::LazyLock` in
  `src/common/precompute_hash.rs`, `src/common/precompute_sample.rs`, and
  `src/common/precompute_sample2.rs`.

## Proto code generation

- `proto/**/*.proto` is the cross-language wire-format source of truth shared
  with `sketchlib-go`.
- The corresponding Rust types are **vendored** under
  `src/proto/generated/sketchlib.v1.rs` and re-exported by `src/proto.rs` as
  `crate::proto::sketchlib`. Downstream users therefore build the crate as
  pure Rust without needing `protoc` or any build script.
- To regenerate after editing any `.proto` file, run from the repository root:

  ```bash
  cargo run --manifest-path tools/gen-proto/Cargo.toml
  ```

  CI enforces that the committed file matches the result of regeneration.
