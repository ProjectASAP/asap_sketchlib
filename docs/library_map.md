# Library Map

## Core Modules

- **`src/common/`** - Foundation for all sketches ([api_common.md](./api/api_common.md))
  - `input.rs` - `DataInput` enum, `OwnedDataInput`, `HeapEntry`, framework enums (`HydraCounter`, `HydraQuery`)
  - `structures/` - High-performance data structures (`Vector1D`, `Vector2D`, `Vector3D`, `BitMatrix`, `CommonHeap`, `MatrixStorage`, `FixedMatrix`)
  - `heap.rs` - `TopKHeap` convenience wrapper for heavy hitter tracking
  - `hash.rs` - Hashing utilities (`hash_for_matrix`, `hash64_seeded`, `SEED_LIST`, `UNIVMON_BOTTOM_LAYER_SEED_INDEX`) plus `SketchHasher` for custom hasher injection and `DigestHasher` / `DigestBuildHasher` for maps keyed by an already-hashed digest
  - `numerical.rs` - `NumericalValue`; `structure_utils.rs` - `Nitro`, `NitroContext`, `ToF64`

- **`src/sketches/`** - Sketch implementations (status source: [apis.md](./apis.md))
  - `Ready` in API index: `countminsketch.rs`, `countsketch.rs`, `hll.rs`, `kll.rs`, `ddsketch.rs`, `countminsketch_heap.rs`, `countsketch_heap.rs`, `countsketch_l2_norm.rs`, `countminsketch_space_saving.rs`, `countsketch_space_saving.rs`, `space_saving.rs`, `bloom.rs`, `elastic.rs`, `coco.rs`
  - `Unstable` in API index: `uniform.rs`, `kmv.rs`
  - Not in the API index: `count_min_fold.rs`, `count_sketch_fold.rs`, `kll_dynamic.rs`, `octo_delta.rs`, `set_aggregator.rs` (`SetAggregator` / `SetAggregatorDelta`), `mode.rs` (`RegularPath` / `FastPath`)
  - A sketch on the ASAPv1 wire format uses a `<sketch>.rs` (algorithm) + `<sketch>/wire.rs` (serialization) split: `hll.rs`, `countminsketch.rs`, `countminsketch_heap.rs`, `countsketch.rs`, `countsketch_heap.rs`, `countsketch_l2_norm.rs`, `countminsketch_space_saving.rs`, `countsketch_space_saving.rs`, `kll.rs`, `kll_dynamic.rs`, `ddsketch.rs`, `elastic.rs`, `coco.rs`, `uniform.rs`, `kmv.rs`, `bloom.rs`, `space_saving.rs` and `set_aggregator.rs` (see [asapv1_wire_format.md](./asapv1_wire_format.md))
  - Two of those directories carry a second, shared wire module: `countminsketch_heap/heap_wire.rs` holds the top-k heap encoding CountMinSketchWithHeap and CountSketchWithHeap share, and `countminsketch_space_saving/summary_wire.rs` holds the encoding CountMinSketchWithSpaceSaving and CountSketchWithSpaceSaving share. `countsketch_l2_norm/wire.rs` also holds the layer sub-payload the UnivMon family inlines

- **`src/sketch_framework/`** - Orchestration and serving layers (status source: [apis.md](./apis.md))
  - `Ready` in API index: `hydra.rs`, `hashlayer.rs`, `univmon.rs`, `univmon_two_tier.rs`, `nitro.rs`, `eh.rs`, `eh_bucket_sketch.rs`, `octo.rs`
  - `Experimental` in API index: `univmon_q.rs`
  - `Unstable` in API index: `eh_univmon.rs`
  - Not in the API index: `tumbling.rs`, `sketch_catalog.rs`
  - The same `<framework>.rs` + `<framework>/wire.rs` split carries the ASAPv1 wire format for `hydra.rs`, `univmon.rs`, `univmon_two_tier.rs`, `univmon_q.rs`, `eh.rs` and `eh_bucket_sketch.rs`

- **`src/asapv1/`** (private) - Serialization plumbing ([message_pack_format.md](./message_pack_format.md)). The format is **ASAPv1**, specified in [asapv1_wire_format.md](./asapv1_wire_format.md)
  - `envelope.rs` — the shared, sketch-agnostic ASAPv1 framing (magic/version/`kind_id` + length prefixes, `encode`/`split`); every `wire.rs` under `src/sketches/` and `src/sketch_framework/` calls into it
  - `kind.rs` — the `kind_id` registry constants

## Documentation

- **`docs/`** - API and feature documentation
  - [apis.md](./apis.md) - Canonical API index with one page per API surface
  - [api_common.md](./api/api_common.md) - Common module canonical reference
  - [features.md](./features.md) - Feature status and roadmap

## Utilities

- The large precomputed hash/sample tables are built lazily at runtime via
  `std::sync::LazyLock` in
  `src/common/precompute_hash.rs` and `src/common/precompute_sample.rs`.
