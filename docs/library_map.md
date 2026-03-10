# Library Map

## Library Map

### Core Modules

- **`src/common/`** - Foundation for all sketches ([api_common.md](./api/api_common.md))
  - `input.rs` - `SketchInput` enum, `HeapItem`, `HHItem`, framework enums (`HydraCounter`, `L2HH`, `HydraQuery`)
  - `structures/` - High-performance data structures (`Vector1D`, `Vector2D`, `Vector3D`, `CommonHeap`, `MatrixStorage`, `FixedMatrix`)
  - `heap.rs` - `HHHeap` convenience wrapper for heavy hitter tracking
  - `hash.rs` - Hashing utilities (`hash_for_matrix`, `hash64_seeded`, `SEEDLIST`, `BOTTOM_LAYER_FINDER`) plus `SketchHasher` for custom hasher injection
  - `mode.rs` is under `src/sketches/` and provides `RegularPath` / `FastPath` type-level insert/estimate path selection

- **`src/sketches/`** - Sketch implementations (status source: [apis.md](./apis.md))
  - `Ready` in API index: `countmin.rs`, `count.rs`, `hll.rs`, `kll.rs`, `ddsketch.rs`, `fold_cms.rs`, `fold_cs.rs`, `cms_heap.rs`, `cs_heap.rs`
  - `Unstable` in API index: `coco.rs`, `elastic.rs`, `uniform.rs`, `kmv.rs`

- **`src/sketch_framework/`** - Orchestration and serving layers (status source: [apis.md](./apis.md))
  - `Ready` in API index: `hydra.rs`, `hashlayer.rs`, `univmon.rs`, `univmon_optimized.rs`, `nitro.rs`, `eh.rs`, `eh_sketch_list.rs`, `tumbling.rs`
  - `Unstable` in API index: `eh_univ_optimized.rs`
  - Infrastructure module: `orchestrator/` (node-level manager used by framework APIs)

### Testing & Benchmarking

- **`benches/`** - Criterion-based performance benchmarks
  - Run with: `cargo bench`

### Documentation

- **`docs/`** - API and feature documentation
  - [apis.md](./apis.md) - Canonical API index with one page per API surface
  - [api_common.md](./api/api_common.md) - Common module canonical reference
  - [features.md](./features.md) - Feature status and roadmap

### Utilities

- **`src/bin/`** - Helper binaries for generating precomputed fixtures (`generate_precomputed_hash`, `generate_precomputed_sample`, `generate_precomputed_sample2`)
