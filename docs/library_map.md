# Library Map

Migrated from `docs/readme_details.md`.

## Library Map

### Core Modules

- **`src/common/`** - Foundation for all sketches ([common_api.md](./common_api.md))
  - `input.rs` - `SketchInput` enum, `HeapItem`, `HHItem`, framework enums (`HydraCounter`, `L2HH`, `HydraQuery`)
  - `structures/` - High-performance data structures (`Vector1D`, `Vector2D`, `Vector3D`, `CommonHeap`, `MatrixStorage`, `FixedMatrix`)
  - `heap.rs` - `HHHeap` convenience wrapper for heavy hitter tracking
  - `hash.rs` - Hashing utilities (`hash_for_matrix`, `hash64_seeded`, `SEEDLIST`, `BOTTOM_LAYER_FINDER`) plus `SketchHasher` for custom hasher injection
  - `mode.rs` is under `src/sketches/` and provides `RegularPath` / `FastPath` type-level insert/estimate path selection

- **`src/sketches/`** - Core sketch implementations ([sketch_api.md](./sketch_api.md))
  - **Recommended** (built on common structures): `countmin.rs`, `count.rs`, `hll.rs`
  - **Additional**: `ddsketch.rs`, `kll.rs`, `kmv.rs`
  - **Legacy**: `coco.rs`, `elastic.rs`, `uniform.rs`, `microscope.rs`, `locher.rs`

- **`src/sketch_framework/`** - Orchestration and serving layers
  - `eh_sketch_list.rs` - Unified interface (`EHSketchList` enum) wrapping all sketch types
  - `hashlayer.rs` - Hash-once-use-many optimization for multiple sketches
  - `hydra.rs` - Multi-dimensional hierarchical heavy hitters (includes `MultiHeadHydra`)
  - `univmon.rs` - Universal monitoring (L1, L2, entropy, cardinality)
  - `univmon_optimized.rs` - `UnivMonPyramid` and `UnivSketchPool` for two-tier sketch dimensions
  - `eh.rs` - Exponential histogram for sliding window queries
  - `eh_univ_optimized.rs` - `EHUnivOptimized` for optimized EH+UnivMon combination
  - `nitro.rs` - `NitroBatch` batch-mode sampling wrapper
  - `orchestrator/` - Node-level manager for sketches and frameworks (EH/HashLayer/Nitro nodes)

### Testing & Benchmarking

- **`benches/`** - Criterion-based performance benchmarks
  - Run with: `cargo bench`

### Documentation

- **`docs/`** - API and feature documentation
  - [sketch_api.md](./sketch_api.md) - Complete sketch API reference with usage examples
  - [common_api.md](./common_api.md) - Data structures and shared utilities
  - [features.md](./features.md) - Feature status and roadmap

### Utilities

- **`src/bin/`** - Helper binaries for generating precomputed fixtures (`generate_precomputed_hash`, `generate_precomputed_sample`, `generate_precomputed_sample2`)

