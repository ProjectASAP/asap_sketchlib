# Feature Status

This document provides a high-level overview of implemented and planned features in sketchlib-rust. For detailed API documentation, see [apis.md](apis.md) and [api_common.md](./api/api_common.md).

---

## Table of Contents

1. [Completed Features](#completed-features)
2. [In Progress](#in-progress)
3. [Planned Features](#planned-features)
4. [Research & Experimental](#research--experimental)

---

## Completed Features

### Core Infrastructure

✅ **Common API** ([api_common.md](./api/api_common.md))

- `SketchInput` - Unified type system for all sketches
- `Vector1D`, `Vector2D`, `Vector3D` - High-performance storage structures
- `MatrixStorage` + `FixedMatrix` - Fixed 5 × 2048 backing for quickstart CMS/CS
- `CommonHeap` & `HHHeap` - Generic and specialized heaps for heavy hitter tracking
- Deterministic hashing with seed management
- `SketchHasher` trait with `DefaultXxHasher` for pluggable hash implementations
- `RegularPath` / `FastPath` modes - Type-level pairing of insert/estimate paths

✅ **Ready Sketch APIs** ([apis.md](apis.md))

- **CountMin** - Frequency estimation with fast paths (2-3x speedup)
- **Count & CountL2HH** - Count sketch with L2 heavy hitter support
- **HyperLogLog** - Three variants (Regular, DataFusion, HIP) for cardinality estimation
- **DDSketch** - Quantile estimation with relative error guarantees
- **KLL** - Quantile estimation with mergeable compactors
- All built on optimized common structures

### Frameworks

✅ **Hydra** - Hierarchical heavy hitters for multi-dimensional queries ([apis.md](apis.md))

- Includes `MultiHeadHydra` for per-dimension counter sets

✅ **UnivMon** - Universal monitoring (L1, L2, entropy, cardinality from single structure) ([apis.md](apis.md))

✅ **HashLayer** - Hash-once-use-many pattern for coordinating multiple sketches with single hash computation

✅ **NitroBatch** - Batch-mode sampling wrapper for CMS/Count FastPath

✅ **EHSketchList** - Unified sketch enum for insert/merge/query across sketch types

✅ **ExponentialHistogram** - Sliding window coordinator for mergeable sketches

⚠️ **EHUnivOptimized** - Hybrid two-tier ExponentialHistogram for UnivMon with sketch memory reuse (currently `Unstable` in [apis.md](apis.md))

✅ **UnivMonPyramid** - Two-tier sketch dimensions with `UnivSketchPool` for optimized insert and memory management

✅ **FoldCMS** - Folding Count-Min Sketch for memory-efficient sub-window aggregation ([fold_sketch_design.md](fold_sketch_design.md))

✅ **FoldCS** - Folding Count Sketch for memory-efficient sub-window aggregation with signed counters and median query ([fold_sketch_design.md](fold_sketch_design.md))

✅ **Orchestrator** - Node-level manager for sketches and frameworks (EH/HashLayer/Nitro)

### Performance Optimizations

✅ **Fast-path methods** - Hash reuse with bit-masking

- `FastPath` mode uses a single hash across rows
- `Vector2D::fast_query_*` uses bit-sliced row selection
- `_with_hash_value` helpers enable cross-sketch hash reuse
- `HashLayer` + `OrchestratedSketch` reuse hashes across sketch collections

✅ **Flat memory layouts** - Cache-friendly row-major storage

✅ **Zero-copy operations** - Direct slice access, borrowed lifetimes

- **TODO**: requires more benchmark

### Sampling

✅ **Nitro sampling** - Streaming Nitro (`Vector2D`) and batch Nitro (`NitroBatch`)

---

## In Progress

### Infrastructure

🚧 **Serialization** - MessagePack (serde) support for most sketches

- **TODO**: requires further testing and need better integrated support

### Performance

🚧 **Performance parity for structured sketches**

- Requires more benchmark on different architectures / machines

### Testing

🚧 **Automated test coverage**

- Needs more unit test
- Needs strict **correctness** test

### Documentation

🚧 **API documentation expansion**

- ✅ `apis.md` - Canonical API index
- ✅ `api/api_common.md` - Complete
- ⚠️ Inline code comments - Partial

### Serialization

🚧 **Full serialization coverage**

- Most sketches supported
- Missing: Some structured variants, Elastic merge states
- Built-in serialization / deserialization function wanted

### API Stability

🚧 **Sketchbook ergonomics**

- Public APIs still evolving
- Naming and structure may change
- EHSketchList/Hydra/UnivMon interfaces stabilizing

---

## Planned Features

### Performance Optimization

📋 **SIMD support**

- Vector operations for counter updates (AVX2/NEON)
- [TODO: Investigate Rust SIMD support and sketch compatibility]

📋 **Custom hash functions**

- Native xxhash algorithm implementation
- Bit-selective hashing: Generate only required bits (e.g., 32-bit instead of 128-bit)
- Goal: Faster hashing when full 128-bit output isn't needed

📋 **Hash reuse extensions**

- Broader hash-domain reuse across more sketch families
- Optional shared hash caches for Hydra/MultiHeadHydra updates

📋 **Prefetching hints**

- Explicit memory prefetch for large sketches
- Improve cache hit rates

### Algorithm Improvements

📋 **Custom RNG for KLL**

- Fast coin-flipping random number generator
- Optimized for KLL compactor operations
- [TODO: Define performance requirements]

📋 **Generic type support for SketchInput**

- Allow custom types `T` to implement `SketchInput`
- Challenges: Trait requirements, lifetime management

📋 **KLL generalization**

- Broader accuracy/space trade-offs
- Enhanced quantile query capabilities

### Framework Enhancements

📋 **OctoSketch coordinator**

- Alternative sketch-serving framework
- [TODO: Define use cases and differences from Hydra/UnivMon]

📋 **Nitro sampling refinements**

- Tighter accuracy/throughput tuning
- Additional sampling tables beyond the 1% preset

### Testing & Quality

📋 **Comprehensive test suite**

- Property tests for all sketches
- Accuracy validation tests
- Heavy hitter detection tests
- Quantile accuracy sweeps

📋 **Benchmark expansion**

- Zipfian distribution workloads
- Heavy hitter mix scenarios
- Cardinality estimation speed
- Query latency percentiles

### Cross Languages support

📋 **Cross Language Usage**

- Serialization needs cross language support

### Migration & Cleanup

📋 **Structured sketch migration**

- Complete migration of legacy sketches to common structures
- Deprecate or remove old implementations
- Achieve API parity (merge, debug, etc.)

---

## Research & Experimental

### Explored But Not Implemented

💡 **Extra hash layer location**

- Where to inject hash value coordination?
- **Data plane** vs **control plane** separation unclear
- Needs design iteration

💡 **Data/control plane separation**

- Current API doesn't clearly separate concerns
- May impact performance optimization opportunities
