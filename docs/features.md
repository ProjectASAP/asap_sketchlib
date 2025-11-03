# Feature Status

This document provides a high-level overview of implemented and planned features in sketchlib-rust. For detailed API documentation, see [sketch_api.md](sketch_api.md) and [common_api.md](common_api.md).

---

## Table of Contents

1. [Completed Features](#completed-features)
2. [In Progress](#in-progress)
3. [Planned Features](#planned-features)
4. [Research & Experimental](#research--experimental)

---

## Completed Features

### Core Infrastructure

✅ **Common API** ([common_api.md](common_api.md))

- `SketchInput` - Unified type system for all sketches
- `Vector1D`, `Vector2D`, `Vector3D` - High-performance storage structures
- `CommonHeap` & `HHHeap` - Generic and specialized heaps for heavy hitter tracking
- Deterministic hashing with seed management

✅ **Recommended Sketches** ([sketch_api.md](sketch_api.md))

- **CountMin** - Frequency estimation with fast paths (2-3x speedup)
- **Count & CountL2HH** - Count sketch with L2 heavy hitter support
- **HyperLogLog** - Three variants (Original, HllDf, HllDs) for cardinality estimation
- All built on optimized common structures

### Frameworks

✅ **Hydra** - Hierarchical heavy hitters for multi-dimensional queries ([sketch_api.md](sketch_api.md))

✅ **UnivMon** - Universal monitoring (L1, L2, entropy, cardinality from single structure) ([sketch_api.md](sketch_api.md))

✅ **HashLayer** - Hash-once-use-many pattern for coordinating multiple sketches with single hash computation

### Performance Optimizations

✅ **Fast-path methods** - Hash reuse with bit-masking

- `fast_insert()` - check [benchmark](benchmark.md) for detail
- `fast_query_min()` / `fast_query_median()` - check [benchmark](benchmark.md) for detail
- Single hash computation across multiple rows

✅ **Flat memory layouts** - Cache-friendly row-major storage

✅ **Zero-copy operations** - Direct slice access, borrowed lifetimes

- **TODO**: requires more benchmark

---

## In Progress

### Infrastructure

🚧 **Serialization** - MessagePack (serde) support for most sketches

- **TODO**: requires further testing and need better integrated support

🚧 **Benchmarking** - Criterion-based performance suite (`cargo bench`)

### Performance

🚧 **Performance parity for structured sketches**

- Requires more benchmark on different architectures / machines

### Testing

🚧 **Automated test coverage**

- Needs more unit test
- Needs strict **correcness** test

### Documentation

🚧 **API documentation expansion**

- ✅ `sketch_api.md` - Complete
- ✅ `common_api.md` - Complete
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
- Chapter/Hydra/UnivMon interfaces stabilizing

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

📋 **Cache-aware hashing**

- Eliminate repeated allocations
- Reuse hash results across multiple sketch operations
- Hash once, use across framework of sketches

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

📋 **NitroSketch-style sampling**

- Research stage
- Sampling layer for sketch acceleration
- [TODO: Define geometry sampling use case]

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
