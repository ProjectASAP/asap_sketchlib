# Sketchlib-rs Feature Overview

This note captures the current surface area of the crate and the rough roadmap. It complements the README by spelling out what is stable, what is mid-flight, and where we plan to invest next.

## What Works Today

- **Shared input primitives** — `SketchInput` wraps the standard value shapes (integers, floats, strings, byte slices) so every sketch speaks the same language. `hash_it` centralises hashing, and `hash_for_all_rows` derives per-row indices from one seed. The fast paths allocate a `Vec` today, so we still profile them, but they demonstrate our hash-reuse strategy.

- **Core sketch suite** — Count-Min, Count/CountUniv, Coco, Elastic, multiple HyperLogLog variants, KLL, Locher, Microscope, and UnivMon ship with serde support and reference binaries in `src/bin/sketch_tester`.

- **Sketchbook orchestration** — `Chapter` unifies insert/query across sketches, enabling higher-level runners like `Hydra` (label combinations) and `ExponentialHistogram` (sliding windows); `UnivMon` that can takes in `L2HH` sketch, although `L2HH` sketch currently only have `CountSketch` as the instance

- **Structured sketches** — `structured::CountMin` and `structured::HyperLogLog` build on `SketchMatrix`/`SketchList`, so new sketches can share storage code instead of re-rolling `Vec<Vec<_>>`; also added `Vector1D`, `Vector2D`, `Vector3D`, targeting for performance.

- **Benchmark scaffolding** — Criterion benches (`cargo bench --bench structured_countmin`) compare `insert` vs `fast_insert` and `estimate` vs `fast_estimate`. Latest runs show fast insert now beating the baseline, while fast estimate holds steady or improves when hash reuse matters.

## WIP & Known Gaps

- **Bring UnivMon into sketchbook** ~~— evolving UnivMon into a “serving algorithm” alongside Hydra/ExponentialHistogram so Chapter can drive it directly. (**First Priority**)~~ Added

- **Performance parity checks** — structured Count-Min fast paths are positive, but other structured sketches need profiling versus their legacy counterparts; `SketchMatrix` and `Vector2D` performance showing mysterious output
  - insertion: `Vector2D` is much faster
  - estimation: `Vector2D` is ~~much~~ slower
  - **GiveUp**: assume insertion is supposed to happen more often than estimation, will keep it as what it is

- **Sketchbook ergonomics** — public APIs still settle; expect naming/shape changes as we converge on a consistent surface.

- **Serialization coverage** — several sketches (structured variants, Elastic merge states) still need first-party serializer/deserializer helpers.

- **Testing depth** — structured sketches and Chapter composition have automated coverage; older sketches rely on manual binaries. We need more property/accuracy tests and CI enforcement.

- **Docs & examples** — README currently showcases Count-Min and Exponential Histogram only. Inline comments and additional runnable snippets are on the todo list.

- **Parity across variants** — structured Count-Min still trails the legacy module for helpers like merge/debug. We must either match functionality or deprecate the old path.

## Feature Explanation

### Manually Implemented Hash

Provide native xxhash algorithm

## Future Directions

- Add **OctoSketch** as another sketch-serving coordinator.
- Ship **full serializer/deserializer coverage** across all sketches, including structured variants.
- Generalise the **KLL** implementation for broader accuracy/space trade-offs.
- Prototype a **NitroSketch-style sampling layer** (research stage).
- Allow custom types (`<T>`) to implement **`SketchInput`** for smoother ingestion.
- Expand automated **testing** so every sketch has functional and accuracy checks.
- Grow the **benchmark catalogue** with Zipfian streams, heavy hitter mixes, and quantile accuracy sweeps.
- Provide **distributed-friendly serialization** with schema versioning plus bindings for Python/Java/Go.
- Continue **structured sketch migration**
- Investigate **cache-aware hashing helpers** that avoid repeated allocations while reusing hash results.

Ideas welcome—open an issue or amend this document when priorities change.
