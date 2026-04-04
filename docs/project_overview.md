# Project Overview

`ASAPSketchLib` is a Rust sketch library for approximate streaming analytics.
It provides reusable data-structure building blocks, production-focused sketch
implementations, and orchestration/windowing frameworks in one crate.

## What This Repo Is

- A shared common layer for input types, hashing, matrix/heap structures, and utilities.
- A set of core sketch implementations (frequency, cardinality, quantile/distribution).
- A framework layer for hierarchical queries, sketch coordination, and windowed analytics.
- An actively evolving codebase focused on performance and API consistency.

## Where To Go Next

- [APIs Index](./apis.md) - Canonical API entry point, including paper references for each sketch.
- [Advanced Use Cases](./advanced_use_cases.md) - Hierarchical queries, sketch coordination, and sliding-window frameworks explained separately.
- [Common Module API](./api/api_common.md) - Shared types, hashing, and structures.
- [Library Map](./library_map.md) - Source-tree module breakdown.
- [Feature Status](./features.md) - Implemented, in-progress, and planned work.
- [Fold Sketch Design](./fold_sketch_design.md) - Detailed algorithmic design for FoldCMS/FoldCS (see note below).
- [Test Coverage Map](./tests.md) - Test organization and coverage notes.

## Current State

- **Ready**: Core sketch APIs marked `Ready` in [apis.md](./apis.md): `CountMin`, `Count Sketch`, `HyperLogLog`, `KLL`, `DDSketch`, `FoldCMS`, `FoldCS`, `CMSHeap`, `CSHeap`
- **Notice**: Core sketch APIs currently marked `Unstable`: `Elastic`, `Coco`, `UniformSampling`, `KMV`
- Framework APIs marked `Ready`: `Hydra`, `HashLayer`, `UnivMon`, `UnivMon Optimized`, `NitroBatch`, `ExponentialHistogram`, `EHSketchList`, `TumblingWindow`
- Framework APIs currently marked `Unstable`: `EHUnivOptimized`
- Shared common-layer APIs are available under [Common Utility APIs](./apis.md#common-utility-apis)
- **Ongoing** work focuses on API stabilization, broader tests, and benchmark depth (see [Feature Status](./features.md))

## Note on FoldCMS / FoldCS

`FoldCMS` and `FoldCS` appear alongside other core sketches in the status table above, but they have a dedicated design document (`fold_sketch_design.md`) for a specific reason: they are an **original algorithmic technique** developed in this codebase, not an implementation of a published standard sketch.

The technique — column-folding — allows sub-window sketches to allocate storage proportional to the actual sub-window cardinality D rather than the full merged-window width W. This produces 3-32x memory savings when D is much smaller than W, with zero additional approximation error. Because the algorithm requires careful exposition of the folding/unfolding mechanics and correctness proofs, it warrants its own detailed design doc. The sketch otherwise follows the same `Ready` status and API conventions as `CountMin` or `HyperLogLog`.
