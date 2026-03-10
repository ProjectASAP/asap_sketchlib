# Project Overview

`sketchlib-rust` is a Rust sketch library for approximate streaming analytics.
It provides reusable data-structure building blocks, production-focused sketch
implementations, and orchestration/windowing frameworks in one crate.

## What This Repo Is

- A shared common layer for input types, hashing, matrix/heap structures, and utilities.
- A set of core sketch implementations (frequency, cardinality, quantile/distribution).
- A framework layer for hierarchical queries, sketch coordination, and windowed analytics.
- An actively evolving codebase focused on performance and API consistency.

## Where To Go Next

- [APIs Index](./apis.md) - Canonical API entry point.
- [Common Module API](./api/api_common.md) - Shared types, hashing, and structures.
- [Library Map](./library_map.md) - Source-tree module breakdown.
- [Feature Status](./features.md) - Implemented, in-progress, and planned work.
- [Fold Sketch Design](./fold_sketch_design.md) - Detailed FoldCMS/FoldCS design.
- [Test Coverage Map](./tests.md) - Test organization and coverage notes.

## Current State

- ✅ Core sketch APIs marked `Ready` in [apis.md](./apis.md): `CountMin`, `Count Sketch`, `HyperLogLog`, `KLL`, `DDSketch`, `FoldCMS`, `FoldCS`, `CMSHeap`, `CSHeap`
- ⚠️ Core sketch APIs currently marked `Unstable`: `Elastic`, `Coco`, `UniformSampling`, `KMV`
- ✅ Framework APIs marked `Ready`: `Hydra`, `HashLayer`, `UnivMon`, `UnivMon Optimized`, `NitroBatch`, `ExponentialHistogram`, `EHSketchList`, `TumblingWindow`
- ⚠️ Framework APIs currently marked `Unstable`: `EHUnivOptimized`
- ✅ Shared common-layer APIs are available under [Common Utility APIs](./apis.md#common-utility-apis)
- 🚧 Ongoing work focuses on API stabilization, broader tests, and benchmark depth (see [Feature Status](./features.md))
