# Project Overview

`asap_sketchlib` is a Rust sketch library for approximate streaming analytics.
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
- [Wrapper Module](./wrapper.md) - Wire-format-aligned sketch variants byte-compatible with `sketchlib-go`.
- [Message Pack Format](./message_pack_format.md) - On-the-wire envelope shared with `sketchlib-go`.
- [Library Map](./library_map.md) - Source-tree module breakdown.
- [Feature Status](./features.md) - Implemented, in-progress, and planned work.
- [Test Coverage Map](./tests.md) - Test organization and coverage notes.

## Current State

- **Ready**: Core sketch APIs marked `Ready` in [apis.md](./apis.md): `CountMin`, `Count`, `HyperLogLog`, `KLL`, `DDSketch`, `CMSHeap`, `CSHeap`
- **Notice**: Core sketch APIs currently marked `Unstable`: `Elastic`, `Coco`, `UniformSampling`, `KMV`
- Framework APIs marked `Ready`: `Hydra`, `HashSketchEnsemble`, `UnivMon`, `UnivMon Optimized`, `NitroBatch`, `ExponentialHistogram`, `EHSketchList`
- Framework APIs currently marked `Unstable`: `EHUnivOptimized`
- Shared common-layer APIs are available under [Common Utility APIs](./apis.md#common-utility-apis)
- **Ongoing** work focuses on API stabilization, broader tests, and benchmark depth (see [Feature Status](./features.md))
