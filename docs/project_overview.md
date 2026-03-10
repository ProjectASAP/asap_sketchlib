# Project Overview

Migrated from `docs/readme_details.md`.

## Archived README Content

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](../LICENSE)

`sketchlib-rust` is a sketch library for native rust sketch, with potential optimization. This repo contains mainly these parts:

- **Building blocks**: located in `/src/common`, contains common structure to build sketches and other common utilities
  - More detail about building block can be found in: [common api](./common_api.md)
- **Native Sketch**: located in `/src/sketches`, contains Rust sketch implementations built on common structures where applicable
  - Core structured sketches include: CountMin, Count, HyperLogLog
- **Sketch Framework**: located in `/src/sketch_framework`, contains sketch serving/orchestration strategies
  - Includes: Hydra, UnivMon, HashLayer, ExponentialHistogram, Nitro, Orchestrator
- **Optimization**: integrated into sketches implementation
  - More detail about optimization techniques/features can be found in: [features](./features.md)


## Current State

- ✅ Core structured sketches are available and actively used: `CountMin`, `Count`, `HyperLogLog`, `KLL`
- ✅ Framework coverage includes `Hydra`, `UnivMon`, `HashLayer`, `ExponentialHistogram`, and `NitroBatch`
- ✅ Folded window sketches are implemented: `FoldCMS` and `FoldCS` ([design doc](./fold_sketch_design.md))
- ✅ Optimized EH path is implemented via `EHUnivOptimized` (hybrid map + sketch tiers with sketch pooling)
- ✅ Hashing is customizable through `SketchHasher` (default: `DefaultXxHasher`)
- 🚧 Ongoing work focuses on feature expansion, broader test coverage, benchmark depth, serialization coverage, and API stabilization

