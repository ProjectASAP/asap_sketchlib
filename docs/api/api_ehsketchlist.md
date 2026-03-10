# API: EHSketchList

Status: `Ready`

## Purpose

Unified enum wrapper for sketch payloads used by EH-style frameworks.

## Type/Struct

```rust
enum EHSketchList {
    CM(CountMin<Vector2D<i32>, FastPath>),
    CS(Count<Vector2D<i32>, FastPath>),
    COUNTL2HH(CountL2HH),
    HLL(HyperLogLog<DataFusion>),
    KLL(KLL),
    DDS(DDSketch),
    COCO(Coco),
    ELASTIC(Elastic),
    UNIFORM(UniformSampling),
    UNIVMON(UnivMon),
}
```

## Constructors

Enum-based; construct by variant.

## Insert/Update

```rust
fn insert(&mut self, val: &SketchInput)
```

## Query

```rust
fn query(&self, key: &SketchInput) -> Result<f64, &'static str>
fn supports_norm(&self, norm: SketchNorm) -> bool
fn sketch_type(&self) -> &'static str
```

## Merge

```rust
fn merge(&mut self, other: &EHSketchList) -> Result<(), &'static str>
```

## Serialization

Serialized through serde as part of parent structures.

## Examples

```rust
use sketchlib_rust::{CountMin, EHSketchList, FastPath, SketchInput, Vector2D};

let mut sk = EHSketchList::CM(CountMin::<Vector2D<i32>, FastPath>::default());
sk.insert(&SketchInput::U64(1));
let _ = sk.query(&SketchInput::U64(1));
```

## Caveats

- Some variant paths still contain `todo!()` branches in input conversion.
- Some merge/query variant combinations are intentionally unsupported.

## Status

Core wrapper in EH and optimized window frameworks.
