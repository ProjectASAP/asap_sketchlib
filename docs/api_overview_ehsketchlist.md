# API Overview: EHSketchList

Migrated from `docs/readme_details.md` -> `API Overview`.

### `EHSketchList`: one enum to drive them all

`EHSketchList` wraps each sketch in a single enum so callers can build pipelines without matching on individual types. The enum normalizes `insert`, `merge`, and `query` across the different sketches and returns helpful errors when an operation is not supported.

**Variants:**

- `CM(CountMin<Vector2D<i32>, FastPath>)` - Count-Min Sketch
- `CS(Count<Vector2D<i32>, FastPath>)` - Count Sketch
- `COUNTL2HH(CountL2HH)` - Count Sketch with L2 heavy hitters
- `HLL(HyperLogLog<DataFusion>)` - HyperLogLog
- `KLL(KLL)` - KLL quantile sketch
- `DDS(DDSketch)` - DDSketch quantile sketch
- `COCO(Coco)` - Coco substring aggregation
- `ELASTIC(Elastic)` - Elastic heavy/light split
- `UNIFORM(UniformSampling)` - Uniform reservoir sampling
- `UNIVMON(UnivMon)` - UnivMon universal monitoring

Construct two `EHSketchList` wrappers over Count-Min sketches.

```rust
use sketchlib_rust::{EHSketchList, CountMin, FastPath, Vector2D, SketchInput};

let mut counts = EHSketchList::CM(CountMin::<Vector2D<i32>, FastPath>::default());
let mut canary = EHSketchList::CM(CountMin::<Vector2D<i32>, FastPath>::default());
let key = SketchInput::String("endpoint=/search".into());
```

Insert values through the unified enum interface.

```rust
counts.insert(&key);
canary.insert(&key);
```

Merge compatible `EHSketchList` variants.

```rust
counts.merge(&canary)?;
```

Query estimates without matching on the underlying sketch.

```rust
let estimate = counts.query(&key)?;
println!("merged sketch estimate = {estimate}");
```

When the underlying sketch does not implement an operation, `EHSketchList::merge` returns an error explaining the mismatch.

