# API Overview: Exponential Histogram

Migrated from `docs/readme_details.md` -> `API Overview`.

### Exponential Histogram: Time-bounded aggregates

Initialize the windowed coordinator with a sketch template.

```rust
use sketchlib_rust::{EHSketchList, ExponentialHistogram, CountMin, FastPath, Vector2D, SketchInput};

let template = EHSketchList::CM(CountMin::<Vector2D<i32>, FastPath>::default());
let mut eh = ExponentialHistogram::new(3, 120, template);
```

Insert timestamped events.

```rust
eh.update(10, &SketchInput::String("flow".into()));
eh.update(70, &SketchInput::String("flow".into()));
```

Query the merged sketch for a given interval.

```rust
if let Some(bucket) = eh.query_interval_merge(0, 120) {
    let estimate = bucket.query(&SketchInput::String("flow".into())).unwrap();
    println!("approximate count inside window = {}", estimate);
}
```

