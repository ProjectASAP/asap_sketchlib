# API Overview: Sketch Frameworks

Migrated from `docs/readme_details.md` -> `API Overview`.

### Sketch Frameworks

#### UnivMon

UnivMon provides a multi-layer pyramid structure for computing frequency moments (L1, L2, cardinality, entropy) over streams using Count Sketch layers and heavy-hitter tracking.

Initialize with custom parameters (heap_size, sketch_rows, sketch_cols, layer_size):

```rust
use sketchlib_rust::UnivMon;

let mut univmon = UnivMon::init_univmon(32, 3, 1024, 4);
```

Insert items (hashing and layer assignment are handled internally):

```rust
use sketchlib_rust::SketchInput;

let key = SketchInput::Str("flow::123");
univmon.insert(&key, 1);
```

Query various statistics:

```rust
let cardinality = univmon.calc_card();
let l1_norm = univmon.calc_l1();
let l2_norm = univmon.calc_l2();
let entropy = univmon.calc_entropy();

println!("cardinality: {}", cardinality);
println!("L1: {}, L2: {}", l1_norm, l2_norm);
println!("entropy: {}", entropy);
```

Merge two UnivMon sketches (must have identical structure):

```rust
use sketchlib_rust::{UnivMon, SketchInput};

let mut um1 = UnivMon::init_univmon(32, 3, 1024, 4);
let mut um2 = UnivMon::init_univmon(32, 3, 1024, 4);

um1.insert(&SketchInput::Str("flow_a"), 10);
um2.insert(&SketchInput::Str("flow_b"), 15);

um1.merge(&um2);
println!("merged L1: {}", um1.calc_l1());
```

#### HashLayer

HashLayer provides a performance optimization for managing multiple sketches by computing hash values once and reusing them across all sketches. It uses `OrchestratedSketch` from the orchestrator module.

Initialize with default sketches (CountMin, Count, HyperLogLog):

```rust
use sketchlib_rust::HashLayer;

let mut layer = HashLayer::default();
```

Or create with custom sketch configuration:

```rust
use sketchlib_rust::{
    CountMin, Count, DataFusion, FastPath, FreqSketch, HashLayer,
    HyperLogLog, OrchestratedSketch, CardinalitySketch, Vector2D,
};

let sketches = vec![
    OrchestratedSketch::Freq(FreqSketch::CountMin(
        CountMin::<Vector2D<i32>, FastPath>::default(),
    )),
    OrchestratedSketch::Freq(FreqSketch::Count(
        Count::<Vector2D<i32>, FastPath>::default(),
    )),
    OrchestratedSketch::Cardinality(CardinalitySketch::HllDf(
        HyperLogLog::<DataFusion>::default(),
    )),
];
let mut layer = HashLayer::new(sketches).unwrap();
```

Insert to all sketches with hash computed once:

```rust
use sketchlib_rust::SketchInput;

for value in 0..10_000 {
    let input = SketchInput::U64(value);
    layer.insert_all(&input);  // Hash computed once, reused for all sketches
}
```

Insert to specific sketch indices:

```rust
let input = SketchInput::U64(42);
layer.insert_at(&[0, 1], &input);  // Only insert to sketches at index 0 and 1
```

Query specific sketches by index:

```rust
let input = SketchInput::U64(42);
let estimate = layer.query_at(0, &input).unwrap();  // Query sketch at index 0
println!("estimate from sketch 0: {}", estimate);
```

#### Hydra

Hydra coordinates multi-dimensional queries by maintaining sketches for all label combinations. It accepts semicolon-delimited keys and automatically fans updates across subpopulations.

Initialize with sketch template (uses CountMin by default):

```rust
use sketchlib_rust::{Hydra, CountMin, FastPath, Vector2D};
use sketchlib_rust::common::input::HydraCounter;

let template = HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::default());
let mut hydra = Hydra::with_dimensions(3, 128, template);
```

Insert with multi-dimensional keys (**semicolon-separated**):

```rust
use sketchlib_rust::SketchInput;

let value = SketchInput::String("error".into());
hydra.update("service=api;route=/users;status=500", &value, None);
```

Query specific label combinations:

```rust
// Query 2D combination
let estimate = hydra.query_frequency(vec!["service=api", "status=500"], &value);
println!("api + 500 errors: {}", estimate);

// Query single dimension
let service_total = hydra.query_frequency(vec!["service=api"], &value);
println!("all api errors: {}", service_total);
```

Hydra with HyperLogLog for cardinality queries:

```rust
use sketchlib_rust::{DataFusion, HyperLogLog, SketchInput};
use sketchlib_rust::common::input::{HydraCounter, HydraQuery};

let hll_template = HydraCounter::HLL(HyperLogLog::<DataFusion>::new());
let mut hydra = Hydra::with_dimensions(5, 128, hll_template);

// Insert user IDs with labels
for user_id in 0..1000 {
    let value = SketchInput::U64(user_id);
    hydra.update("region=us-west;device=mobile", &value, None);
}

// Query distinct users by region
let cardinality = hydra.query_key(vec!["region=us-west"], &HydraQuery::Cardinality);
println!("distinct users in us-west: {}", cardinality);
```

