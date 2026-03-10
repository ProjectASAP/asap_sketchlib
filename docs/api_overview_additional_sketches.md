# API Overview: Additional Sketches

Migrated from `docs/readme_details.md` -> `API Overview`.

### Additional Sketches

The library includes additional specialized sketches. Each follows a consistent lifecycle: construct, insert, query, and optionally merge.

#### KLL (quantile estimation)

```rust
use sketchlib_rust::{KLL, SketchInput};

let mut sketch = KLL::init_kll(200);
let mut peer = KLL::init_kll(200);
```

Stream values into each sketch:

```rust
for sample in [12.0, 18.0, 21.0, 35.0, 42.0] {
    sketch.update(&SketchInput::F64(sample)).unwrap();
}
for sample in [30.0, 33.0, 38.0] {
    peer.update(&SketchInput::F64(sample)).unwrap();
}
```

Merge the peer into the primary:

```rust
sketch.merge(&peer);
```

Query quantiles (argument is a rank in [0, 1]):

```rust
let median = sketch.quantile(0.5);
println!("median value ≈ {median:.3}");
```

Or use the CDF interface for value-based queries:

```rust
let cdf = sketch.cdf();
let fraction = cdf.quantile(32.0);  // fraction of values <= 32
println!("fraction of samples <= 32 ≈ {fraction:.3}");
```

#### DDSketch (relative-error quantiles, mergeable)

DDSketch provides approximate quantile estimation with configurable relative error guarantees.

```rust
use sketchlib_rust::DDSketch;

// alpha is the relative error bound
let mut dds = DDSketch::new(0.01);
```

Insertion:

```rust
dds.add(1.0);
dds.add(5.2);
dds.add(42.0);
```

Quantile queries:

```rust
let p50 = dds.get_value_at_quantile(0.50).unwrap();
let p90 = dds.get_value_at_quantile(0.90).unwrap();
let p99 = dds.get_value_at_quantile(0.99).unwrap();
```

Merge (two DDSketch instances must share the same `alpha`):

```rust
let mut a = DDSketch::new(0.01);
let mut b = DDSketch::new(0.01);

a.add(1.0);
a.add(2.0);
b.add(10.0);
b.add(20.0);

a.merge(&b);
```

#### Elastic (heavy + light split)

Create an Elastic sketch with a heavy bucket array.

```rust
use sketchlib_rust::Elastic;

let mut flows = Elastic::init_with_length(16);
```

Insert flow identifiers into the structure.

```rust
for id in ["api/login", "api/login", "api/search"] {
    flows.insert(id.to_string());
}
```

Query both the heavy bucket and the backing Count-Min.

```rust
let heavy = flows.query("api/login".to_string());
let light = flows.query("api/search".to_string());
println!("heavy flow estimate = {heavy}, light flow estimate = {light}");
```

#### Coco (substring aggregation)

Allocate primary and secondary Coco tables.

```rust
use sketchlib_rust::{Coco, SketchInput};

let mut coco = Coco::init_with_size(64, 4);
let mut shard = Coco::init_with_size(64, 4);
```

Insert weighted updates for composite keys.

```rust
coco.insert("region=us-west|id=42", 5);
coco.insert("region=us-west|id=42", 1);
shard.insert("region=us-west|id=13", 3);
```

Estimate using substring matches.

```rust
let regional = coco.estimate("us-west");
println!("regional count ≈ {}", regional);
```

Merge the shard back into the primary sketch.

```rust
coco.merge(&shard);
```

#### Locher (heavy hitter sampling)

Construct a Locher sketch with three rows of Top-K heaps.

```rust
use sketchlib_rust::sketches::locher::LocherSketch;

let mut sketch = LocherSketch::new(3, 64, 5);
let key = "endpoint=/checkout".to_string();
```

Insert repeated events for a heavy hitter.

```rust
for _ in 0..50 {
    sketch.insert(&key, 1);
}
```

Estimate the adjusted heavy-hitter count.

```rust
println!("heavy estimate ≈ {}", sketch.estimate(&key));
```

