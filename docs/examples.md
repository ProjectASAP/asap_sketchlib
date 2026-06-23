# Examples

Practical end-to-end examples showing common use cases. Each example starts with the exact-data-structure baseline to make the sketch's tradeoff explicit.

---

## Cardinality estimation with HyperLogLog

Count how many distinct user IDs appear in a large stream.

**Exact baseline** — `HashSet` stores every unique ID; memory grows with cardinality.

```rust
use std::collections::HashSet;

let user_ids: Vec<u64> = get_user_ids();

let mut unique_user_ids = HashSet::new();
for &user_id in &user_ids {
    unique_user_ids.insert(user_id);
}

let unique_users = unique_user_ids.len();
println!("unique users: {unique_users}");
```

**Sketch version** — `HyperLogLog<ErtlMLE>` estimates the count using fixed, bounded memory.

```rust
use asap_sketchlib::{ErtlMLE, HyperLogLog, DataInput};

let user_ids: Vec<u64> = get_user_ids();

// ErtlMLE is more accurate than Classic at very low or very high cardinalities.
let mut hll = HyperLogLog::<ErtlMLE>::default();
for &user_id in &user_ids {
    hll.insert(&DataInput::U64(user_id));
}

let unique_users = hll.estimate();
println!("estimated unique users: {unique_users}");
```

API reference: [`docs/api/api_hyperloglog.md`](./api/api_hyperloglog.md)

---

## Frequency estimation with Count-Min Sketch

Count how many times a specific user ID appears in a large stream.

**Exact baseline** — `HashMap` stores one counter per distinct key; memory grows with the number of distinct items.

```rust
use std::collections::HashMap;

let user_ids: Vec<u64> = get_user_ids();

let mut user_counts: HashMap<u64, u64> = HashMap::new();
for &user_id in &user_ids {
    *user_counts.entry(user_id).or_insert(0) += 1;
}

let target_user_id = 101;
let exact_count = user_counts.get(&target_user_id).copied().unwrap_or(0);
println!("exact count for user {target_user_id}: {exact_count}");
```

**Sketch version** — `CountMin<FixedMatrix, FastPath>` estimates frequencies using a compact, fixed-size matrix.

```rust
use asap_sketchlib::{CountMin, DataInput, FixedMatrix, FastPath};

let user_ids: Vec<u64> = get_user_ids();

// FixedMatrix uses a statically-sized backing array; FastPath selects an
// optimized hashing path for throughput-sensitive workloads.
let mut cms = CountMin::<FixedMatrix, FastPath>::default();
for &user_id in &user_ids {
    cms.insert(&DataInput::U64(user_id));
}

let target_user_id = 101;
let estimated_count = cms.estimate(&DataInput::U64(target_user_id));
println!("estimated count for user {target_user_id}: {estimated_count}");
```

API reference: [`docs/api/api_countmin.md`](./api/api_countmin.md)

---

## Quantile estimation with KLL

Compute quantiles (e.g., p50, p99) over a large stream of values.

**Exact baseline** — Polars buffers all values and sorts them to compute quantiles exactly; the sort dominates latency at large scale.

```rust
use polars::prelude::*;

let df = DataFrame::new(vec![Column::new("v".into(), &buf)]).unwrap();

let exprs: Vec<Expr> = (0..=100)
    .map(|i| {
        let p = i as f64 / 100.0;
        col("v")
            .quantile(lit(p), QuantileMethod::Linear)
            .alias(format!("q{i}"))
    })
    .collect();

// Expensive: resolves quantiles over all buffered values.
let result = df.lazy().select(exprs).collect().unwrap();

let mut quantiles = [0.0_f64; 101];
for i in 0..=100 {
    let col = result
        .column(&format!("q{i}"))
        .unwrap()
        .cast(&DataType::Float64)
        .unwrap();
    quantiles[i] = col.f64().unwrap().get(0).unwrap_or(0.0);
}
```

**Sketch version** — `KLL` maintains a compact sketch during insertion; the expensive work happens online rather than in a final sort phase.

```rust
use asap_sketchlib::sketches::kll::Cdf;
use asap_sketchlib::KLL;

let values: Vec<i64> = get_values();

let mut sketch = KLL::<i64>::init_kll(200);
for v in values.iter() {
    sketch.update(v);
}

// Build the CDF once after all insertions.
let cdf: Cdf = sketch.cdf();

// Query any quantile in O(log n) against the cached CDF.
let mut estimates = [0.0_f64; 101];
for i in 0..=100 {
    let p = i as f64 / 100.0;
    estimates[i] = cdf.query(p);
}
```

On a 10M-item Zipf workload (101 quantiles), KLL completes in ~214 ms versus ~7,980 ms for Polars, with the difference dominated by Polars' sort phase.

API reference: [`docs/api/api_kll.md`](./api/api_kll.md)
