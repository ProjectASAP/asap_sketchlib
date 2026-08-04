# API: UnivMon-Q

Status: `Unstable`

## Purpose

`UnivMonQ` combines universal frequency-vector measurements with additive-rank
quantile estimation. Choose it when one stream must answer point frequency,
distinct count, F2, entropy, heavy-hitter, rank, CDF, and quantile queries from
shared mergeable state. For quantiles alone, KLL or DDSketch is normally much
smaller.

The update path uses one 128-bit hash and one physical CountSketch layer per
observation. Disjoint hash fields select each row's bucket/sign, the Joltik
terminal stratum, and the coordinated bottom-k sample priority.

## Types

- `UnivMonQ<H = DefaultXxHasher>`
- `UnivMonQQuery<'a, H = DefaultXxHasher>`
- `UnivMonQConfig`
- `UnivMonQPoint`
- `UnivMonQError`

## Constructors

```rust
fn default() -> Self
fn new(config: UnivMonQConfig) -> Result<Self, UnivMonQError>
fn with_hasher(config: UnivMonQConfig) -> Result<Self, UnivMonQError>
```

`new` selects `DefaultXxHasher`. Use `UnivMonQ::<CustomHasher>::with_hasher`
for the pluggable hash API.

`UnivMonQConfig::with_window_bound(max_updates, failure_probability)` chooses
the smallest level count whose deepest sample fits the candidate table under a
Bernstein tail bound. `max_updates` is the aggregate logical window size across
all merged shards.

## Update

```rust
fn update(&mut self, value: &f64)
fn add<T: NumericalValue>(&mut self, value: &T)
fn update_data_input(&mut self, value: &DataInput) -> Result<(), &'static str>
fn clear(&mut self)
```

`DataInput` strings and byte arrays are rejected. Integer `DataInput` variants
are projected to `f64`, matching the type-erased KLL path; integers above the
exact `f64` range should use an explicitly typed application-level encoding if
distinct identity must be preserved.

Values follow `f64::total_cmp`, so `-0.0` and `0.0` remain distinct and NaNs
have deterministic positions.

## Query

```rust
fn count(&self) -> u64
fn len(&self) -> u64
fn is_empty(&self) -> bool
fn min(&self) -> Option<f64>
fn max(&self) -> Option<f64>

fn estimate_frequency(&self, value: f64) -> u64
fn estimate_distinct(&self) -> f64
fn estimate_f2(&self) -> f64
fn estimate_f3(&self) -> f64
fn estimate_g_sum<F>(&self, g: F) -> f64 where F: Fn(f64) -> f64
fn estimate_entropy(&self) -> f64
fn heavy_hitters(&self, k: usize) -> Vec<(f64, u64)>

fn rank(&self, value: f64) -> Option<u64>
fn quantile(&self, q: f64) -> Option<f64>
fn quantiles(&self, quantiles: &[f64]) -> Vec<Option<f64>>
fn get_value_at_quantile(&self, q: f64) -> Option<f64>
fn cdf(&self) -> Vec<UnivMonQPoint>
fn prepare_queries(&self) -> UnivMonQQuery<'_, H>
fn estimated_memory_bytes(&self) -> usize
```

F3 uses the same universal recurrence with `g(f) = f^3`. The public generic
g-sum API preserves UnivMon's ability to evaluate other compatible frequency
functions.

`prepare_queries` reconstructs candidate frequencies, logical sampled levels,
F2 thresholds, and the ordered CDF once. Its returned immutable view exposes
count/min/max/frequency, F0/F2/F3/g-sum/entropy/heavy hitters, rank, batched
quantiles, and a borrowed CDF slice. Prefer it when a scrape or report asks for
multiple metrics from one snapshot:

```rust
let query = sketch.prepare_queries();
let f0 = query.estimate_distinct();
let f2 = query.estimate_f2();
let f3 = query.estimate_f3();
let entropy = query.estimate_entropy();
let percentiles = query.quantiles(&[0.5, 0.9, 0.99]);
let cdf = query.cdf();
```

The direct methods remain convenient for isolated queries. `quantiles` is the
batched alternative to repeated `quantile` calls and constructs the CDF once.

Setting `ordered_samples = 0` removes ordered-sample memory and disables
non-endpoint rank/quantile/CDF queries while retaining the universal metrics.

## Merge

```rust
fn merge(&mut self, other: &Self) -> Result<(), UnivMonQError>
```

Merges require identical dimensions and `hash_seed`, as well as the same
compile-time hasher type. Terminal counters add, SpaceSaving candidate
summaries use a parallel merge, and coordinated samples are unioned and pruned
by their common priority. With 32-bit counters, callers must size the aggregate
window so no signed cell saturates; use `counter_bits = 64` otherwise.

`UnivMonQ` also implements `TumblingWindowSketch`, so it can be used directly
with `TumblingWindow<UnivMonQ>`. The window adapter treats the `DataInput` key
as the numeric observation, like the existing KLL adapter.

## Serialization

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

These helpers exist for `UnivMonQ<DefaultXxHasher>` and use a validated native
MessagePack DTO. No ASAPv1 kind id or cross-language protobuf contract has been
assigned yet.

## Example

```rust
use asap_sketchlib::{UnivMonQ, UnivMonQConfig};

let config = UnivMonQConfig::default()
    .with_window_bound(100_000, 1e-6)?;
let mut left = UnivMonQ::new(config)?;
let mut right = UnivMonQ::new(config)?;

for value in 0..50_000 {
    left.add(&value);
}
for value in 50_000..100_000 {
    right.add(&value);
}
left.merge(&right)?;

assert!(left.quantile(0.5).is_some());
assert!(left.estimate_distinct() > 0.0);
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Accuracy caveat

This is an empirically validated UnivMon extension, not a replacement for
KLL's formal `O(1/epsilon)` additive-rank guarantee. Its ordered residual sample
has the natural subset-sampling `O(1/epsilon^2)` space behavior, and the joint
CountSketch recovery/residual-ratio analysis remains research work.

## References

- [Liu et al., “One Sketch to Rule Them All,” SIGCOMM 2016](https://doi.org/10.1145/2934872.2934906).
- [Yang et al., “Joltik,” MobiCom 2020](https://doi.org/10.1145/3372224.3419191).
- [Braverman, Krauthgamer, and Yang, “Universal Streaming of Subset Norms”](https://arxiv.org/abs/1812.00241).
- [Karnin, Lang, and Liberty, “Optimal Quantile Approximation in Streams”](https://arxiv.org/abs/1603.05346).
