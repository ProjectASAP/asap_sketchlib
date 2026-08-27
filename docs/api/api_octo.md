# API: OctoSketch

Status: `Ready`

## Purpose

Delta-promotion primitives for multi-threaded sketch updates, based on
the OctoSketch architecture (NSDI 2024). Child sketches maintain small
counters and emit compact deltas when a promotion threshold τ is
reached; a parent sketch absorbs those deltas at full precision.

The paper's three ideas map onto this module as:

| Paper | Here |
| --- | --- |
| Idea 1 — change-based updates (§3.2) | `OctoWorker` emits one counter at a time |
| Idea 2 — adaptive resource allocation (§4.3) | `OctoAdaptiveThreshold` drives a shared `OctoThreshold` |
| Idea 3 — reconstructed data structures (§3.2) | `CmWorkerSketch` / `CountWorkerSketch` drop key storage and use one-byte counters; only the `*TopK*` aggregators hold a heap |

### Two Usage Levels

- **Low-level (this page)**: Call `insert_emit_delta` / `apply_delta`
  directly on sketch structs. You manage threads, channels, and
  scheduling yourself.
- **Turnkey runtime**: Use `OctoRuntime` or `run_octo` for a ready-made
  multi-threaded pipeline. See [Runtime API](#runtime-api) at the bottom.

## Delta Types

Defined in `src/sketches/octo_delta.rs`.

```rust
pub struct CmDelta    { pub row: u32, pub col: u32, pub value: u32 }
pub struct CountDelta { pub row: u32, pub col: u32, pub value: i32 }
pub struct HllDelta   { pub pos: u32, pub value: u8 }
pub struct DdDelta    { pub index: i32, pub value: u64 }

pub struct KeyedCmDelta    { pub key: HeapItem, pub delta: CmDelta }
pub struct KeyedCountDelta { pub key: HeapItem, pub delta: CountDelta }
```

Indices are `u32`, so any geometry the sketch itself supports can be
addressed. The keyed forms carry the flow key the paper's 4-tuple
message includes, which is what lets an aggregator keep a heavy-hitter
heap the workers no longer maintain.

### Promotion Thresholds

τ is a runtime value, not a constant. The constants below are only the
defaults used when a worker is built without an explicit threshold.

| Sketch | Default | Rule |
| --- | --- | --- |
| CountMin | `CM_PROMASK` = 31 | Emit and clear when a counter reaches τ |
| Count | `COUNT_PROMASK` = 31 | Emit and clear when `\|counter\|` reaches τ |
| DDSketch | `DD_PROMASK` = 4 | Emit and clear when a bucket reaches τ |
| HyperLogLog | `HLL_PROMASK` = 0 | Emit when `\|2^C' - 2^C\| >= 2^τ`; never cleared |
| UnivMon | `UNIVMON_PROMASK` = 64 | As Count, with τ halved per layer |

`DD_PROMASK` is much lower on purpose: a bucket that never reaches τ
never reaches the aggregator at all, so DDSketch loses its sparse tail
rather than merely lagging. Size it against your samples-per-bucket, and
read `DdWorkerSketch::held_back` to bound the rank error it costs.

`HLL_PROMASK = 0` makes the parent's registers bit-identical to a
single-threaded sketch at any cardinality — stronger than the paper's
Theorem 4, which only guarantees equality above `2·α_m·m²·2^(τ-2)`. The
cost is that every register improvement is sent.

In practice 0 is the only HLL threshold worth using below that
precondition. A register the worker has held back reads at the parent as
an *empty* bucket rather than a low one, so the harmonic mean the
estimator is built on collapses: over 50k distinct keys, τ=4 estimates
about 3.2k. `HllOctoWorker::with_threshold` panics outright on a τ no
register gain could ever reach (`>= max_hll_threshold(precision)`),
because such a worker promotes nothing at all and leaves the parent
empty rather than merely lagging.

### Choosing τ from an accuracy target

```rust
pub fn threshold_for_error(epsilon: f64, l1: f64, k_prime: usize) -> u32
```

Equation 4 of the paper, `τ = εL1/k'`. `k_prime` is the number of
workers one flow may reach: 1 under `OctoPartition::HashByKey`, the
worker count under `OctoPartition::RoundRobin`.

### Shared, adjustable τ

```rust
let tau = OctoThreshold::new(31);
tau.get(); tau.set(64); tau.increase(1); tau.decrease(1);
```

`OctoThreshold` is an `Arc<AtomicU32>` clamped to `1..=MAX_PROMASK`
(127). The ceiling is a *signed* one-byte worker counter, and it is the
same for every sketch on purpose: one shared threshold serves workers of
different kinds, so a τ that meant 200 in one and 127 in another would
leave the aggregator's controller a band in which raising τ changes
nothing. Clone it into every worker and into `OctoConfig::threshold` so
the controller and the workers refer to the same value.

## CountMin Delta API

Available on `CountMin<S, RegularPath, H>` and `CountMin<S, FastPath, H>`
where `S::Counter = i32`.

```rust
fn insert_emit_delta(&mut self, value: &DataInput, emit: &mut impl FnMut(CmDelta))
fn insert_emit_delta_with_threshold(&mut self, value: &DataInput, threshold: u32, emit: &mut impl FnMut(CmDelta))
fn insert_emit_keyed_delta_with_threshold(&mut self, value: &DataInput, threshold: u32, emit: &mut impl FnMut(KeyedCmDelta))
fn apply_delta(&mut self, delta: CmDelta)
```

`insert_emit_delta` inserts a key and, for each row counter that reaches
τ, emits a delta carrying that counter's value and clears it — Algorithm
1 of the paper. What the child still holds plus what the parent received
always reconstructs a single-threaded pass.

### CountMin Delta Example

```rust
use asap_sketchlib::{CountMin, RegularPath, DataInput, Vector2D};
use asap_sketchlib::sketches::octo_delta::CmDelta;

let mut child = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(3, 4096);
let mut parent = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(3, 4096);

let key = DataInput::U64(42);
child.insert_emit_delta(&key, &mut |delta: CmDelta| {
    parent.apply_delta(delta);
});
```

## Count Sketch Delta API

Same shape as CountMin, on `Count<S, RegularPath, H>` and
`Count<S, FastPath, H>` where `S::Counter = i32`. Counters are signed, so
the threshold is applied to `|counter|` (§4.4).

```rust
fn insert_emit_delta(&mut self, value: &DataInput, emit: &mut impl FnMut(CountDelta))
fn insert_emit_delta_with_threshold(&mut self, value: &DataInput, threshold: u32, emit: &mut impl FnMut(CountDelta))
fn insert_emit_keyed_delta_with_threshold(&mut self, value: &DataInput, threshold: u32, emit: &mut impl FnMut(KeyedCountDelta))
fn apply_delta(&mut self, delta: CountDelta)
```

## HyperLogLog Delta API

Available on all `HyperLogLogImpl<Variant, Registers, H>` variants.

```rust
fn insert_emit_delta(&mut self, obj: &DataInput, emit: &mut impl FnMut(HllDelta))
fn insert_emit_delta_with_threshold(&mut self, obj: &DataInput, threshold: u8, emit: &mut impl FnMut(HllDelta))
fn insert_emit_delta_with_hash(&mut self, hashed_val: u64, emit: &mut impl FnMut(HllDelta))
fn insert_emit_delta_with_hash_and_threshold(&mut self, hashed_val: u64, threshold: u8, emit: &mut impl FnMut(HllDelta))
fn apply_delta(&mut self, delta: HllDelta)
```

`apply_delta` applies a max-update: the parent register is set to
`max(current, delta.value)`.

## DDSketch Delta API

```rust
fn bucket_index_for(&self, value: f64) -> Option<i32>
fn apply_delta(&mut self, delta: DdDelta)
```

`bucket_index_for` exposes the logarithmic mapping so a worker can hold
one-byte counters over the same bucket space; it returns `None` for any
value `add` would itself drop. `apply_delta` advances `count` exactly and
advances `sum`/`min`/`max` with the bucket's representative value — the
same α-bounded estimate a deserialize-and-recompute produces.

## UnivMon Delta API

```rust
fn bottom_layer_for(&self, key: &DataInput) -> usize
fn apply_layered_delta(&mut self, delta: &LayeredCountDelta, fidelity: UnivMonDeltaFidelity)
fn set_total_weight(&mut self, weight: usize)
fn mark_candidates_incomplete(&mut self)
fn candidates_complete(&self) -> &[bool]
```

```rust
pub struct LayeredCountDelta {
    pub layer: u32,
    pub key: HeapItem,
    pub delta: CountDelta,
    pub worker_id: u32,
    pub weight_total: u64,
}
```

An insert reaches layers `0..=bottom_layer_for(key)`, which is a pure
function of the key's hash, so a worker selects exactly the layers a
single-threaded insert would. `apply_layered_delta` writes the counter,
re-reads the layer's estimate and updates that layer's heap — Algorithm
2, per layer.

**Where the speed comes from, and where it stops.** A worker's per-insert
work is a hash and a few one-byte increments; the L2 accumulator, the
median estimate and the heavy-hitter heap all move to the aggregator,
which touches them once per *promotion* rather than once per insert.
Measured with `cargo run --release --example octo_throughput_probe` on a
2M-insert Zipf stream (heap 64, 5x1024, 12 layers):

| τ | worker | vs single-threaded | deltas/insert | aggregator | sustainable | gap to ideal |
| --- | --- | --- | --- | --- | --- | --- |
| 16 | 16.5 M/s | 62x | 1.435 | 0.43 M/s | 0.30 M/s | 0.0000 |
| 31 | 18.4 M/s | 69x | 1.000 | 0.43 M/s | 0.43 M/s | 0.0002 |
| 64 | 20.2 M/s | 76x | 0.441 | 0.40 M/s | 0.90 M/s | 0.0002 |
| 96 | 21.1 M/s | 80x | 0.322 | 0.42 M/s | 1.32 M/s | 0.0004 |
| 127 | 20.6 M/s | 78x | 0.278 | 0.45 M/s | 1.64 M/s | 0.0005 |

Single-threaded `UnivMon::insert` runs at 0.26 M/s on the same stream, so
the worker is 65-86x faster per insert — but one aggregator serves every
worker, so the pipeline's ceiling is the aggregator's rate divided by
deltas per insert. That is why τ decides everything and why the
aggregator drives it (§4.3).

The aggregator's own ceiling is `HHHeap`, not the delta protocol: its
rate falls off as `1/heap_size` (6.84, 1.88, 0.44, 0.13 Mdelta/s at
capacities 4, 16, 64, 256) because the heap rebuilds its whole position
index on every accepted update. Single-threaded UnivMon pays the same
cost on every insert rather than every promotion, which is most of the
gap between 0.26 M/s and 22 M/s.

**The threshold is scaled per layer.** Layer L only receives the keys that
survive L coin flips, so it carries roughly `n / 2^L` of the stream. One
threshold across the whole pyramid is sized for layer 0 and starves the
deep layers outright — and those are exactly the layers the recursive
estimator leans on for cardinality. `univmon_layer_threshold(base, layer)`
halves τ per layer with a floor of 1; measured on a 60k Zipf stream with
12 layers, a flat τ=31 emptied layers 10 and 11 and collapsed the
cardinality estimate from 3387 to 187, while the scaled rule tracks the
single-core answer to 0.6%.

Three pieces of state a delta stream would otherwise lose are handled
explicitly:

- **L2 per row.** `CountL2HH` carries `l2[row] += new² − old²` on every
  counter write, so `CountL2HH::apply_delta` does the same fix-up and a
  parent fed only deltas reports the same `get_l2` as one fed the stream.
- **Total weight.** `bucket_size` is what g-sum queries divide by and it
  cannot be recovered from thresholded counters, so each delta carries the
  emitting worker's running total. The aggregator keeps the newest report
  per worker and sums them — no extra messages, and the total trails by
  only what arrived after each worker's last promotion.
- **Candidate completeness.** See the caveats below.

The worker mirrors `CountL2HH`'s cell mapping through the shared
`l2hh_cell_for_row`, not a copy of it: a UnivMon layer slices one
128-bit hash into per-row columns and takes each row's sign from a
different high bit, under a per-layer seed, so a hand-written copy would
be easy to drift.

## Compact Worker Sketches

```rust
pub struct CmWorkerSketch { /* Vec<u8> */ }
pub struct CountWorkerSketch { /* Vec<i8> */ }
pub struct DdWorkerSketch { /* HashMap<i32, u8> */ }
pub struct L2hhWorkerSketch { /* Vec<i8>, one per UnivMon layer */ }
```

A worker counter is cleared the moment it reaches τ, so it never exceeds
one byte. Against the 32-bit counters a full `CountMin` uses that is the
paper's 4× memory saving, and the workers keep no flow-key storage at
all. Use these when you drive the protocol yourself; `CmOctoWorker` and
friends already do.

---

## Runtime API

> **Feature gate:** The runtime API below requires the `octo-runtime` Cargo feature.
> Enable it with `features = ["octo-runtime"]` in your `Cargo.toml`.
> This pulls in `core_affinity` and `crossbeam-channel` as dependencies.

### OctoConfig

```rust
pub struct OctoConfig {
    pub num_workers: usize,                          // default: 4
    pub pin_cores: bool,                             // default: true
    pub queue_capacity: usize,                       // default: 65536
    pub threshold: OctoThreshold,                    // default: CM_PROMASK
    pub partition: OctoPartition,                    // default: HashByKey
    pub adaptive: Option<OctoAdaptiveThreshold>,     // default: None
}
```

Build one with `..OctoConfig::default()` so later fields stay additive.

### OctoPartition

```rust
pub enum OctoPartition { HashByKey, RoundRobin }
```

`HashByKey` — the default and the paper's setting — sends one flow to one
worker, so `k'` is 1 and the additive `k'τ` term in the error bounds is
as small as it can be. `RoundRobin` spreads load perfectly even under a
skewed key distribution, at the cost of `k' = k`.

Note that either way a *counter* is shared by whatever flows hash into
it, and each worker may hold back up to τ of its own share, so the
provable per-counter gap to a single-threaded sketch is `k·τ`. `k'τ`
bounds only the queried flow's own held-back count.

### OctoAdaptiveThreshold

```rust
pub struct OctoAdaptiveThreshold {
    pub target_queue_len: usize,   // Q, default 10
    pub alpha: f64,                // dead band, default 0.25
    pub interval: Duration,        // default 100µs
    pub min_threshold: u32,
    pub max_threshold: u32,
}
```

The aggregator samples total queue occupancy every `interval`, predicts
the next window with Equation 1 (`Q̂ₜ₊₁ = Qₜ + (Qₜ − Qₜ₋₁)`), and moves τ
by one per Equation 2: down when the prediction falls below
`(1−α)·Q`, up when it rises above `(1+α)·Q`, unchanged in between. Set
`min_threshold` from `threshold_for_error` to hold an accuracy floor.

### OctoRuntime (Streaming)

```rust
fn new<F, PF>(config: &OctoConfig, worker_factory: F, parent_factory: PF) -> Self
fn insert(&mut self, input: DataInput<'_>)
fn insert_batch(&mut self, inputs: &[DataInput<'_>])
fn flush(&mut self)
fn read_handle(&self) -> OctoReadHandle<P>
fn close(&self)
fn finish(self) -> OctoResult<P>
```

### Flushing before a query

Between promotions a worker holds every counter still under τ. For
Count-Min and Count that only leaves the parent low by under τ per cell.
For DDSketch, or a HyperLogLog running a positive threshold, an
un-promoted cell is *absent* from the parent rather than lagging — and a
quantile or a cardinality is exactly a statement about which cells
exist, so those queries are wrong without bound rather than within α.

`flush` hands over every residual counter and waits for the aggregator to
apply it, so the parent answers against every input accepted so far. It
is the point at which a stream is handed over for querying. `finish`
flushes too, so a completed run always answers against the whole stream —
a flushed Count-Min parent equals a single-threaded pass cell for cell.

It costs one message per non-empty cell, which is as much as shipping the
sketch, so call it when a query needs to be right rather than on a timer.
Inserting afterwards is fine; a flush does not seal the runtime.

`OctoWorker::flush` defaults to doing nothing. The `*TopK*` and UnivMon
workers keep that default on purpose: every delta they send carries the
key that produced it, and a worker keeps no key storage, so a residual
cell cannot be attributed back to a key. Their parents stay low by under
τ per cell, and their heavy-hitter heaps do not depend on the residue.

At the low level, `CmWorkerSketch`, `CountWorkerSketch`, `DdWorkerSketch`
and `L2hhWorkerSketch` each expose `flush` directly.

### run_octo (Batch)

```rust
pub fn run_octo<W, P>(
    inputs: &[DataInput<'_>],
    config: &OctoConfig,
    worker_factory: impl Fn(usize) -> W,
    parent_factory: impl FnOnce() -> P,
) -> OctoResult<P>
```

### Concrete Worker / Aggregator Pairs

| Sketch | Worker | Aggregator | Delta Type |
| --- | --- | --- | --- |
| CountMin | `CmOctoWorker` | `CmOctoAggregator` | `CmDelta` |
| CountMin + top-k | `CmTopKOctoWorker` | `CmTopKOctoAggregator` | `KeyedCmDelta` |
| Count | `CountOctoWorker` | `CountOctoAggregator` | `CountDelta` |
| Count + top-k | `CountTopKOctoWorker` | `CountTopKOctoAggregator` | `KeyedCountDelta` |
| DDSketch | `DdOctoWorker` | `DdOctoAggregator` | `DdDelta` |
| UnivMon | `UnivMonOctoWorker` | `UnivMonOctoAggregator` | `LayeredCountDelta` |
| HyperLogLog | `HllOctoWorker` | `HllOctoAggregator` | `HllDelta` |

The `*TopK*` pairs hold the pipeline's only heavy-hitter heap, in the
aggregator: each keyed delta updates the parent counter and then the
heap, which is Algorithm 2.

### Sharing a threshold across the fleet

```rust
let tau = OctoThreshold::new(31);
let config = OctoConfig {
    num_workers: 8,
    threshold: tau.clone(),
    adaptive: Some(OctoAdaptiveThreshold::default()),
    ..OctoConfig::default()
};
let result = run_octo(
    &inputs,
    &config,
    { let tau = tau.clone(); move |_| CmOctoWorker::with_threshold(4, 4096, tau.clone()) },
    || CmOctoAggregator::new(4, 4096),
);
```

## Caveats

- Counts below τ reach the parent only at a flush. `finish` flushes, and
  `OctoRuntime::flush` does it mid-stream; a worker driven directly at the
  low level must be flushed by its caller. The keyed workers cannot flush
  at all — see above.
- DDSketch loses whole buckets rather than lagging on them, so read it
  only after a flush. Between flushes, keep τ small and check
  `held_back()`; measured on a skewed 200k stream, τ=2 costs 7x the
  ideal quantile error, τ=4 costs 19x and τ=8 costs 41x, while periodic
  sketch-merge stays at ideal for 3x the traffic.
- Core pinning is a silent no-op on Apple Silicon: macOS exposes
  `thread_policy_set(THREAD_AFFINITY_POLICY)` but arm64 rejects it, and
  `core_affinity::get_core_ids` reports a count without testing whether
  pinning works. `pin_cores: true` costs nothing there but buys nothing
  either.
- The runtime's `insert` dispatches from the calling thread, so that
  thread is a serialization point. The paper instead has each worker pull
  from its own NIC queue.
- Core pinning silently falls back if the platform has fewer cores than
  `num_workers + 1`, and is unavailable outright on Apple Silicon.
- `insert` after `close` panics.
- A UnivMon layer that thresholds cannot call its candidate set complete,
  so `candidates_complete()` reads false on exactly those layers. Queries
  then take the conservative branch of `heavy_threshold` instead of
  overcounting. Deep layers, whose scaled threshold floors at 1, keep the
  heap's own verdict. At a base threshold of 1 a single worker reproduces
  a single-threaded UnivMon exactly - counters, L2, heaps, total weight
  and g-sum alike.
- DDSketch is the one integrated sketch where delta promotion does not pay
  off. A lagging Count-Min counter is still counted, just low; a DDSketch
  bucket that never reaches τ is absent, and a quantile is a statement
  about where mass sits. On a skewed 200k stream the measured trade is
  τ=1 → exact at 200k messages, τ=2 → 7x ideal error, τ=4 → 19x, τ=8 →
  41x, while periodic merge stays at ideal for 3x the traffic. Prefer
  merge for DDSketch unless τ can be 1.
- Not yet wired up: CocoSketch and Elastic sketch. Both replace a whole
  `(key, counter)` bucket rather than incrementing a cell, so they need a
  bucket-valued delta rather than the cell-valued ones here.

## Status

Core multi-threaded insertion framework; actively used and tested.
