# Advanced Use Cases (Frameworks)

This page covers three distinct categories of advanced usage in `ASAPSketchLib`. These are separate problems that happen to share a common answer: composing multiple sketches together.

---

## 1. Hierarchical Queries

**Goal**: Query a stream broken down by one or more categorical dimensions (e.g., "frequency of errors, grouped by region and service").

**The problem with plain sketches**: A single flat CMS can answer "how often does key X appear?" but it cannot answer "how often does key X appear *within region=us*?" without maintaining a separate sketch per dimension value — which blows up memory for high-cardinality dimensions.

**Solution: `Hydra` and `MultiHeadHydra`**

`Hydra` maintains a hierarchy of sketches keyed by semicolon-separated dimension prefixes. A single `update` call fans out into the appropriate dimension nodes. Queries can then target any prefix subtree.

```rust
use asap_sketchlib::{Hydra, SketchInput};

let mut hydra = Hydra::default();
hydra.update("region=us;service=api", &SketchInput::Str("err"), None);
hydra.update("region=eu;service=db",  &SketchInput::Str("err"), None);

// Query frequency within just the "region=us" subtree
let est = hydra.query_frequency(vec!["region=us"], &SketchInput::Str("err"));
assert!(est >= 1.0);
```

`MultiHeadHydra` extends this to multiple independent dimension hierarchies in parallel (e.g., one head for `region`, another for `service`), each backed by a configurable `HydraCounter` (CMS or Count Sketch variant).

**`HydraCounter`** selects which inner sketch backs each Hydra node. **`HydraQuery`** selects the query type: `Frequency(SketchInput)` or `Quantile(threshold)`.

API reference: [`docs/api/api_hydra.md`](./api/api_hydra.md)

---

## 2. Sketch Coordination (Hash-Once-Use-Many)

**Goal**: Maintain several different sketch statistics over the same stream while minimizing redundant hash computation.

**The problem**: If you need frequency counts, cardinality, and quantiles simultaneously over the same stream, naively inserting into three separate sketches computes the hash of each element three times.

**Solution: `HashSketchEnsemble` and `UnivMon`**

`HashSketchEnsemble` (also referred to as `HashLayer` in the API) computes the element hash once and distributes the pre-computed hash value to all member sketches. This enables correlated multi-sketch inserts with a single hash call.

`UnivMon` goes further: it implements the Universal Monitoring framework (Liu et al., SIGCOMM 2016), which answers L1, L2, entropy, and cardinality queries from a single data structure by organizing CMS sketches in a geometric sampling hierarchy.

`NitroBatch` wraps a CMS or Count Sketch in a batch-sampling mode: elements are Nitro-sampled before insertion, reducing the effective insertion rate while preserving accuracy guarantees.

`OctoSketch` provides an alternative sketch-serving framework for high-throughput coordination.

API references: [`docs/api/api_hashlayer.md`](./api/api_hashlayer.md), [`docs/api/api_univmon.md`](./api/api_univmon.md), [`docs/api/api_nitrobatch.md`](./api/api_nitrobatch.md), [`docs/api/api_octo.md`](./api/api_octo.md)

---

## 3. Sliding and Tumbling Windows

**Goal**: Answer frequency or quantile queries over a recent time window (e.g., "top-K IPs in the last 5 minutes") rather than over the entire stream.

**The problem**: A single sketch accumulates all history. To answer windowed queries, you need to expire old data without replaying the stream.

**Solution: `ExponentialHistogram`, `TumblingWindow`, and `FoldCMS`/`FoldCS`**

`TumblingWindow` divides time into fixed-length non-overlapping epochs. Each epoch gets its own sketch; when an epoch expires, its sketch is dropped.

`ExponentialHistogram` (EH) provides sliding-window semantics. It maintains a sequence of sketch "buckets" of geometrically increasing age. When two same-size buckets accumulate, they are merged pairwise. This keeps the total number of buckets logarithmic in the window size. `EHSketchList` provides a unified enum for inserting into and querying across heterogeneous bucket types.

`FoldCMS` and `FoldCS` are memory-efficient sub-window sketches designed specifically for EH integration. Instead of allocating a full W-column CMS per sub-window, they "fold" the column space to use far fewer physical columns when sub-window cardinality D is much smaller than W. Sub-windows at fold level k use W/2^k physical columns. When two buckets merge in the EH, an unfold-merge step doubles the physical column count, restoring accuracy. See [`docs/fold_sketch_design.md`](./fold_sketch_design.md) for the full algorithm.

`EHUnivOptimized` is an experimental two-tier EH that integrates `UnivMon` with sketch memory reuse (currently `Unstable`).

**When to use which**:

| Use Case | Recommended |
| --- | --- |
| Fixed non-overlapping epochs | `TumblingWindow` |
| Sliding window, standard memory | `ExponentialHistogram` + any mergeable sketch |
| Sliding window, sparse sub-windows (D << W) | `ExponentialHistogram` + `FoldCMS` or `FoldCS` |
| Sliding window + universal monitoring | `EHUnivOptimized` (Unstable) |

API references: [`docs/api/api_exponential_histogram.md`](./api/api_exponential_histogram.md), [`docs/api/api_tumbling_window.md`](./api/api_tumbling_window.md), [`docs/api/api_ehsketchlist.md`](./api/api_ehsketchlist.md), [`docs/api/api_fold_cms.md`](./api/api_fold_cms.md), [`docs/api/api_fold_cs.md`](./api/api_fold_cs.md)
