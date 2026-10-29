# Sketch analytics

This page defines the theoretical error, memory and time analytics asap_sketchlib provides for each sketch, as big O and as callable formulas.

## Conventions

Time is counted in ops, and one op is one read or one write of a counter, register, bucket, item, heap slot, list link or hash-table entry.
Every access counts, including a re-read, except that reading a cell and then writing it (as in `x += w`) counts once.
A hash-table probe counts as one read, and a probe followed by a write to the same entry counts once.
A swap of two slots counts 4, and temporary buffers of items count like state.
Comparing values already read is free, and sorting `p` items or taking the median of `p` values counts `p·lg p`, where `lg x` is `⌈log2 x⌉`.
Hashes, random draws, lengths and capacities are not ops, and neither is growing a container, which is amortized away.
Each op formula is the most one call can cost, unless its section marks it average.

- `n` is the number of updates a state has received, `N` their total weight, and `D` the distinct keys among them.
- `f(x)` is the total weight of key `x`, and `‖f‖₂` is the square root of the sum of `f(x)²` over all keys.
- For a sketch with `d` rows, estimating one key costs `est(d)` ops: `d` for Count-Min, which takes the minimum of `d` counters, and `2d + d·lg d` for Count Sketch, which copies them into a buffer and takes their median.
- A `TopKHeap` update costs `h(k) = 8 + 14·⌊log2 k⌋` for a heap of capacity `k`.
  It probes the heap's index, reads the root, and gives the root's slot, digest and index entries to the new key in 8 ops, or rescores a resident's slot in fewer.
  It then moves the key at most `⌊log2 k⌋` levels, and each level reads both children, swaps two slots, reads and swaps their digests, and repoints two index entries.

Counter matrices pick a column per row in one of two ways.

- On `RegularPath` each row computes its own hash and takes its low 32 bits modulo `w`.
- On `FastPath` each row takes a `lg w`-bit slice of one shared hash modulo `w`, and Count Sketch also takes one sign bit per row.
  The shared hash is 64 bits when the slices fit in 64, 128 bits when they fit in 128, and otherwise each row computes its own hash.
- `q` is the probability that two keys share a column.
  It is at most `1/w + 2^−32` on `RegularPath`.
  On `FastPath` it is `1/w` when `w` is a power of two, and at most `9/(8w)` otherwise, because a slice taken modulo `w` picks some columns twice as often.
  Count-Min bounds use `1/w`, as ASAPPlanner does.
- A median over `d` rows is wrong only when at least half the rows are wrong.
  With each row wrong independently with probability `β`, that happens with probability `P_med(d, β) = P(Binomial(d, β) ≥ ⌈d/2⌉)`.

Merges follow these rules unless a section says otherwise.

- A merge cost is the cost of merging one state into another, so merging `j` states costs `j − 1` merges.
- A merge requires equal parameters, hasher and admission rate, and disjoint inputs, meaning every update went to only one of the states.
- "Inputs may overlap" means an update may have gone to several states, and the merged state still equals one built from all the updates.
- HyperLogLogHip, ExponentialHistogram, ExponentialHistogramUnivMon, Octo, TumblingWindow and HashSketchEnsemble do not merge.

## API

Each sketch type implements one trait in `src/analytics`, and frameworks call their inner sketch's functions through it.

```rust
pub trait SketchAnalytics {
    type Params;
    type Data: Default;
    fn theoretical_memory_bytes(params: &Self::Params, data: &Self::Data) -> usize;
    fn theoretical_update_ops(params: &Self::Params, data: &Self::Data) -> f64;
    fn theoretical_query_ops(statistic: StatisticKind, params: &Self::Params, data: &Self::Data) -> Option<f64>;
    fn theoretical_merge_ops(params: &Self::Params, data: &Self::Data) -> Option<f64>;
    fn theoretical_error(statistic: StatisticKind, params: &Self::Params, data: &Self::Data, delta: Option<f64>) -> Option<ErrorBound>;
    fn params_for(statistic: StatisticKind, epsilon: f64, delta: f64, data: &Self::Data) -> Option<Self::Params>;
}

pub struct ErrorBound {
    pub metric: ErrorMetric,
    pub epsilon: f64,
    pub delta: Option<f64>,
    pub contract: &'static str,
}
```

- `Params` holds the constructor's parameters, such as `CountMinParams { rows, cols }`.
- `Data` holds the data inputs listed in the sketch's section, each as an `Option`.
  When an input is unknown, memory and ops use its upper bound from the section, and `theoretical_error` and `params_for` return `None` if they need it.
- `StatisticKind` is the enum in `src/capability.rs`.
- `theoretical_update_ops` is the cost of the plain `update` call, or of `update_weighted` for a sketch that has only that.
- `theoretical_query_ops`, `theoretical_merge_ops` and `theoretical_error` return `None` for what the sketch does not do or has no formula for.
- `theoretical_memory_bytes` calls the sketch's inherent `theoretical_memory_bytes`, or uses the section's formula where the sketch has none.
- A caller with parameters calls these functions directly, and a caller with a target `(ε, δ)` first calls `params_for`.
- `params_for` returns parameters from the section's sizing rule that meet `(ε, δ)`, or `None` when no parameters the constructor accepts can.
  Parameters the bound does not depend on come from `Params::default()`.

An `ErrorBound` says: with probability at least `1 − δ`, one estimate from one state is off by at most `ε`.
Several estimates hold together with probability at least `1 − Σδ`.

- Each contract says whether its `δ` is fixed by the parameters, as Count-Min's depth fixes it at `e^−d`, or chosen by the caller.
  With a fixed `δ`, `theoretical_error` returns that `δ` for `delta = None` or a `delta` at or above it, and `None` for a `delta` below it.
  With a chosen `δ`, it returns the smallest `ε` at `delta`, or at `DEFAULT_DELTA = 0.01` when `delta` is `None`.
- `ErrorBound.delta` is `None` when the analysis gives only a standard error.
- `ErrorMetric` has the variants of ASAPPlanner's `AccuracyMetric`: `Absolute` (off by at most `ε`), `Absolute(L1)` (at most `ε·N`), `Absolute(L2)` (at most `ε·‖f‖₂`), `Relative` (at most `ε` times the true value) and `Rank` (the returned value's rank is within `ε·n` of the requested one).
  `Absolute(L1)` stands for `Absolute { scale: Some(NormScale::StreamL1) }`, and likewise for L2.
- `contract` names the analysis, using ASAPPlanner's name where ASAPPlanner has the same analysis measured on the same implementation.
- A rank returned as a count is `Absolute(L1)`, and Bloom membership is `Absolute` with `ε = 0` and `δ` the false-positive rate.
- A TopK bound covers each reported count.
  The reported keys are chosen by their own estimates, so its `δ` is the per-key `δ` times the number of keys that could be reported.
  Whether TopK finds the true top keys has no bound.
- Every bound assumes nonnegative weights, no counter overflow and hash seeds chosen independently of the data.
  It covers only the updates a sketch admits under its admission rate, set with `with_admission_rate`.

## Summary

Query is the sketch's main readout, and each row's symbols are defined in that sketch's section.
"`d`, or 1" in the hashes column means `d` on `RegularPath` and 1 on `FastPath` when the slices fit.

| Sketch | Main error | Memory | Update | Query | Merge | Hashes |
|---|---|---|---|---|---|---|
| CountMinSketch | `ε = e/w`, `δ = e^−d`, `Absolute(L1)` | `O(dw)` | `O(d)` | `O(d)` | `O(dw)` | `d`, or 1 |
| CountSketch | `ε = √(q/β)`, `δ` chosen, `Absolute(L2)` | `O(dw)` | `O(d)` | `O(d log d)` | `O(dw)` | `d`, or 1 |
| CountSketchWithL2Norm | as CountSketch | `O(dw)` | `O(d)` | `O(d log d)` | `O(dw)` | 1 |
| CountMinHll | none | `O(dw·2^p)` | `O(d)` | `O(d·2^p)` | `O(dw·2^p)` | 2 |
| MicroCm | none | `O(dwT)` | `O(d + dw/I)` average | `O(dT)` | `O(dwT)` | 1 |
| SpaceSaving | `ε = 1/m`, `δ = 0`, `Absolute(L1)` | `O(m)` | `O(1)` | `O(1)` | `O(m log m)` | 1 |
| ElasticSketch | `ε = e/w`, `δ = e^−min(d, 5)`, `Absolute(L1)` | `O(H + dw)` | `O(d)` | `O(d)` | `O(Hd + dw)` | `1 + d` |
| CocoSketch | none | `O(dw)` | `O(d)` | `O(d)` | `O(d²w)` | `d` |
| Count-Min or Count Sketch with heap | as the base sketch | `O(dw + k)` | `O(est(d) + log k)` | `O(est(d))` | `O(dw + k(est(d) + log k))` | `2d + 1`, or 3 |
| Count-Min or Count Sketch with Space-Saving | as the base sketch | `O(dw + m)` | `O(d)` | `O(est(d))` | `O(dw + m log m)` | `d + 1`, or 2 |
| Count-Min or Count Sketch fold | as the base sketch | `O(d(W/2^F + min(W, D)) + k)` | `O(dE + est(d) + log k)` | `O(dE + est(d))` | `O(d(W/2^F + min(W, D)·E) + k(dE + est(d) + log k))` | `2d + 1` |
| HyperLogLog | `ε = 1.04/√m`, `Relative` | `O(m)` | `O(1)` | `O(m)` | `O(m)` | 1 |
| HyperLogLogHip | `ε = 0.833/√m`, `Relative` | `O(m)` | `O(1)` | `O(1)` | none | 1 |
| Kmv | `δ = e^(−Θ(ε²k))`, `Relative` | `O(k)` | `O(k)` | `O(1)` | `O(k²)` | 1 |
| SetAggregator | exact | `O(Dℓ)` | `O(ℓ)` | `O(1)` | `O(D)` | 1 |
| Bloom | false-positive rate | `O(rc)` bits | `O(r)` | `O(r)` | `O(rc)` | `r`, or 1 |
| Kll | `ε = 2.296/k^0.9723`, `Rank` | `O(k + m log(n/k))` | `O(log k)` average | `O(R log R)` | `O(R + B log B)` | 0 |
| KllDynamic | as Kll | `O(k + m log(n/k))` | `O(log k)` average | `O(R log R)` | `O(R log k)` | 0 |
| DdSketch | `ε = α`, `Relative` | `O(log(max/min)/α)` | `O(1)` | `O(B)` | `O(B)` | 0 |
| UniformSampling | none | `O(ρn)` | `O(ρn)` | `O(ρn)` | `O(ρn log(ρn))` | 0 |
| UnivMon | per readout | `O(L(dw + k))` | `O(d log d + log k)` average | `O(min(Lk, LD)·d log d)` | `O(L(dw + k(d log d + log k)))` | `1 + 2λ(L)` average |
| UnivMonTwoTier | as UnivMon | `O(L_e·d_e·w_e + max(L − L_e, 0)·d_m·w_m + Lk)` | as UnivMon | as UnivMon | as UnivMon | `1 + 2λ(L)` average |
| UnivMonQ | per readout | `O(d·Σw_j + Lk + M)` | `O(d log d + k log k + log M)` | `O(d·Σw_j + Lk(d log d + log k) + M log M)` | as query | 2, or 1 when `M = 0` |

## Counter matrices

### CountMinSketch

`src/sketches/countminsketch.rs`, including every storage and path, and `CountMinF64`.
Parameters: `d` rows and `w` columns, the code's `rows` and `cols`, plus counter type, path (`RegularPath` or `FastPath`) and hasher.
Data inputs: none.

| Item | Big O | Formula |
|---|---|---|
| ItemFrequency error | | `Absolute(L1)`, `ε = e/w`, `δ = e^−d` fixed; contract `count_min_l1_markov_v1` |
| ItemFrequency sizing | | `w = ⌈e/ε⌉`, `d = max(1, ⌈ln(1/δ)⌉)`; on `FastPath`, `w` rounded up to a power of two |
| Total error | | exact; contract `exact_v1` |
| Memory | `O(dw)` | `size_of::<Self>() + d·w·size_of::<Counter>()` |
| Update | `O(d)` | `d` |
| Query ItemFrequency | `O(d)` | `d` |
| Query Total | `O(w)` | `w` |
| Merge | `O(dw)` | `2dw` |

- With probability at least `1 − e^−d`, an estimate overcounts by at most `(e/w)·N`, and it never undercounts (Cormode and Muthukrishnan 2005, Theorem 1).
- An update adds to one counter per row, and a query reads the same counters and returns the smallest.
- Total sums row 0, and a merge reads every counter of the other state and adds it into this one.
- On `FastPath` with `w` not a power of two, `ε` grows to `e·(w + 3r)/(w + r)²` with `r = 2^(lg w) − w`, at most `1.125·e/w`.
- With per-row Nitro sampling (`nitro_update`) on, ItemFrequency has no bound, because the estimate is a median over rows sampled at rate `ρ`, and Total is an unbiased estimate.
  A Nitro update writes `ρ·d` counters on average.
- Integer counters wrap on overflow in release builds.

### CountSketch

`src/sketches/countsketch.rs`.
Parameters: `d` rows and `w` columns, the code's `rows` and `cols` and the paper's `t` and `b`, plus counter type, path and hasher.
Data inputs: none.

| Item | Big O | Formula |
|---|---|---|
| ItemFrequency error | | `Absolute(L2)`, `ε = √(q/β)`, `δ` chosen, with `β` the largest per-row rate with `P_med(d, β) ≤ δ`; contract `count_sketch_l2_median_chosen_row_rate_v1` |
| ItemFrequency sizing | | the odd `d ≤ 19` and width with `q ≤ β·ε²` that minimize `d·w`; on `FastPath`, `w` a power of two |
| Memory | `O(dw)` | `theoretical_memory_bytes(rows, cols)` |
| Update | `O(d)` | `d` |
| Query ItemFrequency | `O(d log d)` | `2d + d·lg d` |
| Merge | `O(dw)` | `2dw` |

- A row is off by more than `ε·‖f‖₂` with probability at most `q/ε²` (Charikar, Chen and Farach-Colton 2002, Section 3).
  Setting that to `β` gives `ε = √(q/β)`, and the median is wrong only when `⌈d/2⌉` rows are.
- At `δ = 0.01` the sizing takes `d = 5` and `w ≈ 9.5/ε²`.
- An update adds the weight with a random sign to one counter per row, and a query copies those counters into a buffer and takes their median.

### CountSketchWithL2Norm

`src/sketches/countsketch_l2_norm.rs`.
Parameters: `d` rows and `w` columns with `d·(lg w + 1) ≤ 128`, the code's `rows` and `cols`, plus a seed index.
Data inputs: none.

| Item | Big O | Formula |
|---|---|---|
| ItemFrequency error | | as CountSketch, with the `FastPath` `q` |
| FrequencyL2 error | | `Relative`, `ε = 1 − √(1 − √(20q))`, `δ = P_med(d, 0.1)` fixed, when `20q < 1`; contract `univmon_layer0_f2_median_chebyshev_binomial_v1` |
| FrequencyL2 sizing | | `w` the smallest width with `20q ≤ (1 − (1 − ε)²)²`, `d` the smallest with `P_med(d, 0.1) ≤ δ`; `None` when `d > 19` or `d·(lg w + 1) > 128` |
| Memory | `O(dw)` | `theoretical_memory_bytes(rows, cols)` |
| Update | `O(d)` | `3d` |
| Query ItemFrequency, FrequencyL2 | `O(d log d)` | `2d + d·lg d` |
| Merge | `O(dw)` | `3dw + d` |

- Each row also keeps the sum of its counters' squares, and FrequencyL2 is the square root of the median of those sums.
- By Chebyshev's inequality, a row's sum is off from `‖f‖₂²` by more than `√(20q)·‖f‖₂²` with probability at most 0.1, and the square root turns that into the `ε` above.
- The 19-row cap is ASAPPlanner's.
- `update_weighted` reads the counter, writes it back, and updates the row's sum.
- A merge reads the other counter, adds it into this one, reads the result back for its square, and writes each row's sum.

### CountMinHll

`src/sketches/countminsketch_hll.rs`, experimental.
Parameters: `d` rows, `w` columns (a power of two) and precision `p`, the code's `rows`, `cols` and `precision`; each cell is a HyperLogLog of `2^p` registers.
Data inputs: none.

| Item | Big O | Formula |
|---|---|---|
| KeyDistinct error | | none |
| Memory | `O(dw·2^p)` | `size_of::<Self>() + d·w·2^p` |
| Update | `O(d)` | `d` |
| Query KeyDistinct | `O(d·2^p)` | `d·2^p` |
| Merge | `O(dw·2^p)` | `2dw·2^p` |

- An update sets one register in one cell per row, and a query reads every register of those `d` cells.
- The readout is the smallest of `d` HyperLogLog estimates, and that minimum has no stated bound.
- Inputs may overlap.

### MicroCm

`src/sketches/microscope.rs`, experimental.
Parameters: `d` rows, `w` columns, `T` sub-windows and zoom base `c`, the code's `rows`, `cols`, `params.t` and `params.c`, plus a clock.
Data inputs: `I`, the updates per sub-window, the code's `items_per_sub_window` and the paper's `W/T`.

| Item | Big O | Formula |
|---|---|---|
| ItemFrequency error | | none |
| Memory | `O(dwT)` | `size_of::<Self>() + 4d·w·⌈(T + 7)/4⌉` |
| Update | `O(d + dw/I)` average | `5d + 5dw/I` while no cell has zoomed, with `I = 1` when unknown |
| Query ItemFrequency | `O(dT)` | `d(2T + 3)` |
| Merge | `O(dwT)` | `dw·(10(T + 2) + 7)` for `c = 2`, `dw·(8(T + 2) + 7)` otherwise |

- A cell holds `T + 2` one-byte pixels, a zoom byte and a 4-byte shutter, each one op.
- An update reads each row's shutter and zoom twice, adds to one pixel and writes the shutter, and a row that zooms out also rewrites its `T + 2` pixels and zoom.
- At each sub-window boundary every cell is rewritten: 5 ops while it has not zoomed, and more when it carries a shutter or zooms in.
- Zoom-out rounding goes both ways by an amount that depends on the data, so the estimate is neither an upper nor a lower bound.
- Merge also requires both states at the same sub-window.

## Heavy hitters

### SpaceSaving

`src/sketches/space_saving.rs`, Space-Saving on the Stream-Summary structure (Metwally et al. 2005).
Parameter: `m` counters, the code's `capacity`.
Data inputs: the bytes each key owns for memory, 0 when unknown, and the TopK size `k`, `m` when unknown.

| Item | Big O | Formula |
|---|---|---|
| ItemFrequency error | | `Absolute(L1)`, `ε = 1/m`, `δ = 0`; contract `space_saving_min_counter_v1` |
| ItemFrequency sizing | | `m = ⌈1/ε⌉` |
| TopK error | | each reported count as ItemFrequency |
| Memory | `O(m)` | `size_of::<Self>() + m·(80 + key bytes) + 40·s(m + 1) + hash_index_bytes::<(u64, Slot)>(m, m)` |
| Update | `O(1)` | 47 at unit weight, plus 2 per count bucket passed |
| Query ItemFrequency | `O(1)` | 4 |
| Query TopK | `O(k)` | `1 + 7·min(k, m)` |
| Merge | `O(m log m)` | `44m + 2m·lg(2m) + 13` |

- In one unmerged state, an estimate is off by at most `N/m`.
  The counters sum to `N`, so the smallest is at most `N/m`, a held key overcounts by at most the smallest counter, and a key not held has `f(x)` at most the smallest counter.
- Total has no accessor, so it has no formula.
- Each entry is 80 bytes plus its key, and each count bucket 32 bytes plus 8 on the free list.
  At most `m + 1` buckets exist at once, and `s(n)` is the slots a vector holds after doubling from 4 to `n`.
  The key index is std's `HashMap` reserved for `m` keys under churn.
- The summary links keys into buckets of equal count.
  An update of a held key moves its entry to the next count's bucket, creating it and dropping the old one if needed, at most 36 ops.
  A new key in a full summary also takes over the lowest bucket's first entry and swaps the keys in the index, at most 47.
- A unit update on an unmerged state passes no count bucket, and a weighted update or a new key seated in an under-full merged state may pass many.
- TopK walks down from the highest bucket, reading each bucket's count and links and each key's entry, and writing each reported key.
- A merge reads both summaries into a temporary map, sorts their up to `2m` entries by count, and seats the top `m` at 22 ops each.
- After a merge, ItemFrequency and TopK have no bound.
- Capacities may differ in a merge, which keeps this state's.

### ElasticSketch

`src/sketches/elastic.rs`.
Parameters: `H` heavy buckets with one flow each, and a light Count-Min of `d × w` `i32` counters, the code's `bucket_count`, `light_rows` and `light_cols`.
Data inputs: `D`, and the bytes each flow id owns for memory, 0 when unknown.

| Item | Big O | Formula |
|---|---|---|
| ItemFrequency error | | `Absolute(L1)`, `0 ≤ estimate − f(x) ≤ (e/w)·N` with `δ = e^−min(d, 5)` fixed; contract `elastic_one_sided_light_cms_v1` |
| ItemFrequency sizing | | `w = ⌈e/ε⌉`, `d = max(1, ⌈ln(1/δ)⌉)`, `None` for `δ < e^−5`; `H` is not set by the bound |
| TopK error | | each reported count as ItemFrequency, with `δ = D·e^−min(d, 5)`; `None` when that is at least 1 |
| TopK sizing | | as ItemFrequency at `δ/D` |
| Memory | `O(H + dw)` | `size_of::<Self>() + H·(40 + key bytes) + 4dw` |
| Update | `O(d)` | `2 + d` |
| Query ItemFrequency | `O(d)` | `1 + d` |
| Query TopK | `O(H(d + log H))` | `H·(5 + d) + 2H·lg H` |
| Merge | `O(Hd + dw)` | `H·(13 + 3d) + 2dw` |

- A flow that holds a heavy bucket and has never been evicted is counted exactly.
  Any other flow's error is the light Count-Min's error on the weight sent to the light part, so the Count-Min bound applies, derived from the update rule.
- Heavy routing uses hash seed 5 and light row `i` uses seed `i`, so light rows past the fifth add nothing to `δ`.
- The bound does not cover `update_heavy_only`, which drops weight, nor `compress_heavy` and `expand_heavy`, which pick flows by their light estimates.
- The bound holds while every flow's count is below `2^28`, where the eviction vote overflows `i32`.
- A heavy bucket is 40 bytes plus the key.
- An update reads and writes one heavy bucket, and adds to one light counter per row when the flow does not stay there.
  While copies left by `expand_heavy` remain, it reads the bucket once more.
- TopK reads each heavy bucket up to three times, estimates its flow, writes it to a buffer, and sorts the buffer twice.
- A merge clones and compares each pair of heavy buckets by estimating both flows, sends the loser into the light part, and adds the light counters.
- After a merge, no readout has a bound.

### CocoSketch

`src/sketches/coco.rs`.
Parameters: `d` rows and `w` columns, the code's `rows` and `cols`.
Data inputs: `D`, and the bytes each key owns for memory, 0 when unknown.

| Item | Big O | Formula |
|---|---|---|
| ItemFrequency, PartialKeySum error | | none |
| Memory | `O(dw)` | `size_of::<Self>() + 32d·w + min(d·w, D)·key bytes` |
| Update | `O(d)` | `d + 1` |
| Query ItemFrequency | `O(d)` | `d` |
| Query PartialKeySum | `O(dw)` | `dw` |
| Merge | `O(d²w)` | `dw·(d + 2)` |

- The paper guarantees only that estimates are unbiased, which gives no bound on one estimate.
- Total has no accessor, so it has no formula.
- `update_weighted` is the update, and it reads one bucket per row, then adds the weight to the smallest and may replace that bucket's key.
- PartialKeySum is `estimate_projected`, `estimate_with_udf` or `estimate_substring`, each reading every bucket once.
- A merge reads every bucket of the other state and replays each held key into this one as an update, computing `d` hashes each.

## Counter matrices with top-k

Each of these pairs a CountMinSketch or CountSketch, the base sketch, with a structure that tracks the top keys.
Each section covers both bases, through `est(d)` and the base sketch's `δ`.

### CountMinSketchWithHeap and CountSketchWithHeap

`src/sketches/countminsketch_heap.rs` and `src/sketches/countsketch_heap.rs`.
Parameters: `d` rows, `w` columns and heap capacity `k`, the code's `rows`, `cols` and `heap_size`, plus counter type and path.
Data inputs: `D`, and the bytes each key owns for memory, 0 when unknown.

| Item | Big O | Formula |
|---|---|---|
| ItemFrequency error | | as the base sketch, with its contract |
| ItemFrequency sizing | | as the base sketch; `k` is not set by the bound |
| TopK error | | each reported count as ItemFrequency at a per-key `δ` of `δ/D`; for Count-Min `δ = D·e^−d` fixed, `None` when that is at least 1; `None` without `D` |
| TopK sizing | | as ItemFrequency at `δ/D` |
| Memory | `O(dw + k)` | `size_of::<Self>() − size_of::<TopKHeap>() + d·w·size_of::<Counter>() + TopKHeap::theoretical_memory_bytes(k, key)` |
| Update | `O(est(d) + log k)` | `d + est(d) + h(k)` |
| Query ItemFrequency | `O(est(d))` | `est(d)` |
| Query TopK | `O(k)` | `k` |
| Merge | `O(dw + k(est(d) + log k))` | `2dw + 3 + u·(3 + est(d) + h(k))`, with `u ≤ 2·min(k, D)` the two heaps' sizes summed |

- On `Vector2D` storage the memory formula is the inherent `theoretical_memory_bytes(rows, cols, heap_size, key)`.
- An update adds to `d` counters, estimates the key from them, then updates the heap.
- TopK returns the heap's `k` entries unsorted.
- A merge adds the counters, copies both heaps' keys into a buffer, clears the heap, then re-estimates each key into it at this state's capacity.
- A key's stored count is its estimate at its own last update, and its true count has not changed since, so the stored count keeps the base sketch's bound.
- CountSketchWithHeap over integer counters stores the median truncated to an integer, which adds up to 0.5 to a reported count's error for even `d`.

### CountMinSketchWithSpaceSaving and CountSketchWithSpaceSaving

`src/sketches/countminsketch_space_saving.rs` and `src/sketches/countsketch_space_saving.rs`.
Parameters: `d` rows, `w` columns and `m` Space-Saving counters, the code's `rows`, `cols` and `capacity`, plus counter type and path.
Data inputs: `D`, and the bytes each key owns for memory, 0 when unknown.

| Item | Big O | Formula |
|---|---|---|
| ItemFrequency error | | as the base sketch, with its contract |
| ItemFrequency sizing | | as the base sketch; `m` is not set by the bound |
| TopK error | | each reported count as ItemFrequency at a per-key `δ` of `δ/min(m, D)`; for Count-Min `δ = min(m, D)·e^−d` fixed, `None` when that is at least 1; `None` without `D` |
| TopK sizing | | as ItemFrequency at `δ/min(m, D)` |
| Memory | `O(dw + m)` | `size_of::<Self>() − size_of::<SpaceSaving>() + d·w·size_of::<Counter>() + SpaceSaving::theoretical_memory_bytes(m, key bytes)` |
| Update | `O(d)` | `d + 47` at unit weight, plus 2 per count bucket passed |
| Query ItemFrequency | `O(est(d))` | `est(d)` |
| Query TopK | `O(m·est(d) + m log m)` | `m'·(5 + est(d)) + m'·lg m'`, with `m' = min(m, D)` |
| Merge | `O(dw + m log m)` | `2dw + 44m + 2m·lg(2m) + 13` |

- An update adds to `d` counters and offers the key to a SpaceSaving summary, whose update costs as in its section.
- TopK copies each key in the summary with its count into a buffer, estimates it from the counters into a second buffer, and sorts.
- A merge adds the counters and merges the summaries as SpaceSaving does, and requires equal `m`.
- With nonnegative integer weights, in one unmerged state, every key with `f(x) > N/m` is in the summary.
  `f64` weights enter the summary rounded up, so this does not hold for fractional weights.

### CountMinSketchFold and CountSketchFold

`src/sketches/count_min_fold.rs` and `src/sketches/count_sketch_fold.rs`.
Parameters: `d` rows, full width `W` (a power of two, at most 65536), fold level `F` and heap capacity `k`, the code's `rows`, `full_cols`, `fold_level` and `heap_size`.
Each row stores `W/2^F` cells, and each cell is a short list of (full column, count) entries.
Data inputs: `D`; `E`, the most entries one cell holds, at most `min(2^F, D)`; `S`, the entries in cells holding more than one, at most `d·min(W, D)`; and the bytes each key owns for memory, 0 when unknown.

| Item | Big O | Formula |
|---|---|---|
| ItemFrequency error | | as the base sketch on `RegularPath`, with `w = W` |
| ItemFrequency sizing | | as the base sketch on `RegularPath`, with `W` rounded up to a power of two; `None` when `W > 65536`; `F` and `k` are not set by the bound |
| TopK error | | as the heap variant, for a state not produced by `merge_same_level` |
| TopK sizing | | as ItemFrequency at `δ/D` |
| Memory | `O(d(W/2^F + min(W, D)) + k)` | `size_of::<Self>() − size_of::<TopKHeap>() + 24d·W/2^F + 32S + TopKHeap::theoretical_memory_bytes(k, key)` |
| Update | `O(dE + est(d) + log k)` | `2dE + d + est(d) + h(k)` |
| Query ItemFrequency | `O(dE + est(d))` | `dE + est(d)` |
| Query TopK | `O(k)` | `k` |
| Merge, same level | `O(d(W/2^F + min(W, D)·E) + k(dE + est(d) + log k))` | `dW/2^F + d·min(W, D)·(2 + E) + min(k, D)·(1 + dE + est(d) + h(k))` |

- A cell is 24 bytes, and each entry in a list is 16 bytes with the list grown by doubling, so at most 32.
- Reading a cell costs 1, and each listed entry it scans costs 1 more.
- An update scans one cell per row for its entry and adds to it, scans the same cells again to estimate the key, then updates the heap.
- A same-level merge reads each cell of the other state and adds each of its entries into this state's matching cell, then re-estimates only the other heap's keys, so this state's heap counts go stale.
- `hierarchical_merge` accepts mixed levels and costs `dW + Σ (dW/2^F_i + 2e_i + k_i·(1 + est(d) + h(k)))`, where state `i` is at level `F_i` and holds `e_i` entries and `k_i` heap keys.
- The fold level does not change the error.
- CountSketchFold truncates an even-`d` median to an integer, which adds up to 0.5 to its error.

## Cardinality and membership

### HyperLogLog

`src/sketches/hll.rs`, `HyperLogLogImpl` with the `Classic` or `Ertl` estimator.
Parameter: precision `p` with `m = 2^p` registers, the code's `precision` and Flajolet et al.'s `b`; 12, 14 and 16 are built in, and `impl_hll_registers!` adds others.
Data inputs: `max_distinct`, an upper bound on `D`.

| Item | Big O | Formula |
|---|---|---|
| Distinct error | | `Relative`, `ε = 1.04/√m`, `δ` unknown; contract `generic_hll_rse_only_no_confidence_v1` |
| Distinct error, `Classic` with `max_distinct` | | `Relative`, `δ = max over D ≤ max_distinct of λ^t/t!`, with `λ = D(D − 1)/(2m)` and `t = ⌊εD⌋ + 1`; contract `classic_hll_linear_counting_collision_bound_v1` |
| Distinct sizing | | the smallest `p` meeting the second contract when it applies, otherwise `p = ⌈2·log2(1.04/ε)⌉` |
| Memory | `O(m)` | `theoretical_memory_bytes(precision)` |
| Update | `O(1)` | 1 |
| Query Distinct | `O(m)` | `m` for `Classic`, `2m + 66 − p` for `Ertl` |
| Merge | `O(m)` | `2m` |

- `1.04/√m` is a standard error, not a bound with a probability (Flajolet et al. 2007, Theorem 1).
- The second contract applies when `max_distinct ≤ m/2`, `max_distinct ≤ 2^17`, `max_distinct/(2(m − max_distinct)) ≤ ε`, and `p` is from 4 to 18.
  There `Classic` always uses linear counting, and `λ^t/t!` bounds the chance that `t` keys land on registers already taken.
- `Ertl` does not use linear counting, so only the standard error applies to it.
- An update raises one register.
  A `Classic` query reads every register, and an `Ertl` query also fills and reads a histogram of `64 − p + 2` bins.
- Inputs may overlap.

### HyperLogLogHip

`src/sketches/hll.rs`, `HyperLogLogHipImpl`.
Parameter: precision `p` with `m = 2^p` registers, the code's `precision`.
Data inputs: none.

| Item | Big O | Formula |
|---|---|---|
| Distinct error | | `Relative`, `ε = √(ln 2)/√m ≈ 0.833/√m`, `δ` unknown; contract `hip_hll_rse_only_no_confidence_v1` |
| Distinct sizing | | `p = ⌈2·log2(0.833/ε)⌉` |
| Memory | `O(m)` | `theoretical_memory_bytes(precision)` |
| Update | `O(1)` | 4 |
| Query Distinct | `O(1)` | 1 |

- The HIP estimator is unbiased with variance about `ln 2·D²/m` (Cohen 2015, Ting 2014), so `ε` is a standard error, for a state built by inserts from one stream.
- An update reads one register, and when the register grows it writes it and three running totals.
- The running estimate depends on arrival order, so two states do not merge.

### Kmv

`src/sketches/kmv.rs`.
Parameter: `k ≥ 2`.
Data inputs: `n`, `D`.

| Item | Big O | Formula |
|---|---|---|
| Distinct error | | `Relative`, exact while `D < k`, `δ = exp(−ε²(k − 1)/((1 + ε)(2 + ε))) + exp(−ε²(k − 1)/(2(1 − ε)))`, the second term 0 for `ε ≥ 1`; contract `kmv_exact_below_k_chernoff_v1` |
| Distinct sizing | | the smallest `k` meeting `δ`, such as 4323 at `ε = 0.05`, `δ = 0.01` |
| Memory | `O(k)` | `theoretical_memory_bytes(k, distinct)` |
| Update | `O(k)` | `k + 3 + 8·⌊log2 k⌋` |
| Query Distinct | `O(1)` | 2 |
| Merge | `O(k²)` | `k·(k + 4 + 8·⌊log2 k⌋)` |

- The sketch keeps the `k` smallest hashes, and the estimate is `k − 1` divided by the largest of them scaled to (0, 1) (Beyer et al. 2007).
  The two terms of `δ` are Chernoff bounds on overestimating and underestimating by more than `ε`.
- An update reads the largest kept hash and stops if the sketch is full and the new hash is larger.
  Otherwise it reads the kept hashes one by one looking for a duplicate, then adds the hash to the heap, which costs 8 ops per level it moves.
- With every key equally frequent and arriving in random order, the duplicate scan reads about `D/2` hashes on average when `D ≤ k` and about `k²/(2D)` when `D > k`.
- The heap starts at `min(k, 1024)` slots and doubles, so it can hold up to about `2k`.
- A merge offers each of the other state's hashes as an update, and inputs may overlap.

### SetAggregator

`src/sketches/set_aggregator.rs`.
No parameters.
Data inputs: `D`, and the mean key length `ℓ` in bytes.

| Item | Big O | Formula |
|---|---|---|
| Distinct, Membership error | | exact; contract `exact_v1` |
| Memory | `O(Dℓ)` | `size_of::<Self>() + hash_map_bytes::<String>(D) + D·ℓ` |
| Update | `O(ℓ)` | 1 |
| Query Distinct, Membership | `O(1)` | 1 |
| Merge | `O(D)` | one per key of the other state |

- An update probes the set once, after copying the key into a new `String`.
- Inputs may overlap.

### Bloom

`src/sketches/bloom.rs`, partitioned.
Parameters: `r` slices and `c` bits per slice, the code's `rows` and `cols` and Kirsch and Mitzenmacher's `k` and `m'`.
Data inputs: `D`.

| Item | Big O | Formula |
|---|---|---|
| Membership error | | `Absolute`, `ε = 0`, `δ = (1 − (1 − 1/c)^D)^r` for a power-of-two `c` up to `2^32`; contract `bloom_partitioned_fpr_v1` |
| Membership sizing | | the `r ≤ 20` and power-of-two `c` that minimize `r·c` and meet `δ` |
| Memory | `O(rc)` bits | `size_of::<Self>() + 8r·⌈c/64⌉` |
| Update | `O(r)` | `r + 1` on `RegularPath`, `2r + 1` on `FastPath` |
| Query Membership | `O(r)` | `r` |
| Merge | `O(rc)` | `2r·⌈c/64⌉ + 2` |

- An inserted key always reads as present, and a key that was not reads as present with probability exactly `δ`, if it was chosen without knowing the hash seeds.
- For other `c` the columns are not equally likely, so `theoretical_error` returns `None`.
- `dimensions_for(n, δ)` meets this rate without minimizing `r·c`, and is not `params_for`.
- An update sets one bit per slice and adds to the inserted count, and a query reads one bit per slice, stopping at the first zero on `RegularPath`.
- A merge ORs the bit arrays a 64-bit word at a time, and inputs may overlap.

## Quantiles and sampling

### Kll

`src/sketches/kll.rs`.
Parameters: `k`, `m` (default 8) and a seed; the paper has no `m`, and its capacity ratio `c` is fixed at 2/3.
Data inputs: `n`.

Kll keeps items in levels, and when a level fills it sorts it, keeps every other item at double weight, and passes them up.
The code clamps `m` into [2, 26602] and then `k` into [`m`, 26602], and below `k` and `m` mean the clamped values.
Level `i` from the top holds at most `cap(i) = max(⌈k·(2/3)^i⌉, m)` items.
After `n` updates there are `L(n)` levels, the paper's `H`: 1 when `n ≤ k` and `2 + ⌊log2(n/(k + 1))⌋` otherwise.
The sketch keeps `R(n) = min(n, cap(0) + … + cap(L(n) − 1))` items, at most `3k + (m + 1)·L(n)`, and the bottom level holds at most `c_b = cap(L(n) − 1)`.

| Item | Big O | Formula |
|---|---|---|
| Quantile error | | `Rank`, `ε = 2.296·s(δ)/k^0.9723`, `δ` chosen; contract `kll_quantile_empirical_99_v1`, or `kll_quantile_empirical_99_delta_scaled_v1` for `δ < 0.01` |
| Quantile sizing | | `k = ⌈(2.296·s(δ)/ε)^(1/0.9723)⌉`, `m = 8`, `None` when `k > 26602` |
| Rank error | | none, see open questions |
| Memory | `O(k + m·log(n/k))` | `theoretical_memory_bytes(k, m, n)` |
| Update | `O(log k)` average | `26 + lg(c_b + 1)` |
| Query Quantile | `O(R log R)` | `3R + R·lg R + lg R` |
| Query Rank | `O(R)` | `R` |
| Merge | `O(R + B log B)` | `B·lg B + 25R − 11B + 10L + 2(L_a + L_b) + 74`, with `R = R_a + R_b` and `B` the two bottom levels' sizes summed |

- `2.296/k^0.9723` is an empirical fit: with 99% probability, the returned value's rank is within `ε·n` of the requested rank.
  On asap_sketchlib it measures at 0.81 to 0.95 times the fit with `m = 8` and at 1.07 times with `m = 2`, so `theoretical_error` returns `None` for `m < 8`.
- `s(δ)` is 1 for `δ ≥ 0.01`.
  Below that it is `z(1 − δ/2)/z(1 − 0.005)`, with `z` the standard normal quantile, which assumes the error is Gaussian and is not measured, and the second contract name marks this.
- An update appends to the bottom level at 6 ops, and when that level is full it sorts it, halves it into the level above, and repeats while levels overflow.
  Averaged over updates, the sort costs `lg(c_b + 1)` and the halving and merging at most 20.
- The worst single update compacts every level, at most `(c_b + 1)·lg(c_b + 1) + 12R − 4c_b + 15L + 18 + (L − 1)(L + 2)/2`.
- A quantile query copies the `R` items, sorts them by value, accumulates their weights, and binary-searches, and `quantile_cached` reuses that until the next update at `lg R` per query.
- A merge copies both states into one buffer, merges each level, sorts the bottom level, halves each overflowing level once, and writes back all 62 level boundaries.
- Quantiles 0 and 1 are exact only before the first compaction, because the extremes are not stored.
- Merge also requires independently seeded inputs, since states built with the same constant seed make the same coin flips.

### KllDynamic

`src/sketches/kll_dynamic.rs`.
Parameters, data inputs and clamping as Kll.

| Item | Big O | Formula |
|---|---|---|
| Quantile error | | as Kll |
| Memory | `O(k + m·log(n/k))` | `size_of::<Self>() + 8·max(3k, 2(R(n) + 1)) + 16·(L(n) + 1)` |
| Update | `O(log k)` average | `5 + lg(c_b + 1) + lg(1.5k)` |
| Query Quantile | `O(R log R)` | `3R + R·lg R + lg R` |
| Query Rank | `O(R)` | `R` |
| Merge | `O(R log k)` | each level's size times its `lg`, summed over both states, plus `25R − 6B + 11L + 2(L_a + L_b) + 11` |

- Every compaction sorts its level again, including the top level of about `1.5k` items, and a merge sorts every level of both states first.
- Merge requirements are Kll's.

### DdSketch

`src/sketches/ddsketch.rs`.
Parameter: `α` in (0, 1), the code's `alpha`, with `γ = (1 + α)/(1 − α)`.
Data inputs: `n`, the smallest and largest magnitude, whether negative values occur, and the largest absolute value `A`.

| Item | Big O | Formula |
|---|---|---|
| Quantile error | | `Relative`, `ε = α`, `δ = 0`; contract `ddsketch_relative_error_alpha_v1` |
| Quantile sizing | | `α = ε` |
| QuantileInterpolated error | | `Relative`, `ε = α` when all values share a sign, contract `ddsketch_interpolated_relative_alpha_v1`; otherwise `Absolute`, `ε = α·A`, contract `ddsketch_interpolated_absolute_alpha_v1` |
| Total, Min, Max error | | exact; contract `exact_v1` |
| Sum error | | `Absolute`, `ε = (n − 1)·2^−53` times the sum of absolute values, `δ = 0`; contract `ddsketch_f64_sum_rounding_v1` |
| Memory | `O(log(max/min)/α)` per sign | `theoretical_memory_bytes(alpha, n, min_magnitude, max_magnitude, negatives)` |
| Update | `O(1)` | 6 |
| Query Quantile | `O(B)` | `B` |
| Query Total, Sum, Min, Max | `O(1)` | 1 |
| Merge | `O(B)` | `B_a + B_b` plus the slots between their lowest and highest buckets |

- For every quantile level `φ`, the returned value is within `α` times the true `⌈φ·n⌉`-th value, because the store never collapses (Masson, Rim and Lee 2019).
- A value `x` goes to bucket `⌊log_γ x⌋`, whose representative is `γ^i·(1 + α)`.
  A store spans `s` buckets from the smallest to the largest magnitude and holds at most `2(s + 126)` slots, the 126 being growth headroom, and `B` is the slots of both signs' stores.
- Magnitudes below about `2e−308` read as 0, and magnitudes above the largest indexable value are dropped, so both are outside the bound.
- After `apply_delta`, which adds another state's bucket counts, Sum is within `α` times the sum of absolute values, and Min and Max are within `α` relative.
- An update adds to the count, sum, min and max, reads the store's offset, and adds to one bucket.
- A quantile query walks the slots until the cumulative count passes `φ·n`.
- Merge requires `α` equal within `1e−12`.

### UniformSampling

`src/sketches/uniform.rs`, experimental.
Parameters: sample rate `ρ` in (0, 1] and a seed.
Data inputs: `n`, and the sample holds `s = ⌈ρ·n⌉` items.

| Item | Big O | Formula |
|---|---|---|
| Sample error | | none |
| Total error | | exact; contract `exact_v1` |
| Memory | `O(ρn)` | `48 + 16·max(4, 2s)` |
| Update | `O(ρn)` | `2 + lg(s + 1) + s` |
| Query Sample | `O(s)` | `s` |
| Merge | `O(s log s)` | `(s_a + s_b)·lg(s_a + s_b)` |

- The sample is not uniform.
  When `⌈ρ·t⌉` grows at arrival `t`, that arrival is always kept, and an item dropped earlier never comes back.
  At `n = 1000` and `ρ = 0.1`, an item's chance of being kept rises from 0.077 in the first tenth of the stream to 0.125 in the last.
- An update binary-searches the sorted sample, shifts later items to insert, and may drop the last one.
- Merge requires `ρ` equal within `f64::EPSILON`.
- `new()` uses a fixed seed, so two samplers built with `new()` make the same choices at the same positions, and their merge is correlated.

## UnivMon family

UnivMon keeps `L` layers, and an update goes to layer 0 and to each further layer with probability one half, so it reaches `λ(L) = 2 − 2^(1−L)` layers on average.

### UnivMon

`src/sketch_framework/univmon.rs`.
Parameters: heap capacity `k`, `d` rows, `w` columns and `L ≤ 19` layers, the code's `heap_size`, `rows`, `cols` and `layers`; each layer is a `CountSketchWithL2Norm` and a heap of `k` keys.
Data inputs: `D_max`, an upper bound on `D`, and the key for memory.

| Item | Big O | Formula |
|---|---|---|
| Total error | | exact; contract `univmon_unit_update_total` |
| FrequencyL2 error | | as CountSketchWithL2Norm on layer 0 |
| Distinct error | | `Relative`, `ε = √(1.35/(k·(δ − ψ)))`, `δ` chosen, `k ≥ 8`; contract `univmon_distinct_complete_top_layer_chebyshev_v1` |
| Distinct sizing | | the smallest `k ≥ 8` with `1.35/(k·ε²) + (e/4)^(k/2) ≤ δ`, and `L ≥ lg(D_max/k) + 2`; `None` when that needs `L > 19` |
| FrequencyEntropy, GSum error | | none |
| Memory | `O(L(dw + k))` | `theoretical_memory_bytes(heap_size, rows, cols, layers, key)` |
| Update | `O(d log d + log k)` average | `1 + λ(L)·(5d + d·lg d + 9 + 16·⌊log2 k⌋)` |
| Query Total | `O(1)` | 1 |
| Query FrequencyL2 | `O(d log d)` | `2d + d·lg d` |
| Query Distinct, GSum, FrequencyEntropy | `O(min(Lk, LD)·d log d)` | `min(Lk, L·D)·(1 + 2d + d·lg d)` |
| Merge | `O(L(dw + k(d log d + log k)))` | `2 + L·(3dw + d + 3 + k·(18 + 4d + 2d·lg d + 30·⌊log2 k⌋))` |

- Distinct uses a Chebyshev bound on the heap estimator, whose variance is at most `1.35·D²/k`.
- `ψ` is the probability that the top layer sees more than `k` keys: 0 when `D_max ≤ k`, `(e/4)^(k/2)` when `L ≥ lg(D_max/k) + 2`, and 1 otherwise.
  `theoretical_error` returns `None` when `delta ≤ ψ`.
- A merged state keeps the Distinct bound, because its top layer overflows only if the union's does.
- Entropy and G-sum are computed from the layers' heaps, and nothing proves those heaps are large enough for the paper's guarantee.
- The bounds cover states built with `update`, and `theoretical_error` returns `None` for a state built with `update_weighted_terminal`.
- `L ≤ 19` because hash seed 19 selects the layers.
- An update adds to the running total, then in each layer it reaches updates `d` counters and row sums, estimates the key, updates that layer's heap, and records whether the heap is full.
- Distinct, GSum and Entropy re-estimate every key in every layer's heap.
- A merge rebuilds each layer as CountSketchWithL2Norm and the heap variants do, and requires the same update mode or one side empty.

### UnivMonTwoTier

`src/sketch_framework/univmon_two_tier.rs`.
Parameters: `k`, `L_e` elephant layers, elephant dimensions `d_e × w_e`, mouse dimensions `d_m × w_m` and `L ≤ 19` layers.
Layer `j` uses the elephant dimensions when `j < L_e` and the mouse dimensions otherwise.
Data inputs as UnivMon.

| Item | Big O | Formula |
|---|---|---|
| Error, every readout | | as UnivMon, with layer 0's dimensions for FrequencyL2 |
| Memory | as the summary | `size_of::<Self>()`, plus each layer's `CountSketchWithL2Norm::theoretical_memory_bytes`, plus `L·TopKHeap::theoretical_memory_bytes(k, key)` |
| Update | as UnivMon | `1 + λ_e·(5d_e + d_e·lg d_e + 9 + 16·⌊log2 k⌋) + λ_m·(5d_m + d_m·lg d_m + 9 + 16·⌊log2 k⌋)` average |
| Query, Merge | as UnivMon | as UnivMon, with each layer's own `d` and `w` |

- An update reaches `λ_e = 2 − 2^(1 − min(L_e, L))` elephant layers and `λ_m = λ(L) − λ_e` mouse layers on average.

### UnivMonQ

`src/sketch_framework/univmon_q.rs`.
Parameters, all fields of `UnivMonQConfig`: `L` layers, top-layer width `W`, halving period `P` (0 for none), `d` odd rows, `b` counter bits, heap capacity `k` and `M` ordered samples, the config's `layers`, `top_layer_cols`, `cols_halving_period`, `rows`, `counter_bits`, `heap_size` and `ordered_samples`, plus a seed index.
Layer `j` has width `w_j = max(W >> ⌊j/P⌋, min(k, W))`, or `W` when `P = 0`, and `w_min` is the narrowest.
Updates are unit-weight `f64` values, and each update goes to exactly one layer.
Data inputs: `n`.

| Item | Big O | Formula |
|---|---|---|
| Total, Min, Max error | | exact; contract `exact_v1` |
| ItemFrequency error | | `Absolute(L2)`, `ε = √(3/w_min)`, `δ = P_med(d, q·w_min/3)` with `q ≤ min(1/w_min + 2^(−lg W), 9/(8·w_min))`; contract `univmonq_terminal_count_sketch_l2_median_binomial_v1` |
| Quantile error | | `Rank`, `ε = √(ln(2/δ)/(2·min(M, n)))`, `δ` chosen, when the heavy set is empty; contract `univmonq_rank_diffuse_occurrence_hoeffding_v1` |
| Rank error | | `Absolute(L1)`, the same `ε` and condition |
| Quantile, Rank sizing | | `M = ⌈ln(2/δ)/(2ε²)⌉` |
| FrequencyL2, Distinct, GSum, FrequencyEntropy, TopK error | | none |
| Memory | `O(d·Σw_j + Lk + M)` | `size_of::<Self>() + L·size_of::<Layer>() + d·(b/8)·Σw_j + 50Lk + 24M` |
| Update | `O(d log d + k log k + log M)` | `18 + 3d + d·lg d + 10·⌊log2 k⌋ + k·(11 + 10·⌊log2 k⌋) + (10 + 10·⌊log2 M⌋ when M > 0)` |
| Query Total, Min, Max | `O(1)` | 1 |
| Query ItemFrequency | `O(d log d)` | `2d + d·lg d` |
| Query every other readout | as the summary | rebuilds every layer's candidate keys first |
| Merge | as the summary | rebuilds the candidate keys and merges the counters and ordered samples |

- ItemFrequency reads only the layer the key went to, whose columns are `lg W` hash bits modulo `w_j`.
- Rank and Quantile read a uniform sample of `M` updates, so Hoeffding's inequality for sampling without replacement gives the bound.
  The heavy set is the frequent values the readout handles separately, and when it is not empty the error depends on the data.
- FrequencyL2, Distinct and GSum come from candidate keys cut off by a threshold, so UnivMon's bounds do not carry over.
- An update adds to the count, min, max and sequence number, adds to one counter per row in its layer, estimates the key, updates the candidate heap, and adds to the ordered sample.
- An update of a key already in the candidate heap costs `5 + 3d + d·lg d` plus the ordered sample, and the `k` term comes from re-queuing up to `k` stale heap entries when a new key enters a full layer.
- Merge also requires the same seed index and a distinct source id, the id of the stream a state was built from, on each input.

## Frameworks

A framework's formulas use its inner sketch's trait functions.
`U_in`, `Q_in`, `M_in` and `mem_in` are the inner update, query and merge ops and memory, `ε_in` and `δ_in` the inner error bound, and `C_in` the inner sketch's counters, registers or items, which a clone or clear touches.
A Kll bound needs independently seeded states, so a framework that merges Kll states cloned from one prototype or built from one seed gives its Kll readouts no bound.

### Hydra

`src/sketch_framework/hydra.rs`.
It holds a `d × w` grid of cells, each a Count-Min, Count Sketch, HyperLogLog (`Ertl`), Kll or UnivMon.
Parameters: `d` rows, `w` columns and schema arity `a ≤ 16`, the code's `rows`, `cols` and the length of `schema` and the paper's `r`, `w` and `D`, plus the cell's parameters.
An update goes to `2^a − 1` subkeys, one per nonempty subset of the schema's fields, and a subpopulation `Q` is one subkey value.
Data inputs: `N`, `n`, the label bytes, and lower bounds on `Q`'s weight `N_Q`, distinct count `D_Q` and `‖f_Q‖₂` for the readouts that use them.

Cells use the `FastPath` column rule.
In each row, other subkeys that share `Q`'s cell add at most `4N(2^a − 1)/w` weight to it with probability at least `1 − q·w/4`, by Markov's inequality.
A query takes the median over rows, which fails with probability `P_med(d, q·w/4)`, and the cells it reads must also hold, which adds `d·δ_in`.

| Readout | Bound | δ | Contract |
|---|---|---|---|
| Count-Min frequency | `Absolute(L1)`, `ε = 4(2^a − 1)(1 + ε_in)/w + ε_in` | `P_med(d, q·w/4) + d·δ_in` | `hydra_cm_cell_markov_median_binomial_v1` |
| Count-Min group total | `Absolute(L1)`, `ε = 4(2^a − 1)/w`, never under | `(q·w/4)^d` | `hydra_cm_group_total_markov_min_v1` |
| Count Sketch frequency | `Absolute(L1)`, `ε = 4(2^a − 1)(1 + ε_in)/w + ε_in` | `P_med(d, q·w/4) + d·δ_in` | `hydra_cs_cell_markov_median_binomial_v1` |
| Kll Rank, Quantile | `Absolute`, `ε = 4N(2^a − 1)/(w·N_Q) + ε_in`, on the rank scaled to [0, 1] | `P_med(d, q·w/4) + d·δ_in` | `hydra_kll_cell_mixture_rank_median_binomial_v1` |
| HyperLogLog Distinct | `Relative`, `ε = ε_in + (1 + ε_in)·4n(2^a − 1)/(w·D_Q)` | unknown | `hydra_hll_cell_collision_median_binomial_v1` |
| UnivMon Total | `Absolute(L1)`, `ε = 4(2^a − 1)/w` | `P_med(d, q·w/4)` | `hydra_univmon_total_markov_median_binomial_v1` |
| UnivMon FrequencyL2 | `Relative`, `ε = ε_in + (1 + ε_in)·4N(2^a − 1)/(w·‖f_Q‖₂)` | `P_med(d, q·w/4) + d·δ_in` | `hydra_univmon_l2_collision_median_binomial_v1` |
| UnivMon Distinct | as HyperLogLog Distinct | `P_med(d, q·w/4) + d·δ_in` | `hydra_univmon_distinct_collision_median_binomial_v1` |
| Kll group total, UnivMon FrequencyEntropy | none | | |

- A HyperLogLog cell counts colliding updates, not their weight, so its term uses `n`, and its `δ` is unknown because `Ertl` gives only a standard error.
- Hydra never merges its own cells, so Kll cells keep their bound.
- `params_for` gives half of `ε` to collisions, `ε_in = (ε/2)/(1 + ε/2)` to the cells, and splits `δ` the same way.
  It sets `w` to the smallest power of two that keeps the collision term at most `ε/2`, `d` to the smallest odd depth with `P_med(d, 1/4) ≤ δ/2`, and the cells to the inner `params_for(ε_in, δ/(2d))`.
- Memory: `size_of::<Hydra>() + d·w·size_of::<HydraCounter>() + (d·w + 1)·(mem_in − size_of::<Inner>())` plus the label bytes.
- An update writes one cell per row for each subkey, `(2^a − 1)·d·U_in`, and a Kll cell repeats its update once per unit of weight.
- An update computes `(2^a − 1)` grid hashes, `d` each when the slices do not fit, plus the cells' own hashes, since each cell hashes the value again.
- A query reads one cell per row and one prototype cell, then takes the median, `(d + 1)·Q_in + d·lg d`.
- A merge merges every cell, `d·w·M_in`, and merging two Hydras with Kll cells gives those readouts no bound.
- Kll cells require nonnegative weights.

### ExponentialHistogram

`src/sketch_framework/eh.rs`.
It keeps an inner sketch per bucket over a sliding window and merges older buckets together.
Parameters: `k` and the window length, the code's `k` and `window` (in time units, where the paper's `N` counts elements).
It merges buckets by their L2 mass when the inner sketch is `CountSketchWithL2Norm` or `UnivMon`, and by their arrival count otherwise.
Data inputs: `N_w` the arrivals in the window, `D_w` the distinct keys in the window, and `G` the number of buckets.

| Readout | Bound | δ | Contract |
|---|---|---|---|
| Count-Min or Count Sketch frequency, by count | `Absolute(L1)`, `ε = ε_EH + ε_in(1 + ε_EH)` | `δ_in` | `eh_l1_boundary_bucket_v1` |
| HyperLogLog Distinct, by count | `Relative`, `ε = ε_in + (1 + ε_in)·ε_EH·N_w/D_w` | unknown | `eh_l1_boundary_bucket_v1` |
| L2 readouts, by mass | `Relative`, `ε = ε_EH2 + ε_in(1 + ε_EH2)` | `(G + 2)·δ_mass + δ_in` | `eh_l2_boundary_bucket_mass_v1` |
| Kll; DdSketch Quantile, Min and Max; CocoSketch; ElasticSketch; UniformSampling | none | | |

- Merging by count, the oldest bucket holds at most `ε_EH = 2/(⌈k/2⌉ + 1)` of the window, about `4/k`, and it is kept or dropped whole.
  This counts arrivals, so these bounds require unit-weight updates.
- Merging by mass compares squared L2 estimates, so `ε_EH2 = ((1 + ε₂)/(1 − ε₂))·√(2/k)`, where `ε₂` is the inner L2 estimate's error and `δ_mass` its failure probability.
- Every bucket starts as a clone of one prototype, so Kll buckets get no bound.
- The bounds hold for windows that end at the newest arrival, and a query endpoint that falls in a gap between two buckets selects the wrong bucket.
- Merging by count keeps at most `G ≤ (⌊k/2⌋ + 2)(⌊log2(2N_w)⌋ + 1)` buckets.
- Memory: `size_of::<EH>() + G·size_of::<EhBucket>() + (G + 1)·(mem_in − size_of::<Inner>())`.
- An update clones the prototype into a new bucket (`C_in`), updates it (`U_in`) and scans the buckets (`G`).
  Merging by count adds at most one merge per update on average, `C_in + M_in + G`.
  Merging by mass, with `E` the cost of one L2 estimate, an update that triggers `g` merges costs `C_in + U_in + E + (2g + 1)·G + g·(C_in + M_in + E)`.
- A query clones one bucket and merges the others in the range into it, at most `2G + C_in + G·M_in`.

`EhBucketSketch` (`src/sketch_framework/eh_bucket_sketch.rs`) wraps one inner sketch, and its analytics are that sketch's.

### ExponentialHistogramUnivMon

`src/sketch_framework/eh_univmon.rs`, experimental.
It is an exponential histogram that merges by L2 mass, with exact `HashMap` buckets for recent data and UnivMon buckets for older data.
Data inputs: `G_s` the UnivMon buckets, and the keys in the exact buckets.

- Only FrequencyL2, as `Relative`, and ItemFrequency, as `Absolute(L2)`, have bounds, because only L2 mass decides the merges.
- From the exact buckets, `ε = √(2/k)` and `δ = 0`, the whole error coming from the oldest bucket being kept or dropped whole; contract `eh_univmon_exact_boundary_bucket_v1`.
- From the UnivMon buckets, `ε = ε_EH2 + ε_in(1 + ε_EH2)` and `δ = (G_s + 2)·δ_mass + δ_in`; contract `eh_univmon_l2_boundary_bucket_v1`.
- Memory: the UnivMon pool's allocated sketches times `mem_in`, plus the exact buckets' keys.
- An update inserts into a map, scans and merges the exact buckets, and when the oldest exact bucket moves to UnivMon it inserts its keys, `keys·U_in`.
- A query over the UnivMon buckets clones one and merges the rest, `C_in + G_s·M_in`.

### NitroBatch

`src/sketch_framework/nitro.rs`.
It admits each update with probability `ρ`, the code's `rate` and the paper's `p`, into a Count-Min or Count Sketch, and an admitted update writes all `d` rows with weight about `1/ρ`.
Data inputs: `N`.

| Readout | Bound | δ | Contract |
|---|---|---|---|
| Count-Min frequency | `Absolute(L1)`, `ε = ε_s + ε_in(1 + ε_s)` with `ε_s = √(2V/(N·(δ − P_med(d, q·w/e))))` | `δ` chosen | `nitro_batch_item_sampling_chebyshev_v1` |
| Count Sketch frequency | none | | |

- `V = (1 − ρ)/ρ + ρ·u(1 − u)`, with `u` the fractional part of `1/ρ`, is the sampling variance per unit of count, at most `1/ρ`.
- The estimate is a median over rows, so Count-Min's `e^−d` does not apply, and `ε_in = e/w`.
- The bound covers `update_batch`, not `update_batch_cached_step`, and states built with the same seed admit the same positions.
- `theoretical_error` returns `None` when `delta ≤ P_med(d, q·w/e)` or `N` is unknown.
- Memory: `size_of::<NitroBatch<S>>() + mem_in − size_of::<Inner>()`.
- An update writes `ρ·d` counters on average, a query reads `d` and takes their median, `2d + d·lg d`, and a merge costs `d·w`.

### Octo

`src/sketch_framework/octo.rs`, behind the `octo-runtime` feature.
`K` workers keep 1-byte counters and send a cell's count to a parent sketch when it reaches a threshold, which adapts between `τ_min` and `τ_max ≤ 127`.
`K` is the code's `num_workers` and the paper's `k`, the thresholds are `min_threshold` and `max_threshold`, and `K'` is 1 when `partition` splits keys by hash and `K` when it goes round robin.

| Parent | After the queues drain, before a flush | After flush |
|---|---|---|
| Count-Min | `Absolute`, `ε = ε_in·N + K'·(τ_max − 1)`, `δ = δ_in`; contract `octo_cm_residual_additive_v1` | the inner bound |
| Count Sketch | `Absolute`, `ε = ε_in·‖f‖₂ + K·(τ_max − 1)`, `δ = δ_in`; contract `octo_cs_residual_additive_v1` | the inner bound |
| Count-Min or Count Sketch with heap | as Count-Min or Count Sketch | the residual term remains, because these workers never flush |
| HyperLogLog | the inner bound when the threshold is 0, otherwise none | the inner bound |
| DdSketch | none | the inner bound |
| UnivMon, Coco, Elastic | none | none |

- Before a flush the parent misses what is still below threshold in the workers, at most `τ_max − 1` per worker per cell, and a read before the queues drain also misses what is in them.
- Memory: `mem_in + K·(size_of::<Worker>() + worker counter bytes) + K·queue_capacity·(size_of::<WorkerMsg>() + size_of::<Delta>() + 16)`.
- An update writes `d` worker counters, and the parent receives at most `d/τ_min` writes per update on average.
- A query is the parent's `Q_in`.

### TumblingWindow

`src/sketch_framework/tumbling.rs`.
It keeps up to `max_windows` closed windows of an inner sketch, aligned to multiples of `window_size`, plus the active one, and takes sketches from a pool of `pool_initial_size`.

- A query merges the chosen windows, so its error is the inner bound over exactly their updates.
- Memory: `size_of::<TumblingWindow>() + A·mem_in`, with `A = max(pool_initial_size, max_windows + 2)`.
- An update is `U_in`, and each window boundary it crosses adds a pool take, a shift of `max_windows` entries and a clear (`C_in`).
- `query_recent(t)` clones the active window and merges up to `t` closed windows into it, `C_in + min(t, max_windows)·M_in`.
- Kll and UnivMonQ windows ignore the weight.

### HashSketchEnsemble

`src/sketch_framework/hashlayer.rs`.
A list of Count-Min, Count Sketch and HyperLogLog sketches that share one hash per insert.

- Each sketch keeps its own bound, and a statement about several adds their `δ`, because they share a hash.
- When the list holds a Count-Min or Count Sketch, its HyperLogLog reads a different hash than a standalone one, so the two do not merge.
- Memory: `size_of::<Self>() + (number of sketches)·size_of::<EnsembleSketch>()` plus each boxed sketch's `mem_in`.
- An update costs the sum of the sketches' `U_in`, and computes one hash when the sketches' slices fit in it.

## Tests

- **Memory.**
  Each memory function is at least `memory_bytes()` after building with the same inputs, and equal where its section gives an exact formula.
- **Ops.**
  A build with the `op-count` feature counts the ops one call performs, and a test checks the count is at most the formula, at two or more parameter sizes.
- **Error.**
  Over independent seeds, the fraction of estimates off by more than `ε` is tested against `δ` as a binomial, never on one fixed seed.
  A deterministic bound is tested on every run, and every test is mutation-checked: breaking the estimator turns it red.
- **Sizing.**
  `theoretical_error(params_for(ε, δ))` meets `(ε, δ)`, and for a single-term sizing rule one step smaller in any parameter does not.

A change to an implementation updates its formulas in the same pull request and in `CHANGELOG.md`, and these tests fail when the two disagree.

## Migration

1. asap_sketchlib renames `estimated_memory_bytes` to `theoretical_memory_bytes`, and adds `SketchAnalytics`, `ErrorBound`, `ErrorMetric`, `StatisticKind::QuantileInterpolated`, the op-counting feature, the trait implementations with their tests, and `memory_bytes()` and `theoretical_memory_bytes` for the sketches and frameworks that lack them.
2. ASAPPlanner's `sketch_shape` and `accuracy/estimators` call these functions, and ASAPPlanner keeps combining errors across a plan and splitting its error budget.
3. ASAPPlanner's `analytical-resource-cost.md` links to this page in place of its summary table.

## Open questions

- Kll `rank()` returns a count, and at `k = 200`, `n = 10^6` it measures at 1.02 times the Quantile fit.
  Should the Rank contract use a fit measured on asap_sketchlib, or the Quantile fit times 1.1?
