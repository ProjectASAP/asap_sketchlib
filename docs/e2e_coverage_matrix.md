# E2E Coverage Matrix

Which public instance is covered by which E2E test, against what ground truth,
under which error metric, on what **statistical trial unit**, and what kind of
claim the tolerance actually is.

The distinction this document exists to make explicit:

> having a test ≠ having an accuracy test ≠ correctly verifying a theoretical
> guarantee — and a correct bound checked on the wrong trial unit is still not
> a verified guarantee.

## How to read the columns

| Column | Meaning |
| --- | --- |
| `public_instance` | The concrete type a caller can construct, at the geometry the test builds |
| `gate` | Cargo feature required, or `default` |
| `constructor` | The public entry point that produces it |
| `ops` | `full` — insert/estimate/merge all exist; `inert` — constructible but no operations; `query-only` — no public per-key estimator |
| `e2e_test` | Test function, in `tests/` |
| `ground_truth` | Exact reference. Never another approximation |
| `error_metric` | What is bounded — and it differs per family |
| `bound_formula` | Evaluated at the instance's own parameters and the exact truth |
| `trial_unit` | What **one draw of the estimator's randomness** is, here |
| `confidence` | What the quoted probability means, and over what |
| `seeds` | Sketch / hash / stream seeds. All fixed, always |
| `status` | One of the six below |

### The six statuses

| Status | Meaning |
| --- | --- |
| `theorem` | A proved bound, checked on a trial unit the proof actually quantifies over |
| `asymptotic model` | An exactly derived standard deviation turned into a tail by a **normal approximation**. The σ is exact; the tail is a model |
| `empirical` | A measured band with no closed form. The test name says so, and the measurement is recorded beside it |
| `structural` | An exact property: equality, one-sidedness, merge exactness, bookkeeping |
| `regression` | One fixed realisation pinned against a threshold the guarantee supplies. Not a probability statement |
| `gap` | Not covered, with the reason recorded in the last section |

## Acceptance rules and why the trial unit matters

Acceptance rules live in [`tests/common/specs.rs`](../tests/common/specs.rs).
A bound `P[error > B] ≤ p` speaks about **one** draw of the randomness the
estimator is built on. Turning `n` observed checks into a binomial tail at `p`
requires `n` independent draws of *that* randomness. Most natural batteries are
not:

| Battery | Why it is one draw, not many |
| --- | --- |
| several `q` off one KLL | one compaction history; a compaction that moves the median moves its neighbours |
| several keys off one Count-Min / Count Sketch | one hash function |
| rising checkpoints on one HLL / KMV | nested state — `n = 10⁶` contains `n = 10⁵` |
| a sketch and its merged twin | for HLL, KMV and counter matrices the merge is *exact*, so it is literally the same number |

Three acceptance rules, and every call site picks the one matching what it
collected:

| Rule | Valid when |
| --- | --- |
| `Tally::assert_none` | structural facts, and **simultaneous** bounds already union-bounded over the whole battery |
| `Tally::assert_independent_binomial` | each recorded check is a fresh seed — sketch, hash, or sampling |
| `Tally::assert_rate_at_most` | one fixed realisation, pinned at the guarantee's own marginal probability |

`TEST_LEVEL = 1e-6` for binomial acceptance; `SIMULTANEOUS_LEVEL = 1e-3` is the
total failure probability a union-bounded battery is sized at. Both are fixed
before the run.

### Simultaneous vs marginal, for the matrix frequency families

The keys of one sketch share one hash, so a binomial over keys is unavailable.
Both families' per-key failure probabilities decay fast enough in the bound's
scale factor that a **union bound over every probed key** stays useful:

| Family | Marginal (per key) | Simultaneous (all `D` keys at once) |
| --- | --- | --- |
| Count-Min | `est − f ≤ e(N−f)/w`, failure `e^-d` | `est − f ≤ b(N−f)/w` with `b = (D/δ)^{1/d}`, failure `δ` over all keys |
| Count Sketch | `\|est−f\| ≤ √(3/w)·‖f₋ᵢ‖₂`, failure `P[Bin(d,1/3) ≥ ⌈d/2⌉]` | same shape at the smallest `κ` with `P[Bin(d,1/κ) ≥ ⌈d/2⌉] ≤ δ/D` |

The simultaneous bound is asserted with **zero** tolerated violations and needs
no independence anywhere. The marginal bound is asserted as a violation *rate*
pin — a regression check, honestly labelled, not a tail test. At `d = 3` the
simultaneous form is weak (it is what three rows actually buy over 4096 keys);
it still catches an estimator off by a constant factor, and the rate pin is what
catches a smaller drift.

## Error metrics, one per family

The most common way to have a test that verifies nothing is to check a sketch
against another family's bound.

| Family | Metric | Formula | Source | Kind |
| --- | --- | --- | --- | --- |
| Count-Min | one-sided **additive** | `est ≥ f` always; `P[est − f > b(N−f)/w] ≤ b^-d` | Cormode & Muthukrishnan 2005, Thm 1 | theorem |
| Count Sketch | two-sided **L2**, rank-independent | `P[\|est−f\| > √(κ/w)·‖f₋ᵢ‖₂] ≤ P[Bin(d, 1/κ) ≥ ⌈d/2⌉]` | Charikar, Chen & Farach-Colton 2002 | theorem |
| F2 from a Count Sketch matrix | **relative**, AMS row-sum | `\|F2_hat/F2 − 1\| ≤ √(2κ/w)`, same median amplification | Alon, Matias & Szegedy 1996 | theorem |
| KLL | **rank** | `rank_incl(v) ≥ q − ε`, `rank_excl(v) ≤ q + ε`; `ε(k) = 2.446/k^0.9433` bounds the **maximum** rank error over a whole grid | *Guarantee*: Karnin, Lang & Liberty 2016 (asymptotic, constants unresolved). *Constant*: Apache DataSketches characterization fit, 99th percentile | empirical constant on a theorem-shaped metric |
| DDSketch | **relative value** | `\|est − true\|/\|true\| ≤ α + ULP slack`, against the exact order statistic **of that implementation's own rank convention** | Masson, Rim & Lee 2019 | theorem (deterministic) |
| HLL Classic / ErtlMLE | **cardinality RSE** | `σ_rel = 1.04/√m`, `m = 2^p` | Flajolet et al. 2007; Ertl 2017 attains the same CRLB | asymptotic model |
| HLL HIP | **cardinality RSE**, tighter | `σ_rel = √(ln2/m)` | Cohen 2015; Ting 2014 | asymptotic model |
| KMV | **cardinality RSE** | exact for `n < k`; `σ_rel(n,k) = √((n−k+1)/(n(k−2)))` above, → `1/√(k−2)` | Bar-Yossef et al. 2002; variance of a reciprocal Beta | asymptotic model |
| Bloom | **no false negatives** + predicted FPP | exact membership; measured FPP vs the filter's own sizing | `tests/e2e_membership.rs` | structural + regression |
| Nitro | **sampling variance** | `\|est − f\| ≤ z·√(f(p·r(1−r) + (1−p)/p))`, `r = frac(1/p)` | NitroSketch SIGCOMM 2019 + this crate's stochastic-rounding weight | asymptotic model |
| Uniform sampling | **retention exact** + SRSWOR | `len = ⌈n·rate⌉` exactly; `Var[mean] = (σ_N²/m)(N−m)/(N−1)` | priority/bottom-k sampling | structural + asymptotic model |
| OctoSketch promotion | **deterministic residual** | `ref − k·τ ≤ octo ≤ ref` per counter | OctoSketch, NSDI 2024, Thm 1 | theorem |

Three pairings that are specifically wrong and are prevented by having separate
spec types: Count Sketch under Count-Min's `ε·N`; KLL under a relative *value*
tolerance; DDSketch inside a *rank* battery. A fourth is now prevented by
`DdRankConvention`: the two DDSketch implementations scored against one truth.

## Matrix-backed frequency families

Storage backends: `Vector2D<i32|i64|i128|f64>` (caller-sized) and the six
fixed layouts `QuickMatrixI32` (= `FixedMatrix`, 5×2048), `QuickMatrixI64`,
`QuickMatrixI128`, `DefaultMatrixI32` (3×4096), `DefaultMatrixI64`,
`DefaultMatrixI128`. Two hashing paths each. Every instance is judged at the
dimensions **it** reports.

All rows below: `gate = default`, `ground_truth = FreqTruth` (exact per-key
counts), `seeds` = stream `0x10BEC700` unless stated, `trial_unit` = one sketch
over one fixed hash.

### CountMin — 20 instances

| public_instance | constructor | ops | e2e_test | error_metric | bound_formula | confidence | status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `CountMin<Vector2D<i32>, Regular\|Fast>` | `Default` | full | `countmin_{regular,fast}_path_instances_satisfy_the_count_min_bound` | one-sided + additive | `est ≥ f`; `b(N−f)/w` | `δ = 1e-3` over all keys | structural + theorem |
| `CountMin<Vector2D<i64>, Regular\|Fast>` | `Default` | full | same | same | same | same | structural + theorem |
| `CountMin<Vector2D<i128>, Regular\|Fast>` | `Default` | full | same | same | same | same | structural + theorem |
| `CountMin<Vector2D<f64>, Regular\|Fast>` | `Default` | full | same | same | same | same | structural + theorem |
| `CountMin<FixedMatrix, Regular\|Fast>` | `Default` (5×2048) | full | same | same | same | same | structural + theorem |
| `CountMin<QuickMatrixI64, Regular\|Fast>` | `Default` (5×2048) | full | same | same | same | same | structural + theorem |
| `CountMin<QuickMatrixI128, Regular\|Fast>` | `Default` (5×2048) | full | same | same | same | same | structural + theorem |
| `CountMin<DefaultMatrixI32, Regular\|Fast>` | `Default` (3×4096) | full | same | same | same | same | structural + theorem |
| `CountMin<DefaultMatrixI64, Regular\|Fast>` | `Default` (3×4096) | full | same | same | same | same | structural + theorem |
| `CountMin<DefaultMatrixI128, Regular\|Fast>` | `Default` (3×4096) | full | same | same | same | same | structural + theorem |

Plus, over the same 20 instances:

| e2e_test | ground_truth | error_metric | bound | trial_unit | status |
| --- | --- | --- | --- | --- | --- |
| `countmin_regular/fast_path_instances_…` (marginal half) | `FreqTruth` | additive | violation rate ≤ `e^-d` | one fixed realisation | regression |
| `countmin_both_paths_are_exact_on_a_collision_free_workload` | exact counts | — | `est == f` on 8 separated keys | deterministic | structural |
| `countmin_counter_widths_carry_the_mass_their_type_allows` | exact | — | no wrap at each width's ceiling | deterministic | structural |

Production-sized and cross-cutting runs:

| public_instance | e2e_test | bound | trial_unit | seeds | status |
| --- | --- | --- | --- | --- | --- |
| `CountMin<Vector2D<i64>, FastPath>` 5×4096 | `countmin_zipf_satisfies_the_count_min_additive_bound_and_shard_merge` | simultaneous + marginal + exact merge equality | one sketch | stream `1001` | theorem + structural + regression |
| `CountMin<Vector2D<i32>, Regular\|Fast>` over `U64`/`F64` inputs | `countmin_satisfies_the_count_min_theorem_on_both_paths` | as above | one sketch per (shape, path, trial) | 3 stream seeds from `1005` | theorem + regression |
| `CountMin<Vector2D<i32>, Regular\|Fast>` | `countmin_both_paths_satisfy_the_bound_on_the_same_stream` | as above | one sketch | `1002` | theorem + regression |

### Count Sketch — 18 instances

`Count<S, path>` for `S ∈ {Vector2D<i32|i64|i128>}` ∪ the six fixed layouts.
`Vector2D<f64>` is **not** a Count Sketch instance: `CountSketchCounter` is
implemented for `i32`, `i64`, `i128` only.

| public_instance | constructor | ops | e2e_test | bound_formula | confidence | status |
| --- | --- | --- | --- | --- | --- | --- |
| `Count<Vector2D<i32\|i64\|i128>, Regular\|Fast>` (6) | `Default` | full | `countsketch_{regular,fast}_path_instances_satisfy_the_l2_bound` | simultaneous `√(κ/w)·‖f₋ᵢ‖₂` | `δ = 1e-3` over all keys | theorem |
| `Count<{FixedMatrix, QuickMatrixI64, QuickMatrixI128, DefaultMatrixI32, DefaultMatrixI64, DefaultMatrixI128}, Regular\|Fast>` (12) | `Default` | full | same | same | same | theorem |
| all 18, marginal half | — | — | same | rate ≤ `P[Bin(d,1/3) ≥ ⌈d/2⌉]` | one realisation | regression |
| all 18 | — | — | `countsketch_both_paths_are_exact_on_a_collision_free_workload` | `est == f` | — | structural |
| `i32`/`i64`/`i128` widths | — | — | `countsketch_counter_widths_carry_signed_mass_in_both_directions` | no wrap, both signs | — | structural |
| `Count<Vector2D<i64>, RegularPath>` 5×4096 | `with_dimensions` | full | `countsketch_turnstile_cancels_and_satisfies_the_l2_median_bound` | L2 + exact cancellation to 0 | stream `1003` | theorem + structural |
| `Count<Vector2D<i32>, Regular\|Fast>`, pooled | `with_dimensions` | full | `countsketch_satisfies_the_l2_median_bound_on_both_paths` | simultaneous (zero tolerated) + marginal rate pin | 3 stream seeds from `1005` | theorem + regression |
| `Count<Vector2D<i64>, RegularPath>` | `with_dimensions` | full | `countsketch_error_stays_rank_independent_within_the_documented_empirical_band` | mean \|error\| per frequency decile, spread ≤ 3× (measured 1.29×) | stream `1007` | **empirical** |

### CMSHeap — 16 constructible instances, 8 of them inert

`CMSHeap::insert` / `insert_many` / `estimate` / `merge` live in an impl bounded
on `S::Counter: Copy + Ord + From<i32> + Into<i64> + AddAssign`.

| public_instance | constructor | ops | e2e_test | bound | status |
| --- | --- | --- | --- | --- | --- |
| `CMSHeap<Vector2D<i32>, Regular\|Fast>` | `Default` / `new` | full | `cmsheap_{regular,fast}_path_instances_satisfy_the_count_min_bound` | simultaneous + marginal + heap/sketch equality + top-k recall | theorem + structural + regression |
| `CMSHeap<Vector2D<i64>, Regular\|Fast>` | `Default` / `new` | full | same | same | theorem + structural + regression |
| `CMSHeap<{FixedMatrix, DefaultMatrixI32, QuickMatrixI64, DefaultMatrixI64}, Regular\|Fast>` (8) | `Default` | full | same | same | theorem + structural + regression |
| `CMSHeap<Vector2D<i128>, Regular\|Fast>` | `new(rows, cols, k)` | **inert** — `i128: !Into<i64>` | `cmsheap_instances_without_an_insert_impl_are_inert_by_construction` | geometry accessors only | structural |
| `CMSHeap<Vector2D<f64>, Regular\|Fast>` | `new(rows, cols, k)` | **inert** — `f64: !Ord`, `!Into<i64>` | same | same | structural |
| `CMSHeap<QuickMatrixI128, Regular\|Fast>` | `Default` | **inert** | same | same | structural |
| `CMSHeap<DefaultMatrixI128, Regular\|Fast>` | `Default` | **inert** | same | same | structural |

**Decision on the eight inert instances: documented, not changed.** Every
alternative costs more than it buys — `TryInto<i64>` makes `insert` fallible or
silently lossy for exactly the counters that motivated `i128`; widening
`HHItem::count` to `i128` changes the heap wire payload shared with `CSHeap`,
Space-Saving and the Octo top-k plans; removing the constructors breaks a type
that composes generically. `CSHeap` already covers `i128` end to end.

### CSHeap — 18 instances, all operational

`CSHeap`'s insert path is bounded on `S::Counter: CountSketchCounter`, which
`i128` **does** satisfy. The previous revision of this document claimed these
six `i128` instances were not insertable; that was wrong.

| public_instance | constructor | ops | e2e_test | bound | status |
| --- | --- | --- | --- | --- | --- |
| `CSHeap<Vector2D<i32\|i64>, Regular\|Fast>` (4) | `Default` / `new` | full | `csheap_{regular,fast}_path_instances_satisfy_the_l2_bound` | simultaneous L2 + marginal rate + heap/sketch equality + recall | theorem + structural + regression |
| `CSHeap<Vector2D<i128>, Regular\|Fast>` (2) | `new(3, 4096, 32)` | full | same | same | theorem + structural + regression |
| `CSHeap<{FixedMatrix, DefaultMatrixI32, QuickMatrixI64, QuickMatrixI128, DefaultMatrixI64, DefaultMatrixI128}, Regular\|Fast>` (12) | `Default` | full | same | same | theorem + structural + regression |
| the 6 `i128` instances | as above | full | `csheap_i128_counters_saturate_into_the_heap_instead_of_wrapping` | `insert_many(i128)` exact below 2^53; `cs_heap_count` saturates at `i64::MAX`; merge stays saturated | structural |

**Decision on the `i128 → i64` heap conversion: documented saturation.**
`Count::estimate` returns the row median as `f64` (a Count Sketch estimate is a
signed median, not a counter read), and the heap stores `i64`. The conversion is
named `cs_heap_count`, is **saturating** (Rust's float→int cast, `NaN → 0`), and
truncates toward zero. Wrapping would corrupt the heap's ordering — the one
failure a top-k structure cannot survive. Above `2^53` the estimate has already
lost precision inside `Count::estimate`; both limits are asserted.

### Other frequency instances

| public_instance | gate | e2e_test | ground_truth | error_metric | bound | trial_unit | seeds | status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `CMSHeap<Vector2D<i64>, Regular>` / `CSHeap<Vector2D<i64>, Regular>` | default | `heaps_satisfy_their_own_bounds_and_stay_heap_consistent` | `FreqTruth` | per family + heap equality + recall | as above | one sketch | stream `1004` | theorem + structural |
| `CountL2HH<DefaultXxHasher>` | default | `countl2hh_weighted_turnstile_satisfies_the_l2_median_bound` | `FreqTruth` | L2 + AMS F2 | `√(3/w)·‖f₋ᵢ‖₂`; `√(6/w)` for F2 | one sketch (one F2 read) | stream `1005`, hash seed idx `11` | theorem |
| `FoldCMS` / `FoldCS` after a 16-way merge | default | `folded_sketches_keep_their_own_bounds_through_a_sixteen_way_merge` | `FreqTruth` | per family at the **folded** width | `w' = w >> fold_level` | one sketch | stream `1009` | theorem + regression |
| portable `CountMinSketch` / `CountSketch` | default | `portable_cms_and_cs_string_keys_satisfy_their_own_bounds` | `FreqTruth` over string keys | per family | as above | one sketch | stream `1006` | theorem + regression |
| portable `CountMinSketchWithHeap` | default | `portable_count_min_with_heap_satisfies_the_count_min_bound_through_merge_and_wire` | `FreqTruth` | additive + heap + wire equality | as above | one sketch | `0xC0905101` | theorem + structural |
| portable `CountSketchWithHeap` | default | `portable_count_sketch_with_heap_satisfies_the_l2_bound_through_merge_and_wire` | `FreqTruth` | L2 + heap + wire | as above | one sketch | `0xC0905101` | theorem + structural |

## Quantiles

### KLL — what the constant is, and what a trial is

`ε(k) = 2.446 / k^0.9433` is **not** a theorem about this implementation. It is
Apache DataSketches' characterization fit to the 99th percentile of the
**maximum** rank error over a whole quantile grid. Two consequences:

1. the quantity is a per-sketch maximum, so the `q` grid of one sketch is *one*
   outcome, not seven Bernoulli draws;
2. the 1% is that outcome's failure probability, so the battery must be over
   **independent compaction seeds**.

Every KLL trial below therefore gets its own seed from `kll_trial_seed(i)`, and
no test name contains `theorem`.

| public_instance | gate | constructor | e2e_test | ground_truth | metric | bound | trial_unit | confidence | seeds | status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `KLL<f64>`, `KLLDynamic<f64>` × k∈{64,200,800} × 6 shapes × 4 feed modes (single, bulk, 4-shard merge, tumbling) | default | `init_kll_with_seed` | `kll_family_stays_within_the_datasketches_maximum_rank_error_characterization` | `NumericTruth` rank intervals | rank | max rank error over the grid ≤ `ε(k)` | one sketch, own compaction seed | 1% per sketch | `kll_trial_seed(0..)`, streams `0xA5A50000+` | empirical characterization |
| `KLL<f64>` k∈{64,256,1024} | default | `init_kll_with_seed` | `kll_rank_error_shrinks_with_k_as_the_characterization_predicts` | `NumericTruth` | rank scaling | worst-of-4 ≤ `ε(k)`; ≥4× improvement over 16× k | 4 fixed seeds per k | none — deterministic pin | `0x5EED0001..4`, stream `0xC0FFEE01` | structural + regression |
| `KLL<T>`, `KLLDynamic<T>` for all 14 `NumericalValue` types × 3 feed modes | default | `init_kll_with_seed` | `every_numeric_type_satisfies_the_kll_rank_error_characterization` | `NumericTruth` over the `to_f64` projection | rank | `ε(200)` | one sketch per (type, impl, mode, repeat) | 1% per sketch | `kll_trial_seed(0..)`, stream `0x57EA0001` | empirical characterization |
| the 8 signed/float types | default | same | `signed_numeric_types_order_negative_values_correctly_in_kll` | `NumericTruth` | rank + range containment | `ε(200)`; answers inside `[min, max]`; median near 0 | one sketch per (type, impl, repeat) | 1% | `kll_trial_seed(0x51640000..)` | empirical + structural |
| `KLL<u128>` above `2^70` | default | same | `the_f64_projection_is_exact_below_two_to_the_53` | `NumericTruth` | rank + projection exactness | `ε(200)`; `(v as f64) as u128 == v` for `v ≤ 2^53` | one sketch per seed, 16 seeds | 1% | `kll_trial_seed(0x2E700000..)` | empirical + structural |
| `TumblingWindow<KLL>` (`query_all`, `query_recent`, active) | default | `TumblingWindow::new` + `KLLConfig{seed}` | `tumbling_kll_windows_are_exact_and_answers_satisfy_the_rank_contract` | exact window slice | rank + exact window count | worst over the 3 views ≤ `ε(200)` | one tumbling window per seed, 16 seeds | 1% | `kll_trial_seed(0x77170000..)`, stream `3009` | empirical + structural |
| portable `HydraKllSketch` per key | default | `with_seed` | `portable_hydra_kll_per_key_medians_satisfy_the_rank_characterization` | per-key `NumericTruth` | rank | worst over both keys ≤ `ε(200)` | one grid per prototype seed, 12 seeds | 1% | `kll_trial_seed(0x5EED0500..)`, streams `3010`/`3011` | empirical characterization |
| portable `KllSketch` (+ merge, msgpack) | default | `with_seed` | `portable_kll_sketch_satisfies_the_rank_error_characterization_through_merge_and_wire` | `NumericTruth` | rank + **wire equality** | `ε(200)`; post-wire answers bit-identical | one sketch per seed × {single, merged}, 12 seeds | 1% | `0x5EED0400 + i·φ` | empirical + structural |
| `KLL` / `KLLDynamic` bulk vs loop | default | `init_with_seed` | `tests/e2e_bulk.rs` (7 tests) | byte-identical sketches | **equality** | serialized bytes and quantile bits identical; rank floor at `ε(200)` | deterministic | fixed | structural |
| `EHSketchList::KLL` | default | `EHSketchList::KLL(init_kll_with_seed)` | `eh_kll_variant_satisfies_the_rank_error_characterization_over_the_retained_window` | `NumericTruth` over the retained span | rank | max over the q grid ≤ `ε(200)` | one histogram per compaction seed, 12 seeds | 1% | seeds from `0x5EED0200` | empirical characterization |

### DDSketch — two implementations, two rank conventions

The two shipped implementations answer a quantile query with **different order
statistics**:

| Implementation | Rank convention | Endpoints |
| --- | --- | --- |
| core `DDSketch::get_value_at_quantile` | `rank = ceil(q·n)`, 1-based → `sorted[ceil(q·n) − 1]` | `q=0`/`q=1` short-circuit to the **exactly retained** min/max |
| portable `DdSketch::quantile` | `target = floor(q·(n−1))`, 0-based → `sorted[floor(q·(n−1))]` — the paper's and DataDog's lower-quantile convention | no min/max scalars on the wire; endpoints are ordinary bucket representatives, α-relative only |

**Decision: keep both, and score each against its own truth.** The portable type
exists to be byte- and answer-compatible with `sketchlib-go`, so its convention
is fixed by an external contract; the core type's convention is what its callers
have read for the life of the API and is what lets `q=0`/`q=1` be exact.
Changing either silently moves numbers under existing callers. What was not
acceptable was leaving it undocumented and scoring both against one truth
helper, which is what the previous revision did. `DdRankConvention` in
`tests/common/specs.rs` now carries the choice.

DDSketch's guarantee is deterministic — bucket width alone, no hash, no sampling
— so **every** DDSketch battery tolerates zero violations and no statistical
model applies. `trial_unit` is not meaningful and is omitted.

| public_instance | gate | e2e_test | ground_truth | metric | bound | seeds | status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `DDSketch` and portable `DdSketch`, α ∈ {0.001, 0.01, 0.05, 0.1} × 6 shapes × 3 sizes | default | `ddsketch_core_and_portable_satisfy_the_relative_value_error_contract` | exact order statistic **per convention** | relative value | `α + 8·ε_f64·(1+\|ln v\|)` | streams from `3005000` | theorem |
| both, at n ∈ {3,4,5,7,10} and ragged q | default | `ddsketch_core_and_portable_answer_different_order_statistics` | exact order statistic per convention | relative value + **index equality** | the two conventions' indices, asserted term by term; answers must differ where the indices do | fixed probes | structural |
| core, all α | default | `ddsketch_core_endpoints_are_exact_and_portable_endpoints_are_alpha_relative` | exact min/max | exactness | `q=0 → min`, `q=1 → max` exactly | stream `4242` | structural |
| portable, all α | default | same | exact min/max | relative value | `α + ULP` — **not** exact | stream `4242` | theorem |
| both, bucket edges `γ^k`, k ∈ {−40,−7,0,1,13,60,200} | default | `ddsketch_satisfies_the_relative_error_contract_at_bucket_boundaries` | the probe value itself | relative value | `α + ULP` at the edge, ±1 ULP either side, and the interior | fixed probes | theorem |
| both, 4-way merge + delta replay | default | `ddsketch_merge_and_delta_replay_preserve_the_relative_error_contract` | exact order statistics per convention | relative value | `α + ULP` | stream `7654321+α` | theorem |
| `DDSketch::add<T>` for all 14 `NumericalValue` types × 3 α | default | `every_numeric_type_satisfies_the_ddsketch_relative_value_error_contract` | exact order statistics of the projection | relative value | `α + ULP` | stream `0x57EA0001` | theorem |
| `DDSketch` over `u128` above `2^70` | default | `the_f64_projection_is_exact_below_two_to_the_53` | exact order statistics | relative value + projection limit | `α + ULP` | `0x57EA0001` | theorem + structural |
| both | default | `ddsketch_rejects_untrackable_values_and_mapping_mismatches`, `portable_ddsketch_rejects_hostile_delta_spans` | — | — | drop non-indexable inputs; reject far-span deltas without allocating | fixed | structural |
| `EHSketchList::DDS` | default | `eh_ddsketch_variant_satisfies_the_relative_value_error_contract_over_the_window` | exact order statistics (core convention) | relative value | `α + ULP` | `0x0E110001` | theorem |

## Cardinality

`z = 4` is a two-sided tail of 6.3e-5 **under a normal approximation**. The σ is
derived exactly; the tail is a model. Every row here is therefore
`asymptotic model`, not `theorem` — HLL's estimators are asymptotically normal
with known deviations near the linear-counting switchover, and KMV's estimator
is a reciprocal Beta variate, right-skewed at small `k`.

| public_instance | gate | constructor | e2e_test | ground_truth | bound | trial_unit | seeds | status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `HyperLogLogP12/P14/P16<Classic>` | default | `new` | `hll_classic_p{12,14,16}_satisfies_its_register_error_model` | exact identity count | `z·1.04/√(2^p)` | **one fresh sketch per checkpoint, disjoint identity namespace** (7 trials) | namespaces `2^40·i` | asymptotic model |
| `HyperLogLogP12/P14/P16<ErtlMLE>` | default | `new` | `hll_ertl_mle_p{12,14,16}_satisfies_the_cramer_rao_error_model` | exact | same constant (CRLB) | same | same | asymptotic model |
| `HyperLogLogHIPP12/P14/P16` | default | `new` | `hll_hip_p{12,14,16}_satisfies_the_hip_error_model` | exact | `z·√(ln2/2^p)` | same | same | asymptotic model |
| all 9 | default | — | same tests | exact | duplicate replay must not move the estimate at all | deterministic | — | structural |
| the 6 mergeable ones | default | — | same tests | exact | **even/odd shard merge reproduces the single pass register for register** — an equality, because registers combine by maximum | deterministic | — | structural |
| portable `HllSketch` × {Regular, Datafusion, Hip} × p{12,14,16} (9) | default | `new` | `portable_hll_variants_and_precisions_satisfy_the_register_error_model` | `HashSet` | `z·1.04/√(2^p)` — the variant is a **wire tag**, not an estimator | one (variant, precision) over its **own** stream seed | `2001..2009` | asymptotic model |
| portable `HllSketch`, disjoint shards | default | — | same | `HashSet` | merged estimate == single pass exactly | deterministic | — | structural |
| `HyperLogLogP12/P14/P16<Classic>` | default | `new` | `hll_accuracy_improves_with_precision_as_the_error_model_predicts` | exact | measured RSE over 6 blocks ≤ 2× predicted; ≥2× improvement p12→p16 | 6 blocks | — | regression |
| `HyperLogLogP12/P14/P16<Classic>` at `n ≈ 2.5m` | default | `new` | `hll_classic_switchover_band_stays_within_the_documented_empirical_band` | exact | 1.5×–10× the asymptotic RSE (measured 2.1×/3.5×/6.3×) | fixed | — | **empirical** — see findings |
| `KMV`, k ∈ {64, 1024, 4096} × 6 regimes (`n<k`, `n=k`, `n>k`) | `experimental` | `KMV::new` + `insert_by_hash` | `kmv_estimates_stay_inside_their_relative_standard_error_band_over_independent_hash_seeds` | exact distinct count | exact for `n<k`; `z·√((n−k+1)/(n(k−2)))` above | **one sketch per (k, hash seed, regime)** over its own identity namespace; 8 independent seed-list hashes | seed indices `{0,1,2,3,7,11,13,17}`, namespaces `2^40·i` | asymptotic model |
| `KMV` at the `n<k` / `n=k` boundary | `experimental` | same | `kmv_is_exact_below_k_and_estimates_at_k` | exact | `estimate == n` for `n<k`; estimator active at `n=k` | deterministic | as above | structural |
| `KMV` over a duplicate stream, + merge | `experimental` | `insert` / `merge` | `kmv_duplicates_are_inert_and_a_shard_merge_reproduces_the_single_pass_exactly` | `HashSet` | replay must not move the estimate; **merge reproduces the single pass exactly** | deterministic | stream `5001` | structural |
| `SetAggregator` | default | `new` | `set_aggregator_union_is_exact` | `HashSet` | exact union | deterministic | stream `2002` | structural |
| `EHSketchList::HLL` | default | — | `eh_hll_variant_satisfies_the_register_error_model_over_the_retained_window` | `HashSet` over the span | `4·1.04/√(2^14)` | one merged sketch = one trial | `0x0E110001` | asymptotic model |

## Sampling

| public_instance | gate | constructor | e2e_test | ground_truth | metric | bound | trial_unit | seeds | status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `NitroBatch<CountMin<Vector2D<i32>, FastPath>>` and `NitroBatch<Count<…>>`, rates {1.0, 0.5, **0.3**, 0.1, **0.07**, 0.01} | default | `with_target_and_seed` | `nitro_estimates_are_unbiased_inside_the_sampling_band_at_every_rate` | exact count | sampling variance | `z·√(f(p·r(1−r) + (1−p)/p))`, `r = frac(1/p)` | one sampling seed (both targets share the admitted subset) | `0x01170001..8` | asymptotic model |
| same | default | same | same test, mean over seeds | exact count | **unbiasedness** | `\|mean − f\| ≤ z·σ/√trials` | 8 independent seeds | same | asymptotic model |
| `NitroBatch<Vector2D<u32>>` | default | `init_nitro_with_seed` | `nitro_over_a_bare_vector2d_target_admits_mass_inside_the_sampling_band` | exact stream length | admitted mass | as above; every row must carry identical mass | one sampling seed | `0x01170001..8` | asymptotic model + structural |
| `NitroBatch` rate = 1.0 | default | same | `nitro_at_full_sampling_is_exact` | exact | — | exact | deterministic | `0x01170001` | structural |
| `NitroBatch` seeding | default | same | `nitro_sampling_is_reproducible_from_its_seed` | — | — | same seed → same result; different seeds differ | deterministic | 8 seeds | structural |
| `NitroBatch::merge`, all 6 rates | default | same | `nitro_merge_sums_admitted_mass_at_the_combined_band` | exact | sampling variance | as above | one disjoint seed pair | `0x01170001..8` | asymptotic model |
| weight saturation | default | — | `nitro_saturates_oversized_weights_instead_of_wrapping` | exact | — | clamp at `i32::MAX`/`u32::MAX` | deterministic | — | structural |
| `UniformSampling`, rates {1.0, 0.5, 0.25, 0.1, 0.01} × sizes {0,1,7,1k,10k,50k} | `experimental` | `with_seed` | `uniform_sampling_retention_is_exact_at_every_rate_and_stream_size` | the input multiset | retention | `len == ⌈n·rate⌉` **exactly**; samples ⊆ stream with multiplicity | deterministic | `0x5A910100+i` | structural |
| `UniformSampling`, rates {0.5, 0.1, 0.01} | `experimental` | `with_seed` | `uniform_sampling_is_a_uniform_sample_without_replacement` | population mean of a skewed stream | sample mean | `z·√((σ_N²/m)(N−m)/(N−1))` — SRSWOR with finite-population correction | **one seed = one draw of the whole priority sequence**; 24 seeds | `0x5A910200..` | asymptotic model |
| `UniformSampling::merge`, all 4 rates | `experimental` | `with_seed` | `uniform_sampling_merge_keeps_the_combined_budget_exactly` | exact | — | `⌈(n₁+n₂)·rate⌉` capped by the pooled entries; totals add; rate mismatch rejected | deterministic | 42/43 | structural |
| `UniformSampling` seeding | `experimental` | `with_seed` | `uniform_sampling_is_reproducible_from_its_seed` | — | — | same seed → same sample; different seed → different | deterministic | 7/8 | structural |
| `EHSketchList::UNIFORM` | `experimental` | — | `eh_uniform_sampling_variant_reports_exact_retention_bookkeeping` | exact | — | `total_seen` exact; samples ⊆ window | deterministic | sampler `0x5A9101` | structural |

## Windowed frameworks

| public_instance | gate | e2e_test | ground_truth | metric | bound | trial_unit | seeds | status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `EHSketchList::CM` | default | `eh_count_min_variant_satisfies_the_count_min_bound_over_the_retained_window` | exact truth over the merged span | additive one-sided | simultaneous + marginal rate | one sketch | `0x0E110001` | theorem + regression |
| `EHSketchList::CS` | default | `eh_count_sketch_variant_satisfies_the_l2_bound_over_the_retained_window` | same | L2 | simultaneous + marginal rate | one sketch | `0x0E110001` | theorem + regression |
| `EHSketchList::COUNTL2HH` | default | `eh_countl2hh_variant_satisfies_the_l2_bound_over_the_retained_window` | same | L2 | as above | one sketch | `0x0E110001` | theorem + regression |
| `EHSketchList::COCO`, `::ELASTIC` | `experimental` | `eh_heavy_hitter_variants_stay_one_sided_over_the_retained_window` | same | one-sided on heavy keys | `est ≥ f` for the true top 32 | deterministic | `0x0E110001` | structural |
| `EHSketchList::HLL` / `::KLL` / `::DDS` | default | see Cardinality / Quantiles above | — | — | — | — | — | — |
| `EHSketchList::UNIVMON` | default | `eh_univmon_variant_reports_the_exact_l1_over_the_retained_window` | exact | L1 exact; L2 by the AMS bound | `calc_l1 == N`; `L2 ∈ [√(1−b), √(1+b)]·L2_true`, `b = √(2κ/w)` at 5×2048 (≈ −4.2%/+4.1%) | one sketch | `0x0E110001` | structural + theorem |
| all 10 variants | mixed | `every_eh_variant_can_merge_into_its_own_kind`, `every_eh_variant_selects_the_documented_merge_norm` | — | — | merge arm present; L2 norm for COUNTL2HH/UNIVMON, L1 otherwise | deterministic | — | structural |
| `ExponentialHistogram` expiry | default | `eh_expires_buckets_past_the_window_and_reports_its_retained_span` | exact retained events | — | no bucket entirely before the cutoff | deterministic | — | structural |
| `TumblingWindow<FoldCMS>` | default | `tumbling_foldcms_weighted_windows_exact_counts` | exact | — | exact weighted counts, flush, rotation | deterministic | — | structural |
| `TumblingWindow<FoldCS>` | default | `tumbling_fold_cs_windows_are_exact_and_answers_satisfy_the_l2_bound` | exact slices | L2 at the folded width | `√(3/w')·‖f₋ᵢ‖₂` | one sketch | `0x0E110001` | theorem + regression |
| `TumblingWindow<UnivMonQ>` | default | `tumbling_univmon_q_windows_carry_exact_aggregates_through_rotation` | exact slices | — | count/min/max exact per window; pool reuse clears | deterministic | `0x0E110001` | structural |
| `EHUnivOptimized` map tier | `experimental` | `eh_univ_optimized_map_tier_exact_windows`, `…_matches_exact_per_key_counts_on_a_skewed_stream` | exact | — | exact per-key counts | deterministic | stream `9001` | structural |
| `EHUnivOptimized` sketch tier | `experimental` | `eh_univ_optimized_promotes_into_the_sketch_tier_and_answers_from_it` | exact over the promoted span | L1 exact; L2/entropy measured | `calc_l1 == N`; L2 ±10%, entropy −10/+40% | one sketch | stream `4242` | structural + **empirical** |
| `EHUnivOptimized` mixed interval, expiry, pool reuse | `experimental` | 3 tests | exact | — | exact L1 over the retained span | deterministic | — | structural |
| `EHUnivOptimized` sketch-tier cardinality | `experimental` | `eh_univ_optimized_sketch_tier_cardinality_is_documented_as_unrecoverable` | exact | — | reported < 10% of true | deterministic | stream `4242` | **empirical** — see findings |

## Composition

| public_instance | gate | e2e_test | ground_truth | metric | bound | trial_unit | seeds | status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `EnsembleSketch::{CountMinFast, CountFast}` | default | `ensemble_members_match_standalone_sketches_fed_the_same_stream` | standalone sketch on the same stream | — | exact equality, key for key | deterministic | `0xC0905101` | structural |
| `EnsembleSketch::{HllErtl, HllClassic, HllHip}` | default | same | `FreqTruth` distinct + standalone | cardinality RSE | each member's own model; the two sides agree within their combined band (different hashes by design) | **two trials — one per hash function**, each scored over all three estimators | `0xC0905101` | asymptotic model |
| all 5 member variants | default | `ensemble_members_satisfy_their_own_error_bounds` | `FreqTruth` / `HashSet` | per family | CM simultaneous + marginal, CS simultaneous + marginal, HLL register, HIP HIP | **one trial** — the three HLL members share the ensemble's single matrix hash | `0xC0905101` | theorem + asymptotic model + regression |
| multi-matrix ensembles, layout compatibility | default | `ensemble_composes_by_hash_layout_and_rejects_incompatible_members` | `FreqTruth` | per family + structural | each member at **its own** width; mismatched rows or packing modes rejected | one sketch | `0xC0905101` | theorem + structural |
| `UnivMonQ` default / `counter_bits=64` / `width_halving_period=2` / explicit `hash_seed` | default | `univmonq_configuration_variants_all_build_and_keep_exact_aggregates` | `NumericTruth` | — | count/min/max exact; config round-trips | deterministic | `0xC0905101` | structural |
| `UnivMonQ` `ordered_samples = 0` | default | `univmonq_with_ordered_samples_disabled_answers_everything_except_ordered_queries` | `NumericTruth` | — | ordered queries `None`; endpoints still exact | deterministic | `0xC0905101` | structural |
| `UnivMonQConfig::with_window_bound` | default | `univmonq_with_window_bound_chooses_a_hierarchy_that_satisfies_its_own_inequality` | Bernstein bound recomputed in-test | — | chosen `levels` is the smallest with `mean + √(2·mean·ln(1/δ)) + (2/3)ln(1/δ) < candidates` | deterministic | δ = 1e-3 | theorem |
| `UnivMonQ::with_hasher_and_source_id`, 4-shard merge | default | `univmonq_multi_shard_merge_with_distinct_source_ids_covers_the_union` | `NumericTruth` | — | merged count/min/max exact | deterministic | `0xC0905101` | structural |
| `UnivMonQ::estimate_frequency` / `estimate_f2` | default | `univmonq_frequency_and_f2_satisfy_the_count_sketch_bounds` | `FreqTruth` | L2 + AMS F2 | `√(3/w)·‖f₋ᵢ‖₂`, `√(6/w)` | one sketch | stream `3008` | theorem |
| `UnivMonQ::{rank, cdf, quantile}`, **both regimes** | default | `univmonq_ordered_queries_satisfy_the_full_documented_bound` | `NumericTruth` | rank, sup over x | `sup_x \|F̂−F\| ≤ 2E_H + P̂_R·ε_R`, all three terms read from `ordered_query_diagnostics` | **one sketch** (the bound is a sup, so one pass/fail over every breakpoint and every probed q); 12 trials varying `hash_seed` and `source_id` | δ = 0.01 per sketch | `hash_seed 3..14`, `source_id 0x0DDE1000+`, streams `0x0DDE0001/2` | theorem |
| `UnivMonQ::{estimate_distinct, estimate_entropy, heavy_hitters}` | default | `univmonq_distinct_entropy_and_recall_stay_within_the_documented_empirical_band` | `FreqTruth` | relative | ±10% distinct, ±10% entropy, ≥8/10 recall | one sketch | stream `3008` | **empirical** |
| `Hydra` with CM / CS / HLL / KLL / UnivMon counters | default | `tests/e2e_frameworks.rs` (29 tests) | exact lattice truth | additive grid bound, `ε(k)` for KLL cells, AMS-derived band for UnivMon `L2Norm`, exact marginals, exact shard merge | derived per counter from the cell's own parameters | one grid | `0x5EED0600` etc. | theorem + structural |
| `UnivMon` standalone | default | `univmon_weighted_metrics_and_fast_insert_parity`, `univmon_pyramid_weighted_metrics` | `FreqTruth` | L1 exact, L2 by the AMS bound, entropy empirical | `calc_l1 == N`; `L2 ∈ [√(1−b), √(1+b)]·L2_true`; entropy ±12% | one sketch | stream `4003` | structural + theorem + empirical |

## OctoSketch

| public_instance | gate | e2e_test | ground_truth | metric | bound | partitions | seeds | status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `CmTopKOctoPlan/Worker/Aggregator` | default | `cm_top_k_point_estimates_trail_the_single_thread_reference_by_at_most_k_tau` | single-thread `CMSHeap` **and** `FreqTruth` | promotion residual + additive | `ref − k·τ ≤ octo ≤ ref`; simultaneous `f − k·τ ≤ est ≤ f + b(N−f)/w` (zero tolerated) + marginal rate pin | HashByKey + RoundRobin | `0x0C701101` | theorem + regression |
| `CountTopKOctoPlan/Worker/Aggregator` | default | `count_top_k_point_estimates_satisfy_the_l2_bound_within_the_promotion_residual` | single-thread `CSHeap` **and** `FreqTruth` | promotion residual + L2 | `\|octo − ref\| ≤ k·τ`; simultaneous L2 + `k·τ` (zero tolerated) + marginal rate pin | both | `0x0C701101` | theorem + regression |
| both top-k plans | default | `top_k_plans_recover_the_heavy_hitters_their_promotion_floor_guarantees` | `FreqTruth` top-k | recall / precision / heap consistency | 100% recall above the `k·τ` floor; nothing in the heap below the k-th count − `k·τ` | both | `0x0C701101` | theorem |
| `CmTopKOctoWorker` below threshold | default | `a_key_below_the_promotion_threshold_never_reaches_the_top_k_aggregator` | exact | — | no promotion under τ; first promotion hands over exactly τ | — | fixed key | structural |
| `CmOctoPlan`, `CountOctoPlan` | default | `theorem_1_bounds_the_count_min_error`, `theorem_3_bounds_the_count_sketch_error` | `FreqTruth` | additive / L2 with the `k'·τ` residual | paper Thms 1 and 3 | both | fixed | theorem |
| `HllOctoPlan` | default | `partition_accuracy::hll_octo_plan_is_register_exact_and_in_band_under_both_partitions` | single-thread HLL **and** exact distinct | register equality + cardinality RSE | registers byte-identical to the single pass; `z·1.04/√(2^14)` | **both** | `0x0C70ACC0` | structural + asymptotic model |
| `DdOctoPlan` | default | `partition_accuracy::dd_octo_plan_is_bucket_exact_after_flush_and_holds_alpha_under_both_partitions` | single-thread `DDSketch` **and** exact order statistics | bucket equality + relative value | every bucket equal after flush; `α + ULP` | **both** | `0x0C70ACC0` | structural + theorem |
| `UnivMonOctoPlan` | default | `partition_accuracy::univmon_octo_plan_reports_the_delivered_l1_and_holds_its_f2_bound_under_both_partitions` | exact `FreqTruth` | delivered L1 + AMS F2 | `calc_l1` == the exact sum of the last `weight_total` each worker reported; `\|F2̂/F2 − 1\| ≤ √(2κ/w)` | **both** | `0x0C70ACC0` | structural + theorem |
| `CocoOctoPlan`, `ElasticOctoPlan` | `experimental` | `partition_accuracy::coco_and_elastic_octo_plans_stay_one_sided_on_heavy_keys_under_both_partitions` | exact `FreqTruth` + single-thread reference | one-sided / ceiling | Coco: `est ≥ f` on the true top 32 and `est ≤ N`; Elastic: `0 ≤ est ≤ N`; the single-threaded reference is held to the same rule | **both** | `0x0C70ACC0` | structural |
| all five plans | mixed | pre-existing `tests/e2e_octo.rs` (mass conservation, flush completeness, sketch-merge baselines) | exact mass / `FreqTruth` | per family | see that suite | HashByKey | fixed | theorem + structural |
| `octo-runtime` scheduling | `octo-runtime` | `mod runtime` (21 tests) | single-threaded replay of the same partition | — | cell-for-cell equality; determinism across runs | both | fixed | structural |

## Heavy hitters and membership

| public_instance | gate | e2e_test | ground_truth | metric | bound | status |
| --- | --- | --- | --- | --- | --- | --- |
| `SpaceSaving` | default | `tests/e2e_heavy_hitters.rs` (20 tests) | `FreqTruth` | error sandwich | `count ≤ est ≤ count + error`; `min_count` ceiling; merges keep the ceiling | theorem + structural |
| `Coco` | `experimental` | `keyed_bucket` module (5 tests) | `FreqTruth` | mass partition + over-attribution | point queries sum to the total **exactly**; heavy keys ≤ `count + e(N−count)/w`; floor 0.5× is a **named empirical pin** (measured 1.00×) | structural + theorem + empirical |
| `Coco` | `experimental` | `coco_point_estimates_are_unbiased_under_heavy_eviction` | `FreqTruth` | unbiasedness | mean over 800 independent runs | asymptotic model |
| `Elastic` | `experimental` | `keyed_bucket` module (4 tests) | `FreqTruth` | one-sided | never underestimates; the elephant is ≤ `f + e(N−f)/light_cols`, the light layer's own Count-Min bound | structural + theorem |
| `Bloom<RegularPath>`, `Bloom<FastPath>` | default | `tests/e2e_membership.rs` (30 tests) | exact sets | no false negatives + predicted FPP | membership exact; measured FPP vs the filter's own sizing; geometry/sizing equalities | structural + regression |

## Findings from this pass

Ordered by severity.

1. **Nitro's integer weight was biased at every rate whose reciprocal is not an
   integer — fixed.** `scaled_increment` wrote `ceil(weight/p)` per admitted
   update, so at the public rate `p = 0.3` every estimate came back
   `f·0.3·4 = 1.2f` — a flat +20%, for every key, in the shipped estimator. The
   public API accepts any `0 < p ≤ 1`, so this was not a corner case. Fixed by
   **stochastic rounding**: `floor(1/p) + Bernoulli(frac(1/p))`, drawn per
   admitted update, giving `E[W] = 1/p` at every rate for `NitroBatch`
   (`admitted_weight`) and for the row-level `Nitro` behind
   `CountMin::fast_insert_nitro` / `Count::fast_insert_nitro`
   (`admitted_delta`, dithered from the skip cursor so it needs no new state and
   leaves the serialized form unchanged). At a reciprocal-integer rate the
   fraction is zero, the draw is skipped, and the emitted stream is
   bit-identical to before. Pinned by
   `nitro_estimates_are_unbiased_inside_the_sampling_band_at_every_rate`, which
   also averages over independent seeds so a systematic offset cannot hide
   inside the sampling band.
2. **The KMV spec had the wrong standard error and the wrong exact-regime
   boundary — fixed.** The estimator `(k−1)/U_(k)` has
   `RSE = √((n−k+1)/(n(k−2))) → 1/√(k−2)`; the suite modelled `1/√(k−1)` and
   called it "marginally conservative", when it is *smaller* and therefore
   stricter. Separately, the exact regime was written `n ≤ k`, but
   `KMV::estimate` returns the retained count only while `len < k`: at `n = k`
   the estimator runs and its standard deviation is about one element. The old
   grid never landed on `k`, so the mistake was invisible. Both fixed, and
   `n = k` is now a covered regime.
3. **The DDSketch batteries scored two different questions against one truth —
   fixed.** The core sketch answers `sorted[ceil(q·n) − 1]` and the portable one
   `sorted[floor(q·(n−1))]`. Both were compared against the core convention, so
   the difference was silently absorbed into `α`. Each is now compared against
   its own order statistic, the divergence is pinned by
   `ddsketch_core_and_portable_answer_different_order_statistics`, and the
   decision to keep both conventions is recorded above.
4. **`ddsketch_endpoints_return_the_exact_min_and_max` asserted something false
   of the portable sketch — fixed.** Its comment claimed the portable twin
   "clamps its bucket representative into [min, max], so its endpoints are exact
   too". It does not: the min/max scalars were removed from the wire, so its
   endpoints are ordinary bucket representatives. The test is split and renamed
   `ddsketch_core_endpoints_are_exact_and_portable_endpoints_are_alpha_relative`.
5. **The `CSHeap` `i128` instances were wrongly documented as uninsertable —
   fixed.** `CSHeap`'s insert path is bounded on `CountSketchCounter`, which
   `i128` satisfies; only `CMSHeap` requires `Into<i64>`. Six instances
   (`Vector2D<i128>`, `QuickMatrixI128`, `DefaultMatrixI128` × two paths) were
   listed as a gap and are now fully covered, including the `i128 → i64` heap
   conversion, which is documented saturation rather than silent wrapping.
6. **Binomial acceptance was applied to dependent checks throughout — fixed.**
   Quantiles of one KLL, keys of one Count Sketch, nested HLL/KMV checkpoints
   and merged twins were all pooled into `Binomial(n, p)` tails. Each battery
   now either reduces to one outcome per independent seed, uses a union-bounded
   simultaneous form, or is labelled a rate pin. Several checks turned out to be
   *exact equalities* being scored as second probabilistic draws — an HLL shard
   merge, a KMV shard merge, a portable HLL shard merge — and are now asserted
   as equalities, which is strictly stronger.
7. **UnivMon-Q's full ordered-query bound was unverifiable — now verified.**
   `E_H` and `m_R` were internal, so the test could only cover the diffuse
   special case. `UnivMonQQuery::ordered_query_diagnostics` was added — a
   read-only view of state the CDF construction already computes, with no
   behavioural or wire change — and the test now evaluates
   `sup_x |F̂ − F| ≤ 2E_H + P̂_R·ε_R` in full, in both the diffuse and the
   concentrated regime, asserting that each regime really is the regime it
   claims to be.
8. **`UnivMonOctoWorker` has no `flush`, so its parent's L1 lags — documented,
   not fixed.** Every other shipped worker (Cm, Count, Dd, Coco, Elastic)
   implements `flush`; UnivMon's cannot, because `LayeredCountDelta` carries the
   flow key that triggered the promotion and `apply_layered_delta` needs it for
   the aggregator's heavy-hitter heap — a counter flushed at end-of-stream has
   no single key to name. So `UnivMon::calc_l1` on an Octo parent is a lower
   bound on `N`, lagging by whatever each worker processed after its last
   emitted delta (31 of 60 000 on the covered stream). Pinned **exactly** by
   `partition_accuracy::univmon_octo_plan_reports_the_delivered_l1_…`, which
   predicts the value from the workers' last reported totals rather than
   tolerating a band. Fixing it means making the delta's key optional, which is
   a wire change.
9. **`EHSketchList::merge` had no `ELASTIC` arm** — fixed in the previous pass.
   `EHBucket::to_merge` discards the `Result`, so an `ExponentialHistogram` over
   the Elastic payload silently dropped every bucket merge while still counting
   bucket sizes. Every heavy key read 0.
10. **HLL Classic's accuracy cliff at the linear-counting switchover** — not
    fixed, documented. At `n ≈ 2.5m` the 2007 estimator's two branches do not
    meet smoothly and the RSE reaches 6.3× the asymptotic `1.04/√m` at p16. A
    real fix is HLL++ bias correction, which changes shipped estimates and the
    golden bytes.
11. **`EHUnivOptimized`'s sketch-tier cardinality is structurally
    unrecoverable** — not fixed, documented. Promotion fires only once a bucket
    holds `layer_size · rows · cols / 2` distinct keys, while UnivMon needs about
    `log2(distinct)` layers to recover `F0`; raising `layer_size` raises the
    promotion threshold in lockstep. Measured: 0–11 reported for windows holding
    16k–40k distinct.
12. **The eight inert `CMSHeap` instances** — documented above, deliberately
    unchanged.

## Test-side defects corrected in this pass

| What | Where | Why it was wrong |
| --- | --- | --- |
| `Binomial(n, p)` over the quantiles of one KLL | every rank battery | one compaction history; and `ε(k)` bounds the *maximum* rank error, not a per-`q` failure rate |
| `Binomial(n, p)` over the keys of one Count-Min / Count Sketch | every matrix battery | one hash function. Replaced by a union-bounded simultaneous bound plus a marginal rate pin |
| `Binomial(n, p)` over nested HLL / KMV checkpoints | cardinality batteries | the state at `10⁶` contains the state at `10⁵`. Replaced by one fresh sketch per checkpoint over a disjoint identity namespace |
| shard merges scored as a second probabilistic check | HLL, KMV, portable HLL | those merges are *exact* — the merged estimate is the same number. Now asserted as equalities |
| `RankErrorSpec::datasketches` presented as a theorem, in tests named `..._contract` / `..._theorem` | KLL suites | it is an empirical characterization fit from another implementation. Renamed to `KllRankSpec` and `..._characterization` throughout |
| `1/√(k−1)` for KMV, called "conservative" | `CardinalityConfidenceSpec` | it is smaller than the true `1/√(k−2)`, so it is stricter, not looser |
| KMV exact regime written `n ≤ k` | `CardinalityConfidenceSpec` | `estimate()` switches to the estimator at `len == k` |
| one truth helper for both DDSketch implementations | DDSketch batteries | they answer different order statistics |
| portable DDSketch endpoints asserted "exact too" | `ddsketch_endpoints_…` | it retains no min/max |
| `assert_between(us.len(), 850.0, 1150.0)` | `uniform_sampling_rate_and_merge` | the retained size is `⌈n·rate⌉` **exactly** — there is no randomness in it to leave slack for |
| `RANK_TOL = 0.03` | `tests/e2e_bulk.rs` | untied to `k`; now `KllRankSpec::datasketches(k).epsilon()` |
| `quantile_band(q, 0.03)` for Hydra KLL cells | `tests/e2e_frameworks.rs` | a Hydra KLL cell is a KLL; the band is `ε(k)` |
| UnivMon `L2` at `±5%` / `±15%` | `e2e_frameworks.rs`, `e2e_windows.rs` | `L2Norm` is `√(F2̂)` from an AMS row median; the band is `[√(1−b), √(1+b)]` with `b = √(2κ/w)` from the sketch's own geometry |
| Coco heavy keys at `[0.5×, 1.5×]` | `coco_point_queries_partition_the_inserted_mass` | the ceiling is derivable (`count + e(total−count)/w`); only the floor is empirical, and it now says so |
| Elastic elephant at `×1.05` | `elastic_never_underestimates_under_eviction_pressure` | the light layer is a Count-Min; its own `e(N−f)/cols` is the right ceiling |
| `CSHeap` heap entry compared with `it.count as f64 == est` | `assert_heap_matches_sketch` | hides the documented saturating conversion; now compares through `cs_heap_count` |

Corrections from the previous pass (Count Sketch under `ε·N`, `1.5 × bound`,
`α × 1.05`, hard-coded 0.02/0.03 KLL tolerances, wall-clock-seeded accuracy
tests, KMV's "4 standard errors" asserting 4%, flat 2%/3% HLL bands, regular/fast
path equality on hand-picked keys, `CountL2HH::get_l2_sqr` treated as exact,
ensemble HLL asserted equal to a standalone) remain in force.

## Seeded constructors and read-only diagnostics added for testing

| Added | Mirrors | Why |
| --- | --- | --- |
| `KLLDynamic::init_with_seed`, `init_kll_with_seed` | `KLL::init_with_seed` | `KLLDynamic` had no seeded constructor, so every accuracy assertion over it was re-rolled per run |
| `NitroBatch::with_target_and_seed`, `init_nitro_with_seed` | `with_target` / `init_nitro` | sampling is where all of Nitro's randomness lives |
| `KllSketch::with_seed`, `new_sketchlib_kll_with_seed` | `KLL::init_kll_with_seed` | the portable facade had only the wall-clock path |
| `HydraKllSketch::with_seed` | as above | every cell shares one prototype, so one seed makes the whole grid reproducible |
| `UnivMonQQuery::ordered_query_diagnostics` → `OrderedQueryDiagnostics` | — | reports the heavy set the CDF used and the residual sample size, so the full ordered-query bound can be evaluated. Read-only; no behavioural or wire change |
| `cs_heap_count` | — | names the `f64 → i64` conversion `CSHeap` uses, so its saturating semantics are API rather than an accidental cast |

## Still uncovered

| Instance / claim | Reason |
| --- | --- |
| `CMSHeap<Vector2D<i128>>`, `CMSHeap<Vector2D<f64>>`, `CMSHeap<QuickMatrixI128>`, `CMSHeap<DefaultMatrixI128>`, both paths (8) | **inert by construction** — no insert impl exists. Their constructibility and geometry accessors *are* pinned by `cmsheap_instances_without_an_insert_impl_are_inert_by_construction`; there is no behaviour beyond that to test |
| `NitroBatch<Vector2D<u32>>` per-key estimates | no public estimator: `NitroEstimate` is implemented only for the `Vector2D<i32>`-backed sketches, and `CountMin::estimate` cannot instantiate over `u32`. Admitted-mass coverage stands in; re-deriving the fast-path hash in a test is exactly how the Nitro estimator once shipped broken |
| `count_min_decodes_go_bytes`, `dd_sketch_decodes_go_bytes`, `hll_sketch_decodes_go_bytes`, `kll_sketch_decodes_go_bytes`, `hydra_kll_decodes_go_bytes` (`tests/msgpack_compat.rs`) | **5 ignored, empty tests — a gap, not coverage.** They need a msgpack fixture produced by `sketchlib-go`; this repository has no Go toolchain and no vendored encoder, so any bytes generated here would be this crate's own output compared against itself. Fabricating them would make the tests pass while testing nothing. The ASAPv1 envelope *does* have cross-language coverage via `asapv1_golden/*.hex` |
| UnivMon's composed `g`-sum constant | the recurrence composes per-layer Count Sketch errors across geometrically sampled substreams and the crate publishes no closed form. What is asserted is the terminal layer's own AMS bound plus the exact L1; entropy stays an empirical band |
| `EHUnivOptimized` sketch-tier L2 / entropy | same reason, at a configuration whose promotion threshold is structurally coupled to `layer_size` (finding 11) |
| User-supplied `SketchHasher` / `MatrixStorage` implementations | out of scope by request: only in-repo concrete and default types are enumerated |
| Independent **hash** seeds for the matrix frequency families | the hash is fixed by the sketch's type parameter and there is no per-instance seed, so a binomial over hash draws is unavailable. The simultaneous bound (no independence needed) and the marginal rate pin are what stand in; KMV reaches genuine hash independence only because `insert_by_hash` is public |
| `octo-runtime` accuracy under thread interleaving | the runtime tests assert equality against a single-threaded replay of the same partition, which is stronger than a band; no separate accuracy battery is warranted |
