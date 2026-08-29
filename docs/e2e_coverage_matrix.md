# E2E Coverage Matrix

Which public instance is covered by which E2E test, against what ground truth,
under which error metric, and whether the tolerance is a **theorem** or a
**documented empirical band**.

The distinction this document exists to make explicit:

> having a test ≠ having an accuracy test ≠ correctly verifying a theoretical
> guarantee.

A row whose `error_metric` is `structural` asserts an exact property
(serialization round trips, one-sided guarantees, merge equality, window
bookkeeping) and needs no bound. A row marked `empirical` asserts measured
behaviour and says so in its test name (`*_stays_within_the_documented_empirical_band`);
it is never presented as theory. Everything else is a theorem, cited in the
spec that computes it.

## How to read the columns

| Column | Meaning |
| --- | --- |
| `public_instance` | The concrete type a caller can construct |
| `e2e_test` | Test function, in `tests/` |
| `ground_truth` | Exact reference. Never another approximation |
| `error_metric` | What is bounded — and it differs per family |
| `bound_formula` | The formula evaluated at the instance's own parameters |
| `confidence` | Per-check failure probability the bound is quoted at |
| `deterministic_seed` | Stream seed / sketch seed. Both fixed, always |
| `status` | `theorem`, `empirical`, `structural`, or `gap` |

Acceptance rules live in [`tests/common/specs.rs`](../tests/common/specs.rs).
Every probabilistic bound is judged by a binomial tail at a fixed test level
(`TEST_LEVEL = 1e-6`), decided before the run: a battery of `n` checks with
per-check failure probability `p` may show at most `c` violations, where `c` is
the smallest count with `P[Bin(n, p) > c] <= 1e-6`. Tolerances are never
adjusted to whatever the current run produced.

## Error metrics, one per family

The single most common way to have a test that verifies nothing is to check a
sketch against another family's bound. These are the metrics in use:

| Family | Metric | Formula | Source |
| --- | --- | --- | --- |
| Count-Min | one-sided **additive** | `est >= f` always; `P[est - f > e(N-f)/w] <= e^-d` | Cormode & Muthukrishnan 2005, Thm 1 |
| Count Sketch | two-sided **L2**, rank-independent | `P[\|est-f\| > sqrt(kappa/w)*\|\|f_-i\|\|_2] <= P[Bin(d, 1/kappa) >= ceil(d/2)]`, `kappa=3` | Charikar, Chen & Farach-Colton 2002 |
| F2 from a Count Sketch matrix | **relative**, AMS row-sum | `\|F2_hat/F2 - 1\| <= sqrt(2*kappa/w)`, same median amplification | Alon, Matias & Szegedy 1996 |
| KLL | **rank** | `rank_incl(v) >= q - eps`, `rank_excl(v) <= q + eps`, `eps(k) = 2.446/k^0.9433` | Karnin, Lang & Liberty 2016; constant from the Apache DataSketches contract at 99% confidence |
| DDSketch | **relative value** | `\|est - true\|/\|true\| <= alpha + ULP slack` vs the exact nearest-rank order statistic | Masson, Rim & Lee 2019 |
| HLL Classic / ErtlMLE | **cardinality RSE** | `\|est/n - 1\| <= z * 1.04/sqrt(m)`, `m = 2^p` | Flajolet et al. 2007; Ertl 2017 attains the same CRLB |
| HLL HIP | **cardinality RSE**, tighter | `\|est/n - 1\| <= z * sqrt(ln2/m)` | Cohen 2015; Ting 2014 |
| KMV | **cardinality RSE** | exact for `n <= k`; `\|est/n - 1\| <= z/sqrt(k-1)` above | Bar-Yossef et al. 2002 |
| Bloom | **no false negatives** + predicted FPP | exact membership; measured FPP vs the filter's own sizing | `tests/e2e_membership.rs` |
| Nitro | **binomial sampling** | `\|est - f\| <= z*sqrt(f(1-p)/p)` | NitroSketch, SIGCOMM 2019 |
| OctoSketch promotion | **deterministic residual** | `ref - k*tau <= octo <= ref` per counter | OctoSketch, NSDI 2024, Thm 1 |

Three pairings that are specifically wrong and are now prevented by having
separate spec types: Count Sketch under Count-Min's `eps*N`; KLL under a
relative *value* tolerance; DDSketch inside a *rank* battery.

## Matrix-backed frequency families

Ten storage backends × two hashing paths. Each instance is judged at the
dimensions **it** reports, so the fixed 5×2048 (`Quick*`) and 3×4096
(`Default*`) layouts are not evaluated at a borrowed `w`.

| public_instance | e2e_test | ground_truth | error_metric | bound_formula | confidence | deterministic_seed | status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `CountMin<Vector2D<i32\|i64\|i128\|f64>, RegularPath>` | `countmin_regular_path_instances_satisfy_the_count_min_bound` | `FreqTruth` | additive one-sided | `e(N-f)/w` | `e^-d` | stream `0x10BEC700` | theorem |
| `CountMin<…, FastPath>` (same 4) | `countmin_fast_path_instances_satisfy_the_count_min_bound` | `FreqTruth` | additive one-sided | `e(N-f)/w` | `e^-d` | stream `0x10BEC700` | theorem |
| `CountMin<FixedMatrix\|DefaultMatrixI32\|QuickMatrixI64\|QuickMatrixI128\|DefaultMatrixI64\|DefaultMatrixI128, Regular\|Fast>` (12) | same two tests | `FreqTruth` | additive one-sided | `e(N-f)/w` | `e^-d` | stream `0x10BEC700` | theorem |
| all 20 `CountMin` instances | `countmin_both_paths_are_exact_on_a_collision_free_workload` | exact counts | structural | `est == f` | — | 8 fixed keys | structural |
| `CountMin<Vector2D<i64>, FastPath>` (production-sized) | `countmin_zipf_satisfies_the_count_min_additive_bound_and_shard_merge` | `FreqTruth` | additive one-sided + merge equality | `e(N-f)/w` | `e^-d` | stream `1001` | theorem |
| `CountMin<Vector2D<i32>, Regular\|Fast>` over `U64`/`F64` inputs | `countmin_satisfies_the_count_min_theorem_on_both_paths` | `FreqTruth` | additive one-sided | `e(N-f)/w` | `e^-d` | 3 stream seeds from `1005` | theorem |
| `Count<Vector2D<i32\|i64\|i128>, Regular\|Fast>` + 6 fixed storages (18) | `countsketch_{regular,fast}_path_instances_satisfy_the_l2_bound` | `FreqTruth` | L2, rank-independent | `sqrt(3/w)*\|\|f_-i\|\|_2` | `P[Bin(d,1/3) >= ceil(d/2)]` | stream `0x10BEC700` | theorem |
| all 18 `Count` instances | `countsketch_both_paths_are_exact_on_a_collision_free_workload` | exact counts | structural | `est == f` | — | 8 fixed keys | structural |
| `Count<Vector2D<i64>, RegularPath>` (production-sized) | `countsketch_turnstile_cancels_and_satisfies_the_l2_median_bound` | `FreqTruth` | L2 + exact cancellation | `sqrt(3/w)*\|\|f_-i\|\|_2` | as above | stream `1003` | theorem |
| `Count<Vector2D<i32>, Regular\|Fast>`, pooled trials | `countsketch_satisfies_the_l2_median_bound_on_both_paths` | `FreqTruth` | L2 | `sqrt(3/w)*\|\|f_-i\|\|_2` | as above | 3 stream seeds from `1005` | theorem |
| `Count<Vector2D<i64>, RegularPath>` | `countsketch_error_stays_rank_independent_within_the_documented_empirical_band` | `FreqTruth` deciles | mean \|error\| per frequency decile | spread ≤ 3× (measured 1.29×) | — | stream `1007` | empirical |
| `CMSHeap<{Vector2D<i32>,Vector2D<i64>,FixedMatrix,DefaultMatrixI32,QuickMatrixI64,DefaultMatrixI64}, Regular\|Fast>` (12) | `cmsheap_{regular,fast}_path_instances_satisfy_the_count_min_bound` | `FreqTruth` | additive one-sided + heap consistency + recall | `e(N-f)/w` | `e^-d` | stream `0x10BEC700` | theorem |
| `CSHeap<same 6 storages, Regular\|Fast>` (12) | `csheap_{regular,fast}_path_instances_satisfy_the_l2_bound` | `FreqTruth` | L2 + heap consistency + recall | `sqrt(3/w)*\|\|f_-i\|\|_2` | as above | stream `0x10BEC700` | theorem |
| `CMSHeap`/`CSHeap` `<QuickMatrixI128\|DefaultMatrixI128, Regular\|Fast>` (4 each) | — | — | — | — | — | — | **gap — not insertable** (see below) |
| `CMSHeap<Vector2D<i64>, RegularPath>` / `CSHeap<Vector2D<i64>, RegularPath>` | `heaps_satisfy_their_own_bounds_and_stay_heap_consistent` | `FreqTruth` | per-family bound + heap/sketch equality + top-k recall | as above | as above | stream `1004` | theorem |
| `i32` / `i64` / `i128` counter widths | `countmin_counter_widths_carry_the_mass_their_type_allows`, `countsketch_counter_widths_carry_signed_mass_in_both_directions` | exact | structural | no wrap at each width's ceiling | — | fixed keys | structural |
| `CountL2HH<DefaultXxHasher>` | `countl2hh_weighted_turnstile_satisfies_the_l2_median_bound` | `FreqTruth` | L2 + AMS F2 | `sqrt(3/w)*\|\|f_-i\|\|_2`; `sqrt(6/w)` for F2 | as above | stream `1005`, hash seed idx `11` | theorem |
| `FoldCMS` / `FoldCS` after a 16-way merge | `folded_sketches_keep_their_own_bounds_through_a_sixteen_way_merge` | `FreqTruth` | per-family bound at the **folded** width | `e(N-f)/w'`, `sqrt(3/w')*\|\|f_-i\|\|_2`, `w' = w >> fold_level` | `e^-d` / median tail | stream `1009` | theorem |
| portable `CountMinSketch` / `CountSketch` | `portable_cms_and_cs_string_keys_satisfy_their_own_bounds` | `FreqTruth` over string keys | per-family bound | as above | as above | stream `1006` | theorem |

### Known gap: 128-bit heap variants

`CMSHeap<QuickMatrixI128>`, `CMSHeap<DefaultMatrixI128>` and their `CSHeap`
twins have `Default` impls (so they construct and compile) but their insert
paths are bounded on `S::Counter: Into<i64>`, which `i128` does not satisfy.
They are **constructible but not insertable**, so there is no behaviour to
test. Four instances per family. Fixing this is a library change (widening the
heap's count type or bounding on `TryInto<i64>`), not a test change.

## Quantiles

| public_instance | e2e_test | ground_truth | error_metric | bound_formula | confidence | deterministic_seed | status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `KLL<f64>`, `KLLDynamic<f64>` × k∈{64,200,800} × 6 distributions × 4 feed modes | `kll_family_satisfies_the_datasketches_normalized_rank_error_contract` | `NumericTruth` rank intervals | rank | `eps(k) = 2.446/k^0.9433` | 0.01 per query | sketch `0x5EED0001..4`, stream `0xA5A50000+` | theorem |
| `KLL<f64>` k∈{64,256,1024} | `kll_rank_error_shrinks_with_k_as_the_contract_predicts` | `NumericTruth` | rank scaling | worst error ≤ `eps(k)`; ≥4× improvement over 16× k | — | sketch `0x5EED0001..4`, stream `0xC0FFEE01` | theorem |
| `KLL<T>`, `KLLDynamic<T>` for all 14 `NumericalValue` types | `every_numeric_type_satisfies_the_kll_rank_error_contract` | `NumericTruth` over the `to_f64` projection | rank | `eps(200)` | 0.01 | sketch `0x4E170001..3`, stream `0x57EA0001` | theorem |
| the 8 signed/float types | `signed_numeric_types_order_negative_values_correctly_in_kll` | `NumericTruth` | rank + range containment | `eps(200)`; answers inside `[min, max]` | 0.01 | same | theorem |
| `KLL<u128>` above `2^70` | `the_f64_projection_is_exact_below_two_to_the_53` | `NumericTruth` | rank + projection exactness | `eps(200)`; `(v as f64) as u128 == v` for `v <= 2^53` | 0.01 | `0x57EA0001` | theorem |
| `TumblingWindow<KLL>` (`query_all`, `query_recent`, active) | `tumbling_kll_windows_are_exact_and_answers_satisfy_the_rank_contract` | exact window slice | rank + exact window count | `eps(200)` | 0.01 | sketch `0x77170001`, stream `3009` | theorem |
| portable `HydraKllSketch` per key | `portable_hydra_kll_per_key_medians_satisfy_the_rank_contract` | per-key `NumericTruth` | rank | `eps(200)` | 0.01 | streams `3010`/`3011` | theorem |
| portable `KllSketch` (+ merge, msgpack round trip) | `portable_kll_sketch_satisfies_the_rank_error_contract_through_merge_and_wire` | `NumericTruth` | rank + wire equality | `eps(200)` | 0.01 | sketch `0x5EED0400` | theorem |
| `DDSketch` and portable `DdSketch`, alpha ∈ {0.001, 0.01, 0.05, 0.1} × 6 shapes × 3 sizes | `ddsketch_core_and_portable_satisfy_the_relative_value_error_contract` | exact nearest-rank order statistics | relative value | `alpha + 8*eps_f64*(1+\|ln v\|)` | deterministic — **zero** violations tolerated | streams from `3005000` | theorem |
| both, all alphas | `ddsketch_endpoints_return_the_exact_min_and_max` | exact min/max | structural | `q=0 -> min`, `q=1 -> max` exactly | — | stream `4242` | structural |
| both, bucket edges `gamma^k` for k ∈ {−40,−7,0,1,13,60,200} | `ddsketch_satisfies_the_relative_error_contract_at_bucket_boundaries` | the probe value itself | relative value | `alpha + ULP` at the edge, ±1 ULP either side, and the interior | zero tolerated | fixed probes | theorem |
| both, 4-way merge + delta replay | `ddsketch_merge_and_delta_replay_preserve_the_relative_error_contract` | exact order statistics | relative value | `alpha + ULP` | zero tolerated | stream `7654321+alpha` | theorem |
| `DDSketch::add<T>` for all 14 `NumericalValue` types × 3 alphas | `every_numeric_type_satisfies_the_ddsketch_relative_value_error_contract` | exact order statistics of the projection | relative value | `alpha + ULP` | zero tolerated | stream `0x57EA0001` | theorem |
| `DDSketch` over `u128` above `2^70` | `the_f64_projection_is_exact_below_two_to_the_53` | exact order statistics | relative value + projection limit | `alpha + ULP` | zero tolerated | `0x57EA0001` | theorem |
| both | `ddsketch_rejects_untrackable_values_and_mapping_mismatches`, `portable_ddsketch_rejects_hostile_delta_spans` | — | structural | drop non-indexable inputs; reject far-span deltas without allocating | — | fixed | structural |

## Cardinality

| public_instance | e2e_test | ground_truth | error_metric | bound_formula | confidence | deterministic_seed | status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `HyperLogLogP12/P14/P16<Classic>` | `hll_classic_p{12,14,16}_satisfies_its_register_error_model` | exact identity count | cardinality RSE | `z * 1.04/sqrt(2^p)`, `z=4` | 6.3e-5 | identities `0..n` | theorem |
| `HyperLogLogP12/P14/P16<ErtlMLE>` | `hll_ertl_mle_p{12,14,16}_satisfies_the_cramer_rao_error_model` | exact | cardinality RSE | same constant (CRLB) | 6.3e-5 | identities `0..n` | theorem |
| `HyperLogLogHIPP12/P14/P16` | `hll_hip_p{12,14,16}_satisfies_the_hip_error_model` | exact | cardinality RSE | `z * sqrt(ln2/2^p)` | 6.3e-5 | identities `0..n` | theorem |
| all 9 above | same tests | exact | structural | duplicate replay must not move the estimate at all | — | — | structural |
| the 6 mergeable ones | same tests | exact | cardinality RSE | disjoint even/odd shard merge in the same band | 6.3e-5 | — | theorem |
| portable `HllSketch` × {Regular, Datafusion, Hip} × p{12,14,16} (9) | `portable_hll_variants_and_precisions_satisfy_the_register_error_model` | `HashSet` | cardinality RSE | `z * 1.04/sqrt(2^p)` — the variant is a **wire tag**, not an estimator | 6.3e-5 | streams `2001+p` | theorem |
| `HyperLogLogP12/P14/P16<Classic>` | `hll_accuracy_improves_with_precision_as_the_error_model_predicts` | exact | measured RSE over 6 blocks | ≤ 2× predicted; ≥ 2× improvement p12→p16 | — | identities `0..6e6` | theorem |
| `HyperLogLogP12/P14/P16<Classic>` at `n = 2.5m` | `hll_classic_switchover_band_stays_within_the_documented_empirical_band` | exact | measured RSE | 1.5×–10× the asymptotic RSE (measured 2.1×/3.5×/6.3×) | — | identities `0..` | **empirical** — see finding below |
| `KMV`, exact and estimated regimes | `kmv_satisfies_its_relative_standard_error_across_both_regimes` | exact | cardinality RSE | exact for `n<=k`; `z/sqrt(k-1)`, `z=4` → 6.25% at k=4096 | 6.3e-5 | identities `0..n` | theorem |
| `KMV` over a duplicate-bearing stream | `kmv_over_a_duplicate_bearing_stream_satisfies_its_error_model` | `HashSet` | cardinality RSE | as above | 6.3e-5 | stream `5001` | theorem |
| `SetAggregator` | `set_aggregator_union_is_exact` | `HashSet` | structural | exact union | — | stream `2002` | structural |

## Windowed frameworks

| public_instance | e2e_test | ground_truth | error_metric | bound_formula | confidence | deterministic_seed | status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `EHSketchList::CM` in an `ExponentialHistogram` | `eh_count_min_variant_satisfies_the_count_min_bound_over_the_retained_window` | exact truth over the merged bucket span | additive one-sided | `e(N-f)/w` | `e^-d` | stream `0x0E110001` | theorem |
| `EHSketchList::CS` | `eh_count_sketch_variant_satisfies_the_l2_bound_over_the_retained_window` | same | L2 | `sqrt(3/w)*\|\|f_-i\|\|_2` | median tail | `0x0E110001` | theorem |
| `EHSketchList::COUNTL2HH` | `eh_countl2hh_variant_satisfies_the_l2_bound_over_the_retained_window` | same | L2 | as above | median tail | `0x0E110001` | theorem |
| `EHSketchList::COCO`, `::ELASTIC` | `eh_heavy_hitter_variants_stay_one_sided_over_the_retained_window` | same | one-sided on heavy keys | `est >= f` for the true top 32 | — | `0x0E110001` | structural |
| `EHSketchList::HLL` | `eh_hll_variant_satisfies_the_register_error_model_over_the_retained_window` | `HashSet` over the span | cardinality RSE | `4 * 1.04/sqrt(2^14)` | 6.3e-5 | `0x0E110001` | theorem |
| `EHSketchList::KLL` | `eh_kll_variant_satisfies_the_rank_error_contract_over_the_retained_window` | `NumericTruth` over the span | rank | `eps(200)` | 0.01 | sketch `0x5EED0200` | theorem |
| `EHSketchList::DDS` | `eh_ddsketch_variant_satisfies_the_relative_value_error_contract_over_the_window` | exact order statistics | relative value | `alpha + ULP` | zero tolerated | `0x0E110001` | theorem |
| `EHSketchList::UNIVMON` | `eh_univmon_variant_reports_the_exact_l1_over_the_retained_window` | exact | L1 exact; L2 empirical | `calc_l1 == N`; L2 within ±15% (measured 3%) | — | `0x0E110001` | structural + empirical |
| `EHSketchList::UNIFORM` (experimental) | `eh_uniform_sampling_variant_reports_exact_retention_bookkeeping` | exact | structural | `total_seen` exact; samples ⊆ window | — | sampler `0x5A9101` | structural |
| all 10 variants | `every_eh_variant_can_merge_into_its_own_kind`, `every_eh_variant_selects_the_documented_merge_norm` | — | structural | merge arm present; L2 norm for COUNTL2HH/UNIVMON, L1 otherwise | — | — | structural |
| `ExponentialHistogram` expiry | `eh_expires_buckets_past_the_window_and_reports_its_retained_span` | exact retained events | structural | no bucket entirely before the cutoff | — | — | structural |
| `TumblingWindow<KLL>` | `tumbling_kll_windows_are_exact_and_answers_satisfy_the_rank_contract` | exact slices | rank + exact window count | `eps(200)` | 0.01 | sketch `0x77170001` | theorem |
| `TumblingWindow<FoldCMS>` | `tumbling_foldcms_weighted_windows_exact_counts` | exact | structural | exact weighted counts, flush, rotation | — | — | structural |
| `TumblingWindow<FoldCS>` | `tumbling_fold_cs_windows_are_exact_and_answers_satisfy_the_l2_bound` | exact slices | L2 at the folded width | `sqrt(3/w')*\|\|f_-i\|\|_2` | median tail | stream `0x0E110001` | theorem |
| `TumblingWindow<UnivMonQ>` | `tumbling_univmon_q_windows_carry_exact_aggregates_through_rotation` | exact slices | structural | count/min/max exact per window; pool reuse clears | — | stream `0x0E110001` | structural |
| `EHUnivOptimized` map tier | `eh_univ_optimized_map_tier_exact_windows`, `…_matches_exact_per_key_counts_on_a_skewed_stream` | exact | structural | exact per-key counts | — | stream `9001` | structural |
| `EHUnivOptimized` sketch tier | `eh_univ_optimized_promotes_into_the_sketch_tier_and_answers_from_it` | exact over the promoted span | L1 exact; L2/entropy empirical | `calc_l1 == N`; L2 ±10%, entropy −10/+40% | — | stream `4242` | structural + empirical |
| `EHUnivOptimized` mixed interval, expiry, pool reuse | `…_answers_a_mixed_map_and_sketch_interval`, `…_expires_buckets_past_the_window`, `…_reuses_pooled_sketches_without_leaking_state` | exact | structural | exact L1 over the retained span | — | — | structural |
| `EHUnivOptimized` sketch-tier cardinality | `eh_univ_optimized_sketch_tier_cardinality_is_documented_as_unrecoverable` | exact | — | reported < 10% of true | — | stream `4242` | **empirical** — see finding below |

## Composition

| public_instance | e2e_test | ground_truth | error_metric | bound_formula | confidence | deterministic_seed | status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `EnsembleSketch::{CountMinFast, CountFast}` | `ensemble_members_match_standalone_sketches_fed_the_same_stream` | standalone sketch on the same stream | structural | exact equality, key for key | — | stream `0xC0905101` | structural |
| `EnsembleSketch::{HllErtl, HllClassic, HllHip}` | same | `FreqTruth` distinct + standalone | cardinality RSE | each member's own model; ensemble and standalone agree within their combined band (they use different hashes by design) | 6.3e-5 | `0xC0905101` | theorem |
| all 5 member variants | `ensemble_members_satisfy_their_own_error_bounds` | `FreqTruth` / `HashSet` | per-family | Count-Min additive, Count L2, HLL register, HIP HIP | as each | `0xC0905101` | theorem |
| multi-matrix ensembles, layout compatibility | `ensemble_composes_by_hash_layout_and_rejects_incompatible_members` | `FreqTruth` | per-family + structural | each member judged at **its own** width; mismatched rows or packing modes rejected | as each | `0xC0905101` | theorem + structural |
| `NitroBatch<CountMin<Vector2D<i32>, FastPath>>`, `NitroBatch<Count<…>>`, rates 1.0/0.5/0.1/0.01 | `nitro_estimates_stay_inside_the_binomial_sampling_band_on_every_target` | exact count | binomial sampling | `z*sqrt(f(1-p)/p)`, `z=4` | 6.3e-5 | sampling seeds `0x01170001..4` | theorem |
| `NitroBatch<Vector2D<u32>>` (all 4 rates) | `nitro_over_a_bare_vector2d_target_admits_mass_inside_the_sampling_band` | exact count | binomial sampling on admitted mass | as above | 6.3e-5 | same | theorem |
| `NitroBatch` rate = 1.0 | `nitro_at_full_sampling_is_exact` | exact | structural | exact | — | `0x01170001` | structural |
| `NitroBatch` seeding | `nitro_sampling_is_reproducible_from_its_seed` | — | structural | same seed → same result; different seeds → different | — | 4 seeds | structural |
| `NitroBatch::merge`, saturation | `nitro_merge_sums_admitted_mass_at_the_combined_band`, `nitro_saturates_oversized_weights_instead_of_wrapping` | exact | binomial + structural | as above; clamp at `i32::MAX`/`u32::MAX` | 6.3e-5 | — | theorem + structural |
| `UnivMonQ` default / `counter_bits=64` / `width_halving_period=2` / explicit `hash_seed` | `univmonq_configuration_variants_all_build_and_keep_exact_aggregates` | `NumericTruth` | structural | count/min/max exact; config round-trips | — | stream `0xC0905101` | structural |
| `UnivMonQ` `ordered_samples = 0` | `univmonq_with_ordered_samples_disabled_answers_everything_except_ordered_queries` | `NumericTruth` | structural | ordered queries `None`; endpoints still exact | — | `0xC0905101` | structural |
| `UnivMonQConfig::with_window_bound` | `univmonq_with_window_bound_chooses_a_hierarchy_that_satisfies_its_own_inequality` | Bernstein bound recomputed in-test | structural | chosen `levels` is the smallest with `mean + sqrt(2·mean·ln(1/δ)) + (2/3)ln(1/δ) < candidates` | δ = 1e-3 | — | theorem |
| `UnivMonQ::with_hasher_and_source_id`, 4-shard merge | `univmonq_multi_shard_merge_with_distinct_source_ids_covers_the_union` | `NumericTruth` | structural | merged count/min/max exact | — | `0xC0905101` | structural |
| `UnivMonQ::estimate_frequency` / `estimate_f2` | `univmonq_frequency_and_f2_satisfy_the_count_sketch_bounds` | `FreqTruth` | L2 + AMS F2 | `sqrt(3/w)*\|\|f_-i\|\|_2`, `sqrt(6/w)` | median tail | stream `3008` | theorem |
| `UnivMonQ::{rank, cdf, quantile}` in the diffuse regime | `univmonq_ordered_queries_satisfy_the_residual_occurrence_bound_when_diffuse` | `NumericTruth` | rank, from the occurrence sample | `eps_R = sqrt(ln(2/δ)/(2 m_R))` | δ = 0.01 | stream `0x0DDE0001` | theorem (partial — see below) |
| `UnivMonQ::{estimate_distinct, estimate_entropy, heavy_hitters}` | `univmonq_distinct_entropy_and_recall_stay_within_the_documented_empirical_band` | `FreqTruth` | relative | ±10% distinct, ±10% entropy, ≥8/10 recall | — | stream `3008` | **empirical** |
| portable `CountMinSketchWithHeap` | `portable_count_min_with_heap_satisfies_the_count_min_bound_through_merge_and_wire` | `FreqTruth` | additive one-sided + heap + wire equality | `e(N-f)/w` | `e^-d` | stream `0xC0905101` | theorem |
| portable `CountSketchWithHeap` | `portable_count_sketch_with_heap_satisfies_the_l2_bound_through_merge_and_wire` | `FreqTruth` | L2 + heap + wire | `sqrt(3/w)*\|\|f_-i\|\|_2` | median tail | `0xC0905101` | theorem |

### UnivMon-Q ordered queries: what is and is not verified

The documented contract is
`sup_x |F_hat(x) − F(x)| <= 2 E_H + P_hat_R * eps_R`. `E_H` is the frequency
error over the sketch's **internally recovered** heavy set, and neither that
set nor `m_R` is reachable through the public API, so the complete theorem
**cannot be verified from outside the crate** and the test does not claim to.

What it verifies instead is the strongest contract public state supports: the
adaptive gate is `F2_hat / N^2 >= 1 / ordered_samples`, and both sides are
public. On a stream where the gate provably does not fire, the heavy set is
empty, so `E_H = 0`, `P_hat_R = 1`, and the whole bound collapses to the
distribution-free occurrence bound `eps_R`, with `m_R` observable as the number
of CDF breakpoints. The test asserts the gate premise first and fails loudly if
it is not met.

## OctoSketch

| public_instance | e2e_test | ground_truth | error_metric | bound_formula | confidence | deterministic_seed | status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `CmTopKOctoPlan/Worker/Aggregator`, HashByKey + RoundRobin | `cm_top_k_point_estimates_trail_the_single_thread_reference_by_at_most_k_tau` | single-thread `CMSHeap` **and** `FreqTruth` | promotion residual + additive | `ref − k·tau <= octo <= ref`; `f − k·tau <= est <= f + e(N−f)/w` | deterministic; `e^-d` | stream `0x0C701101` | theorem |
| `CountTopKOctoPlan/Worker/Aggregator`, both partitions | `count_top_k_point_estimates_satisfy_the_l2_bound_within_the_promotion_residual` | single-thread `CSHeap` **and** `FreqTruth` | promotion residual + L2 | `\|octo − ref\| <= k·tau`; `\|est − f\| <= sqrt(3/w)\|\|f_-i\|\|_2 + k·tau` | deterministic; median tail | `0x0C701101` | theorem |
| both top-k plans, both partitions | `top_k_plans_recover_the_heavy_hitters_their_promotion_floor_guarantees` | `FreqTruth` top-k | recall / precision / heap consistency | recall = 100% above the `k·tau` promotion floor; nothing in the heap below the k-th count − `k·tau` | deterministic | `0x0C701101` | theorem |
| `CmTopKOctoWorker` below threshold | `a_key_below_the_promotion_threshold_never_reaches_the_top_k_aggregator` | exact | structural | no promotion under `tau`; first promotion hands over exactly `tau` | — | fixed key | structural |
| `CmOctoPlan`, `CountOctoPlan` | pre-existing `theorem_1_bounds_the_count_min_error`, `theorem_3_bounds_the_count_sketch_error` | `FreqTruth` | additive / L2 with the `k'·tau` residual | paper's Thms 1 and 3 | `delta = 2^-d` | fixed | theorem, **both partitions** |
| `HllOctoPlan`, `DdOctoPlan`, `CocoOctoPlan`, `ElasticOctoPlan`, `UnivMonOctoPlan` | pre-existing `tests/e2e_octo.rs` (mass conservation, flush completeness, sketch-merge baselines) | exact mass / `FreqTruth` | per-family | see that suite | — | fixed | theorem, **HashByKey only** |

The `HashByKey` premise is named in the assertions where it matters: under it a
flow's whole count lands on one worker so it promotes `floor(f/tau)` times,
while `RoundRobin` splits the same flow `k` ways. Recall is therefore measured
per partition rather than averaged.

Partition coverage is uneven and this pass did not change that: `CmOctoPlan`,
`CountOctoPlan` and the two top-k plans run under both `HashByKey` and
`RoundRobin`; the HLL, DDSketch, Coco, Elastic and UnivMon plans are driven
under `HashByKey` only outside the `octo-runtime` feature. Their invariants
(mass conservation, flush completeness) are partition-independent, but their
accuracy comparisons are not.

## Heavy hitters and membership (pre-existing, unchanged)

| public_instance | e2e_test | ground_truth | error_metric | status |
| --- | --- | --- | --- | --- |
| `SpaceSaving` | `tests/e2e_heavy_hitters.rs` (Space-Saving group, 20 tests) | `FreqTruth` | error sandwich, `min_count` ceiling | theorem |
| `Coco`, `Elastic` | `tests/e2e_heavy_hitters.rs` (`keyed_bucket` module, 9 tests) | `FreqTruth` | over-attribution / one-sided under eviction | theorem |
| `Bloom<RegularPath>`, `Bloom<FastPath>` | `tests/e2e_membership.rs` (30 tests) | exact sets | no false negatives; measured FPP vs the filter's own sizing | theorem |
| `Hydra` with CM / CS / HLL / KLL / UnivMon counters | `tests/e2e_frameworks.rs` (29 tests) | exact lattice truth | additive grid bound, exact marginals, exact shard merge | theorem |

## Production findings from this pass

1. **`EHSketchList::merge` had no `ELASTIC` arm** — fixed. `EHBucket::to_merge`
   discards the `Result`, so an `ExponentialHistogram` over the Elastic payload
   silently dropped every bucket merge while still counting bucket sizes. Every
   heavy key read 0. Pinned by
   `eh_heavy_hitter_variants_stay_one_sided_over_the_retained_window` and
   `every_eh_variant_can_merge_into_its_own_kind`.
2. **HLL Classic's accuracy cliff at the linear-counting switchover** — not
   fixed, documented. At `n ≈ 2.5m` the 2007 estimator's two branches do not
   meet smoothly and the RSE reaches 6.3× the asymptotic `1.04/sqrt(m)` at p16.
   The old checkpoint grid stepped over the band for every precision. Pinned by
   `hll_classic_switchover_band_stays_within_the_documented_empirical_band`,
   which fails if the cliff is ever removed so the test gets updated rather than
   left stale. A real fix is HLL++ bias correction, which changes shipped
   estimates and the golden bytes.
3. **`EHUnivOptimized`'s sketch-tier cardinality is structurally
   unrecoverable** — not fixed, documented. Promotion fires only once a bucket
   holds `layer_size * rows * cols / 2` distinct keys, while UnivMon needs about
   `log2(distinct)` layers to recover `F0`; raising `layer_size` raises the
   promotion threshold in lockstep, so the two requirements cannot both be met
   by tuning. Measured: 0–11 reported for windows holding 16k–40k distinct.
   Pinned by `eh_univ_optimized_sketch_tier_cardinality_is_documented_as_unrecoverable`.
4. **`CMSHeap`/`CSHeap` `i128` instances are constructible but not insertable** —
   not fixed, documented above.

## Seeded constructors added for deterministic testing

Four public constructors were added so accuracy tests can reproduce a failure.
Each mirrors an existing one and changes nothing about the unseeded path.

| Added | Mirrors | Why |
| --- | --- | --- |
| `KLLDynamic::init_with_seed`, `KLLDynamic::init_kll_with_seed` | `KLL::init_with_seed` / `init_kll_with_seed` | `KLLDynamic` had no seeded constructor at all, so every accuracy assertion over it was re-rolled per run. The seed is stored so `clear()` re-seeds from it, and is `#[serde(skip)]` — it describes how the sketch was built, not what it holds, so the wire format is unchanged |
| `NitroBatch::with_target_and_seed`, `NitroBatch::init_nitro_with_seed` | `NitroBatch::with_target` / `init_nitro` | sampling is where all of Nitro's randomness lives; without a seed the admitted subset differs every run |
| `KllSketch::with_seed`, `new_sketchlib_kll_with_seed` | `KLL::init_kll_with_seed` | the portable facade had only the wall-clock path |
| `HydraKllSketch::with_seed` | as above | every cell shares one prototype, so one seed makes the whole grid reproducible |

Documented in `docs/api/api_kll.md` and `docs/api/api_nitrobatch.md`.

## Test-side defects corrected

| What | Where it was | Why it was wrong |
| --- | --- | --- |
| Count Sketch checked against Count-Min's `eps*N` with `delta = e^-rows` | `countsketch_error_bound_covers_most_keys_on_both_paths` | Count Sketch's error is `L2/sqrt(w)`, two-sided and rank-independent; `eps*N` is enormously looser on a skewed stream, so passing it said almost nothing |
| `1.5 * bound` with no explanation | Count Sketch median and portable CS tests | an undeclared 50% widening of a bound is not that bound |
| `alpha * 1.05` for DDSketch | `ddsketch_alpha_across_distributions_core_and_portable` | accepts results 5% past the advertised guarantee — the one thing the guarantee forbids. Measured worst case is exactly `alpha`, so the slop was unnecessary as well as wrong |
| KLL checked with a hard-coded 0.02/0.03 rank tolerance | quantile suites | untied to `k`: passes identically at k=64 and k=800, so it cannot see `k` failing to reach the compactors |
| `KLL::init_kll` (wall-clock seeded) in accuracy tests | `kll_quantile_rank_bands_and_shard_merge` and others | a failure would not reproduce |
| `KLLDynamic::init_kll` only | all KLLDynamic tests | no seeded constructor existed; one was added |
| KMV "4 standard errors" asserting 4% at k=4096 | `kmv_*` | `4/sqrt(4095) = 6.25%`, so the comment and the number disagreed. The band is now computed from `z` and `k` |
| HLL at a flat 2%/3% for every precision and estimator | `hll_variants_checkpoints_and_shard_merge` | 4.9σ at p16 and 1.2σ at p12 — simultaneously too loose and too tight, and it held HIP to Classic's constant |
| `NitroBatch::with_target` (OS-seeded) with ±5%/±10% bands | `nitro_unbiased_across_rates_cm_and_cs_targets` | re-rolled every run and unrelated to the estimator's variance; a seeded constructor was added |
| Regular/fast path *equality* asserted on 4 hand-picked keys | `countmin_regular_fast_paths_agree_on_stream` | the two paths use different hash functions and legitimately disagree on about a third of keys; the equality held by coincidence at those dimensions |
| `CountL2HH::get_l2_sqr` treated as exact | `countl2hh_weighted_f2_with_decrements` | it is the AMS row-median estimator over the sketch's own counters, with a real bound |
| Ensemble HLL asserted equal to a standalone HLL | new test, corrected before landing | the ensemble feeds HLL the shared *matrix* hash, not the canonical seed — equally accurate, not equal |
| Ensemble "same dimensions" assumed | new test, corrected before landing | compatibility is by hash *layout* (`rows`, packing mode); different widths compose fine and each folds with its own `cols` |

## Still uncovered

| Instance | Reason |
| --- | --- |
| `CMSHeap`/`CSHeap` over `QuickMatrixI128`, `DefaultMatrixI128` (8 instances) | not insertable — `S::Counter: Into<i64>` excludes `i128`. Library change required |
| `NitroBatch<Vector2D<u32>>` per-key estimates | no public estimator: `NitroEstimate` is implemented only for the `Vector2D<i32>`-backed sketches, and `CountMin::estimate` cannot instantiate over `u32`. Admitted-mass coverage stands in; re-deriving the fast-path hash in a test is exactly how the Nitro estimator once shipped broken |
| `UnivMonQ`'s full ordered-query theorem | `E_H` and `m_R` are not public. The diffuse-regime half is verified; see above |
| User-supplied `SketchHasher` / `MatrixStorage` implementations | out of scope by request: only in-repo concrete and default types are enumerated |
| `octo-runtime` thread-scheduling paths | covered by the pre-existing `mod runtime` tests under `--features octo-runtime`; this pass added no new instances there |
| `RoundRobin` accuracy comparisons for the HLL, DDSketch, Coco, Elastic and UnivMon Octo plans | pre-existing gap, unchanged by this pass; their partition-independent invariants are covered |
| `EHSketchList::UNIFORM` inside the accuracy batteries | reservoir sampling has no per-key or per-quantile guarantee to assert; its retention bookkeeping is covered exactly |
