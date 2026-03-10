# Test Matrix

One component per section. Each section contains a table with test name, description, and what the test validates.

## How To Run

```bash
cargo test
```

## Sketches

### CountMin

Test file: [`src/sketches/countmin.rs`](../src/sketches/countmin.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `dimension_test` | Dimension test. | Construction defaults and configuration invariants are validated. |
| `fast_insert_same_estimate` | Fast insert same estimate. | Fast-path behavior remains consistent with the regular path. |
| `merge_adds_counters_element_wise` | Merge adds counters element wise. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `merge_requires_matching_dimensions` | Merge requires matching dimensions. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `cm_regular_path_correctness` | Cm regular path correctness. | Core functional behavior for this component path is validated. |
| `cm_fast_path_correctness` | Cm fast path correctness. | Fast-path behavior remains consistent with the regular path. |
| `cm_error_bound_zipf` | Cm error bound Zipf. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `cm_error_bound_uniform` | Cm error bound uniform. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `count_min_round_trip_serialization` | Count min round trip serialization. | Serialization/deserialization preserves component state and behavior after round trip. |

### Count Sketch

Test file: [`src/sketches/count.rs`](../src/sketches/count.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `default_initializes_expected_dimensions` | Default initializes expected dimensions. | Construction defaults and configuration invariants are validated. |
| `with_dimensions_uses_custom_sizes` | With dimensions uses custom sizes. | Construction defaults and configuration invariants are validated. |
| `insert_updates_signed_counters_per_row` | Insert updates signed counters per row. | Count-Sketch sign/hash update logic is applied correctly. |
| `fast_insert_produces_consistent_estimates` | Fast insert produces consistent estimates. | Fast-path behavior remains consistent with the regular path. |
| `insert_produces_consistent_estimates` | Insert produces consistent estimates. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `estimate_recovers_frequency_for_repeated_key` | Estimate recovers frequency for repeated key. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `fast_path_recovers_repeated_insertions` | Fast path recovers repeated insertions. | Fast-path behavior remains consistent with the regular path. |
| `merge_adds_counters_element_wise` | Merge adds counters element wise. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `merge_requires_matching_dimensions` | Merge requires matching dimensions. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `zipf_stream_stays_within_twenty_percent_for_most_keys` | Zipf stream stays within twenty percent for most keys. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `cs_regular_path_correctness` | CS regular path correctness. | Core functional behavior for this component path is validated. |
| `cs_fast_path_correctness` | CS fast path correctness. | Fast-path behavior remains consistent with the regular path. |
| `cs_error_bound_zipf` | CS error bound Zipf. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `cs_error_bound_uniform` | CS error bound uniform. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `count_sketch_round_trip_serialization` | Count sketch round trip serialization. | Serialization/deserialization preserves component state and behavior after round trip. |
| `countl2hh_estimates_and_l2_are_consistent` | Countl2hh estimates and L2 are consistent. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `countl2hh_merge_combines_frequency_vectors` | Countl2hh merge combines frequency vectors. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `countl2hh_round_trip_serialization` | Countl2hh round trip serialization. | Serialization/deserialization preserves component state and behavior after round trip. |

### HyperLogLog

Test file: [`src/sketches/hll.rs`](../src/sketches/hll.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `hyperloglog_accuracy_within_two_percent` | Hyperloglog accuracy within two percent. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `hlldf_accuracy_within_two_percent` | Hlldf accuracy within two percent. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `hllds_accuracy_within_two_percent` | Hllds accuracy within two percent. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `hyperloglog_merge_within_two_percent` | Hyperloglog merge within two percent. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `hlldf_merge_within_two_percent` | Hlldf merge within two percent. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `hyperloglog_round_trip_serialization` | Hyperloglog round trip serialization. | Serialization/deserialization preserves component state and behavior after round trip. |
| `hlldf_round_trip_serialization` | Hlldf round trip serialization. | Serialization/deserialization preserves component state and behavior after round trip. |
| `hllds_round_trip_serialization` | Hllds round trip serialization. | Serialization/deserialization preserves component state and behavior after round trip. |
| `hll_correctness_test` | HLL correctness test. | Core functional behavior for this component path is validated. |

### KLL

Test file: [`src/sketches/kll.rs`](../src/sketches/kll.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `coin_bit_cache_behavior` | Coin bit cache behavior. | Core functional behavior for this component path is validated. |
| `coin_state_never_zero` | Coin state never zero. | Core functional behavior for this component path is validated. |
| `distributions_quantiles_stay_within_rank_error` | Distributions quantiles stay within rank error. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `test_sketch_input_api` | Test sketch input API. | Core functional behavior for this component path is validated. |
| `test_forced_compact` | Test forced compact. | Core functional behavior for this component path is validated. |
| `test_no_compact` | Test no compact. | Core functional behavior for this component path is validated. |
| `merge_preserves_quantiles_within_tolerance` | Merge preserves quantiles within tolerance. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `cdf_handles_empty_sketch` | CDF handles empty sketch. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `kll_round_trip_rmp` | KLL round trip rmp. | Serialization/deserialization preserves component state and behavior after round trip. |

### DDSketch

Test file: [`src/sketches/ddsketch.rs`](../src/sketches/ddsketch.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `insert_and_query_basic` | Insert and query basic. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `empty_quantile_returns_none` | Empty quantile returns none. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `dds_uniform_distribution_quantiles` | DDS uniform distribution quantiles. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `dds_zipf_distribution_quantiles` | DDS Zipf distribution quantiles. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `dds_normal_distribution_quantiles` | DDS normal distribution quantiles. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `dds_exponential_distribution_quantiles` | DDS exponential distribution quantiles. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `merge_two_sketches_combines_counts_and_bounds` | Merge two sketches combines counts and bounds. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `dds_serialization_round_trip` | DDS serialization round trip. | Serialization/deserialization preserves component state and behavior after round trip. |

### FoldCMS

Test file: [`src/sketches/fold_cms.rs`](../src/sketches/fold_cms.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `cell_starts_empty` | Cell starts empty. | Construction defaults and configuration invariants are validated. |
| `cell_single_insert` | Cell single insert. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `cell_single_accumulates` | Cell single accumulates. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `cell_collision_upgrades_to_collided` | Cell collision upgrades to collided. | Core functional behavior for this component path is validated. |
| `cell_collided_accumulates` | Cell collided accumulates. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `cell_collided_third_entry` | Cell collided third entry. | Core functional behavior for this component path is validated. |
| `cell_merge_from_empty` | Cell merge from empty. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `cell_merge_from_single` | Cell merge from single. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `cell_merge_from_collision` | Cell merge from collision. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `cell_iter_empty` | Cell iter empty. | Core functional behavior for this component path is validated. |
| `cell_iter_single` | Cell iter single. | Core functional behavior for this component path is validated. |
| `cell_iter_collided` | Cell iter collided. | Core functional behavior for this component path is validated. |
| `fold_cms_dimensions` | Fold CMS dimensions. | Construction defaults and configuration invariants are validated. |
| `fold_cms_level_zero_is_full` | Fold CMS level zero is full. | Construction defaults and configuration invariants are validated. |
| `fold_cms_rejects_non_power_of_two` | Fold CMS rejects non power of two. | Invalid inputs or incompatible states are detected and handled as expected. |
| `fold_cms_rejects_excessive_fold_level` | Fold CMS rejects excessive fold level. | Invalid inputs or incompatible states are detected and handled as expected. |
| `fold_cms_insert_query_single_key` | Fold CMS insert query single key. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `fold_cms_insert_accumulates` | Fold CMS insert accumulates. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `fold_cms_absent_key_returns_zero` | Fold CMS absent key returns zero. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `fold_cms_multiple_keys` | Fold CMS multiple keys. | Core functional behavior for this component path is validated. |
| `fold_cms_matches_standard_cms_exact` | Fold CMS matches standard CMS exact. | Core functional behavior for this component path is validated. |
| `fold_cms_matches_standard_cms_insert_many` | Fold CMS matches standard CMS insert many. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `same_level_merge_adds_counts` | Same level merge adds counts. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `same_level_merge_matches_standard_cms_merge` | Same level merge matches standard CMS merge. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `unfold_merge_reduces_level` | Unfold merge reduces level. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `unfold_merge_preserves_counts` | Unfold merge preserves counts. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `unfold_merge_matches_standard_cms_merge` | Unfold merge matches standard CMS merge. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `hierarchical_merge_four_sketches` | Hierarchical merge four sketches. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `unfold_full_matches_flat_counters` | Unfold full matches flat counters. | Core functional behavior for this component path is validated. |
| `to_flat_counters_matches_standard_cms` | To flat counters matches standard CMS. | Core functional behavior for this component path is validated. |
| `sparse_subwindow_has_few_collisions` | Sparse subwindow has few collisions. | Temporal/windowed behavior remains correct under time-based scenarios. |
| `heap_tracks_heavy_hitters` | Heap tracks heavy hitters. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `heap_survives_same_level_merge` | Heap survives same level merge. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `heap_survives_unfold_merge` | Heap survives unfold merge. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `fold_cms_error_bound_zipf` | Fold CMS error bound Zipf. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `scenario_rate_limiting` | Scenario rate limiting. | Temporal/windowed behavior remains correct under time-based scenarios. |
| `scenario_error_frequency` | Scenario error frequency. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `large_window_merge_benchmark_cms` | Large window merge benchmark CMS. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `scenario_ddos_detection` | Scenario ddos detection. | Temporal/windowed behavior remains correct under time-based scenarios. |
| `scatter_merge_matches_standard_cms_n1_to_n8` | Scatter merge matches standard CMS n1 to n8. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `unfold_to_single_pass_preserves_flat_counters` | Unfold to single pass preserves flat counters. | Core functional behavior for this component path is validated. |
| `unfold_to_same_level_returns_clone` | Unfold to same level returns clone. | Core functional behavior for this component path is validated. |
| `hierarchical_merge_mixed_fold_levels` | Hierarchical merge mixed fold levels. | Merge behavior preserves expected aggregate semantics and internal invariants. |

### FoldCS

Test file: [`src/sketches/fold_cs.rs`](../src/sketches/fold_cs.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `fold_cs_dimensions` | Fold CS dimensions. | Construction defaults and configuration invariants are validated. |
| `fold_cs_level_zero_is_full` | Fold CS level zero is full. | Construction defaults and configuration invariants are validated. |
| `fold_cs_rejects_non_power_of_two` | Fold CS rejects non power of two. | Invalid inputs or incompatible states are detected and handled as expected. |
| `fold_cs_rejects_excessive_fold_level` | Fold CS rejects excessive fold level. | Invalid inputs or incompatible states are detected and handled as expected. |
| `fold_cs_insert_query_single_key` | Fold CS insert query single key. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `fold_cs_insert_accumulates` | Fold CS insert accumulates. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `fold_cs_absent_key_returns_zero` | Fold CS absent key returns zero. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `fold_cs_multiple_keys` | Fold CS multiple keys. | Core functional behavior for this component path is validated. |
| `fold_cs_sign_application` | Fold CS sign application. | Count-Sketch sign/hash update logic is applied correctly. |
| `fold_cs_matches_standard_cs_exact` | Fold CS matches standard CS exact. | Core functional behavior for this component path is validated. |
| `fold_cs_matches_standard_cs_flat_counters` | Fold CS matches standard CS flat counters. | Core functional behavior for this component path is validated. |
| `fold_cs_matches_standard_cs_insert_many` | Fold CS matches standard CS insert many. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `same_level_merge_adds_counts` | Same level merge adds counts. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `same_level_merge_matches_standard_cs_merge` | Same level merge matches standard CS merge. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `unfold_merge_reduces_level` | Unfold merge reduces level. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `unfold_merge_preserves_counts` | Unfold merge preserves counts. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `unfold_merge_matches_standard_cs_merge` | Unfold merge matches standard CS merge. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `hierarchical_merge_four_sketches` | Hierarchical merge four sketches. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `unfold_full_matches_flat_counters` | Unfold full matches flat counters. | Core functional behavior for this component path is validated. |
| `to_flat_counters_matches_standard_cs` | To flat counters matches standard CS. | Core functional behavior for this component path is validated. |
| `sparse_subwindow_has_few_collisions` | Sparse subwindow has few collisions. | Temporal/windowed behavior remains correct under time-based scenarios. |
| `heap_tracks_heavy_hitters` | Heap tracks heavy hitters. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `heap_survives_same_level_merge` | Heap survives same level merge. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `heap_survives_unfold_merge` | Heap survives unfold merge. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `fold_cs_error_bound_zipf` | Fold CS error bound Zipf. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `large_window_merge_benchmark_cs` | Large window merge benchmark CS. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `scatter_merge_matches_standard_cs_n1_to_n8` | Scatter merge matches standard CS n1 to n8. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `unfold_to_single_pass_preserves_flat_counters` | Unfold to single pass preserves flat counters. | Core functional behavior for this component path is validated. |
| `unfold_to_same_level_returns_clone` | Unfold to same level returns clone. | Core functional behavior for this component path is validated. |
| `hierarchical_merge_mixed_fold_levels` | Hierarchical merge mixed fold levels. | Merge behavior preserves expected aggregate semantics and internal invariants. |

### CMSHeap

Test file: [`src/sketches/cms_heap.rs`](../src/sketches/cms_heap.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `insert_and_estimate` | Insert and estimate. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `heap_tracks_top_k` | Heap tracks top K. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `merge_reconciles_heaps` | Merge reconciles heaps. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `insert_many_updates_estimate_and_heap` | Insert many updates estimate and heap. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `bulk_insert_updates_multiple_keys` | Bulk insert updates multiple keys. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `clear_heap_keeps_cms_counters` | Clear heap keeps CMS counters. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `from_storage_uses_storage_dimensions` | From storage uses storage dimensions. | Construction defaults and configuration invariants are validated. |
| `merge_refreshes_existing_self_heap_entries` | Merge refreshes existing self heap entries. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `fast_path_insert_and_estimate` | Fast path insert and estimate. | Fast-path behavior remains consistent with the regular path. |
| `fast_path_insert_many_and_bulk_insert` | Fast path insert many and bulk insert. | Fast-path behavior remains consistent with the regular path. |
| `fast_path_heap_tracks_top_k` | Fast path heap tracks top K. | Fast-path behavior remains consistent with the regular path. |
| `fast_path_merge_refreshes_existing_self_heap_entries` | Fast path merge refreshes existing self heap entries. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `default_construction` | Default construction. | Construction defaults and configuration invariants are validated. |
| `default_construction_fixed_backends_parity` | Default construction fixed backends parity. | Fast-path behavior remains consistent with the regular path. |
| `merge_requires_matching_dimensions_panics` | Merge requires matching dimensions panics. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `heap_entries_match_cms_estimates_after_mutations` | Heap entries match CMS estimates after mutations. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `bulk_insert_equivalent_to_repeated_insert` | Bulk insert equivalent to repeated insert. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `regular_vs_fast_equivalence_on_same_stream` | Regular vs fast equivalence on same stream. | Fast-path behavior remains consistent with the regular path. |
| `merge_with_empty_other_and_empty_self` | Merge with empty other and empty self. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `duplicate_candidate_keys_during_merge_do_not_corrupt_heap` | Duplicate candidate keys during merge do not corrupt heap. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `zipf_stream_top_k_recall_regular_fast_budget` | Zipf stream top K recall regular fast budget. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `zipf_stream_top_k_recall_fast_path_fast_budget` | Zipf stream top K recall fast path fast budget. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `zipf_stream_regular_fast_heap_overlap` | Zipf stream regular fast heap overlap. | Accuracy/error behavior stays within expected bounds on representative workloads. |

### CSHeap

Test file: [`src/sketches/cs_heap.rs`](../src/sketches/cs_heap.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `insert_and_estimate` | Insert and estimate. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `heap_tracks_top_k` | Heap tracks top K. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `merge_reconciles_heaps` | Merge reconciles heaps. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `insert_many_updates_estimate_and_heap` | Insert many updates estimate and heap. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `bulk_insert_updates_multiple_keys` | Bulk insert updates multiple keys. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `clear_heap_keeps_cs_counters` | Clear heap keeps CS counters. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `from_storage_uses_storage_dimensions` | From storage uses storage dimensions. | Construction defaults and configuration invariants are validated. |
| `merge_refreshes_existing_self_heap_entries` | Merge refreshes existing self heap entries. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `fast_path_insert_and_estimate` | Fast path insert and estimate. | Fast-path behavior remains consistent with the regular path. |
| `fast_path_insert_many_and_bulk_insert` | Fast path insert many and bulk insert. | Fast-path behavior remains consistent with the regular path. |
| `fast_path_heap_tracks_top_k` | Fast path heap tracks top K. | Fast-path behavior remains consistent with the regular path. |
| `fast_path_merge_refreshes_existing_self_heap_entries` | Fast path merge refreshes existing self heap entries. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `default_construction` | Default construction. | Construction defaults and configuration invariants are validated. |
| `default_construction_fixed_backends_parity` | Default construction fixed backends parity. | Fast-path behavior remains consistent with the regular path. |
| `merge_requires_matching_dimensions_panics` | Merge requires matching dimensions panics. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `heap_entries_match_cs_estimates_after_mutations` | Heap entries match CS estimates after mutations. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `bulk_insert_equivalent_to_repeated_insert` | Bulk insert equivalent to repeated insert. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `regular_vs_fast_equivalence_on_same_stream` | Regular vs fast equivalence on same stream. | Fast-path behavior remains consistent with the regular path. |
| `merge_with_empty_other_and_empty_self` | Merge with empty other and empty self. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `duplicate_candidate_keys_during_merge_do_not_corrupt_heap` | Duplicate candidate keys during merge do not corrupt heap. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `zipf_stream_top_k_recall_regular_fast_budget` | Zipf stream top K recall regular fast budget. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `zipf_stream_top_k_recall_fast_path_fast_budget` | Zipf stream top K recall fast path fast budget. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `zipf_stream_regular_fast_heap_overlap` | Zipf stream regular fast heap overlap. | Accuracy/error behavior stays within expected bounds on representative workloads. |

### Elastic

Test file: [`src/sketches/elastic.rs`](../src/sketches/elastic.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `heavy_bucket_tracks_repeated_flow_exactly` | Heavy bucket tracks repeated flow exactly. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `light_sketch_counts_colliding_flows` | Light sketch counts colliding flows. | Core functional behavior for this component path is validated. |

### Coco

Test file: [`src/sketches/coco.rs`](../src/sketches/coco.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `insert_then_estimate_matches_full_value_for_partial_key` | Insert then estimate matches full value for partial key. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `estimate_with_udf_allows_custom_partial_matching` | Estimate with udf allows custom partial matching. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `merge_combines_tables_without_losing_counts` | Merge combines tables without losing counts. | Merge behavior preserves expected aggregate semantics and internal invariants. |

### KMV

Test file: [`src/sketches/kmv.rs`](../src/sketches/kmv.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `assert_accuracy` | Assert accuracy. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `assert_merge_accuracy` | Assert merge accuracy. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `assert_serialization_round_trip` | Assert serialization round trip. | Serialization/deserialization preserves component state and behavior after round trip. |

<!-- ### Locher

Test file: [`src/sketches/locher.rs`](../src/sketches/locher.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `locher_estimate_tracks_inserted_frequency` | Locher estimate tracks inserted frequency. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `median_handles_even_and_empty_inputs` | Median handles even and empty inputs. | Core behavior for insert/query/update and deterministic semantics is validated. | -->

<!-- ### MicroScope

Test file: [`src/sketches/microscope.rs`](../src/sketches/microscope.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `insert_and_query_track_recent_volume` | Insert and query track recent volume. | Temporal/windowed behavior remains correct under time-based scenarios. |
| `merge_combines_counters_for_matching_windows` | Merge combines counters for matching windows. | Merge behavior preserves expected aggregate semantics and internal invariants. | -->

### UniformSampling

Test file: [`src/sketches/uniform.rs`](../src/sketches/uniform.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `sample_count_tracks_rate` | Sample count tracks rate. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `samples_are_drawn_from_input_stream` | Samples are drawn from input stream. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `merge_combines_samples_using_rate_based_target` | Merge combines samples using rate based target. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `merge_rejects_different_rates` | Merge rejects different rates. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `sample_access_is_stable` | Sample access is stable. | Core behavior for insert/query/update and deterministic semantics is validated. |

## Sketch Frameworks

### Hydra

Test file: [`src/sketch_framework/hydra.rs`](../src/sketch_framework/hydra.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `hydra_updates_countmin_frequency` | Hydra updates countmin frequency. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `hydra_updates_countmin_frequency_multiple_values` | Hydra updates countmin frequency multiple values. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `hydra_round_trip_serialization` | Hydra round trip serialization. | Serialization/deserialization preserves component state and behavior after round trip. |
| `multihead_hydra_updates_multiple_dimensions` | Multihead hydra updates multiple dimensions. | Construction defaults and configuration invariants are validated. |
| `hydra_subpopulation_frequency_test` | Hydra subpopulation frequency test. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `hydra_subpopulation_cardinality_test` | Hydra subpopulation cardinality test. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `hydra_tracks_kll_quantiles` | Hydra tracks KLL quantiles. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `hydra_kll_single_label_cdfs` | Hydra KLL single label cdfs. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `hydra_kll_multi_label_cdfs` | Hydra KLL multi label cdfs. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `hydra_kll_extreme_queries` | Hydra KLL extreme queries. | Core functional behavior for this component path is validated. |
| `test_count_min_frequency_query` | Test count min frequency query. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `test_count_min_invalid_query_types` | Test count min invalid query types. | Invalid inputs or incompatible states are detected and handled as expected. |
| `test_hll_cardinality_query` | Test HLL cardinality query. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `test_kll_quantile_query` | Test KLL quantile query. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `test_univmon_universal_queries` | Test univmon universal queries. | Core functional behavior for this component path is validated. |
| `test_merge_counters` | Test merge counters. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `test_count_frequency_query` | Test count frequency query. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `test_count_invalid_query_types` | Test count invalid query types. | Invalid inputs or incompatible states are detected and handled as expected. |

### HashLayer

Test file: [`src/sketch_framework/hashlayer.rs`](../src/sketch_framework/hashlayer.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `test_hashlayer_insert_all` | Test hashlayer insert all. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `test_hashlayer_insert_at_specific_indices` | Test hashlayer insert at specific indices. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `test_hashlayer_query_all` | Test hashlayer query all. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `test_hashlayer_with_hash_optimization` | Test hashlayer with hash optimization. | Core functional behavior for this component path is validated. |
| `test_hashlayer_hll_cardinality` | Test hashlayer HLL cardinality. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `test_hashlayer_direct_access` | Test hashlayer direct access. | Core functional behavior for this component path is validated. |
| `test_hashlayer_bounds_checking` | Test hashlayer bounds checking. | Invalid inputs or incompatible states are detected and handled as expected. |
| `test_hashlayer_custom_sketches` | Test hashlayer custom sketches. | Core functional behavior for this component path is validated. |

### UnivMon

Test file: [`src/sketch_framework/univmon.rs`](../src/sketch_framework/univmon.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `univmon_round_trip_serialization` | Univmon round trip serialization. | Serialization/deserialization preserves component state and behavior after round trip. |
| `update_populates_bucket_size_and_heavy_hitters` | Update populates bucket size and heavy hitters. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `merge_with_combines_heavy_hitters` | Merge with combines heavy hitters. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `univmon_layers_use_different_seeds` | Univmon layers use different seeds. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `univmon_cardinality_is_positive` | Univmon cardinality is positive. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `univmon_bucket_size_tracked_correctly` | Univmon bucket size tracked correctly. | Core functional behavior for this component path is validated. |
| `univmon_basic_operation` | Univmon basic operation. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `test_statistical_accuracy` | Test statistical accuracy. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `univmon_random_data_matches_ground_truth_within_five_percent` | Univmon random data matches ground truth within five percent. | Accuracy/error behavior stays within expected bounds on representative workloads. |

### UnivMon Optimized

Test file: [`src/sketch_framework/univmon_optimized.rs`](../src/sketch_framework/univmon_optimized.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `pool_basic_take_put` | Pool basic take put. | Core functional behavior for this component path is validated. |
| `pool_free_resets_sketch` | Pool free resets sketch. | Core functional behavior for this component path is validated. |
| `pyramid_basic_insert_and_query` | Pyramid basic insert and query. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `pyramid_fast_insert_matches_standard` | Pyramid fast insert matches standard. | Fast-path behavior remains consistent with the regular path. |
| `pyramid_two_tier_dimensions` | Pyramid two tier dimensions. | Construction defaults and configuration invariants are validated. |
| `pyramid_free_resets_state` | Pyramid free resets state. | Core functional behavior for this component path is validated. |
| `pyramid_merge_combines_data` | Pyramid merge combines data. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `pyramid_accuracy_zipf` | Pyramid accuracy Zipf. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `pyramid_fast_insert_accuracy` | Pyramid fast insert accuracy. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `pyramid_memory_savings_vs_uniform` | Pyramid memory savings vs uniform. | Accuracy/error behavior stays within expected bounds on representative workloads. |

### NitroBatch

Test file: [`src/sketch_framework/nitro.rs`](../src/sketch_framework/nitro.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `nitro_batch_countmin_error_bound_zipf` | Nitro batch countmin error bound Zipf. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `nitro_batch_count_error_bound_zipf` | Nitro batch count error bound Zipf. | Accuracy/error behavior stays within expected bounds on representative workloads. |

### ExponentialHistogram

Test file: [`src/sketch_framework/eh.rs`](../src/sketch_framework/eh.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `constructor_infers_merge_norm` | Constructor infers merge norm. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `l1_merge_invariant_same_size` | L1 merge invariant same size. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `l2_merge_invariant_sum_l22` | L2 merge invariant sum l22. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `merge_recomputes_l2_mass` | Merge recomputes L2 mass. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `test_basic_insertion_and_query` | Test basic insertion and query. | Core behavior for insert/query/update and deterministic semantics is validated. |

### EHSketchList

Test file: [`src/sketch_framework/eh_sketch_list.rs`](../src/sketch_framework/eh_sketch_list.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `insert_routes_to_countl2hh_and_univmon` | Insert routes to countl2hh and univmon. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `count_sketch_insert_and_query_round_trip` | Count sketch insert and query round trip. | Serialization/deserialization preserves component state and behavior after round trip. |
| `ddsketch_insert_and_quantile_query_round_trip` | DDSketch insert and quantile query round trip. | Serialization/deserialization preserves component state and behavior after round trip. |
| `supports_norm_whitelist_is_enforced` | Supports norm whitelist is enforced. | Invalid inputs or incompatible states are detected and handled as expected. |

### EHUnivOptimized

Test file: [`src/sketch_framework/eh_univ_optimized.rs`](../src/sketch_framework/eh_univ_optimized.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `basic_insertion_and_query` | Basic insertion and query. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `map_merge_bounds_volume` | Map merge bounds volume. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `promotion_creates_sketch_buckets` | Promotion creates sketch buckets. | Core functional behavior for this component path is validated. |
| `window_expiration` | Window expiration. | Temporal/windowed behavior remains correct under time-based scenarios. |
| `hybrid_query_returns_sketch` | Hybrid query returns sketch. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `cover_check` | Cover check. | Core functional behavior for this component path is validated. |
| `accuracy_known_distribution` | Accuracy known distribution. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `pool_used_during_promotion` | Pool used during promotion. | Core functional behavior for this component path is validated. |
| `correctness_map_only_exact` | Correctness map only exact. | Core functional behavior for this component path is validated. |
| `correctness_subinterval_query` | Correctness subinterval query. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `correctness_expired_data_excluded` | Correctness expired data excluded. | Core functional behavior for this component path is validated. |
| `correctness_volume_bounded_long_stream` | Correctness volume bounded long stream. | Core functional behavior for this component path is validated. |
| `correctness_pool_recycling_across_cycles` | Correctness pool recycling across cycles. | Core functional behavior for this component path is validated. |
| `correctness_sketch_merge_preserves_metrics` | Correctness sketch merge preserves metrics. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `accuracy_zipf_distribution_sketch_tier` | Accuracy Zipf distribution sketch tier. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `accuracy_uniform_distribution` | Accuracy uniform distribution. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `accuracy_sliding_window` | Accuracy sliding window. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `accuracy_varies_with_k` | Accuracy varies with K. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `accuracy_suffix_queries` | Accuracy suffix queries. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `accuracy_distribution_shift` | Accuracy distribution shift. | Accuracy/error behavior stays within expected bounds on representative workloads. |

### TumblingWindow

Test file: [`src/sketch_framework/tumbling.rs`](../src/sketch_framework/tumbling.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `pool_take_returns_preallocated` | Pool take returns preallocated. | Core functional behavior for this component path is validated. |
| `pool_take_allocates_when_empty` | Pool take allocates when empty. | Core functional behavior for this component path is validated. |
| `pool_put_recycles` | Pool put recycles. | Core functional behavior for this component path is validated. |
| `fold_cms_clear_resets_to_empty` | Fold CMS clear resets to empty. | Core functional behavior for this component path is validated. |
| `fold_cs_clear_resets_to_empty` | Fold CS clear resets to empty. | Core functional behavior for this component path is validated. |
| `kll_clear_resets_to_empty` | KLL clear resets to empty. | Core functional behavior for this component path is validated. |
| `zero_window_size_panics` | Zero window size panics. | Invalid inputs or incompatible states are detected and handled as expected. |
| `window_closes_on_time_advance` | Window closes on time advance. | Temporal/windowed behavior remains correct under time-based scenarios. |
| `window_evicts_oldest_beyond_max` | Window evicts oldest beyond max. | Temporal/windowed behavior remains correct under time-based scenarios. |
| `window_pool_recycles_on_eviction` | Window pool recycles on eviction. | Temporal/windowed behavior remains correct under time-based scenarios. |
| `query_all_matches_manual_merge` | Query all matches manual merge. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `query_recent_selects_subset` | Query recent selects subset. | Temporal/windowed behavior remains correct under time-based scenarios. |
| `fold_cms_tumbling_hierarchical_merge` | Fold CMS tumbling hierarchical merge. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `kll_tumbling_quantile_accuracy` | KLL tumbling quantile accuracy. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `flush_closes_active_window` | Flush closes active window. | Temporal/windowed behavior remains correct under time-based scenarios. |
| `fold_cs_tumbling_basic` | Fold CS tumbling basic. | Core functional behavior for this component path is validated. |
| `fold_cs_tumbling_hierarchical_merge` | Fold CS tumbling hierarchical merge. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `fold_cms_tumbling_accuracy_zipf` | Fold CMS tumbling accuracy Zipf. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `fold_cms_hierarchical_vs_flat_merge` | Fold CMS hierarchical vs flat merge. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `fold_cs_tumbling_accuracy_zipf` | Fold CS tumbling accuracy Zipf. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `kll_tumbling_multi_quantile_accuracy` | KLL tumbling multi quantile accuracy. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `kll_tumbling_distribution_shift` | KLL tumbling distribution shift. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `tumbling_eviction_correctness` | Tumbling eviction correctness. | Temporal/windowed behavior remains correct under time-based scenarios. |
| `tumbling_query_recent_accuracy` | Tumbling query recent accuracy. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `fold_cms_tumbling_heap_correctness` | Fold CMS tumbling heap correctness. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `fold_cs_tumbling_query_recent_accuracy` | Fold CS tumbling query recent accuracy. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `kll_tumbling_query_recent_accuracy` | KLL tumbling query recent accuracy. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `fold_cs_tumbling_heap_correctness` | Fold CS tumbling heap correctness. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `fold_cms_tumbling_vs_monolithic` | Fold CMS tumbling vs monolithic. | Core functional behavior for this component path is validated. |
| `fold_cs_tumbling_vs_monolithic` | Fold CS tumbling vs monolithic. | Core functional behavior for this component path is validated. |
| `kll_tumbling_vs_monolithic` | KLL tumbling vs monolithic. | Core functional behavior for this component path is validated. |
| `tumbling_single_window_accuracy` | Tumbling single window accuracy. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `tumbling_very_small_windows` | Tumbling very small windows. | Temporal/windowed behavior remains correct under time-based scenarios. |
| `tumbling_skewed_load` | Tumbling skewed load. | Core functional behavior for this component path is validated. |

## Common

### Common Hash Utilities

Test file: [`src/common/hash.rs`](../src/common/hash.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `hash128_seeded_preserves_cardinality` | Hash128 seeded preserves cardinality. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `hash128_seeded_is_deterministic_for_repeated_inputs` | Hash128 seeded is deterministic for repeated inputs. | Core behavior for insert/query/update and deterministic semantics is validated. |

### Common Heap Utilities

Test file: [`src/common/heap.rs`](../src/common/heap.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `heap_retains_top_k_items_by_count` | Heap retains top K items by count. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `update_count_increments_existing_entry` | Update count increments existing entry. | Core functional behavior for this component path is validated. |
| `clean_resets_heap_state` | Clean resets heap state. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `test_min_heap_basic` | Test min heap basic. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `test_max_heap_basic` | Test max heap basic. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `test_bounded_heap_capacity` | Test bounded heap capacity. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `test_update_at` | Test update at. | Core functional behavior for this component path is validated. |
| `test_custom_struct_with_ord` | Test custom struct with ord. | Core functional behavior for this component path is validated. |
| `test_topk_use_case` | Test topk use case. | Core functional behavior for this component path is validated. |
| `test_heap_size` | Test heap size. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `test_topk_with_custom_comparator` | Test topk with custom comparator. | Core functional behavior for this component path is validated. |
| `test_exact_topk_heap_replacement` | Test exact topk heap replacement. | Top-K/heavy-hitter tracking and updates behave as expected. |

### Common Structure Utilities

Test file: [`src/common/structure_utils.rs`](../src/common/structure_utils.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `median_test` | Median test. | Core behavior for insert/query/update and deterministic semantics is validated. |

### Vector2D (Common Structure)

Test file: [`src/common/structures/vector2d.rs`](../src/common/structures/vector2d.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `required_bits_match_expected_thresholds` | Required bits match expected thresholds. | Core functional behavior for this component path is validated. |

