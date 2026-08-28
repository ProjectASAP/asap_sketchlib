# Test Matrix

One component per section. Each section contains a table with test name, description, and what the test validates.

## How To Run

```bash
cargo test
```

## Sketches

### CountMin

Test file: [`src/sketches/countminsketch.rs`](../src/sketches/countminsketch.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `dimension_test` | Default/custom dimensions initialize zeroed counters. | Verifies default dimensions (`rows=3`, `cols=4096`), custom dimensions (`3x17`), and zero-initialized counters after construction. |
| `fast_insert_same_estimate` | Fast and regular insert paths produce identical estimates. | Inserts five string keys once into both `RegularPath` and `FastPath` sketches (`3x64`) and asserts equal estimates for every key. |
| `merge_adds_counters_element_wise` | Merge sums counters element-wise for matching dimensions. | Merges two `2x32` sketches after inserting the same key (`1` on left, `2` on right) and checks merged per-row target counters equal `3`. |
| `countmin_insert_emit_delta_emits_at_threshold_and_resets_period` | Worker-path delta emission fires at promotion threshold and resets. | Inserts key into `3x64` CMS via `insert_emit_delta`; verifies no delta emitted before `CM_PROMASK` inserts, then exactly `3` deltas (one per row) with `value == CM_PROMASK` at threshold, no extra deltas in next sub-threshold window, and another batch of `3` at the next threshold. |
| `countmin_apply_delta_increments_parent_counter` | Apply delta increments parent counter. | Constructs a `CmDelta{row=1, col=5, value=CM_PROMASK}`, applies it to a `3x64` parent CMS, and verifies the target counter at `(1,5)` equals `CM_PROMASK`. |
| `cm_regular_path_correctness` | Regular-path hashing, counters, and estimates are exact on a deterministic stream. | Recomputes expected counter indices for `I32(0..9)` using per-row hashing, asserts exact full-matrix equality after one pass, doubled counters after second pass, and estimate `== 2` for each inserted key. |
| `cm_fast_path_correctness` | Fast-path counter placement matches bit-sliced hash mapping. | Recomputes expected fast-path indices for `I32(0..9)` from one hash plus row bit-slices/mask bits and asserts exact full-matrix equality. |
| `cm_error_bound_zipf` | Zipf-stream error bound holds for regular and fast paths. | On `200_000` Zipf samples with domain `8192` and exponent `1.1`, checks both paths satisfy: number of distinct queried keys with `\|estimate - true\| < epsilon * N` is `> (1 - delta) * distinct_key_count`, with `epsilon = e / cols`, `delta = e^-rows`. |
| `cm_error_bound_uniform` | Uniform-stream error bound holds for regular and fast paths. | On `200_000` uniform samples in `[100.0, 1000.0]`, checks both paths satisfy: number of distinct queried keys with `\|estimate - true\| < epsilon * N` is `> (1 - delta) * distinct_key_count`, with `epsilon = e / cols`, `delta = e^-rows`. |
| `count_min_round_trip_serialization` | Serialization round trip preserves full sketch state. | Serializes/deserializes a populated `3x8` regular-path sketch and verifies dimensions plus the full counter array are unchanged. |

### Count

Test file: [`src/sketches/countsketch.rs`](../src/sketches/countsketch.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `default_initializes_expected_dimensions` | Default dimensions initialize zeroed counters. | Verifies default `Count` dimensions (`rows=3`, `cols=4096`) and that all counters are zero after construction. |
| `with_dimensions_uses_custom_sizes` | Custom dimensions initialize zeroed rows. | Verifies `with_dimensions(3, 17)` applies requested shape and each row slice is zero-initialized. |
| `insert_updates_signed_counters_per_row` | Regular insert applies per-row signed updates. | After one insert of key `"alpha"` into a `3x64` sketch, checks each row’s hashed counter equals that row’s expected sign (`+1` or `-1`). |
| `fast_insert_produces_consistent_estimates` | Fast-path single inserts return unit estimates. | Inserts five string keys once into a fast-path sketch (`4x128`) and asserts estimate `== 1.0` for each key. |
| `insert_produces_consistent_estimates` | Regular-path single inserts return unit estimates. | Inserts five string keys once into a regular-path sketch (`3x64`) and asserts estimate `== 1.0` for each key. |
| `estimate_recovers_frequency_for_repeated_key` | Regular path recovers repeated-key frequency. | Inserts key `"theta"` 37 times into a regular-path sketch (`3x64`) and asserts estimate `== 37.0`. |
| `fast_path_recovers_repeated_insertions` | Fast path recovers repeated insertions across keys. | Inserts five keys for 5 rounds into a fast-path sketch (`4x256`) and asserts estimate `== 5.0` for each key. |
| `merge_adds_counters_element_wise` | Merge sums signed counters for matching dimensions. | Merges two regular-path `2x32` sketches after inserting the same key (`1` on left, `2` on right) and checks per-row target counters equal `sign(row,key) * 3`. |
| `count_child_insert_emits_at_threshold` | Worker-path delta emission fires after sufficient inserts. | Inserts key into `3x64` `Count` via `insert_emit_delta` for `200` iterations and verifies at least `3` deltas (one per row) are emitted. |
| `zipf_stream_stays_within_twenty_percent_for_most_keys` | Zipf stream keeps relative error under 20% for most keys. | On Zipf stream (`rows=5`, `cols=8192`, `domain=8192`, `exponent=1.1`, `N=200_000`), computes per-key relative error and requires at least 70% of keys with error `< 0.20`. |
| `cs_regular_path_correctness` | Regular-path counter/sign mapping and estimates are exact on deterministic inserts. | Recomputes expected signed counter updates for `I32(0..9)` using regular hashing/sign logic, asserts exact matrix match after one pass, doubled counters after second pass, and estimate `== 2.0` for each inserted key. |
| `cs_fast_path_correctness` | Fast-path row-hash/sign mapping matches expected counters. | Recomputes expected fast-path updates for `I32(0..9)` using matrix hash row slices and row signs, then asserts exact full-matrix equality. |
| `cs_error_bound_zipf` | Zipf-stream error bound check passes for regular and fast paths. | On Zipf samples (`domain=8192`, `exponent=1.1`, `N=200_000`) with default dimensions, both paths require: count of distinct queried keys with `\|estimate - true\| < epsilon * N` is `> (1 - delta) * distinct_key_count`, with `epsilon = e / cols`, `delta = e^-rows`. |
| `cs_error_bound_uniform` | Uniform-stream error bound check passes for regular and fast paths. | On uniform samples in `[100.0, 1000.0]` with `N=200_000` and default dimensions, requires for both paths that in-bound distinct keys exceed `(1-delta)` fraction (`delta = e^-rows`); regular path uses `epsilon = sqrt(e / cols)` and bound `epsilon * L2_norm`, fast path uses `epsilon = e / cols` and bound `epsilon * N`. |
| `count_sketch_round_trip_serialization` | Serialization round trip preserves full `Count` state. | Serializes/deserializes a populated regular-path `3x8` sketch and verifies dimensions plus full counter array are unchanged. |
| `countl2hh_estimates_and_l2_are_consistent` | CountL2HH updates keep estimate and L2 consistent. | For `CountL2HH(3x32)`, applies `+5` then `-2` to one key, verifies estimates `5.0` then `3.0`, and asserts non-trivial L2 (`>= 3.0`). |
| `countl2hh_merge_combines_frequency_vectors` | CountL2HH merge combines per-key frequencies. | Merges two `CountL2HH(3x32)` sketches with same key counts `4` and `9`, then verifies merged estimate `== 13.0`. |
| `countl2hh_round_trip_serialization` | CountL2HH serialization round trip preserves estimate and L2. | Serializes/deserializes `CountL2HH::with_dimensions_and_seed(3,32,7)` after updates, verifying rows/cols and that both estimate and L2 remain unchanged (within `f64::EPSILON`). |

### HyperLogLog

Test file: [`src/sketches/hll.rs`](../src/sketches/hll.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `hll_child_insert_emits_on_improvement` | Child insert emits delta only on register improvement. | Inserts a key via `insert_emit_delta` into `HyperLogLog<Classic>`; verifies exactly `1` delta emitted on the first insert and `0` additional deltas on a duplicate insert. |
| `hyperloglog_accuracy_within_two_percent` | Classic HyperLogLog stays within 2% relative error across scale checkpoints. | Inserts sequential unique `U64` values and checks at targets `[10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000]` that relative error `\|estimate-truth\|/truth <= 0.02`. |
| `hll_ertl_accuracy_within_two_percent` | ErtlMLE HyperLogLog stays within 2% relative error across scale checkpoints. | Applies the same checkpointed unique-stream accuracy test as classic HLL, requiring relative error `<= 0.02` at each target cardinality. |
| `hllds_accuracy_within_two_percent` | HIP HyperLogLog stays within 2% relative error across scale checkpoints. | Applies the same checkpointed unique-stream accuracy test to `HyperLogLogHIP`, requiring relative error `<= 0.02` at each target cardinality. |
| `hyperloglog_p12_accuracy_within_two_percent` | P12 Classic HyperLogLog stays within P12 error tolerance across scale checkpoints. | Applies the same checkpointed unique-stream accuracy test to `HyperLogLogP12<Classic>`, requiring relative error `<= P12_ERROR_TOLERANCE` at each target cardinality. |
| `hll_ertl_p12_accuracy_within_two_percent` | P12 ErtlMLE HyperLogLog stays within P12 error tolerance across scale checkpoints. | Applies the same checkpointed accuracy test to `HyperLogLogP12<ErtlMLE>`, requiring relative error `<= P12_ERROR_TOLERANCE`. |
| `hllds_p12_accuracy_within_two_percent` | P12 HIP HyperLogLog stays within P12 error tolerance across scale checkpoints. | Applies the same checkpointed accuracy test to `HyperLogLogHIPP12`, requiring relative error `<= P12_ERROR_TOLERANCE`. |
| `hyperloglog_merge_within_two_percent` | Classic HyperLogLog merge remains within 2% relative error. | Splits unique stream into even keys (left) and odd keys (right), merges sketches at each target checkpoint, and requires merged relative error `<= 0.02`. |
| `hll_ertl_merge_within_two_percent` | ErtlMLE HyperLogLog merge remains within 2% relative error. | Uses the same even/odd split merge scenario and requires merged relative error `<= 0.02` at each target checkpoint. |
| `hyperloglog_p12_merge_within_two_percent` | P12 Classic HyperLogLog merge remains within P12 error tolerance. | Applies the same even/odd split merge scenario to `HyperLogLogP12<Classic>`, requiring merged relative error `<= P12_ERROR_TOLERANCE`. |
| `hll_ertl_p12_merge_within_two_percent` | P12 ErtlMLE HyperLogLog merge remains within P12 error tolerance. | Applies the same even/odd split merge scenario to `HyperLogLogP12<ErtlMLE>`, requiring merged relative error `<= P12_ERROR_TOLERANCE`. |
| `hyperloglog_round_trip_serialization` | Classic HyperLogLog round trip preserves bytes and estimate stability. | After inserting `100_000` unique values, verifies serialized payload is non-empty, `deserialize -> reserialize` bytes are identical, and estimate drift is within `0.02 * max(original_est, 1.0)`. |
| `hll_ertl_round_trip_serialization` | ErtlMLE HyperLogLog round trip preserves bytes and estimate stability. | Applies the same `100_000`-value serialization round-trip checks: non-empty bytes, byte-for-byte reserialization equality, and bounded estimate drift. |
| `hllds_round_trip_serialization` | HIP HyperLogLog round trip preserves bytes and estimate stability. | Applies the same `100_000`-value serialization round-trip checks for `HyperLogLogHIP`: non-empty bytes, byte-for-byte reserialization equality, and bounded estimate drift. |
| `hyperloglog_p12_round_trip_serialization` | P12 Classic HyperLogLog round trip preserves bytes and estimate stability. | Applies the same `100_000`-value serialization round-trip checks for `HyperLogLogP12<Classic>`: non-empty bytes, byte-for-byte reserialization equality, and bounded estimate drift. |
| `hll_ertl_p12_round_trip_serialization` | P12 ErtlMLE HyperLogLog round trip preserves bytes and estimate stability. | Applies the same `100_000`-value serialization round-trip checks for `HyperLogLogP12<ErtlMLE>`: non-empty bytes, byte-for-byte reserialization equality, and bounded estimate drift. |
| `hllds_p12_round_trip_serialization` | P12 HIP HyperLogLog round trip preserves bytes and estimate stability. | Applies the same `100_000`-value serialization round-trip checks for `HyperLogLogHIPP12`: non-empty bytes, byte-for-byte reserialization equality, and bounded estimate drift. |
| `hll_correctness_test` | Register update logic matches expected bucket/index behavior for all HLL variants. | Runs fixed hashed inserts against Classic, ErtlMLE, and HIP variants; asserts exact expected register values at specific bucket indices and confirms an untouched bucket remains zero. |

### KLL

Test file: [`src/sketches/kll.rs`](../src/sketches/kll.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `coin_bit_cache_behavior` | Coin consumes cached random bits in deterministic bit order. | From a fixed seed, validates 3 successive 64-bit RNG blocks are consumed bit-by-bit (`0..63`) before refill, matching expected xorshift-derived bits exactly. |
| `coin_state_never_zero` | Coin state is never zero, including zero-seed initialization. | Verifies `Coin::from_seed(0)` normalizes to non-zero state and remains non-zero across 128 tosses. |
| `distributions_quantiles_stay_within_rank_error` | KLL quantiles stay within 2% rank tolerance across distributions and scales. | For `k=200`, checks quantiles `{0,0.1,0.25,0.5,0.75,0.9,1}` on Uniform (`0..100,000,000`) and Zipf (`1,000,000..10,000,000`, domain `8192`, exponent `1.1`) streams at sizes `[1_000, 5_000, 20_000, 100_000, 1_000_000, 5_000_000]`; each estimate must fall within truth interval defined by `q +/- 0.02`. |
| `test_data_input_api` | DataInput numeric API is accepted and non-numeric input is rejected. | Inserts `I32`, `I64`, `F64`, `F32`, and `U32` values, checks median query lies between `20.0` and `40.2`, and verifies string input returns error `KLL sketch only accepts numeric inputs`. |
| `test_forced_compact` | Small-capacity KLL triggers compaction and keeps median in valid compacted outcomes. | With `KLL::init(3,3)` and inserts `[10,20,30,40,50]`, asserts median query is one of `{30.0, 40.0}` under forced compaction. |
| `test_no_compact` | Larger-capacity KLL avoids compaction for small stream and returns exact median. | With `KLL::init_kll(8)` and inserts `[10,20,30,40,50]`, asserts median query equals `30.0`. |
| `merge_preserves_quantiles_within_tolerance` | Merging two KLL sketches preserves quantiles within 2% rank tolerance. | Splits 10,000 uniform samples (`1,000,000..10,000,000`, seed `0xC0FFEE`) across two `k=200` sketches by index parity, merges, and checks quantiles `{0,0.1,0.25,0.5,0.75,0.9,1}` remain within `q +/- 0.02` truth bounds. |
| `cdf_handles_empty_sketch` | Empty KLL CDF queries return zero-valued defaults. | For empty `KLL::init_kll(64)`, asserts `cdf.quantile(123.0) == 0.0`, `cdf.query(0.5) == 0.0`, and `cdf.query_li(0.5) == 0.0`. |
| `kll_round_trip_rmp` | RMP round trip preserves KLL structure, packed data, and queried quantiles. | Serializes/deserializes `KLL::init_kll(256)` after 5,000 uniform updates (`0..1,000,000`, seed `0xDEAD_BEEF`), verifies non-empty bytes, core fields and packed arrays (`levels`, `items`) are identical, and CDF queries at `{0,0.1,0.25,0.5,0.75,0.9,1}` match within `f64::EPSILON`. |
| `generic_kll_i64_sanity` | Generic `KLL<T>` path works for non-`f64` numeric types. | Builds `KLL<i64>`, inserts `1..=20_000` through the typed `update(&T)` API, checks approximate count and p50/p90 quantiles, verifies merge on two `KLL<i64>` instances, and confirms MessagePack round-trip preserves weighted count. |

### KLLDynamic

Test file: [`src/sketches/kll_dynamic.rs`](../src/sketches/kll_dynamic.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `distributions_quantiles_stay_within_rank_error` | KLLDynamic quantiles stay within 2% rank tolerance across distributions and scales. | For `k=200`, checks quantiles `{0,0.1,0.25,0.5,0.75,0.9,1}` on Uniform (`0..100,000,000`) and Zipf (`1,000,000..10,000,000`, domain `8192`, exponent `1.1`) streams at sizes `[1_000, 5_000, 20_000, 100_000, 1_000_000, 5_000_000]`; each estimate must fall within truth interval defined by `q +/- 0.02`. |
| `test_data_input_api` | `KLLDynamic<f64>` accepts numeric `DataInput` and rejects non-numeric input. | Inserts `I32`, `I64`, `F64`, `F32`, and `U32` values through `update_data_input`, checks median query lies between `20.0` and `40.2`, and verifies string input returns error `KLL sketch only accepts numeric inputs`. |
| `test_forced_compact` | Small-capacity KLLDynamic triggers compaction and keeps median in valid compacted outcomes. | With `KLLDynamic::init(3,3)` and typed inserts `[10,20,30,40,50]`, asserts median query is one of `{30.0, 40.0}` under forced compaction. |
| `test_no_compact` | Larger-capacity KLLDynamic avoids compaction for small stream and returns exact median. | With `KLLDynamic::init_kll(8)` and typed inserts `[10,20,30,40,50]`, asserts median query equals `30.0`. |
| `merge_preserves_quantiles_within_tolerance` | Merging two KLLDynamic sketches preserves quantiles within 2% rank tolerance. | Splits 10,000 uniform samples (`1,000,000..10,000,000`, seed `0xC0FFEE`) across two `k=200` sketches by index parity, merges, and checks quantiles `{0,0.1,0.25,0.5,0.75,0.9,1}` remain within `q +/- 0.02` truth bounds. |
| `cdf_handles_empty_sketch` | Empty KLLDynamic CDF queries return zero-valued defaults. | For empty `KLLDynamic::<f64>::init_kll(64)`, asserts `cdf.quantile(123.0) == 0.0`, `cdf.query(0.5) == 0.0`, and `cdf.query_li(0.5) == 0.0`. |
| `kll_dynamic_round_trip_rmp` | RMP round trip preserves KLLDynamic structure, packed data, and queried quantiles. | Serializes/deserializes `KLLDynamic::init_kll(256)` after 5,000 uniform updates (`0..1,000,000`, seed `0xDEAD_BEEF`), verifies non-empty bytes, core fields and packed arrays (`levels`, `items`) are identical, and CDF queries at `{0,0.1,0.25,0.5,0.75,0.9,1}` match within `f64::EPSILON`. |
| `generic_kll_dynamic_i64_sanity` | Generic `KLLDynamic<T>` path works for non-`f64` numeric types. | Builds `KLLDynamic<i64>`, inserts `1..=20_000` through the typed `update(&T)` API, checks approximate count and p50/p90 quantiles, and confirms MessagePack round-trip preserves weighted count. |

### DDSketch

Test file: [`src/sketches/ddsketch.rs`](../src/sketches/ddsketch.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `insert_and_query_basic` | Basic insert/query preserves count semantics and quantile monotonicity. | Inserts mixed values `[0.0, -5.0, 1.0, 2.0, 3.0, 10.0, 50.0, 100.0, 1000.0]`, verifies non-positive values are ignored (`count == 7`), and checks queried quantiles at `{0.0, 0.5, 0.9, 0.99, 1.0}` are monotone and bounded by sketch min/max. |
| `empty_quantile_returns_none` | Empty sketch returns no quantiles and zero count. | For a new `DDSketch(alpha=0.01)`, asserts `get_value_at_quantile` returns `None` for `p in {0.0, 0.5, 1.0}` and `get_count() == 0`. |
| `dds_uniform_distribution_quantiles` | Uniform-distribution quantiles stay within configured relative error. | With `alpha=0.01`, samples sizes `[1_000, 5_000, 20_000]` from uniform range `[1_000_000, 10_000_000]` (seeded), and requires relative error `<= 0.01` at quantiles `{0, 0.1, 0.25, 0.5, 0.75, 0.9, 1}` against sorted-truth quantiles. |
| `dds_zipf_distribution_quantiles` | Zipf-distribution quantiles stay within configured relative error. | With `alpha=0.01`, samples sizes `[1_000, 5_000, 20_000]` from Zipf range `[1_000_000, 10_000_000]` (domain `8192`, exponent `1.1`, seeded), and requires relative error `<= 0.01` at quantiles `{0, 0.1, 0.25, 0.5, 0.75, 0.9, 1}`. |
| `dds_normal_distribution_quantiles` | Normal-distribution quantiles stay within configured relative error. | With `alpha=0.01`, samples sizes `[1_000, 5_000, 20_000]` from normal distribution (`mean=1000.0`, `std=100.0`, positive finite values retained), and requires relative error `<= 0.01` at quantiles `{0, 0.1, 0.25, 0.5, 0.75, 0.9, 1}`. |
| `dds_exponential_distribution_quantiles` | Exponential-distribution quantiles stay within near-1% relative error. | With `alpha=0.01`, `lambda=1e-3`, and sample sizes `[1_000, 5_000, 20_000]`, requires relative error `<= 0.011 + 1e-9` at quantiles `{0, 0.1, 0.25, 0.5, 0.75, 0.9, 1}`. |
| `merge_two_sketches_combines_counts_and_bounds` | Merge combines counts and preserves quantile boundary invariants. | Merges sketches built from `[1,2,3,4]` and `[5,10,20]`, then verifies merged `count=7`, `min=1`, `max=20`, exact boundary quantiles (`q0=1`, `q1=20`), and median lies within `[1,20]`. |
| `dds_serialization_round_trip` | Serialization round trip preserves count, bounds, and selected quantiles. | Serializes/deserializes a populated sketch (`alpha=0.01`), verifies non-empty bytes, equal `count/min/max`, and exact quantile matches at `{0.0, 0.1, 0.5, 0.9, 1.0}`. |

### CMSHeap

Test file: [`src/sketches/countminsketch_topk.rs`](../src/sketches/countminsketch_topk.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `insert_and_estimate` | Repeated inserts increment Count-Min estimate for one key. | Inserts `"hello"` 5 times into `CMSHeap::new(3,64,10)` and verifies `estimate("hello") == 5`. |
| `heap_tracks_top_k` | Heap keeps highest-frequency keys within top-k capacity. | Inserts keys `1..5` with frequencies `10,20,30,40,50` into `top_k=3` sketch and verifies heap counts are exactly `[30,40,50]`. |
| `merge_reconciles_heaps` | Merge combines counters and refreshes heap counts from merged sketch. | Merges two sketches containing `"merge_key"` counts `10` and `20`, then verifies merged estimate and heap count are both `30`. |
| `insert_many_updates_estimate_and_heap` | `insert_many` updates estimate and heap entry consistently. | Calls `insert_many("many", 11)` and verifies `estimate == 11` plus heap entry count `11`. |
| `bulk_insert_updates_multiple_keys` | `bulk_insert` updates multiple keys and heavy-hitter counts correctly. | Inserts stream `[7,8,7,9,7]` and verifies estimates `7->3`, `8->1`, `9->1`, with heap count for key `7` equal to `3`. |
| `clear_heap_keeps_cms_counters` | Clearing heap does not clear CMS counters. | After `insert_many("persist",5)`, calls `clear_heap()`, verifies estimate remains `5`, then one more insert rebuilds heap entry to `6`. |
| `from_storage_uses_storage_dimensions` | `from_storage` preserves backend dimensions and requested heap capacity. | Builds from `Vector2D::init(4,128)` with `top_k=9` and verifies `rows=4`, `cols=128`, `heap.capacity=9`. |
| `merge_refreshes_existing_self_heap_entries` | Merge refreshes pre-existing self heap keys to merged estimates. | After merging sketches with `a-key` counts `10` and `5`, verifies merged `a-key` estimate `15` and heap entry count `15`. |
| `fast_path_insert_and_estimate` | Fast path repeated inserts keep estimate exact for single key. | Inserts `"fast"` 7 times into fast-path sketch and verifies estimate `7`. |
| `fast_path_insert_many_and_bulk_insert` | Fast path batched APIs keep heap and estimate in sync. | Applies `insert_many("fast-many",6)` plus bulk inserts adding 2 more hits, then verifies estimate and heap count are `8`. |
| `fast_path_heap_tracks_top_k` | Fast path heap still preserves top-k ordering under weighted updates. | Inserts keys `1..5` with counts `10,20,30,40,50` via `insert_many` and verifies heap counts `[30,40,50]`. |
| `fast_path_merge_refreshes_existing_self_heap_entries` | Fast path merge refreshes self heap entries using merged totals. | Merges sketches where `"a-fast"` contributes `10` and `5` across sides, then verifies estimate and heap count are `15`. |
| `default_construction` | Default CMSHeap constructor uses expected dimensions and heap capacity. | Verifies `CMSHeap::<Vector2D<i64>, RegularPath>::default()` has `rows=3`, `cols=4096`, and `heap.capacity=DEFAULT_TOP_K`. |
| `default_construction_fixed_backends_parity` | Default constructors across storage backends keep intended size/capacity contracts. | Verifies defaults for Fixed/Quick backends are `5x2048`, DefaultMatrix backends are `3x4096`, and all regular/fast variants use `DEFAULT_TOP_K`. |
| `merge_requires_matching_dimensions_panics` | Merge panics on incompatible sketch dimensions. | Verifies merging `CMSHeap::new(3,256,4)` with `CMSHeap::new(4,256,4)` panics with dimension-mismatch message. |
| `heap_entries_match_cms_estimates_after_mutations` | Every heap entry count matches current CMS estimate after updates and merge. | Checks heap-entry equality to `estimate(key)` both before and after merging another mutated sketch. |
| `bulk_insert_equivalent_to_repeated_insert` | Bulk insert is equivalent to repeated single inserts. | Compares bulk vs repeated insertion on same stream and verifies identical per-key estimates and heap counts for keys `1..5`. |
| `regular_vs_fast_equivalence_on_same_stream` | Regular and fast wrappers agree on identical deterministic stream. | Feeds same 10-item string stream to both paths and verifies per-key estimates and heap counts match for `{alpha,beta,gamma,delta,epsilon}`. |
| `merge_with_empty_other_and_empty_self` | Merge behavior is stable when one side is empty. | Verifies merging non-empty with empty leaves counts unchanged and merging empty-self with non-empty copies counts/heap visibility correctly. |
| `duplicate_candidate_keys_during_merge_do_not_corrupt_heap` | Duplicate merge candidates do not duplicate heap entries. | Merges sketches both containing `"dup"`; verifies merged count `19`, heap size within capacity, and exactly one heap entry for `"dup"`. |
| `zipf_stream_top_k_recall_regular_fast_budget` | Regular path heap achieves high top-k recall on Zipf stream. | On Zipf stream (`rows=3`, `cols=4096`, `top_k=16`, `domain=1024`, `exponent=1.1`, `N=20_000`), verifies heap size bound, entry-count consistency, and recall hits `>= 15` vs truth top-16. |
| `zipf_stream_top_k_recall_fast_path_fast_budget` | Fast path heap achieves high top-k recall on Zipf stream. | Runs same Zipf setup in fast mode and verifies heap size bound, entry-count consistency, and recall hits `>= 15`. |
| `zipf_stream_regular_fast_heap_overlap` | Regular and fast heaps substantially overlap on Zipf heavy hitters. | On shared Zipf stream (`top_k=16`), verifies key overlap ratio between regular and fast top-k heaps is at least `0.8`. |

### CSHeap

Test file: [`src/sketches/countsketch_topk.rs`](../src/sketches/countsketch_topk.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `insert_and_estimate` | Repeated inserts increment `Count` estimate for one key. | Inserts `"hello"` 5 times into `CSHeap::new(5,256,10)` and verifies estimate is `5.0` within `1e-9`. |
| `heap_tracks_top_k` | Heap keeps highest-frequency keys within top-k capacity. | Inserts keys `1..5` with frequencies `100,200,300,400,500` into `top_k=3` sketch and verifies heap counts are exactly `[300,400,500]`. |
| `merge_reconciles_heaps` | Merge combines counters and refreshes heap counts from merged sketch. | Merges two sketches containing `"merge_key"` counts `10` and `20`, then verifies estimate is `30.0` and heap count is `30`. |
| `insert_many_updates_estimate_and_heap` | `insert_many` updates estimate and heap entry consistently. | Calls `insert_many("many", 17)` and verifies estimate `17.0` plus heap entry count equals estimated count. |
| `bulk_insert_updates_multiple_keys` | `bulk_insert` updates multiple keys and heavy-hitter counts correctly. | Inserts stream `[7,8,7,9,7]`, verifies estimate for key `7` is `3.0`, and heap count for key `7` matches estimate cast to integer. |
| `clear_heap_keeps_cs_counters` | Clearing heap does not clear `Count` counters. | After `insert_many("persist",5)`, calls `clear_heap()`, verifies estimate remains `5.0`, then one more insert repopulates heap with updated estimate count. |
| `from_storage_uses_storage_dimensions` | `from_storage` preserves backend dimensions and requested heap capacity. | Builds from `Vector2D::init(4,128)` with `top_k=9` and verifies `rows=4`, `cols=128`, `heap.capacity=9`. |
| `merge_refreshes_existing_self_heap_entries` | Merge refreshes pre-existing self heap keys to merged estimates. | Merges sketches where `"a-key"` is updated on both sides (`120` and `40`), then verifies heap count for `"a-key"` equals merged estimate. |
| `fast_path_insert_and_estimate` | Fast path repeated inserts keep estimate exact for single key. | Inserts `"fast"` 7 times into fast-path sketch and verifies estimate is `7.0` within `1e-9`. |
| `fast_path_insert_many_and_bulk_insert` | Fast path batched APIs keep heap and estimate in sync. | Applies `insert_many("fast-many",6)` plus bulk inserts adding 2 hits, then verifies estimate is `8.0` and heap count matches it. |
| `fast_path_heap_tracks_top_k` | Fast path heap preserves top-k ordering under weighted updates. | Inserts keys `1..5` with counts `100,200,300,400,500` via `insert_many` and verifies heap counts `[300,400,500]`. |
| `fast_path_merge_refreshes_existing_self_heap_entries` | Fast path merge refreshes self heap entries using merged totals. | Merges fast sketches where `"a-fast"` is updated on both sides (`120` and `40`) and verifies heap count equals merged estimate. |
| `default_construction` | Default CSHeap constructor uses expected dimensions and heap capacity. | Verifies `CSHeap::<Vector2D<i64>, RegularPath>::default()` has `rows=3`, `cols=4096`, and `heap.capacity=DEFAULT_TOP_K`. |
| `default_construction_fixed_backends_parity` | Default constructors across storage backends keep intended size/capacity contracts. | Verifies defaults for Fixed/Quick backends are `5x2048`, DefaultMatrix backends are `3x4096`, and all regular/fast variants use `DEFAULT_TOP_K`. |
| `merge_requires_matching_dimensions_panics` | Merge panics on incompatible sketch dimensions. | Verifies merging `CSHeap::new(5,256,4)` with `CSHeap::new(6,256,4)` panics with dimension-mismatch message. |
| `heap_entries_match_cs_estimates_after_mutations` | Every heap entry count matches current sketch estimate after updates and merge. | Checks heap-entry equality to `estimate(key)` both before and after merging another mutated sketch. |
| `bulk_insert_equivalent_to_repeated_insert` | Bulk insert is equivalent to repeated single inserts. | Compares bulk vs repeated insertion on same stream and verifies per-key estimates match within `1e-9` plus identical heap counts for keys `1..5`. |
| `regular_vs_fast_equivalence_on_same_stream` | Regular and fast wrappers agree on identical deterministic stream. | Feeds same 10-item string stream to both paths and verifies per-key estimates match within `1e-9` and heap counts match for `{alpha,beta,gamma,delta,epsilon}`. |
| `merge_with_empty_other_and_empty_self` | Merge behavior is stable when one side is empty. | Verifies merging non-empty with empty leaves estimates/heap size unchanged and merging empty-self with non-empty reproduces estimates and heap visibility. |
| `duplicate_candidate_keys_during_merge_do_not_corrupt_heap` | Duplicate merge candidates do not duplicate heap entries. | Merges sketches both containing `"dup"`; verifies heap count equals merged estimate, heap size stays within capacity, and only one heap entry exists for `"dup"`. |
| `zipf_stream_top_k_recall_regular_fast_budget` | Regular path heap achieves high top-k recall on Zipf stream. | On Zipf stream (`rows=5`, `cols=4096`, `top_k=16`, `domain=1024`, `exponent=1.1`, `N=20_000`), verifies heap size bound, entry-count consistency, and recall hits `>= 15` vs truth top-16. |
| `zipf_stream_top_k_recall_fast_path_fast_budget` | Fast path heap achieves high top-k recall on Zipf stream. | Runs same Zipf setup in fast mode and verifies heap size bound, entry-count consistency, and recall hits `>= 15`. |
| `zipf_stream_regular_fast_heap_overlap` | Regular and fast heaps substantially overlap on Zipf heavy hitters. | On shared Zipf stream (`top_k=16`), verifies key overlap ratio between regular and fast top-k heaps is at least `0.8`. |

### SpaceSaving

Test file: [`tests/e2e_space_saving.rs`](../tests/e2e_space_saving.rs)

Unit tests: [`src/sketches/space_saving.rs`](../src/sketches/space_saving.rs)

Conformance: [`tests/conformance_kit.rs`](../tests/conformance_kit.rs) runs `space_saving_passes_frequency_conformance`, the shared one-sided `frequency_battery` over 1,024 counters.

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `a_monitored_key_is_sandwiched_by_its_error` | Every monitored key's estimate brackets its true count. | Over a seeded Zipf stream, verifies each monitored key reads at or above its true count and no further above it than that key's own `error`, and that the summary is saturated at its capacity. |
| `an_unmonitored_key_never_exceeds_the_minimum_count` | Dropped keys stay under the `min_count` ceiling. | Verifies every key the summary evicted has a true count at or below `min_count`, and that `upper_bound` and `error` for such a key both report that ceiling. |
| `the_true_heavy_hitters_are_reported_exactly_and_in_order` | `top_k` returns the real heaviest keys in descending order. | Compares `top_k` against exact `FreqTruth` ordering and verifies the reported counts are exact and carry zero error, a true heavy hitter never having been displaced. |
| `a_guaranteed_key_really_outranks_everything_dropped` | `is_guaranteed` is a claim about the real stream. | Verifies every key accepted by `is_guaranteed` has a true count strictly above the largest true count among the keys the summary dropped. |
| `an_arrival_displaces_the_minimum_and_inherits_its_count` | Eviction hands the victim's count to the arrival as its error. | Drives a hand-built sequence through a two-counter summary and verifies the new key takes the minimum slot at `min + 1` with the old minimum as its error allowance, while `total` still counts every arrival. |
| `the_stream_summary_lists_stay_well_formed_under_eviction` | The bucket and counter lists stay traversable under sustained eviction. | Across capacities `1`, `2`, `17`, `256`, and `1_024`, verifies residency equals `min(capacity, distinct)`, the bucket walk reaches every counter exactly once in descending count order, and `entries` agrees with the walk. |
| `a_weighted_arrival_matches_repeating_it` | `insert_many` matches that many single inserts. | Builds two summaries over the same stream, one key-at-a-time and one weighted per key, and verifies equal totals, equal residency, and equal per-key estimates. |
| `a_summary_larger_than_the_domain_is_exact` | A summary with room for the whole domain is exact. | With capacity above the distinct count, verifies every estimate equals the true count, every error is `0`, and `min_count` stays `0`. |
| `a_merge_keeps_the_total_and_never_reads_low` | Merge conserves the inserted mass and stays one-sided. | Partitions the stream into two shards, merges their summaries, and verifies the merged total equals the whole stream length, residency stays within capacity, and no monitored key reads below either side's truth. |
| `a_serde_round_trip_preserves_every_answer` | Serde round trip preserves state and the rebuilt key index. | Round-trips through `rmp-serde` and verifies residency, capacity, total, `min_count`, and per-key estimate/error all match, then confirms the decoded summary still takes inserts against its rebuilt index. |
| `an_empty_summary_answers` | An empty summary answers every query without panicking. | Verifies `len`/`total`/`min_count` are zero, point queries return `0`, `is_guaranteed` is false, and `top_k`/`entries` are empty. |
| `a_weighted_arrival_matches_repeating_it_under_eviction` | A weighted arrival matches repeating it once the summary is evicting. | Feeds the same Zipf stream key by key into two 64-counter summaries, one arrival at a time and one `insert_many` per key, verifies the run saturates all 64 counters, and that `total`, `min_count`, and the whole sorted key/count/error entry list agree. |
| `bulk_insert_matches_repeated_inserts_and_a_zero_weight_is_inert` | `bulk_insert` is the loop it replaces, and a zero weight records nothing. | Verifies a 128-counter summary filled by `bulk_insert` matches one filled one at a time on `total` and on `top_k(usize::MAX)`, then that `insert_many` at weight `0` for a resident key and for an absent one leaves `total`, residency, and `top_k` untouched. |
| `a_zero_capacity_summary_still_answers` | A summary asked for no counters still holds one. | Verifies `with_capacity(0)` reports a capacity of `1`, and that after ten distinct keys residency is `1`, `total` is `10`, and the single held counter reads `10` with an error of `9`. |
| `a_merge_into_an_under_full_summary_keeps_the_ceiling_honest` | A merge carries the peer's ceiling even when there are counters to spare. | Merges a one-counter summary that saw key 7 ten times and key 8 twenty times into an empty 33-counter summary, then verifies residency is `1` and below capacity, `upper_bound` for the dropped key 7 still covers its true `10`, and `is_guaranteed` is false for the key that survived. |
| `a_merge_chain_stays_one_sided_against_the_truth` | A chain of merges over asymmetric capacities stays one-sided. | Shards the stream three ways by position into summaries of capacity 64, 512, and 7, merges them in turn, and verifies the merged `total` equals the stream length, residency stays within capacity, every true key's `upper_bound` covers its true count, every monitored key reads at or above its truth, and every key `is_guaranteed` accepts truly outranks the largest dropped key. |
| `a_merge_picks_the_same_survivors_every_time` | The merge depends on what the summaries hold, not on arrival order. | Merges two 200-counter summaries over keys `0..150` and `100..250`, verifies the union fills the capacity at 200 with exactly the 50 shared keys at count `2`, that five repeats give an identical key/count/error list, and that reversing both arrival orders gives the same list. |
| `a_round_trip_carries_the_merged_ceiling` | The ceiling a merge established survives the wire. | Merges a 4-counter summary of the stream's second half into a 32-counter summary of its first half, round-trips through `rmp-serde`, and verifies `min_count`, `total`, and residency are unchanged, every key's estimate matches, and every true key's decoded `upper_bound` still covers its true count. |
| `crafted_state_fails_closed` | Serialized state no run could have produced is refused. | Verifies hand-built payloads with a zero capacity, more entries than the capacity, a counter at count `0`, an error above its count, and the same key twice all fail to decode, while a well-formed two-counter state decodes and answers `top_k` and `estimate`. |
| `a_fresh_summary_is_well_formed` | A fresh summary passes the structural check. | For `with_capacity(4)`, verifies `validate()` succeeds, `min_count` is `0`, and `capacity` is `4`. |
| `a_capacity_of_zero_floors_at_one` | A requested capacity of zero floors at one counter. | Verifies `with_capacity(0)` reports a capacity of `1`, and that after two distinct keys `validate()` passes, residency is `1`, and the survivor reads `2`. |
| `a_weighted_arrival_displaces_the_minimum_and_starts_above_it` | A weighted eviction starts the arrival above the count it displaced. | In a two-counter summary holding `1` at 5 and `2` at 2, `insert_many(3, 4)` is verified to evict key 2, leaving key 3 at `6` with error `2`, key 2 at `0`, key 1 at `5`, `min_count` at `5`, `total` at `11`, and `validate()` passing. |
| `a_weighted_raise_passes_every_bucket_below_its_destination` | A multi-hop raise walks the bucket list rather than skipping it. | Over counters at 1, 2, 3, and 4, `insert_many(1, 9)` lifts key 1 from the bottom to the top; verifies the descending walk becomes `[(1,10), (4,4), (3,3), (2,2)]` and `validate()` passes. |
| `counts_saturate_and_keep_the_bucket_order` | Counts saturate at `u64::MAX` instead of wrapping. | Raises a counter seeded at `u64::MAX - 2` twice more past the ceiling, verifies `validate()` after each raise, both keys read `u64::MAX`, `total` is `u64::MAX`, and the two-counter walk stays in descending order. |
| `an_eviction_from_a_saturated_counter_stays_sound` | Evicting a saturated counter keeps the structure sound. | A one-counter summary holding `u64::MAX` is evicted by a new key; verifies `validate()` passes, residency is `1`, the arrival reads `u64::MAX` with an error of `u64::MAX`, and the evicted key's `upper_bound` is `u64::MAX`. |
| `a_merge_saturates_instead_of_wrapping` | Merge saturates rather than wrapping past the ceiling. | Merges a three-counter peer into a two-counter summary already at `u64::MAX` and `u64::MAX - 1`; verifies `validate()` passes, residency stays `2`, `total` is `u64::MAX`, both survivors read `u64::MAX`, and all four keys' `upper_bound` is `u64::MAX`. |
| `a_merge_carries_the_ceiling_into_an_under_full_summary` | A merge into an empty summary still raises its ceiling. | Merging a one-counter peer that saw key 7 ten times and key 8 twenty times into an empty 33-counter summary is verified to leave residency `1` below capacity, `min_count` at or above `10`, `upper_bound(7)` at or above `10`, `is_guaranteed(8)` false, and `validate()` passing. |
| `a_chain_of_merges_keeps_the_ceiling_above_everything_dropped` | Two merges compound their ceilings. | Merges two one-counter peers (7 at 10 against 8 at 20, then 9 at 5 against 10 at 7) into a 5-counter summary; verifies `validate()` after each merge, residency below capacity, `upper_bound` covering keys 7 and 9 at their true 10 and 5, and `min_count` at or above `15`. |
| `a_key_that_re_enters_after_a_merge_never_reads_low` | A key re-entering a merged summary comes back above the ceiling. | After a merge drops key 7 at its true 12, one further insert is verified to seat it at `min_count + 1` with `min_count` as its error, at or above its true `13`, with `validate()` passing. |
| `randomized_operations_keep_the_structure_sound` | Randomized weighted inserts keep the structure sound at every step. | Drives 4,000 `insert_many` calls of weight 1, 3, 11, or 97 over a 96-key domain at capacities 1, 2, 7, 64, and 257, calling `validate()` after every step, then checks residency equals `min(capacity, distinct)`, `total` equals the inserted mass, the walk is the right length and descending, and every truth is bracketed by its counter's estimate and error. |
| `randomized_merges_keep_the_structure_sound` | Randomized merges keep the structure sound and one-sided. | For five capacity/domain pairs, merges two independently fuzzed summaries into a fuzzed one and then inserts 200 more keys, calling `validate()` after each merge and after the inserts, and checks the error sandwich against the combined truth, `total` equal to the combined mass, and residency within capacity. |
| `a_decoded_summary_rebuilds_both_link_directions` | Decoding rebuilds the bucket and counter links, not just the counts. | Round-trips a fuzzed 48-counter summary through `rmp-serde` and verifies the decoded summary passes `validate()`, matches residency, `min_count`, and `total`, still brackets every truth, and walks the same key/count set. |
| `a_crafted_state_fails_closed` | `rebuild` refuses state the algorithm could not have produced. | Verifies a zero capacity, more entries than the capacity, a counter at count `0`, an error above its count, and a duplicated key are each rejected with the matching complaint in the message. |
| `a_declared_capacity_is_not_allocated_on_decode` | A huge declared capacity is not allocated on decode. | A state declaring a capacity of `1 << 40` with a single entry is verified to rebuild, pass `validate()`, report that capacity, and hold one counter. |

### Bloom

Test file: [`tests/e2e_membership.rs`](../tests/e2e_membership.rs)

Conformance: [`tests/conformance_kit.rs`](../tests/conformance_kit.rs) runs the shared `membership_battery` on both hash paths against a filter sized for 20,000 keys at a 1% target: `bloom_passes_membership_conformance` over `Bloom<FastPath>` and `bloom_regular_path_passes_membership_conformance` over `Bloom<RegularPath>`. The battery also checks a `predicted-false-positive-rate` band, holding the measured rate to within a quarter either side of the filter's own `predicted_fpp`.

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `an_inserted_key_is_never_reported_absent` | No false negative, on either hash path. | Fills a filter sized for 20,000 keys at a 1% target and verifies every inserted key reports present on both the `RegularPath` and `FastPath` filters, which decode columns differently. |
| `an_empty_filter_rejects_every_probe` | A fresh filter reports nothing present. | Verifies `is_empty` and a zero insert count, and that probes disjoint from the member set are rejected on both hash paths. |
| `the_measured_false_positive_rate_matches_what_the_sizing_predicts` | The delivered rate honours the target and matches `predicted_fpp`. | Across targets `0.1`, `0.01`, and `0.001`, measures the rate over probes disjoint from the members and verifies measured and predicted rates both stay at or under the target and agree within a quarter either way. |
| `the_fill_based_estimate_tracks_the_measured_rate` | `estimated_fpp` reads the bits set, not the insert count. | Inserts the member set three times and verifies `estimated_fpp` still tracks the measured rate within a quarter either way although the insert count has tripled. |
| `repeated_inserts_leave_the_bits_unchanged` | Fill is a function of the distinct keys alone. | Builds one filter with single inserts and one with duplicated inserts over the same keys and verifies identical `count_ones` and `fill_ratio`. |
| `a_union_equals_the_filter_of_the_concatenated_stream` | Merge is an exact union, which is what makes the filter shardable. | Splits a stream across two same-geometry filters, merges them, and verifies the same set bits and insert count as a single-pass filter, plus identical answers on members and non-members alike. |
| `sizing_meets_the_target_and_is_one_power_of_two_from_missing_it` | `dimensions_for` is pinned to its contract rather than to its own expression. | For `(n, p)` pairs from `(1, 0.5)` through `(10_000, 1e-12)`, verifies every slice is a power of two and the row count stays within `BLOOM_MAX_SLICES`, that the chosen geometry's `predicted_fpp(n)` meets the target, and that halving `cols` misses it. |
| `degenerate_geometries_still_answer` | Degenerate geometries answer rather than panic. | Verifies a one-bit filter answers and reports an `estimated_fpp` of `1.0` once set, and that a single-row filter still holds every member it was given. |
| `clearing_restores_an_empty_filter` | `clear` restores the constructed state and keeps the dimensions. | After clearing a filled filter, verifies it is empty with a zero insert count, unchanged rows and columns, and no member reported present. |
| `a_serde_round_trip_preserves_every_answer` | Serde round trip preserves every answer. | Round-trips through `rmp-serde` and verifies matching dimensions, insert count, and set bits, plus identical answers on members and non-members. |
| `storage_is_one_bit_per_cell` | Packed storage is one bit per cell, not one byte. | For an `8x4096` filter, verifies `bit_capacity` equals `rows * cols` and `size_in_bytes` is one eighth of it. |
| `sizing_never_asks_for_more_slices_than_the_seed_list_has` | The slice count stops at the number of seeds the hasher has. | For 10,000 keys at targets `1e-7`, `1e-9`, and `1e-12`, verifies the row count stays within `BLOOM_MAX_SLICES`, that no two rows of the filled filter hold identical bits, and that `effective_rows` equals the row count. |
| `sizing_stays_inside_the_allocation_ceiling` | A target the seed list cannot reach yields the widest geometry that fits. | For `expected_items` up to `usize::MAX` and targets `f64::MIN_POSITIVE`, `0.0`, and `2.0`, verifies `cols` is at least one and a power of two, rows fall in `1..=BLOOM_MAX_SLICES`, and `rows * cols` stays within `BLOOM_MAX_BITS`. |
| `a_nan_target_rate_is_rejected` | A NaN target rate panics. | Verifies `with_capacity(1_000, f64::NAN)` panics with "target false-positive rate must be finite". |
| `an_infinite_target_rate_is_rejected` | An infinite target rate panics. | Verifies `with_capacity(1_000, f64::INFINITY)` panics with "target false-positive rate must be finite". |
| `a_negative_infinite_target_rate_is_rejected` | A negative infinite target rate panics. | Verifies `with_capacity(1_000, f64::NEG_INFINITY)` panics with "target false-positive rate must be finite". |
| `extra_slices_past_the_seed_list_do_not_sharpen_the_filter` | Slices past the seed list buy storage and a hash, not selectivity. | Fills a `BLOOM_MAX_SLICES x 2^14` filter and one five rows wider over the same 20,000 keys and verifies the measured rates are equal and non-zero, `effective_rows` stays at `BLOOM_MAX_SLICES`, both `predicted_fpp` values match, the measured rate sits within five standard errors of the prediction, and `estimated_fpp` is no more than 1.5x the measured rate. |
| `slices_repeat_exactly_at_the_seed_list_boundary` | Row `r` and row `r + BLOOM_MAX_SLICES` receive the same seed. | For a `BLOOM_MAX_SLICES + 5` by 1,024 filter over 2,000 keys, verifies the only rows holding identical bits are exactly the pairs `(r, r + BLOOM_MAX_SLICES)`, on both hash paths. |
| `the_two_hash_paths_agree_only_where_the_geometry_gives_each_row_its_own_hash` | Whether the paths land on the same bits is a property of the geometry. | Verifies `8 x 65_536` needs 136 hash bits, so both paths fall back to one seeded hash per row and set identical bits, while `7 x 65_536` fits a packed hash and the paths differ; also verifies that second shape is `BLOOM_DEFAULT_ROWS` by `BLOOM_DEFAULT_COLS`. |
| `a_filter_cannot_be_decoded_into_the_other_hash_path` | The serialized form carries which path built the filter. | Verifies regular-path bytes fail to decode as `Bloom<FastPath>` with an error naming both paths, fast-path bytes fail as `Bloom<RegularPath>` and as the unannotated `Bloom`, and fast-path bytes decoded back into `Bloom<FastPath>` still report every member present. |
| `a_fast_path_serde_round_trip_preserves_every_answer` | The fast path round-trips like the regular one, tag and all. | Round-trips a fast-path filter sized for 20,000 keys at a 1% target and verifies matching dimensions, insert count, and set bits, plus identical answers on 20,000 non-member probes. |
| `a_fast_path_union_equals_the_filter_of_the_concatenated_stream` | Merge is an exact union on the fast path too. | Splits 10,000 keys by parity across two `7 x 2^14` fast-path filters, merges them, and verifies the same set bits and insert count as a single-pass filter, with every key reported present. |
| `merging_filters_of_different_widths_panics` | A merge across slice widths panics. | Verifies merging a `7 x 2^14` filter with a `7 x 2^13` one panics with "bit matrices must have the same dimensions". |
| `merging_filters_of_different_slice_counts_panics` | A merge across slice counts panics. | Verifies merging a `7 x 2^14` filter with an `8 x 2^14` one panics with "bit matrices must have the same dimensions". |
| `bulk_insert_matches_inserting_one_at_a_time` | `bulk_insert` is the loop, not a different filter. | On both hash paths, verifies a filter filled by `bulk_insert` over 20,000 keys has the same set bits and insert count as one filled key by key. |

### Elastic

Test file: [`src/sketches/elastic.rs`](../src/sketches/elastic.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `init_with_dimensions_sizes_both_parts` | Both parts take the requested dimensions. | `init_with_dimensions(12, 2, 256)` is verified to yield 12 heavy buckets and a 2x256 light layer. |
| `init_with_length_keeps_the_default_light_layer` | The old constructor is unchanged. | `init_with_length(8)` is verified to yield 8 heavy buckets over `DEFAULT_LIGHT_ROWS` x `DEFAULT_LIGHT_COLS`. |
| `an_empty_heavy_table_is_rejected` | A zero bucket count panics. | `init_with_length(0)` is verified to panic with "at least one heavy bucket" rather than divide by zero in the bucket index. |
| `a_negative_heavy_table_is_rejected` | A negative bucket count panics. | `init_with_length(-1)` is verified to panic rather than widen to a huge `usize`. |
| `an_empty_light_layer_is_rejected` | A zero-row light layer panics. | `init_with_dimensions(8, 0, 4096)` is verified to panic with "non-empty light layer". |
| `a_zero_width_light_layer_is_rejected` | A zero-column light layer panics. | `init_with_dimensions(8, 3, 0)` is verified to panic with "non-empty light layer". |
| `heavy_bucket_tracks_repeated_flow_exactly` | Heavy bucket tracks repeated flow exactly. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `light_sketch_counts_colliding_flows` | Light sketch counts colliding flows. | Core functional behavior for this component path is validated. |
| `eviction_moves_the_resident_flow_into_the_light_layer` | Takeover evicts the resident flow, not the arriving one. | After 10 inserts of a resident and `LAMBDA * 10` inserts of a colliding flow, verifies the bucket holds the arrival with `(vote_pos, vote_neg, eviction) = (1, 1, true)`, `query(resident) == 10`, and `query(arrival) == 80`. |
| `expansion_doubles_the_heavy_table` | Copy operation doubles bucket count. | After `expand_heavy()` on an 8-bucket table, verifies `bktlen` and `heavy.len()` are both `16`. |
| `expansion_preserves_every_existing_estimate` | Lemma 3.2 keeps estimates put across a doubling. | Records `query` for 24 flows over 8 buckets, doubles, and verifies every estimate is unchanged. |
| `repeated_expansion_keeps_estimates_intact` | Two doublings in a row stay correct. | Same 24 flows through two `expand_heavy()` calls to `bktlen = 32`, all estimates unchanged. |
| `an_insert_onto_a_stale_copy_replaces_it` | Incremental cleanup drops stale copies. | Finds a stale bucket after a doubling, inserts a key that hashes to it, and verifies the arrival takes the slot at `vote_pos = 1` while the displaced flow keeps its mass elsewhere. |
| `merge_after_expansion_does_not_double_count` | Flushing an expanded table spills each flow once. | Doubles a 24-flow table, merges an empty peer, and verifies every estimate equals its true count rather than twice it. |
| `merge_does_not_double_count_an_expanded_peer` | The peer's stale copies are skipped too. | Merges an expanded 24-flow peer into an unexpanded sketch and verifies no estimate doubles. |
| `maximum_merging_does_not_double_count_an_expanded_peer` | Same for max merging. | As above through `merge_max`. |
| `full_bucket_count_counts_residents_above_the_threshold` | Full-bucket count matches the threshold. | With 6 flows at 10 votes each, verifies `full_bucket_count(9) == 6` and `full_bucket_count(10) == 0`. |
| `compression_shrinks_the_heavy_table` | Active compression divides the bucket count. | After `compress_heavy(4)` on a 16-bucket table, verifies `bktlen` and `heavy.len()` are both `4`. |
| `compression_keeps_the_larger_flow_and_spills_the_smaller` | The bigger resident wins its group. | Puts a 30-vote flow in bucket 0 and a 3-vote flow in bucket 4, halves the table, and verifies the big flow still reads `30` from the heavy part while the small one has left it and reads back at least `3` from the light layer. |
| `compression_neither_loses_nor_doubles_mass` | Compression only ever adds error. | Compresses 40 flows over 16 buckets by 4 and verifies no flow underestimates and the summed estimate rises without doubling. |
| `a_ratio_that_does_not_divide_the_table_is_rejected` | Lemma 3.2 needs `w' \| w`. | `compress_heavy(3)` on an 8-bucket table panics with "must divide the bucket count". |
| `compression_after_expansion_does_not_double_count` | Stale copies are dropped before grouping. | Expands 12 buckets to 24 — putting each twin 12 apart — compresses by 3 so twins land in different groups, verifies no flow is resident twice, then merges an empty peer and verifies every estimate equals its true count. |
| `expand_then_compress_returns_to_the_original_size` | Doubling and halving round-trips. | `expand_heavy()` then `compress_heavy(2)` returns an 8-bucket table to 8 buckets with no flow underestimated. |
| `heavy_only_insert_never_touches_the_light_layer` | Overload mode leaves every light counter alone. | Seeds the light layer through `insert`, snapshots all `2x64` counters, then runs `insert_heavy_only` across a vacant seat, a match, discarded arrivals, and a takeover, and verifies the snapshot is unchanged. |
| `heavy_only_takeover_inherits_the_evicted_flow_size` | Takeover carries the evicted flow's size to the arrival. | After 10 resident votes and `LAMBDA * 10` colliding arrivals, verifies the bucket becomes `(arrival, vote_pos=10, vote_neg=0)` rather than starting at `1`. |
| `heavy_only_takeover_inherits_the_eviction_flag` | Takeover carries the bucket's flag to the arrival. | Seeds a resident bucket's `eviction` to `false` and to `true` in turn, drives a takeover through `insert_heavy_only`, and verifies the arrival reads back the seeded value rather than a forced `true`. |
| `heavy_only_takeover_discards_the_evicted_flow_as_designed` | The evicted flow's size is dropped, not spilled. | In the same scenario, verifies `query` on the evicted flow returns `0` against a true count of `10`. |
| `heavy_only_matches_insert_while_buckets_seat_and_match` | Seating and matching agree with the normal path. | Feeds 6 flows into a 16-bucket table through both paths and verifies every bucket field and every `query` result matches. |
| `merge_keeps_uncontested_flows_in_the_heavy_part` | Merge leaves elephants in the heavy part. | Verifies post-merge `query` returns exactly `30` and `18`, that both flows are still resident with those vote counts, and that every bucket carries the eviction flag. |
| `merge_keeps_the_larger_flow_on_a_contested_bucket` | A contested bucket goes to the larger flow. | With a 20-count and a 9-count flow on one bucket, verifies the 20 keeps the bucket and the loser reads back at `>= 9` from the light layer. |
| `merge_keeps_the_peers_flow_when_it_is_the_larger` | Each side is sized against its own sketch. | A 3-count local flow loses its bucket to the peer's 50-count flow; querying the peer's flow against the local sketch would read near 0 and flip the outcome. |
| `merge_sums_the_votes_of_a_flow_both_sides_held` | A shared elephant is summed, not replaced. | A flow with 30 left and 20 right comes back resident with `vote_pos == 50`. |
| `merge_never_underestimates_across_a_large_flow_set` | Merging preserves the one-sided guarantee. | 60 flows, half of them shared, through a `2x64` light layer; every flow reads back at or above its true count. |
| `merge_does_not_leave_a_stale_copy_as_a_resident` | Expansion copies do not survive a merge. | After a doubling and a merge, `heavy_hitters` reports each flow exactly once; the merge clears the stale flag, so a kept copy would look live. |
| `merge_preserves_colliding_flow_mass` | Merge preserves mass for bucket-colliding flows. | Merges two sketches whose flows share a heavy bucket and verifies both estimates stay at or above their true counts. |
| `a_bucket_reoccupied_after_merge_still_reads_the_light_layer` | A post-merge resident keeps its flushed mass. | After merging a 30-count flow away and re-inserting it once, verifies `query` returns `31` rather than `1`. |
| `maximum_merging_never_underestimates_disjoint_flows` | Maximum merging keeps Elastic's one-sided guarantee. | Merges two sketches over 80 disjoint flows through a `2x64` light layer and verifies every per-flow estimate is at or above its true count. |
| `maximum_merging_is_tighter_than_sum_merging` | Maximum merging beats sum merging on disjoint flows. | Runs the same 80-flow disjoint input through `merge` and `merge_max` and verifies no flow is looser under max and at least one is strictly tighter; measured totals are 434 against 359 for a truth of 275. |
| `maximum_merging_underestimates_a_mouse_flow_both_sides_saw` | Maximum merging's precondition, pinned as behavior. | A mouse flow kept out of the heavy part by a hot flow, inserted 30 times left and 20 times right, reads back as `30` after `merge_max` and `50` after `merge`. |
| `maximum_merging_sums_a_flow_both_heavy_parts_held` | The restriction is on the light half only. | A shared flow resident on both sides comes back with `vote_pos == 50` under `merge_max`, since the heavy parts are combined bucket by bucket either way. |
| `maximum_merging_keeps_the_larger_flow_on_a_contested_bucket` | Maximum merging contests buckets the same way. | The 20-count flow keeps the bucket against a 9-count peer, and the loser reads back at `>= 9`. |
| `heavy_hitters_reports_every_resident_above_the_threshold` | Heavy hitter detection reports the right set. | Over four residents of a 256-bucket table (50/30/12/3), verifies `heavy_hitters(20)` is exactly the 50 and 30 flows, `heavy_hitters(100)` is empty, and `heavy_hitters(1)` has all four. |
| `heavy_hitters_includes_a_flow_sitting_exactly_on_the_threshold` | The threshold is inclusive. | A flow of exactly 20 is reported at `threshold = 20` and one of 19 is not, matching the reference's `val >= threshold`. |
| `heavy_hitters_does_not_report_a_flow_twice_after_expansion` | Expansion does not duplicate hitters. | After `expand_heavy()` leaves every resident a stale copy, verifies three flows come back once each rather than twice. |
| `heavy_changes_reports_only_moves_past_the_threshold` | Heavy change detection filters by size of move. | Over rising (10->55), falling (60->8), and steady (40->42) flows, verifies only the first two are reported at `threshold = 20`. |
| `heavy_changes_covers_a_flow_present_in_only_one_window` | A flow in one window only is a change. | Verifies a flow of 40 that vanishes reports `(40, 0)` and one of 45 that appears reports `(0, 45)`. |
| `heavy_changes_reports_each_flow_once` | Each flow appears once in the change list. | A flow resident in both windows, each expanded so it also has a stale copy, reaches the id list four times and is reported once. |

### Coco

Test file: [`src/sketches/coco.rs`](../src/sketches/coco.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `insert_then_estimate_matches_full_value_for_partial_key` | Insert then estimate matches full value for partial key. | Core behavior for insert/query/update and deterministic semantics is validated; the substring query and `estimate_key` both return `5`. |
| `estimate_with_udf_allows_custom_partial_matching` | Estimate with udf allows custom partial matching. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `tied_minimum_buckets_are_chosen_uniformly_at_random` | Buckets tied at the smallest value are picked uniformly. | Inserts one key into 2,000 fresh `32x4` tables, where all four mapped buckets tie at `0`, and verifies each row takes between 200 and 800 of the landings rather than row 0 taking all of them. |
| `the_three_queries_disagree_on_a_key_that_prefixes_another` | The three query shapes are distinguished on prefixing keys. | With `k1=7` and `k10=5` inserted, verifies `estimate_substring("k1")` returns `12` while `estimate_key` and `estimate_projected` both return `7`. |
| `estimate_projected_aggregates_full_keys_sharing_a_partial_key` | Projection aggregates full keys onto one partial key. | Reproduces the paper's figure 7: two full keys on srcip `19.98.10.26` sum to `1041`, and the lone `34.52.73.17` key returns `856`. |
| `recorded_flows_yields_each_occupied_bucket_once` | The query front-end lists every recorded flow exactly once. | After 20 weighted inserts into a `32x4` table, verifies the iterator yields one entry per occupied bucket and that no key appears twice. |
| `group_by_agrees_with_per_key_projected_queries` | One-pass grouping matches the per-key scan. | Over 60 inserts across 5 families, verifies every `group_by` entry equals `estimate_projected` for the same partial key. |
| `group_by_preserves_the_inserted_mass` | Grouping conserves the inserted mass under eviction. | Drives 400 inserts of weight 3 through an `8x2` table and verifies the grouped totals still sum to `1200`. |
| `group_by_reproduces_the_papers_figure_seven` | Grouping reproduces the paper's worked example. | Groups `19.98.10.26\|80=521`, `19.98.10.26\|443=520`, and `34.52.73.17\|118=856` by srcip and verifies exactly two entries, `1041` and `856`. |
| `a_key_occupies_at_most_one_bucket_per_row` | A key never gains a second home in the table. | After 64 inserts of one key into a `32x4` table, verifies exactly one bucket holds it and `estimate_key` returns `64`. |
| `estimate_key_never_exceeds_the_inserted_mass` | Point queries stay inside the table mass. | Over 500 weighted inserts across 40 keys in an `8x2` table, verifies the table mass equals the inserted mass and no per-key estimate exceeds it. |
| `merge_combines_tables_without_losing_counts` | Merge combines tables without losing counts. | Merge behavior preserves expected aggregate semantics and internal invariants. |

### KMV

Test file: [`src/sketches/kmv.rs`](../src/sketches/kmv.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `assert_accuracy` | Assert accuracy. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `assert_merge_accuracy` | Assert merge accuracy. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `assert_serialization_round_trip` | Assert serialization round trip. | Serialization/deserialization preserves component state and behavior after round trip. |

### UniformSampling

Test file: [`src/sketches/uniform.rs`](../src/sketches/uniform.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `sample_count_tracks_rate` | Sample count tracks rate. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `samples_are_drawn_from_input_stream` | Samples are drawn from input stream. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `merge_combines_samples_using_rate_based_target` | Merge combines samples using rate based target. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `merge_rejects_different_rates` | Merge rejects different rates. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `sample_access_is_stable` | Sample access is stable. | Core behavior for insert/query/update and deterministic semantics is validated. |

## Sketch Frameworks

### Hydra

Test file: [`src/sketch_framework/hydra.rs`](../src/sketch_framework/hydra.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `hydra_updates_countmin_frequency` | Hydra updates countmin frequency. | Updates `"user;session"` with value `"event"` 5 times and verifies combined query `>= 5` while an unrelated key query is exactly `0.0`. |
| `hydra_updates_countmin_frequency_multiple_values` | Hydra updates countmin frequency multiple values. | Inserts values `I64(0..4)` with multiplicity `i` under one key, verifies per-value fan-out query `>= i`, and checks unrelated-key query returns `0.0`. |
| `hydra_round_trip_serialization` | Hydra round trip serialization. | After mixed inserts, verifies MessagePack round trip keeps non-empty payload, preserves dimensions/template type, and keeps queried frequencies exactly unchanged. |
| `multihead_hydra_updates_multiple_dimensions` | Multihead hydra updates multiple dimensions. | With two heads (`events`, `latency`), repeated updates make full-key and fan-out frequency queries for each head return at least `3.0`. |
| `hydra_subpopulation_frequency_test` | Hydra subpopulation frequency test. | On a fixed labeled dataset, asserts exact subpopulation frequencies for single-label, multi-label, full-key, and disjoint cross-population queries (including zero-result case). |
| `hydra_subpopulation_cardinality_test` | Hydra subpopulation cardinality test. | Using HLL-backed counters, checks single/multi/full-key cardinalities are approximately `3.0` (within `EPSILON`) and disjoint/unknown keys return `0.0`. |
| `hydra_tracks_kll_quantiles` | Hydra tracks KLL quantiles. | For inserted samples `[10,20,30,40,50]`, verifies CDF query at `30.0` is `0.6` (within `1e-9`) and empty-bucket query returns `0.0`. |
| `hydra_kll_single_label_cdfs` | Hydra KLL single label cdfs. | For each label group, verifies exact expected CDF levels `{1/3, 2/3, 1}` at chosen thresholds using `EPSILON` tolerance. |
| `hydra_kll_multi_label_cdfs` | Hydra KLL multi label cdfs. | Verifies exact CDF values for multi-label combinations and confirms a non-overlapping key pair returns CDF `0.0`. |
| `hydra_kll_extreme_queries` | Hydra KLL extreme queries. | Confirms CDF boundary behavior (`0` below range, `1` above range) for known keys and `0` for unknown keys. |
| `test_count_min_frequency_query` | Test count min frequency query. | Inserts one key three times into `HydraCounter::CM`, then verifies `Frequency` query succeeds and returns exactly `3.0`. |
| `test_count_min_invalid_query_types` | Test count min invalid query types. | Verifies unsupported CM queries return errors, including exact message for `Quantile` (`"Count-Min Sketch Counter does not support Quantile Query"`). |
| `test_hll_cardinality_query` | Test HLL cardinality query. | Inserts `100` unique items plus one duplicate and verifies `Cardinality` query succeeds with estimate constrained to `(90.0, 110.0)`. |
| `test_kll_quantile_query` | Test KLL quantile query. | Inserts values `1..=100` and verifies median query succeeds with estimate within `+/-5` of `50.0`. |
| `test_univmon_universal_queries` | Test univmon universal queries. | Inserts `A` 10 times and `B` 20 times, then checks `L1=30.0`, cardinality is approximately `2.0` (`abs err < 0.5`), and entropy is positive. |
| `test_merge_counters` | Test merge counters. | Merges two CM counters and verifies frequency sum (`2.0`) for shared key, then confirms merging with mismatched counter type (`HLL`) returns error. |
| `test_count_frequency_query` | Test count frequency query. | Inserts one `Count` key four times and verifies `Frequency` query succeeds with exact result `4.0`. |
| `test_count_invalid_query_types` | Test count invalid query types. | Verifies unsupported `Count` queries fail, including exact `Quantile` error message and error on `Cardinality`. |

### HashSketchEnsemble

Test file: [`src/sketch_framework/hashlayer.rs`](../src/sketch_framework/hashlayer.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `test_insert_and_estimate` | Insert and frequency estimate accuracy on Zipf stream. | On Zipf stream (`N=10_000`, `domain=1000`, `exp=1.5`), builds default 2-sketch ensemble (CMS + Count, `3x4096`) and verifies average relative error for both CMS (index `0`) and Count (index `1`) frequency estimates over sampled keys is below `0.1`. |
| `test_insert_at` | Targeted insert updates only specified indices. | Inserts only at index `[0]` via `insert_at`, then verifies CMS at index `0` has a positive estimate while Count at index `1` returns `0.0`. |
| `test_insert_with_hash_matches_insert` | Pre-computed hash insert matches regular insert. | Builds two identical ensembles; one uses `insert()`, the other uses `hash_input()` + `insert_with_hash()`. Verifies CMS estimates at index `0` are identical for a probe key. |
| `test_hll_cardinality` | HLL-only ensemble cardinality accuracy. | Builds HLL-only ensemble (`HyperLogLog<ErtlMLE>`), inserts Zipf stream, and verifies cardinality relative error vs true distinct count is `< 0.02`. |
| `test_estimate_on_hll_returns_error` | Frequency query on HLL sketch returns error. | Calls `estimate()` on an HLL-only ensemble and verifies it returns `Err`. |
| `test_cardinality_on_cms_returns_error` | Cardinality query on CMS sketch returns error. | Calls `cardinality()` on a CMS+Count ensemble and verifies it returns `Err`. |
| `test_direct_access` | Index-based get/get_mut and sketch type reporting. | Verifies `get(0)` and `get(1)` return `Some`, `get(2)` returns `None`, and `get_mut(0)` reports `sketch_type() == "CountMin"`. |
| `test_bounds_checking` | Out-of-bounds queries return errors. | Confirms `estimate(999, ...)`, `cardinality(999)`, and `estimate_with_hash(999, ...)` all return `Err`. |
| `test_custom_dimensions` | Custom-dimension ensemble insert and estimate. | Builds 2-sketch ensemble (CMS + Count, `5x2048`), verifies `len=2`/non-empty, inserts Zipf stream, and confirms both indices return positive estimates. |
| `test_mixed_matrix_and_hll` | Mixed CMS + HLL ensemble queries. | Builds ensemble with one CMS and one `HyperLogLog<ErtlMLE>`, inserts Zipf stream, verifies CMS estimate at index `0` is positive and HLL cardinality error at index `1` is `< 0.05`. |
| `test_push_compatible` | Push compatible sketch succeeds. | Creates single-CMS ensemble (`3x4096`), pushes a Count sketch with matching dimensions, verifies `push` returns `Ok` and `len=2`. |
| `test_push_incompatible_rejected` | Push incompatible sketch is rejected. | Creates single-CMS ensemble (`3x4096`), pushes a Count sketch with different dimensions (`5x2048`), verifies `push` returns `Err`. |

### UnivMon

Test file: [`src/sketch_framework/univmon.rs`](../src/sketch_framework/univmon.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `univmon_round_trip_serialization` | Univmon round trip serialization. | After weighted inserts, verifies non-empty serialization and round-trip preservation of configuration fields, `bucket_size`, `L1/L2/entropy` (`<1e-6` drift), and cardinality (`< EPSILON` drift). |
| `update_populates_bucket_size_and_heavy_hitters` | Update populates bucket size and heavy hitters. | Inserting one hot key `40` times sets `bucket_size=40`, tracks key in heavy-hitter heap with count `>=20`, and yields exact `L1=40` and `cardinality=1`. |
| `merge_with_combines_heavy_hitters` | Merge with combines heavy hitters. | Merging sketches with disjoint heavy keys verifies merged left heap contains both contributions (`left=25`, `right=30`) while right heap retains `right=30`. |
| `univmon_layers_use_different_seeds` | Univmon layers use different seeds. | Verifies hash outputs for the same key with seed indices `0..3` are all pairwise different. |
| `univmon_cardinality_is_positive` | Univmon cardinality is positive. | After inserting `20` distinct flow keys, cardinality estimate is exactly `20.0`. |
| `univmon_bucket_size_tracked_correctly` | Univmon bucket size tracked correctly. | Inserts counts `100`, `200`, `150` for three flows and verifies `bucket_size` equals total `450`. |
| `univmon_basic_operation` | Univmon basic operation. | On fixed mixed workload, verifies exact aggregate metrics `cardinality=10.0` and `L1=131.0`. |
| `test_statistical_accuracy` | Test statistical accuracy. | On heavy/medium/noise synthetic distribution, verifies relative error for both `L2` and `entropy` is below `0.15`. |
| `univmon_random_data_matches_ground_truth_within_five_percent` | Univmon random data matches ground truth within five percent. | Over `10_000` random weighted updates, requires relative error `<= 0.05` for `cardinality`, `L1`, `L2`, and `entropy` against exact truth map. |

### UnivMon Optimized

Test file: [`src/sketch_framework/univmon_optimized.rs`](../src/sketch_framework/univmon_optimized.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `pool_basic_take_put` | Pool basic take put. | Validates pool accounting: initial preallocation (`available=2`, `allocated=2`), on-demand allocation when empty (`allocated=3`), and reuse on put/take without further allocation. |
| `pool_free_resets_sketch` | Pool free resets sketch. | Confirms returning a used sketch to pool resets state so retaken sketch has `bucket_size=0` and near-zero `L2` in layer `0`. |
| `pyramid_basic_insert_and_query` | Pyramid basic insert and query. | For simple inserts, verifies exact aggregate state with `bucket_size=65`, `L1` approximately `65` (`<1e-6`), and `cardinality=3`. |
| `pyramid_fast_insert_matches_standard` | Pyramid fast insert matches standard. | On identical 500-item stream, verifies standard vs fast paths keep identical `bucket_size`, with `L1` deviation `<10%` and cardinality deviation `<15%`. |
| `pyramid_two_tier_dimensions` | Pyramid two tier dimensions. | Verifies two-tier layout metadata for configured pyramid (`layer_size=8`, `elephant_layers=4`). |
| `pyramid_free_resets_state` | Pyramid free resets state. | After bulk inserts, `free()` resets sketch to empty baseline (`bucket_size=0`, layer-0 `L2` approximately `0`). |
| `pyramid_merge_combines_data` | Pyramid merge combines data. | Merging disjoint halves verifies merged `L1` stays within `10%` of the sum of pre-merge `L1` values. |
| `pyramid_accuracy_zipf` | Pyramid accuracy Zipf. | On heavy/medium/light Zipf-like workload, requires relative error `<15%` for `L1`, `L2`, cardinality, and entropy. |
| `pyramid_fast_insert_accuracy` | Pyramid fast insert accuracy. | Using `fast_insert` only, requires relative error `<15%` for `L1`, `L2`, cardinality, and entropy versus exact frequency map. |
| `pyramid_memory_savings_vs_uniform` | Pyramid memory savings vs uniform. | Verifies pyramid column budget is smaller than uniform baseline and computed memory savings exceed `30%`. |

### NitroBatch

Test file: [`src/sketch_framework/nitro.rs`](../src/sketch_framework/nitro.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `nitro_batch_countmin_error_bound_zipf` | Nitro batch countmin error bound Zipf. | On Zipf stream (`rows=3`, `cols=4096`, `N=200_000`), verifies CountMin estimates satisfy in-bound key count `> (1-delta)*distinct` using `epsilon=e/cols`, `delta=e^-rows`, and bound `epsilon*N`. |
| `nitro_batch_count_error_bound_zipf` | Nitro batch count error bound Zipf. | Applies the same probabilistic in-bound criterion to `Count` median estimates with `epsilon=e/cols`, `delta=e^-rows`, and bound `epsilon*N`. |

### ExponentialHistogram

Test file: [`src/sketch_framework/eh.rs`](../src/sketch_framework/eh.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `constructor_infers_merge_norm` | Constructor infers merge norm. | Verifies constructor infers `SketchNorm::L1` for CM payload and `SketchNorm::L2` for `COUNTL2HH` payload. |
| `l1_merge_invariant_same_size` | L1 merge invariant same size. | Under repeated updates with `k=2`, verifies L1 merge policy compacts buckets so `bucket_count < 10`. |
| `l2_merge_invariant_sum_l22` | L2 merge invariant sum l22. | With `k=1` and weighted updates, verifies L2 merge rule keeps bucket count bounded (`bucket_count <= 2`). |
| `merge_recomputes_l2_mass` | Merge recomputes L2 mass. | After L2 merges, verifies bounded bucket count (`<=2`) and non-negative recomputed `l2_mass` for every payload bucket. |
| `test_basic_insertion_and_query` | Test basic insertion and query. | After one update at `t=100`, verifies single bucket presence, exact min/max timestamps (`100`), and successful interval merge query for `[100,100]`. |

### EHSketchList

Test file: [`src/sketch_framework/eh_sketch_list.rs`](../src/sketch_framework/eh_sketch_list.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `insert_routes_to_countl2hh_and_univmon` | Insert routes to countl2hh and univmon. | Verifies variant routing by checking `COUNTL2HH` estimate `>=9` after 9 inserts and `UNIVMON` `bucket_size=6` after 6 inserts. |
| `count_sketch_insert_and_query_round_trip` | Count insert and query round trip. | Confirms the `Count` variant updates/query path by inserting one key and verifying returned estimate is at least `1.0`. |
| `ddsketch_insert_and_quantile_query_round_trip` | DDSketch insert and quantile query round trip. | Inserts `10,20,30` into DDSketch variant and verifies queried median (`q=0.5`) lies within `[10.0, 30.0]`. |
| `supports_norm_whitelist_is_enforced` | Supports norm whitelist is enforced. | Validates norm capability matrix: `CM/CS/DDS` support `L1` only, while `COUNTL2HH/UNIVMON` support `L2` only. |

### EHUnivOptimized

Test file: [`src/sketch_framework/eh_univ_optimized.rs`](../src/sketch_framework/eh_univ_optimized.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `basic_insertion_and_query` | Basic insertion and query. | For updates `{(1,5),(2,3),(1,2)}` across `[100,102]`, verifies map-tier result with exact counts (`1->7`, `2->3`, `total=10`) plus `L1=10` and `cardinality=2`. |
| `map_merge_bounds_volume` | Map merge bounds volume. | With `k=1` and 50 one-count updates, verifies merge policy bounds growth so `bucket_count < 50`. |
| `promotion_creates_sketch_buckets` | Promotion creates sketch buckets. | Under small promotion thresholds and many distinct updates, verifies at least one map bucket is promoted (`um_buckets` becomes non-empty). |
| `window_expiration` | Window expiration. | With `window=100`, advancing to `t=200` after earlier inserts confirms expiration by forcing oldest surviving `min_time` to recent range (`>=100` or `==200`). |
| `hybrid_query_returns_sketch` | Hybrid query returns sketch. | After forcing both map and sketch tiers to coexist, verifies interval query spanning both returns `EHUnivQueryResult::Sketch` (not map-only). |
| `cover_check` | Cover check. | Verifies coverage logic transitions from false (empty) to true for contained intervals and remains false when query extends outside observed range. |
| `accuracy_known_distribution` | Accuracy known distribution. | On fixed known histogram, verifies query estimates for `L1`, `L2`, cardinality, and entropy each stay within `10%` relative error. |
| `pool_used_during_promotion` | Pool used during promotion. | With bounded preallocated pool, promotion workload verifies sketch-tier creation and confirms pool allocation accounting remains active (`total_allocated >= 2`). |
| `correctness_map_only_exact` | Correctness map only exact. | For map-only regime, verifies `L1/L2/cardinality/entropy` each match exact truth within `1%` tolerance. |
| `correctness_subinterval_query` | Correctness subinterval query. | For two-phase stream, verifies full-interval query recovers `L1` approximately `200` and `cardinality` approximately `2` within `5%` tolerance. |
| `correctness_expired_data_excluded` | Correctness expired data excluded. | After sliding beyond window cutoff, verifies very old segment is excluded by checking earliest retained bucket time is at least `50`. |
| `correctness_volume_bounded_long_stream` | Correctness volume bounded long stream. | Over `20_000` updates with `k=4`, verifies EH volume bound by requiring maximum observed bucket count `< 200`. |
| `correctness_pool_recycling_across_cycles` | Correctness pool recycling across cycles. | Long-run expiration/promotion cycling keeps pool bounded (`total_allocated < 50`) and still returns valid interval query results. |
| `correctness_sketch_merge_preserves_metrics` | Correctness sketch merge preserves metrics. | After repeated promotions/merges, verifies each sketch bucket has positive `L2^2` and stored `l22` stays within `1%` relative difference of recomputed value. |
| `accuracy_zipf_distribution_sketch_tier` | Accuracy Zipf distribution sketch tier. | On heavy/medium/light Zipf-like stream in sketch tier, requires `L1/L2/cardinality/entropy` relative errors each `<= 15%`. |
| `accuracy_uniform_distribution` | Accuracy uniform distribution. | On uniform stream, requires `L1/L2/cardinality/entropy` relative errors each `<= 10%`. |
| `accuracy_sliding_window` | Accuracy sliding window. | Across suffix and periodic sliding-window queries, verifies average relative error for `L1`, `L2`, cardinality, and entropy is each below `15%`. |
| `accuracy_varies_with_k` | Accuracy varies with K. | For `k in {2,8,32}`, verifies per-k average of `L1/L2` relative errors remains under `15%` on same fixed stream/window. |
| `accuracy_suffix_queries` | Accuracy suffix queries. | Across suffix lengths `[1000,2000,5000,8000]`, verifies worst observed `L2` relative error remains below `20%`. |
| `accuracy_distribution_shift` | Accuracy distribution shift. | For two-phase distribution shift stream, verifies full-span `L1/L2/cardinality/entropy` estimates each stay within `15%` relative error. |

## Common

### Common Hash Utilities

Test file: [`src/common/hash.rs`](../src/common/hash.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `hash128_seeded_preserves_cardinality` | Hash128 seeded preserves cardinality. | With `SEED_IDX=0` and `SAMPLE_SIZE=5000`, verifies uniform and Zipf sample unique-input counts exactly match unique-hash counts (no observed collisions). |
| `hash128_seeded_is_deterministic_for_repeated_inputs` | Hash128 seeded is deterministic for repeated inputs. | For fixed key `"deterministic-key"` and seed `3`, verifies 100 repeated `hash128_seeded` calls always equal the first hash value. |
| `digest_hasher_spreads_digests_that_share_their_low_bits` | `DigestHasher` mixes rather than passing a digest through. | Hashes `0..1024` shifted left by 16, so every digest shares its ten low bits, and verifies more than 550 of the 1,024 low-bit buckets are occupied rather than the single bucket a pass-through hash would fill; the unshifted range is held to the same bound. |
| `digest_hasher_is_deterministic` | `DigestHasher` returns the same value for the same digest. | For `0`, `1`, `u64::MAX`, and `0xdead_beef`, verifies repeated calls agree, and that `0` and `1` hash differently. |

### Common Heap Utilities

Test file: [`src/common/heap.rs`](../src/common/heap.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `heap_retains_top_k_items_by_count` | Heap retains top K items by count. | For `HHHeap::new(3)` updated with counts `1..5`, verifies heap size is `3` and retained counts are exactly `[3,4,5]`. |
| `update_count_increments_existing_entry` | Update count increments existing entry. | Repeatedly updates key `alpha` with counts `1,2,3` and verifies stored heap entry count is `3` (incremental update, not replacement). |
| `clean_resets_heap_state` | Clean resets heap state. | After inserting two items into `HHHeap::new(2)`, `clear()` is verified to leave the heap empty. |
| `test_min_heap_basic` | Test min heap basic. | For `CommonHeap::<i32, KeepSmallest>::new_min(5)`, verifies `peek=1` and pop order `1,3,5,7`, then `None`. |
| `test_max_heap_basic` | Test max heap basic. | For `CommonHeap::<i32, KeepLargest>::new_max(5)`, verifies `peek=7` and pop order `7,5,3,1`, then `None`. |
| `test_bounded_heap_capacity` | Test bounded heap capacity. | With min-heap capacity `3`, verifies length never exceeds 3 and final retained values are `[5,7,10]` after pushing `5,3,7,1,10`. |
| `test_update_at` | Test update at. | After mutating an internal element (`heap[1]=3`) and calling `update_at(1)`, verifies heap root updates so `peek()` becomes `3`. |
| `test_custom_struct_with_ord` | Test custom struct with ord. | Uses `HHItem` values with counts `5,3,7` and verifies min-heap ordering by checking root count is `3`. |
| `test_topk_use_case` | Test topk use case. | Simulated top-k flow keeps only counts `[3,4,5]` at capacity 3 and verifies lookup for `key-4` succeeds with count `4`. |
| `test_heap_size` | Test heap size. | Verifies both `CommonHeap<u64, KeepSmallest>` and `CommonHeap<u64, KeepLargest>` sizes equal `size_of::<Vec<u64>>() + size_of::<usize>()`. |
| `test_topk_with_custom_comparator` | Test topk with custom comparator. | With custom comparator and capacity 3, verifies low-count insert is rejected/replaced as expected so heap size is 3 and root count is `5`. |
| `test_exact_topk_heap_replacement` | Test exact topk heap replacement. | Reproduces TopK-style find/update flow for keys `1..5`, verifies retained counts `[3,4,5]`, finds `key-4` with count `4`, then verifies `clear()` makes heap empty. |
| `the_index_survives_a_long_churning_stream` | The index stays exact through eviction, promotion, and re-entry. | At capacities 1, 2, 7, 64, and 512, streams 20,000 skewed draws over a 2,048-key domain and every 97 steps verifies `slots` carries each resident's digest, every position is listed in its own bucket, `find_heap_item` returns that position, and the index holds no entry beyond the residents; residency ends at `min(capacity, distinct)`. |
| `the_residents_are_the_k_largest_counts` | The residents are exactly the `k` largest counts. | Over 20,000 skewed draws across a 512-key domain into a 32-slot heap, verifies against a brute-force ranking that every resident's count is current and at or above the 32nd largest count, and that the heap is full. |
| `re_scoring_a_resident_never_duplicates_it` | A resident is re-scored in place rather than seated twice. | Updates `hot` and `warm` 50 rounds each into a 4-slot heap and verifies the index stays consistent, residency is `2`, `find` locates `hot`, and its count reads `50`. |
| `a_zero_capacity_heap_turns_everything_away` | A zero-capacity heap accepts nothing and stays consistent. | Verifies 32 updates into `HHHeap::new(0)` all return `false`, the heap stays empty, `find` returns `None`, and the index is consistent. |
| `clearing_drops_the_index_with_the_heap` | `clear` drops the index along with the entries. | After filling an 8-slot heap from 32 keys, verifies `clear()` leaves it empty with `find` returning `None` and a consistent index, then refills from 40 fresh keys and verifies the index is consistent again at residency `8`. |
| `a_decoded_heap_rebuilds_its_index` | The index is derived, so decoding rebuilds it rather than carrying it. | Round-trips a 16-slot heap fed 5,000 skewed draws through `rmp-serde` and verifies the decoded index is consistent, residency and capacity match, every resident is still found, and a further update re-scores a resident to `1_000_000` with the index still consistent. |
| `the_indexed_heap_matches_the_rebuild_implementation` | Retention does not depend on how the index is maintained. | At capacities 0, 1, 2, 3, 7, 8, 64, and 257, runs 30,000 skewed draws over a 4,096-key domain through both the shipped heap and a reference that rebuilds its key index in full after every accepted update, and verifies at every step that the completeness flag agrees and that both arrays hold the same key and count at every position. |
| `the_indexed_heap_matches_the_rebuild_implementation_on_string_keys` | The same agreement on the owned-key path and its different hash. | Runs 20,000 `flow::<n>` string keys over a 2,048-key domain into a 32-slot heap and verifies the shipped and rebuild implementations agree on the completeness flag and on every position's key and count at every step. |
| `the_two_agree_when_counts_move_in_both_directions` | Falling counts sink a resident back down and the two still agree. | Feeds a 16-slot heap 10,000 updates whose counts oscillate over `-30..=30` rather than only climbing, and verifies the shipped and rebuild implementations agree on the completeness flag and on every position at every step. |
| `the_heap_is_reproducible_across_runs` | The same stream gives the same heap, array position included. | Runs 20,000 skewed draws over a 1,024-key domain into a 64-slot heap five times over and verifies all five key/count snapshots are identical, so neither the index nor the map's iteration order reaches the output. |
| `a_bucket_holding_both_sides_of_a_swap_is_unchanged` | Swapping two entries of one bucket leaves the bucket alone. | With heap positions `0` and `1` both carrying digest `7`, `swap_entry(0, 1)` is verified to leave the slots as `[7, 7]` and the bucket listing positions `[0, 1]`. |
| `a_swap_moves_only_its_own_entry_out_of_a_shared_bucket` | A swap patches the matching entry, not the bucket head. | With digest `7` listed at positions `[2, 0]` and digest `9` at `[1]`, `swap_entry(0, 1)` is verified to leave slots `[9, 7, 7]`, digest `7` listing `[1, 2]`, and digest `9` listing `[0]`; patching the head instead would corrupt position 2. |
| `dropping_one_entry_keeps_the_rest_of_its_bucket` | Dropping one position leaves the bucket's other positions. | With digest `7` listed at `[0, 3, 5]`, `drop_entry(7, 3)` is verified to leave `[0, 5]`. |
| `dropping_the_last_entry_removes_the_bucket` | The bucket goes when its last position does. | With digest `7` listed at `[4]`, `drop_entry(7, 4)` is verified to remove the key from the index entirely. |
| `two_shared_buckets_follow_the_sift` | The index tracks the slots through a long run of sifts. | Seeds a 16-item `HHHeap` so residents alternate between two digests, then runs 200 `rescore` calls and verifies after each that every position is listed under the digest its slot names and that the buckets together name every heap position exactly once. |

### Common Structure Utilities

Test file: [`src/common/structure_utils.rs`](../src/common/structure_utils.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `median_test` | Median test. | For 1,000 seeded random arrays of lengths 3, 4, and 5, verifies `compute_median_inline_f64` exactly matches sort-based median for every case. |

### Vector2D (Common Structure)

Test file: [`src/common/structures/vector2d.rs`](../src/common/structures/vector2d.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `required_bits_match_expected_thresholds` | Required bits match expected thresholds. | Verifies `Vector2D::get_required_bits()` returns `64` for `(3,4096)`, `32` for `(3,64)`, and `128` for `(5,1_048_576)`. |

### BitMatrix (Common Structure)

Test file: [`src/common/structures/bit_matrix.rs`](../src/common/structures/bit_matrix.rs)

| test_name | test_description | what_is_tested |
| --- | --- | --- |
| `every_cell_owns_exactly_one_bit` | Each cell owns one bit and no other cell's. | Across 33 geometries (`1`, `2`, and `5` rows by columns `1`, `2`, `7`, `63`, `64`, `65`, `100`, `127`, `128`, `129`, and `255`), sets every cell in turn and verifies it read clear beforehand, reads set afterwards, and that `count_ones` rises by exactly one each time, ending at `rows * cols` with a `fill_ratio` of `1.0`. |
| `row_padding_is_never_reachable_or_counted` | Rows are padded out to whole words and the padding never counts. | Over the same 33 geometries, fills every addressable cell and verifies `size_in_bytes` is `rows * cols.div_ceil(64) * 8` while `count_ones` is exactly `rows * cols`. |
| `put_and_clear_reset_individual_bits` | `put(.., false)` and `clear` both return the matrix to empty. | Over the same 33 geometries, sets every cell through `put(.., true)`, clears them one at a time through `put(.., false)` and verifies `count_ones` is `0` and `fill_ratio` is `0.0`, then sets the last cell and verifies `clear()` empties it again. |
| `a_column_past_the_last_one_panics_rather_than_aliasing` | A column past `cols` panics instead of landing in row padding. | Verifies `set(0, 100)` on a `3x100` matrix panics with "(0, 100) is outside a 3x100 bit matrix". |
| `a_column_reaching_the_next_row_panics` | A column that reaches the next row's words panics. | Verifies `set(0, 128)` on a `3x100` matrix panics with "(0, 128) is outside a 3x100 bit matrix" rather than aliasing row 1. |
| `a_row_past_the_last_one_panics` | A row past the last one panics. | Verifies `get(3, 0)` on a `3x100` matrix panics with "(3, 0) is outside a 3x100 bit matrix". |
| `put_checks_bounds_too` | `put` is bounds-checked like `set` and `get`. | Verifies `put(0, 120, true)` on a `2x100` matrix panics with "(0, 120) is outside a 2x100 bit matrix". |
| `union_takes_the_bitwise_or` | `union_from` is the bitwise or of the two grids. | Unions a `3x100` matrix holding `(0,5)` and `(1,99)` with one holding `(1,99)` and `(2,0)`, then verifies `count_ones` is `3` and all three cells read set. |
| `union_across_geometries_panics` | A union across geometries panics. | Verifies `union_from` between a `3x100` and a `3x101` matrix panics with "bit matrices must have the same dimensions". |
| `a_zero_dimension_is_rejected` | A zero dimension panics at construction. | Verifies `BitMatrix::new(0, 64)` panics with "a bit matrix needs both dimensions". |
| `a_round_trip_recomputes_the_derived_fields` | A decoded matrix folds hashes exactly as the original does. | Over the same 33 geometries, round-trips through `rmp-serde` and verifies the dimensions and every cell match, then drives one `hash_for_matrix` digest through `fast_insert` on both the original and the decoded matrix and verifies they set the same cells. |
| `a_payload_that_does_not_fit_its_dimensions_is_rejected` | A word count that disagrees with the dimensions fails at decode. | Verifies a `3x100` payload carrying 2 words is refused with "needs 6 words, got 2", one carrying 9 words is refused, and a zero-row payload is refused with "needs both dimensions". |
| `the_wire_form_carries_only_the_stored_fields` | The wire carries `words`, `rows`, and `cols` and nothing derived from them. | Verifies a fresh `3x100` matrix encodes byte for byte identically to a struct of just those three fields. |
