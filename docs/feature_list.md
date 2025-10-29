# Feature Inventory

## Common Building Blocks

- `SketchInput<'a>` (`src/common/input.rs`)
  - Capabilities: enum covers `I32`, `I64`, `U32`, `U64`, `F32`, `F64`, `Str`, `String`, `Bytes`; hashing helpers `hash_it` and `hash_for_all_rows`; deterministic `SEEDLIST` with `LASTSTATE`.
  - Notes: hashing uses `twox_hash::XxHash64`; `hash_for_all_rows` allocates a fresh `Vec<u64>`.
- `Vector1D<T>` (`src/common/structures.rs`)
  - Capabilities: `init`, `filled`, `fill`, `from_vec`, iterator helpers, `update_if_greater`, `update_if_smaller`, `update_one_counter`.
  - Notes: thin clone-based wrapper around `Vec<T>`.
- `Vector2D<T>` (`src/common/structures.rs`)
  - Capabilities: `init`, `from_fn`, `fill`, dimension accessors, row slicing, `update_one_counter`, `fast_insert`, `query_one_counter`, `fast_query`.
  - Notes: fast paths reuse a single hash across rows; callers supply bounds guarantees.
- `Vector3D<T>` (`src/common/structures.rs`)
  - Capabilities: `init`.
  - Notes:
- `Heap`: 
- `hash_for_all_rows` (`src/common/input.rs`)
  - Capabilities: derives per-row 13-bit windows from a single `hash_it` call.
  - Notes: staging helper for future optimized paths.

## Core Sketches

- `CountMin` (`src/sketches/countmin.rs`)
  - Constructors: `with_dimensions`, `init_cm_with_row_col`, `init_count_min`, `default`.
  - Update: `insert`, `fast_insert`, `insert_cm`.
  - Query / Reporting: `estimate`, `fast_estimate`, `get_est`, `as_storage`, `as_storage_mut`, `debug`.
  - Merge: `merge`.
  - Serde: `serialize_to_bytes`, `serialize`, `deserialize_from_bytes`, `deserialize`.
  - Extras: backed by `Vector2D<u64>`; fast paths reuse shared hashes.
- `Count` (`src/sketches/count.rs`)
  - Constructors: `init_count`, `init_count_with_rc`, `default`.
  - Update: `insert_count`.
  - Query / Reporting: `get_est`, `debug`.
  - Merge: `merge`.
  - Serde:
  - Extras: median-of-sign strategy for resilient estimates.
- `CountUniv` (`src/sketches/count.rs`)
  - Constructors: `init_count`, `init_countuniv_with_rc`, `default`.
  - Update: `insert_once`, `insert_with_count`, `insert_with_count_without_l2`, `update_and_est`, `update_and_est_without_l2`.
  - Query / Reporting: `get_est`, `get_l2`, `get_l2_sqr`, `debug`.
  - Merge: `merge`.
  - Serde:
  - Extras: tracks per-row `l2` norms alongside counters.
- `Coco<'a>` (`src/sketches/coco.rs`)
  - Constructors: `new`, `init_with_size`, `default`.
  - Update: `insert`.
  - Query / Reporting: `estimate`, `estimate_with_udf`, `debug`.
  - Merge: `merge`.
  - Serde:
  - Extras: buckets retain optional full keys for partial matching.
- `Elastic` (`src/sketches/elastic.rs`)
  - Constructors: `new`, `init_with_length`, `default`.
  - Update: `insert`.
  - Query / Reporting: `query`.
  - Merge:
  - Serde: derives `Serialize`, `Deserialize`.
  - Extras: heavy bucket backed by `CountMin` light sketch; `HeavyBucket::evict` handles swaps.
- `HyperLogLog` (`src/sketches/hll.rs`)
  - Constructors: `new`, `default`.
  - Update: `insert`.
  - Query / Reporting: `indicator`, `get_est`.
  - Merge: `merge`.
  - Serde: `serialize_to_bytes`, `serialize`, `deserialize_from_bytes`, `deserialize`.
  - Extras: 14-bit precision with `NUM_REGISTERS = 16384`.
- `HllDf` (`src/sketches/hll.rs`)
  - Constructors: `new`, `default`.
  - Update: `insert`.
  - Query / Reporting: `get_est`.
  - Merge: `merge`.
  - Serde: `serialize_to_bytes`, `serialize`, `deserialize_from_bytes`, `deserialize`.
  - Extras: implements Ertl estimator via `get_histogram`, `hlldf_sigma`, `hlldf_tau`.
- `HllDs` (`src/sketches/hll.rs`)
  - Constructors: `new`, `default`.
  - Update: `insert`.
  - Query / Reporting: `get_est`.
  - Merge:
  - Serde: `serialize_to_bytes`, `serialize`, `deserialize_from_bytes`, `deserialize`.
  - Extras: HIP estimator maintains `kxq0`, `kxq1`, `est`; `merge` intentionally panics.
- `KLL` (`src/sketches/kll.rs`)
  - Constructors: `init_kll`.
  - Update: `update`, `compact`, `update_size`.
  - Query / Reporting: `rank`, `count`, `quantile`, `cdf`, `print_compactors`.
  - Merge: `merge`.
  - Serde: derives `Serialize`, `Deserialize`.
  - Extras: layered `Compactor`s with `Coin` toss sampling.
- `LocherSketch` (`src/sketches/locher.rs`)
  - Constructors: `new`.
  - Update: `insert`.
  - Query / Reporting: `estimate`.
  - Merge:
  - Serde: derives `Serialize`, `Deserialize`.
  - Extras: per-cell `TopKHeap`; median subtracts noise via row totals.
- `MicroScope` (`src/sketches/microscope.rs`)
  - Constructors: `init_microscope`.
  - Update: `insert`, `delete`.
  - Query / Reporting: `query`, `debug`.
  - Merge: `merge`.
  - Serde: derives `Serialize`, `Deserialize`.
  - Extras: zoom-in/out logic in `carry_in`; maintains sub-window counters.
- `UniformSampling` (`src/sketches/uniform.rs`)
  - Constructors: `new`, `with_seed`.
  - Update: `update`, `update_input`.
  - Query / Reporting: `samples`, `sample_at`, `len`, `is_empty`, `total_seen`, `sample_rate`.
  - Merge: `merge`.
  - Serde: derives `Serialize`, `Deserialize`.
  - Extras: priority-based reservoir using custom RNG; merges reconcile priorities.
- `GeometrySampling`: feature: changing sample rate based on receiving rate
- `UnivMon` (`src/sketches/univmon.rs`)
  - Constructors: `init_univmon`, `new_univmon_pytamid`.
  - Update: `update`, `update_optimized`, `update_pyramid`.
  - Query / Reporting: `calc_l1`, `calc_l2`, `calc_entropy`, `calc_card`, `get_bucket_size`, `get_memory_kb`, `get_memory_kb_pyramid`, `free`.
  - Merge: `merge_with`.
  - Serde: derives `Serialize`, `Deserialize`.
  - Extras: layers stack `CountUniv` with `TopKHeap` heavy hitters; `find_bottom_layer_num` guides updates.
- `TopKHeap` (`src/sketches/heap.rs`)
  - Constructors: `init_heap`, `init_heap_from_heap`.
  - Update: `update`, `update_count`, `clean`.
  - Query / Reporting: `find`, `get_memory_bytes`, `print_heap`.
  - Merge:
  - Serde: derives `Serialize`, `Deserialize`.
  - Extras: maintains `Vec<Item>` min-heap with helpers `swap`, `update_order_up`, `update_order_down`.

## Sketchbook Orchestration

- `Chapter<'a>` (`src/sketchbook/chapter.rs`)
  - Responsibilities: enum wrapper for `CountMin`, `Coco`, `CountUniv`, `Elastic`, `HllDf`, `KLL`, `UniformSampling`, `LocherSketch`, `UnivMon`; exposes unified `insert`, `merge`, `query`, `get_type`.
  - Notes: handles type-specific conversions (e.g., `iv_to_f64` for `KLL`, string-only paths for `Elastic` and `Locher`).
- `Hydra<'a>` (`src/sketchbook/hydra.rs`)
  - Responsibilities: coordinates matrix of `Chapter` clones; `new`, `update`, `query_key` fan out label combinations and compute row medians.
  - Notes: relies on `hash_it` per row; any `Chapter` variant supporting `query` can participate.
- `ExponentialHistogram<'a>` (`src/sketchbook/eh.rs`)
  - Responsibilities: time-window coordinator with `new`, `update_window`, `update`, `cover`, `query_interval_merge`, `get_min_time`, `get_max_time`, `volume_count`, `print_buckets`, `get_memory_info`.
  - Notes: merges buckets once `k/2 + 2` same-sized volumes accumulate; prunes expired volumes on update.
- `EHVolume<'a>` (`src/sketchbook/eh.rs`)
  - Responsibilities: wraps `Chapter` snapshot with `size`, `min_time`, `max_time`; `to_merge` folds in compatible volumes.
  - Notes:

## Utility Re-Exports

- `hash_it`, `LASTSTATE`, `SEEDLIST`, `SketchInput` (`src/sketches/utils.rs`)
  - Description: re-exported from `common::input` for sketch modules.
- `iv_to_f64` (`src/sketches/utils.rs`)
  - Description: converts numeric `SketchInput` variants into `f64` for quantile sketches.
- new hash function: based on bits-requirement
  - maybe only 32 bit, maybe only 64 bit
  - 128 bit at most
    - 5 row: 25bit -> 2^24
    - 8 row: 16bit -> 2^15

low priority: fast range + range reduction to replace mod op

low priority: if SIMD native in rust, then add it

super low priority: multi thread sketch insertion
