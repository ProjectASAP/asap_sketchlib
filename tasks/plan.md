# Implementation Plan: KLL bulk_update API (#88)

## Overview
Add `bulk_update` to `KLL` so callers can ingest a slice of values in one call instead of looping `update(&T)` one at a time. First cut is a correct, deterministic loop that invalidates the memoized CDF once and preserves exact `update`-equivalent semantics (counts, levels, coin state, bytes). A second, optional optimized path sorts the batch and weight-merges level-by-level when the batch is large enough to amortize the sort; otherwise we keep the simple loop. Work is vertical: each task ships a usable API + harness-backed verification.

## Architecture Decisions
- **Naming:** `bulk_update(&mut self, values: &[T])` for `KLL<T: NumericalValue>` and `bulk_update_data_input(&mut self, values: &[DataInput])` for `KLL<f64>` Hydra path, matching existing `CountMin::bulk_insert(&[DataInput])` vs `KLL::update(&T)` naming (issue says `bulk_update`).
- **Semantics:** `bulk_update` must be *exactly* equivalent to `for v in values { update(v) }` for correctness, determinism, and existing error bounds. No new compaction logic in Phase 1; optimization in Phase 3 is gated and must also be equivalence-proved.
- **Cache invalidation:** `push_value` currently drops `cdf_cache` per item (`kll.rs:395`). Bulk must drop once at entry and not per item in the fast path, but retain per-compaction drops in `compact:434` as cheap insurance. `clear/merge/ensure_levels_sorted` already drop.
- **Determinism:** Seeded `KLL::init_with_seed(k, seed)` consumes `Coin::toss` bits in insertion order. Bulk must consume bits in the same order as the loop replay, so seeded sketches remain byte-identical to the repeated loop.
- **Generic placement:** Impl on `KLL<T: NumericalValue>` so `KLL<f64>` and `KLL<i64>` both benefit; `KLLDynamic` and `HydraCounter::KLL` reuse via delegation, not new storage.
- **No new dependencies:** Pure-Rust, no `protoc` requirement (per `Cargo.toml:38` note).

## Task List

### Phase 1: Foundation — correct, loop-based bulk

#### Task 1: Core `KLL::bulk_update` (generic loop)
Deliver a usable API.

#### Task 2: `KLL::bulk_update_data_input` for Hydra path
Wire `DataInput::F64/F32/I32/...` batch through the numeric bulk.

#### Task 3: Equivalence + edge tests via existing harness
Prove `bulk_update` ≡ repeated `update` on the harness.

### Checkpoint: Foundation
- [ ] `cargo test --test conformance_kit` + `cargo test --test e2e_quantiles` green
- [ ] `cargo clippy -- -D warnings` green, `cargo fmt --check` green

### Phase 2: Core — determinism and integration

#### Task 4: Seeded determinism + large-batch invariant
Prove coin-state and capacity handling under bulk.

#### Task 5: `KLLDynamic` / `HydraCounter` bulk routing
Expose bulk at the framework layer where `TumblingWindow<KLL>` and Hydra already batch.

### Checkpoint: Core
- [ ] Seeded byte-identical repro passes (Hydra `TumblingWindow` rotation)
- [ ] No harness regression (`e2e_quantiles`, `e2e_frameworks`)

### Phase 3: Polish — optional optimization, docs, bench

#### Task 6: Optional sorted-batch fast path (threshold-gated)
Only if the naive loop shows measurable win on `perf_probe`.

#### Task 7: Docs, examples, changelog, version note
Close #88 with user-facing docs.

### Checkpoint: Complete
- [ ] All acceptance criteria met, `cargo test --all-features` 671+ green
- [ ] `examples/perf_probe.rs` before/after logged, no neutral keep
- [ ] Ready for review

## Risks and Mitigations
| Risk | Impact | Mitigation |
|------|--------|------------|
| Stale CDF after bulk (forgot to invalidate) | High — silent wrong quantiles | Invalidate once on entry + keep existing per-compaction drops; add `cdf_cache_invalidated_by_bulk` test and `conformance_kit` KLL-cached battery |
| Seeded determinism drift (different toss order) | High — breaks `TumblingWindow` byte-identical across hosts | Bulk must call `push_value` in order; add seeded byte-identical test vs loop (same seed, same input slice) |
| Level overflow on huge bulk (exceeds `max_capacity` in one call) | Med — panic or silent mis-compact | Reuse existing `compress_while_updating` per item; add 200k-item bulk test; optimized path will use the same cascade as `merge` |
| API name mismatch (`bulk_update` vs `bulk_insert`) | Low — docs churn | Match `KLL::update` verb and note alias in `docs/apis.md`; keep `bulk_insert` as deprecated alias if needed |
| Performance regression vs loop (extra allocation) | Med | Benchmark `perf_probe` before/after; only keep optimization if it beats variance (>5%) |

## Open Questions
- Bulk size threshold for the sorted optimization? Start with `values.len() > 4*k` (≈800 for `k=200`) and measure; make it `const BULK_SORT_THRESHOLD: usize` so it can be tuned.
- Should `bulk_update` return `Result<(), _>` for non-numeric `DataInput` batch, or silently skip like `update_data_input` does per element? Proposal: `bulk_update_data_input` returns `Result` and stops on first non-numeric, matching `update_data_input` per-element error semantics — needs human confirm.
- Hydra subpopulation bulk: `HydraKllSketch::bulk_update(key, &[f64])` vs pushing per value through `Hydra::insert`? Confirm whether Hydra should expose bulk at the `HydraCounter` layer or at `Hydra::bulk_insert(&[(key, value)])`.

## Parallelization
- Tasks 1 and 2 are sequential (share `kll.rs` impl block).
- Task 3 tests can be written in parallel once Task 1 lands.
- Task 5 (`KLLDynamic`/Hydra) can parallelize with Task 4 after Task 1.
- Task 7 docs can parallelize with Task 6 bench.

## References
- Issue #88: `KLL` lacks `bulk_update`; only `update(&T)` exists.
- Existing bulk patterns: `src/sketches/countminsketch.rs:295` `bulk_insert`, `src/common/structures/matrix_storage.rs:214` row-major negative result.
- KLL impl: `src/sketches/kll.rs:394` `push_value`, `kll.rs:430` `compact`, `kll.rs:605` `merge` (weight-preserving), `kll.rs:743` `clear`, `kll.rs:878` `ensure_levels_sorted`, `kll.rs:508` `invalidate_cdf_cache`.
- Harness: `tests/common/conformance.rs:348` `quantile_battery`, `tests/conformance_kit.rs:330` `KLL-cached`, `tests/e2e_quantiles.rs`, `examples/perf_probe.rs`.
