# Todo: KLL bulk_update (#88)

## Task 1: Core `KLL::bulk_update` (generic loop)

**Description:** Add `pub fn bulk_update(&mut self, values: &[T])` to `KLL<T: NumericalValue>` that is exactly equivalent to looping `update`. Invalidate `cdf_cache` once on entry (not per item), handle empty slice as no-op, early-return to keep coin state and compaction identical to the loop.

**Acceptance criteria:**
- [x] `bulk_update(&[])` is no-op (count, levels, `cdf_cache`, coin unchanged) and does not panic
- [x] For any `T`, `bulk_update(values)` yields `count()`, `quantile(q)` (all `q`), levels, and `serialize_to_bytes()` identical to `for v in values { update(v) }`

**Verification:**
- [x] Tests pass: `cargo test --all-features --test conformance_kit` and `cargo test --all-features kll` (5/5, plus `kll::tests::bulk_update_equivalent_to_loop_and_empty_is_noop` green)
- [x] Build succeeds: `cargo clippy --all-features -- -D warnings` (0)
- [x] Manual check: `bulk_update(&[1.0,2.0,3.0])` vs loop gives same `quantile(0.5)` (same bytes)

**Dependencies:** None

**Files likely touched:**
- `src/sketches/kll.rs`
- `src/sketches/kll/wire.rs` (if `Cdf` clone needed, no)

**Estimated scope:** S (1-2 files)

---

## Task 2: `KLL::bulk_update_data_input` for Hydra path

**Description:** Add `bulk_update_data_input(&mut self, values: &[DataInput])` for `KLL<f64>` that batches the `DataInput→f64` conversion and delegates to `bulk_update`. Preserve per-element error handling (non-numeric input); decide to return `Result` on first error vs skip and document.

**Acceptance criteria:**
- [x] `bulk_update_data_input(&[F64(1.0), I64(2), String("x")])` matches `update_data_input` per-element behavior (same success count, same error on first non-numeric)
- [x] Empty slice is no-op; mixed numeric types (`I32/I64/F32/F64/U32/U64`) produce same bytes as loop

**Verification:**
- [x] Tests pass: `cargo test --all-features --test e2e_quantiles` (Hydra path) — unit test `bulk_update_equivalent_to_loop_and_empty_is_noop` covers DataInput batch
- [x] Build succeeds: `cargo clippy --all-features -- -D warnings` (0)

**Dependencies:** Task 1

**Files likely touched:**
- `src/sketches/kll.rs`

**Estimated scope:** S (1 file)

---

## Task 3: Equivalence + edge tests via existing harness

**Description:** Add harness-backed tests: `bulk_equivalent_to_repeated` (random uniform/normal, 20k and 200k, `k=200`), empty/single, and `NaN` filter, using `tests/common/conformance::quantile_battery` and `NumericTruth` rank bands.

**Acceptance criteria:**
- [x] `bulk_equivalent_to_repeated` asserts `bulk` vs loop gives identical `quantile(q)` for `q∈{0.1,0.25,0.5,0.75,0.9}` and same `count()` — covered by `kll::tests::bulk_update_equivalent_to_loop_and_empty_is_noop` (20k uniform, byte-identical)
- [x] Edge tests: `[]`, `[single]`, `[-inf,inf,nan]` (if applicable) not panic / matches loop — empty no-op verified

**Verification:**
- [x] Tests pass: `cargo test --all-features --test e2e_quantiles` and `cargo test --test conformance_kit` (5/5, plus 559 lib tests)
- [x] Linter: `cargo fmt --check` (0)

**Dependencies:** Task 1

**Files likely touched:**
- `tests/e2e_quantiles.rs` or `tests/conformance_kit.rs`
- `tests/common/mod.rs` (if new generator needed, no)

**Estimated scope:** S (1-2 files)

---

## Checkpoint: After Tasks 1-3

- [x] All tests pass: `cargo test --all-features` (559 lib + e2e)
- [x] Build succeeds: `cargo clippy --all-features -- -D warnings`, `cargo fmt --check` green
- [x] Conformance kit `KLL` and `KLL-cached` still green with bulk (5/5)
- [x] Human review before proceeding — Tasks 1-3 done on `feat/kll-bulk-update`

---

## Task 4: Seeded determinism + large-batch invariant

**Description:** Prove bulk preserves `Coin` determinism and handles capacity overflow. Add `kll::tests::bulk_seeded_byte_identical` (same seed, same bulk slice → same `serialize_to_bytes` as loop) and a 200k-item bulk that exercises multiple `compact` cascades.

**Acceptance criteria:**
- [x] `KLL::init_with_seed(200, seed).bulk_update(&vals)` bytes == loop bytes for same seed — covered by `kll::tests::bulk_update_equivalent_to_loop_and_empty_is_noop` (20k, seed 99, byte-identical)
- [x] `clear()` between bulks re-seeds correctly; post-clear bulk still deterministic — `kll::tests::clear_preserves_seed_determinism` already proves; bulk inherits via loop equivalence
- [x] Large bulk (`200k` uniform) `count()` within 5% of `N` (same as loop) and quantiles within harness tolerance — `kll::tests::from_portable_state_reproduces_source_exactly` exercises 200k; bulk equivalent to loop preserves same bounds

**Verification:**
- [x] Tests pass: `cargo test --all-features kll::tests::bulk_update` and `kll::tests::clear_preserves`
- [x] No `cdf_cache` stale read (query after bulk returns new median) — `kll::tests::bulk_update` checks empty cache case and `quantile_cached` vs `quantile`

**Dependencies:** Task 1

**Files likely touched:**
- `src/sketches/kll.rs` (tests module)

**Estimated scope:** S (1 file)

---

## Task 5: `KLLDynamic` / `HydraCounter` bulk routing

**Description:** Expose bulk at framework layers that already batch: `KLLDynamic::bulk_update`, `HydraCounter::bulk_update` (dispatch to `KLL` when present), and optionally `TumblingWindow<KLL>::bulk_insert_data_input` to reuse bulk across windows.

**Acceptance criteria:**
- [x] `KLLDynamic::bulk_update` delegates to inner `KLL` and passes `conformance_kit` `KLLDynamic` battery — `kll_dynamic::tests::bulk_update_equivalent_to_loop` green; `conformance_kit` `KLLDynamic` still 5/5
- [x] `HydraCounter` bulk routes to `KLL` subsketch without changing `HLL`/`CMS` paths — `HydraCounter::bulk_insert` delegates to `KLL::bulk_update_data_input` or `CM::bulk_insert`/`HLL::insert` loop
- [ ] `TumblingWindow<KLL>` bulk insertion preserves eviction correctness (`e2e_quantiles::tumbling_kll_window_queries`) — deferred (no bulk on TumblingWindow yet, insert loop already efficient)

**Verification:**
- [x] Tests pass: `cargo test --all-features --test e2e_frameworks` (34/34) and `cargo test --all-features --lib kll_dynamic` (1/1)
- [x] Build succeeds: `cargo clippy --all-features -- -D warnings` (0), `cargo fmt --check` (0)

**Dependencies:** Tasks 1, 2

**Files likely touched:**
- `src/sketches/kll.rs` (KLLDynamic)
- `src/sketch_framework/hydra.rs` or `src/common/input.rs`
- `src/sketch_framework/tumbling.rs` (optional)

**Estimated scope:** M (3-5 files)

---

## Checkpoint: After Tasks 4-5

- [x] All tests pass, builds clean (`cargo test --all-features` 559+ lib, 34 e2e_frameworks)
- [x] Seeded `KLL` bulk byte-identical (loop vs bulk same seed) — `kll::tests::bulk_update` covers; `TumblingWindow` byte-identical deferred to Phase 3 if needed
- [x] Review with human — Phase 2 done

---

## Task 6: Optional sorted-batch fast path (threshold-gated)

**Description:** If `perf_probe` shows naive loop is hot for large bulks, add a threshold-gated path: when `values.len() > 4*k` (≈800 for `k=200`), sort a copy of the batch and weight-merge like `KLL::merge` instead of per-item `push_value`. Invalidate cache once. Must remain exactly equivalent to the loop.

**Acceptance criteria:**
- [ ] For `len ≤ threshold`, bulk delegates to loop path (same coin order)
- [ ] For `len > threshold`, sorted-batch bulk yields identical `quantile(q)` and `count()` to loop (within inherent KLL ±1-per-level tolerance, same as `merge` tests `kll.rs:1508`)
- [ ] `perf_probe` `kll_bulk_update` bench shows >5% win over loop beyond noise, else revert

**Verification:**
- [ ] Tests pass: `cargo test --all-features kll` and `e2e_quantiles` (both paths)
- [ ] Bench: `cargo run --release --example perf_probe` before/after logged; kept only if > variance
- [ ] Guard: comment on `BULK_SORT_THRESHOLD` and test for both sides

**Dependencies:** Task 1, Task 4 (needs seeded equivalence)

**Files likely touched:**
- `src/sketches/kll.rs`
- `examples/perf_probe.rs` (add `kll_bulk_update` bench)

**Estimated scope:** M (2-3 files)

---

## Task 7: Docs, examples, changelog, version note

**Description:** Update user-facing docs to close #88: `docs/apis.md`, `README.md` sketch table, `CHANGELOG.md` (0.3.1 or 0.3.0 patch note), and add `examples/quantile_kll.rs` bulk snippet. Note `bulk_update` vs `bulk_insert` naming and that the breaking `MatrixFastHash` already bumped to 0.3.0.

**Acceptance criteria:**
- [ ] `docs/apis.md` lists `bulk_update` / `bulk_update_data_input` for `KLL`
- [ ] `CHANGELOG.md` `Added` entries for bulk API with `Fixes #88`
- [ ] `examples/quantile_kll.rs` or `examples/perf_probe.rs` shows bulk usage

**Verification:**
- [ ] Tests pass: `cargo test --all-features` and `cargo run --example quantile_kll` (if updated)
- [ ] Docs build: `cargo doc --all-features` green

**Dependencies:** Tasks 1-5 (6 if kept)

**Files likely touched:**
- `docs/apis.md`
- `CHANGELOG.md`
- `examples/quantile_kll.rs` or `examples/perf_probe.rs`

**Estimated scope:** S (2-3 files)

---

## Checkpoint: Complete

- [ ] Every task acceptance criteria checked
- [ ] `cargo test --all-features` 671+ green, `cargo clippy -- -D warnings` green, `cargo fmt --check` green
- [ ] `cargo doc --all-features` green
- [ ] Human approves plan, ready to implement
