# Data-driven E2E Testing

Every sketch in `asap_sketchlib` is validated end-to-end against exact ground
truth from seeded synthetic streams, and every approximate answer is judged
against **its own family's** error bound.

Each family declares an explicit vector of data cases and an explicit vector
of sketch variants. Its matrix test evaluates their Cartesian product using a
fresh sketch and exact ground truth for every pair.

[`docs/e2e_coverage_matrix.md`](../docs/e2e_coverage_matrix.md) is the
authority on which public instance is covered by which test, under which bound,
and whether that bound is a theorem or a documented empirical band.

## Layout

| Path | Purpose |
| --- | --- |
| `common/streams.rs` | Seeded typed generators configured by `ZipfConfig`, `UniformConfig`, `NormalConfig` and `ExponentialConfig`, plus independent adversarial value transforms |
| `common/truth.rs` | `FreqTruth`, `NumericTruth`, `CardinalityTruth` and `MembershipTruth` exact calculators |
| `common/assertions.rs` | Shared assertion helpers |
| `common/specs/` | Error models split into frequency, quantile, cardinality, sampling and statistical modules |
| `common/storage.rs` | Concrete fixed storage types needed by the explicit variant vectors |
| `e2e_frequency.rs` / `e2e_cardinality.rs` / `e2e_quantiles.rs` | Deep per-family suites, each against its own theorem |
| `e2e_numeric_types.rs` | Every `NumericalValue` type through `KLL<T>`, `KLLDynamic<T>` and `DDSketch::add<T>` |
| `e2e_windows.rs` | Every `EHSketchList` variant in an `ExponentialHistogram`, and every `TumblingWindow` payload |
| `e2e_composition.rs` | `HashSketchEnsemble`, `UnivMonQ`'s config surface, and the portable sketch+heap facade |
| `e2e_nitro.rs` | Every Nitro ingestion path — row-level `CountMin`/`Count`, `NitroBatch::insert` / `insert_cached_step`, and the bare `Vector2D<u32>` target — under one sampling model, plus merge, seeding, saturation and serde/context continuation. **All Nitro E2E behaviour lives here**; the sampler's own structural tests are unit tests in `src/common/structure_utils.rs` and `src/sketch_framework/nitro.rs`, where the cursor and skip counter are visible without a public accessor |
| `e2e_frameworks.rs` | Hydra's subpopulation lattice and UnivMon composition |
| `e2e_heavy_hitters.rs` | Space-Saving's error sandwich, `min_count` ceiling and Stream-Summary lists under sustained eviction, plus CocoSketch and Elastic frequency/merge properties |
| `e2e_membership.rs` | Bloom: no false negative, exact union, and the delivered false-positive rate against its own sizing |
| `e2e_octo.rs` | OctoSketch delta-promotion invariants, the `octo-runtime` pipeline, and conformance to the paper's Theorems 1-4 and its sketch-merge baseline |
| `e2e_experimental.rs` | The remaining `feature = "experimental"` sketches: KMV, UniformSampling, EHUnivOptimized |
| `bug_verification.rs` | Regression tests for fixed wrong-query-results bugs |
| `spec_self_tests.rs` | The specs checking themselves — the median's bad-row threshold, the binomial tails it selects, the four-row counter-example behind it, and the simultaneous `kappa` search. Its own binary because a `#[test]` in `common/specs.rs` runs once per suite that says `mod common;` |

## Onboarding a new sketch

1. Add the concrete constructor to the matching E2E's `variant_cases` vector.

2. Add any new distribution configuration to its `data_cases` vector.

3. Assert the sketch's actual bound with the matching spec from
   `common/specs.rs`, evaluated at the parameters the instance reports:

   ```rust
   let spec = CountSketchSpec::new(sketch.rows(), sketch.cols());
   spec.assert_contract("MyNewSketch", &truth, |k| sketch.estimate(k), &context);
   ```

   If no spec fits, add one — one spec per metric, with the formula, the
   per-check failure probability, and the citation in its doc comment. Do not
   reuse another family's.

4. Add deeper, sketch-specific checks (merge semantics, windowing,
   serialization) as a test in the matching `e2e_*.rs` suite.

5. Add a row to `docs/e2e_coverage_matrix.md`.

## Rules

- All randomness flows through seeded generators; no `rand::rng()` in tests,
  and no sketch constructed by a wall-clock-seeded constructor. Stream seeds
  and sketch seeds are separate and both are pinned.
- Ground truth is tracked exactly while generating — never re-derived from
  another approximation.
- Tolerances are **computed from the instance's own parameters**, not written
  down. A tolerance that does not move with `k`, `w` or the precision is not
  that family's bound.
- Theory-backed tests are named `*_satisfies_<theorem_or_bound>`; measured ones
  are named `*_stays_within_the_documented_empirical_band` and record the
  configuration, seed and measured value they came from.
- Never widen a bound to make a failure pass. If the correct bound exposes a
  defect, keep the reproducing test and fix the implementation.
- Matrix checks use ordinary assertions and fail at the first mismatching
  variant/data pair, with both labels included in the failure message.

## Running

```bash
cargo test --test e2e_frequency         # explicit frequency variant × data matrix
cargo test --test e2e_numeric_types      # every NumericalValue type
cargo test --all-features               # everything, incl. e2e_experimental
cargo run --release --example accuracy_probe --features experimental
                                         # heavy release-only probes
```
