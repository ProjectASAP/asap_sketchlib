# E2E Testing Harness & Conformance Kit

Every sketch in `asap_sketchlib` is validated end-to-end against exact ground
truth from seeded synthetic streams. New sketches **must** go through the
same conformance batteries before landing — copy an adapter from
`tests/conformance_kit.rs`, run the batteries, done.

## Layout

| Path | Purpose |
| --- | --- |
| `common/mod.rs` | Seeded stream generators (`zipf_u64`, `uniform_u64`, `normal_f64`, `exponential_f64`, adversarial `log_uniform_f64`), exact truth trackers (`FreqTruth`, `NumericTruth`), assertion helpers |
| `common/conformance.rs` | Capability traits + standard conformance batteries |
| `conformance_kit.rs` | Reference adapters: established sketches running through the kit (copy these) |
| `e2e_frequency.rs` / `e2e_cardinality.rs` / `e2e_quantiles.rs` / `e2e_frameworks.rs` | Deep per-family suites with hand-tuned tolerances |
| `e2e_heavy_hitters.rs` | CocoSketch and Elastic through the batteries, plus the heavy-hitter properties no battery models (`feature = "experimental"`) |
| `e2e_membership.rs` | Bloom: no false negative, exact union, and the delivered false-positive rate against its own sizing |
| `e2e_space_saving.rs` | Space-Saving: the error sandwich, the `min_count` ceiling, and the Stream-Summary lists under sustained eviction |
| `e2e_octo.rs` | OctoSketch delta-promotion invariants, the `octo-runtime` pipeline, and conformance to the paper's Theorems 1-4 and its sketch-merge baseline |
| `e2e_experimental.rs` | The remaining `feature = "experimental"` sketches: KMV, UniformSampling, EHUnivOptimized |
| `bug_verification.rs` | Regression tests for fixed wrong-query-results bugs |

## Onboarding a new sketch

1. **Pick the batteries that match what your sketch promises.**

   | Your sketch estimates… | Battery | Trait to impl |
   | --- | --- | --- |
   | Per-key frequency | `frequency_battery` | `FrequencyOps<K>` |
   | …never underestimates (CMS-style) | same, `one_sided: true` in `FrequencySpec` | — |
   | …with signed/turnstile updates | `turnstile_battery` | `SignedFrequencyOps<K>` |
   | Set membership | `membership_battery` | `MembershipOps<K>` |
   | Distinct count | `cardinality_battery` | `CardinalityOps` |
   | Quantiles of a numeric stream | `quantile_battery` | `QuantileOps` |
   | Mergeable shards | `merge_equivalence_battery` | add `MergeOps` |

2. **Write one adapter struct** implementing the trait(s) by delegating to
   your public API. See any adapter in `tests/conformance_kit.rs`.

3. **Run the battery** with production-shaped dimensions and tolerances from
   your sketch's documentation:

   ```rust
   conformance::quantile_battery(
       "MyNewSketch",
       || MyAdapter(MySketch::with_defaults()),
       &values,
       QuantileSpec::default(),
   )
   .assert_ok();
   ```

4. **Add deeper, sketch-specific checks** (merge semantics, windowing,
   serialization) as a test in the matching `e2e_*.rs` suite.

## Rules

- All randomness flows through seeded generators; no `rand::rng()` in tests.
- Ground truth is tracked exactly while generating — never re-derived from
  another approximation.
- Tolerances must be justified: theory bounds when they exist (α for DDS,
  rank error for KLL, ε·N for CMS), otherwise documented empirical values
  consistent with existing suites.
- Batteries accumulate failures into a `BatteryReport`; call `.assert_ok()`
  once at the end so a single run reports every violation, not just the first.

## Running

```bash
cargo test --test conformance_kit        # kit + reference adapters
cargo test --all-features               # everything, incl. e2e_experimental
cargo run --release --example accuracy_probe --features experimental
                                         # heavy release-only probes
```
