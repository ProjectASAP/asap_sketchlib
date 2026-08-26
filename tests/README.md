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
| `e2e_experimental.rs` | Same for `feature = "experimental"` sketches |
| `bug_verification.rs` | Regression tests for fixed wrong-query-results bugs |

## Onboarding a new sketch

1. **Pick the batteries that match what your sketch promises.**

   | Your sketch estimates… | Battery | Trait to impl |
   | --- | --- | --- |
   | Per-key frequency | `frequency_battery` | `FrequencyOps<K>` |
   | …never underestimates (CMS-style) | same, `one_sided: true` in `FrequencySpec` | — |
   | …with signed/turnstile updates | `turnstile_battery` | `SignedFrequencyOps<K>` |
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
