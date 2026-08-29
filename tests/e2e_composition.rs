//! Composition layers: `HashSketchEnsemble`, `NitroBatch`, `UnivMonQ`'s
//! configuration surface, and the portable facade types that pair a sketch
//! with a heap.
//!
//! The common thread is that none of these change what a sketch guarantees —
//! they change how it is fed. So each is checked against a **standalone
//! reference** built from the same stream, plus the underlying sketch's own
//! error metric. A composition layer that quietly altered state would show up
//! as a divergence from the reference, not as a widened tolerance.

mod common;

use common::specs::{
    CardinalityConfidenceSpec, CountMinSpec, CountSketchSpec, RankErrorSpec,
    SamplingConfidenceSpec, Tally,
};
use common::{FreqTruth, NumericTruth, uniform_u64, zipf_u64};

use asap_sketchlib::{
    Classic, Count, CountMin, CountMinSketchWithHeap, CountSketchWithHeap, DataInput,
    EnsembleSketch, ErtlMLE, FastPath, HashSketchEnsemble, HyperLogLog, HyperLogLogHIP, KllSketch,
    MessagePackCodec, NitroBatch, UnivMonQ, UnivMonQConfig, Vector2D,
};

const ROWS: usize = 3;
const COLS: usize = 4_096;
const N: usize = 40_000;
const DOMAIN: usize = 2_048;
const STREAM_SEED: u64 = 0xC090_5101;

// ------------------------------------------------------ HashSketchEnsemble

/// The ensemble's whole purpose is to compute one hash and hand it to several
/// sketches, so every member is compared against a standalone sketch fed the
/// same stream through its own `insert`.
///
/// The two kinds of member have different obligations, and conflating them
/// would make this test either vacuous or wrong:
///
/// - **matrix members** (`CountMinFast`, `CountFast`) receive exactly the hash
///   their own fast path would have computed, so they must match a standalone
///   sketch *exactly*, key for key;
/// - **HLL members** receive the low 64 bits of that shared matrix hash, not
///   the canonical seed `HyperLogLog::insert` uses. Two different hash
///   functions land the same stream in different registers, so the ensemble's
///   readings are equally accurate but not equal. They are held to their own
///   cardinality bands, and to agreeing with the standalone readings within
///   the two estimators' combined band.
#[test]
fn ensemble_members_match_standalone_sketches_fed_the_same_stream() {
    let stream = zipf_u64(N, DOMAIN, 1.1, STREAM_SEED);

    let mut ens: HashSketchEnsemble = HashSketchEnsemble::new(vec![
        EnsembleSketch::from(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
            ROWS, COLS,
        )),
        EnsembleSketch::from(Count::<Vector2D<i32>, FastPath>::with_dimensions(
            ROWS, COLS,
        )),
        EnsembleSketch::from(HyperLogLog::<ErtlMLE>::new()),
        EnsembleSketch::from(HyperLogLog::<Classic>::new()),
        EnsembleSketch::from(HyperLogLogHIP::new()),
    ])
    .expect("all matrix members share dimensions");

    let mut ref_cm = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(ROWS, COLS);
    let mut ref_cs = Count::<Vector2D<i32>, FastPath>::with_dimensions(ROWS, COLS);
    let mut ref_ertl = HyperLogLog::<ErtlMLE>::new();
    let mut ref_classic = HyperLogLog::<Classic>::new();
    let mut ref_hip = HyperLogLogHIP::new();

    let mut truth = FreqTruth::default();
    for k in &stream {
        let d = DataInput::U64(*k);
        truth.observe(*k as i64);
        ens.insert(&d);
        ref_cm.insert(&d);
        ref_cs.insert(&d);
        ref_ertl.insert(&d);
        ref_classic.insert(&d);
        ref_hip.insert(&d);
    }

    let context = format!(
        "rows={ROWS} cols={COLS} zipf(1.1) domain={DOMAIN} n={N} seed={STREAM_SEED:#x}, \
         members: CountMinFast, CountFast, HllErtl, HllClassic, HllHip"
    );

    // Matrix members: identical estimates, key for key.
    let mut cm_tally = Tally::default();
    let mut cs_tally = Tally::default();
    for (k, _) in truth.pairs() {
        let d = DataInput::U64(k as u64);
        let a = ens.estimate(0, &d).expect("CountMinFast cell");
        let b = ref_cm.estimate(&d) as f64;
        cm_tally.record(a == b, || format!("key {k}: ensemble {a} standalone {b}"));

        let c = ens.estimate(1, &d).expect("CountFast cell");
        let e = ref_cs.estimate(&d);
        cs_tally.record(c == e, || format!("key {k}: ensemble {c} standalone {e}"));
    }
    cm_tally.assert_none("ensemble CountMinFast vs standalone", &context);
    cs_tally.assert_none("ensemble CountFast vs standalone", &context);

    // HLL members are deliberately *not* bit-identical to a standalone HLL.
    // The ensemble feeds them the low 64 bits of the shared **matrix** hash
    // (seed index 0), while `HyperLogLog::insert` hashes with the canonical
    // seed index. Two different hash functions land the same stream in
    // different registers, so the two are equally accurate rather than equal —
    // which is the point of sharing a hash, not a defect.
    //
    // What must hold is that the ensemble's readings sit in the same
    // cardinality band as the standalone ones, and agree with them to within
    // the sum of their sampling errors.
    let distinct = truth.distinct();
    let register_spec = CardinalityConfidenceSpec::hll(14, 4.0);
    let hip_spec = CardinalityConfidenceSpec::hll_hip(14, 4.0);
    let mut card_tally = Tally::default();
    for (idx, label, reference, spec) in [
        (2usize, "HllErtl", ref_ertl.estimate() as f64, register_spec),
        (
            3,
            "HllClassic",
            ref_classic.estimate() as f64,
            register_spec,
        ),
        (4, "HllHip", ref_hip.estimate() as f64, hip_spec),
    ] {
        let got = ens.cardinality(idx).expect("hll cell");
        spec.tally_into(&mut card_tally, got, distinct);
        spec.tally_into(&mut card_tally, reference, distinct);
        // Both estimate the same truth, so they cannot be further apart than
        // their two bands allow.
        let gap = (got - reference).abs() / distinct as f64;
        assert!(
            gap <= 2.0 * spec.tolerance(),
            "ensemble {label} reads {got} while the standalone reads {reference}; the gap \
             {gap:.5} exceeds the two estimators' combined band {:.5}. They use different \
             hashes, so they need not be equal — but they must agree this closely. {context}",
            2.0 * spec.tolerance()
        );
    }
    card_tally.assert_within(
        "ensemble and standalone HLL members / cardinality bands",
        register_spec.per_check_failure(),
        &format!("{context}, distinct={distinct}"),
    );
}

/// Every member still satisfies its own family's bound after riding the shared
/// hash path. `CountFast` gets the L2 bound, not Count-Min's; the HLL members
/// get their register models.
#[test]
fn ensemble_members_satisfy_their_own_error_bounds() {
    let stream = zipf_u64(N, DOMAIN, 1.1, STREAM_SEED);
    let mut ens: HashSketchEnsemble = HashSketchEnsemble::new(vec![
        EnsembleSketch::from(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
            ROWS, COLS,
        )),
        EnsembleSketch::from(Count::<Vector2D<i32>, FastPath>::with_dimensions(
            ROWS, COLS,
        )),
        EnsembleSketch::from(HyperLogLog::<ErtlMLE>::new()),
        EnsembleSketch::from(HyperLogLog::<Classic>::new()),
        EnsembleSketch::from(HyperLogLogHIP::new()),
    ])
    .expect("ensemble");

    let mut truth = FreqTruth::default();
    let mut distinct = std::collections::HashSet::new();
    for k in &stream {
        ens.insert(&DataInput::U64(*k));
        truth.observe(*k as i64);
        distinct.insert(*k);
    }

    let context =
        format!("rows={ROWS} cols={COLS} zipf(1.1) domain={DOMAIN} n={N} seed={STREAM_SEED:#x}");
    CountMinSpec::new(ROWS, COLS).assert_contract(
        "ensemble CountMinFast",
        &truth,
        |k| ens.estimate(0, &DataInput::U64(k as u64)).expect("cm"),
        &context,
    );
    CountSketchSpec::new(ROWS, COLS).assert_contract(
        "ensemble CountFast",
        &truth,
        |k| ens.estimate(1, &DataInput::U64(k as u64)).expect("cs"),
        &context,
    );

    let mut card_tally = Tally::default();
    let register_spec = CardinalityConfidenceSpec::hll(14, 4.0);
    let hip_spec = CardinalityConfidenceSpec::hll_hip(14, 4.0);
    register_spec.tally_into(&mut card_tally, ens.cardinality(2).unwrap(), distinct.len());
    register_spec.tally_into(&mut card_tally, ens.cardinality(3).unwrap(), distinct.len());
    hip_spec.tally_into(&mut card_tally, ens.cardinality(4).unwrap(), distinct.len());
    card_tally.assert_within(
        "ensemble HLL members / cardinality bands",
        register_spec.per_check_failure(),
        &format!("{context}, distinct={}", distinct.len()),
    );
}

/// Compatibility inside an ensemble is by **hash layout**, not by literal
/// dimensions.
///
/// The shared hash is `hash_for_matrix_seeded_with_mode(0, mode, rows, input)`,
/// where `mode` is chosen from `rows * mask_bits(cols)`: it decides whether the
/// row hashes are packed into one `u64`, one `u128`, or one value per row. Two
/// sketches with the same `(mode, rows)` can share that hash even at different
/// widths, because each one folds the row bits with its *own* `cols`.
///
/// So a 4096-column and a 2048-column member coexist correctly, and what must
/// be rejected is a member whose row count — or whose width pushes it into a
/// different packing mode — makes the shared hash unusable for it.
#[test]
fn ensemble_composes_by_hash_layout_and_rejects_incompatible_members() {
    // Same rows, same packing mode, *different* widths: compatible, and each
    // member must answer correctly at its own width.
    let mut mixed = HashSketchEnsemble::<asap_sketchlib::DefaultXxHasher>::new(vec![
        EnsembleSketch::from(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
            ROWS, COLS,
        )),
        EnsembleSketch::from(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
            ROWS,
            COLS / 2,
        )),
        EnsembleSketch::from(Count::<Vector2D<i32>, FastPath>::with_dimensions(
            ROWS, COLS,
        )),
    ])
    .expect("members sharing a hash layout must be accepted even at different widths");
    assert_eq!(mixed.len(), 3);

    let stream = zipf_u64(N, DOMAIN, 1.1, STREAM_SEED);
    let mut truth = FreqTruth::default();
    for k in &stream {
        mixed.insert(&DataInput::U64(*k));
        truth.observe(*k as i64);
    }
    let context = format!("zipf(1.1) domain={DOMAIN} n={N} seed={STREAM_SEED:#x}");
    CountMinSpec::new(ROWS, COLS).assert_contract(
        "ensemble member at full width",
        &truth,
        |k| mixed.estimate(0, &DataInput::U64(k as u64)).expect("cm"),
        &context,
    );
    // The half-width member is judged at *its* width; folding must use each
    // sketch's own `cols`, not the ensemble's first member's.
    CountMinSpec::new(ROWS, COLS / 2).assert_contract(
        "ensemble member at half width",
        &truth,
        |k| mixed.estimate(1, &DataInput::U64(k as u64)).expect("cm"),
        &context,
    );
    CountSketchSpec::new(ROWS, COLS).assert_contract(
        "ensemble Count member",
        &truth,
        |k| mixed.estimate(2, &DataInput::U64(k as u64)).expect("cs"),
        &context,
    );

    // A different row count is a different hash layout: rejected, at
    // construction and on push.
    assert!(
        HashSketchEnsemble::<asap_sketchlib::DefaultXxHasher>::new(vec![
            EnsembleSketch::from(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                ROWS, COLS
            )),
            EnsembleSketch::from(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                ROWS + 2,
                COLS
            )),
        ])
        .is_err(),
        "members with different row counts must be rejected"
    );
    let before = mixed.len();
    assert!(
        mixed
            .push(EnsembleSketch::from(
                Count::<Vector2D<i32>, FastPath>::with_dimensions(ROWS + 2, COLS)
            ))
            .is_err(),
        "push must reject a member with a different row count"
    );
    assert_eq!(
        mixed.len(),
        before,
        "a rejected push must not add the sketch"
    );

    // A width that pushes the same row count into a wider packing mode is also
    // incompatible. The layout reserves `mask_bits(cols) + 1` bits per row (the
    // extra bit is Count Sketch's sign), so at 5 rows: 1024 columns needs
    // 5 * 11 = 55 bits and packs into a `u64`, while 4096 columns needs
    // 5 * 13 = 65 and spills into a `u128`.
    assert!(
        HashSketchEnsemble::<asap_sketchlib::DefaultXxHasher>::new(vec![
            EnsembleSketch::from(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                5, 1024
            )),
            EnsembleSketch::from(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                5, 4096
            )),
        ])
        .is_err(),
        "members whose widths select different packing modes must be rejected"
    );
    // ...while two widths on the same side of that boundary compose fine.
    assert!(
        HashSketchEnsemble::<asap_sketchlib::DefaultXxHasher>::new(vec![
            EnsembleSketch::from(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(5, 512)),
            EnsembleSketch::from(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                5, 1024
            )),
        ])
        .is_ok(),
        "two widths that select the same packing mode must compose"
    );

    // HLL members carry no matrix dimensions, so they compose with any grid.
    assert!(
        mixed
            .push(EnsembleSketch::from(HyperLogLogHIP::new()))
            .is_ok(),
        "an HLL member has no dimensions to clash with"
    );
}

// ------------------------------------------------------------------- Nitro

/// Sampling rates covered. `1.0` is the degenerate full-sampling case, where
/// the band collapses to zero width and the estimate must be exact.
const NITRO_RATES: [f64; 4] = [1.0, 0.5, 0.1, 0.01];
/// Fixed sampling-RNG seeds. `NitroBatch::with_target` seeds from the OS, so
/// an accuracy assertion on one would be re-rolled every run; the seeded
/// constructor is what makes these reproducible.
const NITRO_SEEDS: [u64; 4] = [0x0117_0001, 0x0117_0002, 0x0117_0003, 0x0117_0004];
/// `z = 4` on the binomial sampling band: two-sided failure 6.3e-5 per check.
const NITRO_Z: f64 = 4.0;

/// Nitro admits each update with probability `p` and writes weight `ceil(1/p)`,
/// so a key of true count `f` is estimated by `Binomial(f, p) * ceil(1/p)` —
/// unbiased, with standard deviation `sqrt(f (1-p) / p)`. The accepted band is
/// `z` of those, computed per rate rather than a flat percentage: at `f =
/// 100_000` it is +-0.4% at `p = 0.5` but +-12.6% at `p = 0.01`, and a single
/// fixed +-5% would be simultaneously far too loose for one and impossible for
/// the other.
#[test]
fn nitro_estimates_stay_inside_the_binomial_sampling_band_on_every_target() {
    const COUNT: i64 = 100_000;
    let key = 42i64;
    let data = vec![key; COUNT as usize];

    for &rate in &NITRO_RATES {
        let spec = SamplingConfidenceSpec::new(rate, NITRO_Z);
        for &seed in &NITRO_SEEDS {
            // Count-Min target. Only one key is present, so the sketch itself
            // is exact and the whole error budget is sampling noise.
            let mut cm = NitroBatch::with_target_and_seed(
                rate,
                CountMin::<Vector2D<i32>, FastPath>::with_dimensions(5, 2048),
                seed,
            );
            cm.insert(&data);
            let est = cm.estimate_median(&DataInput::I64(key));
            if let Err(detail) = spec.check(est, COUNT as f64, 0.0) {
                panic!("NitroBatch<CountMin> seed={seed:#x}: {detail}");
            }

            // Count Sketch target: same sampling model, signed estimator.
            let mut cs = NitroBatch::with_target_and_seed(
                rate,
                Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 2048),
                seed,
            );
            cs.insert(&data);
            let cs_est = cs.estimate_median(&DataInput::I64(key));
            if let Err(detail) = spec.check(cs_est, COUNT as f64, 0.0) {
                panic!("NitroBatch<Count> seed={seed:#x}: {detail}");
            }
        }
    }
}

/// Full sampling is not approximate: at `rate = 1.0` every update is admitted
/// with weight 1, so a single-key stream must come back exactly.
#[test]
fn nitro_at_full_sampling_is_exact() {
    let data = vec![7i64; 50_000];
    let mut cm = NitroBatch::with_target_and_seed(
        1.0,
        CountMin::<Vector2D<i32>, FastPath>::with_dimensions(5, 2048),
        NITRO_SEEDS[0],
    );
    cm.insert(&data);
    assert_eq!(
        cm.estimate_median(&DataInput::I64(7)),
        50_000.0,
        "rate=1.0 must admit every update with unit weight"
    );
}

/// Two runs with the same seed must be bit-identical, and two runs with
/// different seeds must differ — otherwise the seed is not reaching the
/// sampling RNG and the "deterministic" constructor is decorative.
#[test]
fn nitro_sampling_is_reproducible_from_its_seed() {
    let data = vec![5i64; 20_000];
    let run = |seed: u64| {
        let mut n = NitroBatch::with_target_and_seed(
            0.1,
            CountMin::<Vector2D<i32>, FastPath>::with_dimensions(5, 2048),
            seed,
        );
        n.insert(&data);
        n.estimate_median(&DataInput::I64(5))
    };
    assert_eq!(
        run(NITRO_SEEDS[0]),
        run(NITRO_SEEDS[0]),
        "the same seed must produce the same admitted subset"
    );
    let distinct: std::collections::HashSet<u64> =
        NITRO_SEEDS.iter().map(|s| run(*s).to_bits()).collect();
    assert!(
        distinct.len() > 1,
        "four different seeds all produced the same estimate; the seed is not \
         reaching the sampling RNG"
    );
}

/// `NitroBatch<Vector2D<u32>>` — the bare-storage target reached by
/// `init_nitro` — has **no public query path**: `NitroEstimate` is implemented
/// only for the `Vector2D<i32>`-backed Count-Min and Count Sketch, and
/// `CountMin::estimate` cannot be instantiated over a `u32` counter because it
/// needs `Counter: From<i32>`. So there is no per-key estimate to check here,
/// and this test does not invent one by re-deriving the fast-path hash: doing
/// exactly that is how the Nitro estimator once shipped broken while its tests
/// passed.
///
/// What *is* publicly observable, and is what Nitro actually controls, is the
/// total admitted mass. Every admitted update writes `ceil(1/rate)` into one
/// cell of every row, so the sum over any single row is
/// `admitted * ceil(1/rate)` — an unbiased estimate of the stream length with
/// the same binomial sampling band, computed without touching a hash.
#[test]
fn nitro_over_a_bare_vector2d_target_admits_mass_inside_the_sampling_band() {
    const COUNT: i64 = 100_000;
    let data = vec![11i64; COUNT as usize];

    for &rate in &NITRO_RATES {
        let spec = SamplingConfidenceSpec::new(rate, NITRO_Z);
        for &seed in &NITRO_SEEDS {
            let mut nitro = NitroBatch::init_nitro_with_seed(rate, seed);
            nitro.insert(&data);
            let target = nitro.target();
            let row0_mass: f64 = (0..target.cols())
                .map(|c| target.query_one_counter(0, c) as f64)
                .sum();
            if let Err(detail) = spec.check(row0_mass, COUNT as f64, 0.0) {
                panic!(
                    "NitroBatch<Vector2D<u32>> rate={rate} seed={seed:#x}: row-0 admitted \
                     mass out of band: {detail}"
                );
            }
            // Every row receives the same weight from every admitted update,
            // so all rows must carry identical total mass.
            for r in 1..target.rows() {
                let mass: f64 = (0..target.cols())
                    .map(|c| target.query_one_counter(r, c) as f64)
                    .sum();
                assert_eq!(
                    mass, row0_mass,
                    "row {r} carries {mass} but row 0 carries {row0_mass}; every row must \
                     receive each admitted update (rate={rate} seed={seed:#x})"
                );
            }
        }
    }
}

/// Merging two same-rate Nitro batches sums their admitted mass, so a key
/// split across both must come back at the combined band.
#[test]
fn nitro_merge_sums_admitted_mass_at_the_combined_band() {
    const HALF: i64 = 50_000;
    let key = 3i64;
    for &rate in &[0.5f64, 0.1] {
        let spec = SamplingConfidenceSpec::new(rate, NITRO_Z);
        let mut a = NitroBatch::with_target_and_seed(
            rate,
            CountMin::<Vector2D<i32>, FastPath>::with_dimensions(5, 2048),
            NITRO_SEEDS[0],
        );
        let mut b = NitroBatch::with_target_and_seed(
            rate,
            CountMin::<Vector2D<i32>, FastPath>::with_dimensions(5, 2048),
            NITRO_SEEDS[1],
        );
        a.insert(&vec![key; HALF as usize]);
        b.insert(&vec![key; HALF as usize]);
        a.merge(&b);
        let est = a.estimate_median(&DataInput::I64(key));
        if let Err(detail) = spec.check(est, (HALF * 2) as f64, 0.0) {
            panic!("NitroBatch merge rate={rate}: {detail}");
        }
    }
}

/// Weight saturation: the scaled increment is clamped into the counter's
/// domain rather than wrapping into a decrement.
#[test]
fn nitro_saturates_oversized_weights_instead_of_wrapping() {
    assert_eq!(
        asap_sketchlib::nitro_delta_saturated_i32(u64::MAX),
        i32::MAX,
        "an oversized weight must clamp to i32::MAX, not wrap negative"
    );
    assert_eq!(
        asap_sketchlib::nitro_delta_saturated_u32(u64::MAX),
        u32::MAX,
        "an oversized weight must clamp to u32::MAX"
    );
    assert_eq!(asap_sketchlib::nitro_delta_saturated_i32(7), 7);
    assert_eq!(asap_sketchlib::nitro_delta_saturated_u32(7), 7);

    // A rate low enough to push `ceil(1/rate)` past `i32::MAX` must still
    // leave counters non-negative.
    let mut tiny = NitroBatch::with_target_and_seed(
        1e-9,
        CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 64),
        NITRO_SEEDS[0],
    );
    tiny.insert(&vec![1i64; 4_000]);
    let est = tiny.estimate_median(&DataInput::I64(1));
    assert!(
        est >= 0.0,
        "a saturating weight must not turn Count-Min counters negative; got {est}"
    );
}

// ---------------------------------------------------------------- UnivMonQ

/// The configuration surface: each field must produce a working sketch whose
/// exact aggregates stay exact, and the ones that gate a feature must gate it.
#[test]
fn univmonq_configuration_variants_all_build_and_keep_exact_aggregates() {
    let values: Vec<f64> = uniform_u64(20_000, 100_000, STREAM_SEED)
        .into_iter()
        .map(|v| v as f64)
        .collect();
    let truth = NumericTruth::new(values.clone());

    let base = UnivMonQConfig::default();
    let variants: Vec<(&str, UnivMonQConfig)> = vec![
        ("default", base),
        (
            "counter_bits=64",
            UnivMonQConfig {
                counter_bits: 64,
                ..base
            },
        ),
        (
            "width_halving_period=2",
            UnivMonQConfig {
                width_halving_period: 2,
                ..base
            },
        ),
        (
            "explicit hash_seed",
            UnivMonQConfig {
                hash_seed: 3,
                ..base
            },
        ),
    ];

    for (name, config) in variants {
        let mut q = UnivMonQ::new(config).unwrap_or_else(|e| panic!("{name} must build: {e:?}"));
        for v in &values {
            q.update(v);
        }
        assert_eq!(q.count() as usize, values.len(), "{name}: exact count");
        assert_eq!(q.min(), Some(truth.min()), "{name}: exact min");
        assert_eq!(q.max(), Some(truth.max()), "{name}: exact max");
        assert!(
            q.quantile(0.5).is_some(),
            "{name}: ordered queries are enabled, so quantile must answer"
        );
        assert_eq!(
            q.config().counter_bits,
            config.counter_bits,
            "{name}: config must round-trip through the sketch"
        );
    }
}

/// `ordered_samples = 0` is the documented way to switch ordered queries off.
/// The exact aggregates and the frequency/moment estimators must keep working;
/// only `rank`, `cdf` and interior `quantile` go dark.
#[test]
fn univmonq_with_ordered_samples_disabled_answers_everything_except_ordered_queries() {
    let config = UnivMonQConfig {
        ordered_samples: 0,
        ..UnivMonQConfig::default()
    };
    let mut q = UnivMonQ::new(config).expect("ordered_samples=0 is a valid config");
    let values: Vec<f64> = uniform_u64(10_000, 50_000, STREAM_SEED)
        .into_iter()
        .map(|v| v as f64)
        .collect();
    for v in &values {
        q.update(v);
    }
    let truth = NumericTruth::new(values.clone());

    assert_eq!(q.count() as usize, values.len());
    assert_eq!(q.min(), Some(truth.min()));
    assert_eq!(q.max(), Some(truth.max()));
    assert!(
        q.estimate_f2() > 0.0,
        "F2 does not depend on ordered samples"
    );
    assert!(
        q.estimate_distinct() > 0.0,
        "distinct does not depend on ordered samples"
    );

    assert_eq!(q.rank(values[0]), None, "rank must be unavailable");
    assert!(q.cdf().is_empty(), "cdf must be empty");
    assert_eq!(q.quantile(0.5), None, "interior quantiles must be None");
    // The endpoints are served from the exact min/max, not from samples.
    assert_eq!(q.quantile(0.0), Some(truth.min()), "q=0 is the exact min");
    assert_eq!(q.quantile(1.0), Some(truth.max()), "q=1 is the exact max");
}

/// `with_window_bound` picks the smallest hierarchy whose deepest sample still
/// fits the candidate table at the requested failure probability. The chosen
/// `levels` must actually satisfy that inequality, and must grow with the
/// window it is asked to cover.
#[test]
fn univmonq_with_window_bound_chooses_a_hierarchy_that_satisfies_its_own_inequality() {
    const DELTA: f64 = 1e-3;
    let base = UnivMonQConfig::default();

    let mut previous = 0usize;
    for max_updates in [10_000u64, 1_000_000, 100_000_000] {
        let cfg = base
            .with_window_bound(max_updates, DELTA)
            .unwrap_or_else(|e| panic!("window bound for {max_updates} updates: {e:?}"));

        // Re-derive the Bernstein bound the constructor used: the deepest
        // level sees `max_updates / 2^(levels-1)` updates in expectation, and
        // its upper tail must sit under the candidate table's capacity.
        let log_inv_delta = (1.0 / DELTA).ln();
        let mean = max_updates as f64 / 2f64.powi((cfg.levels - 1) as i32);
        let upper = mean + (2.0 * mean * log_inv_delta).sqrt() + (2.0 / 3.0) * log_inv_delta;
        assert!(
            upper < cfg.candidates as f64,
            "levels={} leaves the deepest stratum with an upper bound of {upper:.1}, \
             above the {} candidate slots",
            cfg.levels,
            cfg.candidates
        );
        // And it must be the *smallest* such hierarchy.
        if cfg.levels > 2 {
            let mean_lower = max_updates as f64 / 2f64.powi((cfg.levels - 2) as i32);
            let upper_lower = mean_lower
                + (2.0 * mean_lower * log_inv_delta).sqrt()
                + (2.0 / 3.0) * log_inv_delta;
            assert!(
                upper_lower >= cfg.candidates as f64,
                "levels={} is not minimal: {} levels would already fit",
                cfg.levels,
                cfg.levels - 1
            );
        }
        assert!(
            cfg.levels >= previous,
            "a larger window must not need a shallower hierarchy"
        );
        previous = cfg.levels;

        // The chosen config must build and answer.
        let mut q = UnivMonQ::new(cfg).expect("chosen config must be valid");
        for i in 0..1_000u64 {
            q.update(&(i as f64));
        }
        assert_eq!(q.count(), 1_000);
    }

    assert!(
        base.with_window_bound(1_000, 1.5).is_err(),
        "a failure probability outside (0, 1) must be rejected"
    );
}

/// Merging shards requires globally unique occurrence source IDs; with them,
/// the merged sketch's exact aggregates cover the union.
#[test]
fn univmonq_multi_shard_merge_with_distinct_source_ids_covers_the_union() {
    const SHARDS: usize = 4;
    let config = UnivMonQConfig::default();
    let values: Vec<f64> = uniform_u64(40_000, 1_000_000, STREAM_SEED)
        .into_iter()
        .map(|v| v as f64)
        .collect();
    let truth = NumericTruth::new(values.clone());

    let mut shards: Vec<UnivMonQ> = (0..SHARDS)
        .map(|i| {
            UnivMonQ::with_hasher_and_source_id(config, i as u64 + 1)
                .expect("explicit source id must be accepted")
        })
        .collect();
    for (i, v) in values.iter().enumerate() {
        shards[i % SHARDS].update(v);
    }
    for (i, s) in shards.iter().enumerate() {
        assert_eq!(
            s.source_id(),
            i as u64 + 1,
            "shard {i} must report the source id it was built with"
        );
    }

    let mut merged = shards.remove(0);
    for s in &shards {
        merged.merge(s).expect("distinct source ids must merge");
    }
    assert_eq!(
        merged.count() as usize,
        values.len(),
        "merged count must cover every observation"
    );
    assert_eq!(merged.min(), Some(truth.min()), "merged min must be exact");
    assert_eq!(merged.max(), Some(truth.max()), "merged max must be exact");
}

// -------------------------------------------------------- Portable facade

/// The portable sketch-plus-heap types, on a real stream against exact truth:
/// the point estimate under the right family's bound, the heap consistent with
/// it, and both surviving a MessagePack round trip and a merge.
#[test]
fn portable_count_min_with_heap_satisfies_the_count_min_bound_through_merge_and_wire() {
    const HEAP: usize = 32;
    let stream = zipf_u64(N, DOMAIN, 1.1, STREAM_SEED);
    let mut truth = FreqTruth::default();
    let mut single = CountMinSketchWithHeap::new(ROWS, COLS, HEAP);
    let mut left = CountMinSketchWithHeap::new(ROWS, COLS, HEAP);
    let mut right = CountMinSketchWithHeap::new(ROWS, COLS, HEAP);
    for (i, k) in stream.iter().enumerate() {
        truth.observe(*k as i64);
        let key = format!("k{k}");
        single.update(&key, 1.0);
        if i % 2 == 0 {
            left.update(&key, 1.0);
        } else {
            right.update(&key, 1.0);
        }
    }

    let context = format!(
        "rows={ROWS} cols={COLS} heap={HEAP} zipf(1.1) domain={DOMAIN} n={N} seed={STREAM_SEED:#x}"
    );
    let spec = CountMinSpec::new(ROWS, COLS);
    spec.assert_contract(
        "portable CountMinSketchWithHeap",
        &truth,
        |k| single.estimate(&format!("k{k}")),
        &context,
    );

    // The heap must agree with the sketch it sits beside.
    let mut heap_tally = Tally::default();
    for item in single.topk_heap_items() {
        let est = single.estimate(&item.key);
        heap_tally.record(item.value == est, || {
            format!(
                "key {}: heap holds {} but the sketch estimates {est}",
                item.key, item.value
            )
        });
    }
    heap_tally.assert_none("portable CountMinSketchWithHeap heap consistency", &context);

    // Merge, then the same contract.
    let merged = CountMinSketchWithHeap::merge_refs(&[&left, &right]).expect("merge");
    spec.assert_contract(
        "portable CountMinSketchWithHeap after merge",
        &truth,
        |k| merged.estimate(&format!("k{k}")),
        &context,
    );

    // Wire round trip, then the same contract again: serialization must not
    // change a single answer.
    let bytes = single.to_msgpack().expect("encode");
    let decoded = CountMinSketchWithHeap::from_msgpack(&bytes).expect("decode");
    let mut wire_tally = Tally::default();
    for (k, _) in truth.pairs() {
        let key = format!("k{k}");
        let a = single.estimate(&key);
        let b = decoded.estimate(&key);
        wire_tally.record(a == b, || format!("key {key}: before {a} after {b}"));
    }
    wire_tally.assert_none("portable CountMinSketchWithHeap wire round trip", &context);
    spec.assert_contract(
        "portable CountMinSketchWithHeap after a wire round trip",
        &truth,
        |k| decoded.estimate(&format!("k{k}")),
        &context,
    );
}

#[test]
fn portable_count_sketch_with_heap_satisfies_the_l2_bound_through_merge_and_wire() {
    const HEAP: usize = 32;
    const CS_ROWS: usize = 5;
    let stream = zipf_u64(N, DOMAIN, 1.1, STREAM_SEED);
    let mut truth = FreqTruth::default();
    let mut single = CountSketchWithHeap::new(CS_ROWS, COLS, HEAP);
    let mut left = CountSketchWithHeap::new(CS_ROWS, COLS, HEAP);
    let mut right = CountSketchWithHeap::new(CS_ROWS, COLS, HEAP);
    for (i, k) in stream.iter().enumerate() {
        truth.observe(*k as i64);
        let key = format!("k{k}");
        single.update(&key, 1.0);
        if i % 2 == 0 {
            left.update(&key, 1.0);
        } else {
            right.update(&key, 1.0);
        }
    }

    let context = format!(
        "rows={CS_ROWS} cols={COLS} heap={HEAP} zipf(1.1) domain={DOMAIN} n={N} seed={STREAM_SEED:#x}"
    );
    let spec = CountSketchSpec::new(CS_ROWS, COLS);
    spec.assert_contract(
        "portable CountSketchWithHeap",
        &truth,
        |k| single.estimate(&format!("k{k}")),
        &context,
    );

    let mut heap_tally = Tally::default();
    for item in single.topk_heap_items() {
        let est = single.estimate(&item.key);
        heap_tally.record(item.value == est, || {
            format!(
                "key {}: heap holds {} but the sketch estimates {est}",
                item.key, item.value
            )
        });
    }
    heap_tally.assert_none("portable CountSketchWithHeap heap consistency", &context);

    let merged = CountSketchWithHeap::merge_refs(&[&left, &right]).expect("merge");
    spec.assert_contract(
        "portable CountSketchWithHeap after merge",
        &truth,
        |k| merged.estimate(&format!("k{k}")),
        &context,
    );

    let bytes = single.to_msgpack().expect("encode");
    let decoded = CountSketchWithHeap::from_msgpack(&bytes).expect("decode");
    spec.assert_contract(
        "portable CountSketchWithHeap after a wire round trip",
        &truth,
        |k| decoded.estimate(&format!("k{k}")),
        &context,
    );
}

/// The portable KLL facade under the rank-error contract, seeded so a failure
/// reproduces. `KllSketch::new` seeds from the wall clock; `with_seed` is what
/// an accuracy test must use.
#[test]
fn portable_kll_sketch_satisfies_the_rank_error_contract_through_merge_and_wire() {
    const K: u16 = 200;
    const SKETCH_SEED: u64 = 0x5EED_0400;
    let values: Vec<f64> = uniform_u64(N, 1_000_000, STREAM_SEED)
        .into_iter()
        .map(|v| v as f64)
        .collect();
    let truth = NumericTruth::new(values.clone());

    let mut single = KllSketch::with_seed(K, SKETCH_SEED);
    let mut left = KllSketch::with_seed(K, SKETCH_SEED ^ 0xAAAA);
    let mut right = KllSketch::with_seed(K, SKETCH_SEED ^ 0x5555);
    for (i, v) in values.iter().enumerate() {
        single.update(*v);
        if i % 2 == 0 {
            left.update(*v);
        } else {
            right.update(*v);
        }
    }
    left.merge(&right).expect("same-k merge");

    let spec = RankErrorSpec::datasketches(K as usize);
    let qs = [0.1f64, 0.25, 0.5, 0.75, 0.9];
    let context =
        format!("k={K} sketch_seed={SKETCH_SEED:#x} uniform n={N} stream_seed={STREAM_SEED:#x}");

    let mut tally = Tally::default();
    spec.tally_into(&mut tally, truth.sorted(), &qs, |q| single.quantile(q));
    spec.tally_into(&mut tally, truth.sorted(), &qs, |q| left.quantile(q));

    // Wire round trip must preserve every answer bit for bit.
    let bytes = single.to_msgpack().expect("encode");
    let decoded = KllSketch::from_msgpack(&bytes).expect("decode");
    assert_eq!(decoded.k(), K, "k must survive the wire");
    assert_eq!(
        decoded.count(),
        single.count(),
        "retained mass must survive the wire"
    );
    let mut wire_tally = Tally::default();
    for &q in &qs {
        let (a, b) = (single.quantile(q), decoded.quantile(q));
        wire_tally.record(a == b, || format!("q={q}: before {a} after {b}"));
    }
    wire_tally.assert_none("portable KllSketch wire round trip", &context);
    spec.tally_into(&mut tally, truth.sorted(), &qs, |q| decoded.quantile(q));

    // A mismatched `k` must be refused rather than silently merged.
    let other_k = KllSketch::with_seed(K * 2, SKETCH_SEED);
    assert!(
        single.merge(&other_k).is_err(),
        "merging sketches with different k must fail"
    );

    tally.assert_within(
        "portable KllSketch / normalized rank error",
        spec.failure_probability,
        &format!("{context}; single pass, two-shard merge, and post-wire, q grid {qs:?}"),
    );
}
