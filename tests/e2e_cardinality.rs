//! E2E cardinality pipelines on synthetic unique streams.
//!
//! Every band here is computed from the estimator's own relative standard
//! error and an explicit Gaussian quantile `z`; none is a flat percentage.
//! That distinction matters because the three estimators do *not* share an
//! error model:
//!
//! - `Classic` is Flajolet et al.'s register estimator, RSE `1.04 / sqrt(m)`.
//! - `ErtlMLE` is the maximum-likelihood estimator over the same registers; it
//!   attains the Cramer-Rao bound `sqrt(3 ln 2 - 1) / sqrt(m) = 1.0389/sqrt(m)`,
//!   so the same constant applies.
//! - `HIP` integrates the insertion history and is strictly tighter, RSE
//!   `sqrt(ln 2 / m) = 0.8326 / sqrt(m)`. Holding it to Classic's constant
//!   would pass a HIP implementation that had silently degraded to a register
//!   estimator, so it is held to its own.
//!
//! A single hard-coded 2% would be simultaneously too loose at p16 (4.9 sigma)
//! and too tight at p12 (1.2 sigma), which is why it is gone.

mod common;

use common::specs::{CardinalityConfidenceSpec, Tally};
use common::streams::uniform_u64;

use asap_sketchlib::message_pack_format::portable::hll::{HllSketch, HllVariant};
use asap_sketchlib::sketches::hll::{HyperLogLogHIPImpl, HyperLogLogImpl};
use asap_sketchlib::{
    Classic, DataInput, ErtlMLE, HyperLogLogHIPP12, HyperLogLogHIPP14, HyperLogLogHIPP16,
    HyperLogLogP12, HyperLogLogP14, HyperLogLogP16, SetAggregator,
};

asap_sketchlib::impl_hll_bucket_list!(HllBucketListP10, 10, 1_usize << 10);
asap_sketchlib::impl_hll_bucket_list!(HllBucketListP13, 13, 1_usize << 13);
asap_sketchlib::impl_hll_bucket_list!(HllBucketListP18, 18, 1_usize << 18);

const CUSTOM_CHECKPOINTS_P10: [u64; 4] = [100, 1_000, 20_000, 200_000];
const CUSTOM_CHECKPOINTS_P13: [u64; 4] = [1_000, 10_000, 100_000, 500_000];
const CUSTOM_CHECKPOINTS_P18: [u64; 3] = [10_000, 100_000, 1_500_000];

/// Gaussian quantile for every cardinality band below. `z = 4` is a two-sided
/// failure probability of 6.3e-5 per check; with a few dozen checks per
/// battery the binomial acceptance rule then tolerates zero failures, which is
/// the intent — an estimator four standard errors out is broken, not unlucky.
const Z: f64 = 4.0;

/// Checkpoints spanning the linear-counting regime (far below the register
/// count) and the raw-estimator regime well above it.
///
/// They deliberately avoid `n` between roughly `2m` and `4m`, where the 2007
/// estimator switches from linear counting to the raw indicator and its error
/// is *not* `1.04/sqrt(m)`. That band has its own test below rather than being
/// swept under this one's tolerance.
const CHECKPOINTS: [u64; 7] = [10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000];

/// Runs one core HLL type across every checkpoint, in three ingestion modes:
/// a single pass, a replay of the same identities (which must not move the
/// estimate at all, since registers only ever take a maximum), and a merge of
/// two disjoint shards.
///
/// `$merge` says whether the type supports merging: HIP's estimate is
/// maintained incrementally from its own insertion history, so it has no
/// merge operation and only the first two modes apply.
macro_rules! hll_battery {
    ($name:ident, $ty:ty, $precision:literal, $model:ident, mergeable) => {
        hll_battery!($name, $ty, $precision, $model, mergeable, CHECKPOINTS);
    };
    ($name:ident, $ty:ty, $precision:literal, $model:ident, mergeable, $checkpoints:expr) => {
        hll_battery!(@body $name, $ty, $precision, $model, $checkpoints,
            |single: &$ty, even: &$ty, odd: &$ty| {
                let mut merged = even.clone();
                merged.merge(odd);
                Some((
                    merged.registers_as_slice() == single.registers_as_slice(),
                    merged.estimate() == single.estimate(),
                ))
            });
    };
    ($name:ident, $ty:ty, $precision:literal, $model:ident, not_mergeable) => {
        hll_battery!($name, $ty, $precision, $model, not_mergeable, CHECKPOINTS);
    };
    ($name:ident, $ty:ty, $precision:literal, $model:ident, not_mergeable, $checkpoints:expr) => {
        hll_battery!(@body $name, $ty, $precision, $model, $checkpoints,
            |_s: &$ty, _e: &$ty, _o: &$ty| None);
        // HIP has no `merge`: its estimate is maintained incrementally from the
        // sketch's own insertion history, so two shards cannot be combined
        // without replaying one of them.
    };
    (@body $name:ident, $ty:ty, $precision:literal, $model:ident, $checkpoints:expr, $merge:expr) => {
        #[test]
        fn $name() {
            let spec = CardinalityConfidenceSpec::$model($precision, Z);
            let mut tally = Tally::default();
            let checkpoints: &[u64] = &$checkpoints;
            let context = format!(
                "{} p{} : m={} sigma_rel={:.5} z={Z} tolerance={:.5}; one trial = one \
                 sketch over its own disjoint identity namespace, checkpoints {:?}",
                stringify!($ty),
                $precision,
                1usize << $precision,
                spec.sigma_rel(),
                spec.tolerance(),
                checkpoints
            );

            // One fresh sketch per checkpoint, each over an identity namespace
            // disjoint from every other. Feeding one accumulating sketch and
            // reading it at rising `n` would produce *nested* estimates — the
            // state at 10^6 contains the state at 10^5 — which share every
            // register and are emphatically not independent trials.
            let mut largest: Option<$ty> = None;
            for (i, &target) in checkpoints.iter().enumerate() {
                let base = IDENTITY_NAMESPACE_STRIDE * (i as u64 + 1);
                let mut sketch = <$ty>::new();
                for k in 0..target {
                    sketch.insert(&DataInput::U64(base + k));
                }
                spec.tally_into(&mut tally, sketch.estimate() as f64, target as usize);
                largest = Some(sketch);
            }

            let mut biggest = largest.expect("the checkpoint list is non-empty");
            let target = checkpoints[checkpoints.len() - 1];
            let base = IDENTITY_NAMESPACE_STRIDE * (checkpoints.len() as u64);

            // Duplicate replay: re-inserting every identity already seen must
            // leave the estimate bit-identical, because a register only ever
            // moves to a larger leading-zero count. Structural, not a band.
            let before = biggest.estimate();
            for k in 0..target.min(200_000) {
                biggest.insert(&DataInput::U64(base + k));
            }
            assert_eq!(
                biggest.estimate(),
                before,
                "replaying seen identities moved the estimate. {context}"
            );

            // Shard merge over the *same* identities is an **equality**, not a
            // band: HLL registers combine by elementwise maximum, so merging
            // the even and odd halves reproduces the single pass register for
            // register.
            {
                const MERGE_N: u64 = 200_000;
                let mbase = IDENTITY_NAMESPACE_STRIDE * (checkpoints.len() as u64 + 1);
                let mut single = <$ty>::new();
                let mut even = <$ty>::new();
                let mut odd = <$ty>::new();
                for k in 0..MERGE_N {
                    let d = DataInput::U64(mbase + k);
                    single.insert(&d);
                    if k % 2 == 0 {
                        even.insert(&d);
                    } else {
                        odd.insert(&d);
                    }
                }
                let merge_shards: fn(&$ty, &$ty, &$ty) -> Option<(bool, bool)> = $merge;
                if let Some((registers_match, estimate_match)) =
                    merge_shards(&single, &even, &odd)
                {
                    assert!(
                        registers_match,
                        "a disjoint even/odd shard merge must reproduce the single pass \
                         register for register. {context}"
                    );
                    assert!(
                        estimate_match,
                        "identical registers must give an identical estimate. {context}"
                    );
                }
            }

            tally.assert_independent_binomial(
                concat!(stringify!($name), " / cardinality confidence band"),
                spec.per_check_failure(),
                &context,
            );
        }
    };
}

/// Identity namespaces are `stride * i .. stride * i + n`, far enough apart
/// that no two trials share an identity and therefore no two share a hash.
const IDENTITY_NAMESPACE_STRIDE: u64 = 1 << 40;

// Classic and Ertl-MLE share the register error model; HIP has its own.
hll_battery!(
    hll_classic_p12_satisfies_its_register_error_model,
    HyperLogLogP12<Classic>,
    12,
    hll,
    mergeable
);
hll_battery!(
    hll_classic_p14_satisfies_its_register_error_model,
    HyperLogLogP14<Classic>,
    14,
    hll,
    mergeable
);
hll_battery!(
    hll_classic_p16_satisfies_its_register_error_model,
    HyperLogLogP16<Classic>,
    16,
    hll,
    mergeable
);
hll_battery!(
    hll_ertl_mle_p12_satisfies_the_cramer_rao_error_model,
    HyperLogLogP12<ErtlMLE>,
    12,
    hll,
    mergeable
);
hll_battery!(
    hll_ertl_mle_p14_satisfies_the_cramer_rao_error_model,
    HyperLogLogP14<ErtlMLE>,
    14,
    hll,
    mergeable
);
hll_battery!(
    hll_ertl_mle_p16_satisfies_the_cramer_rao_error_model,
    HyperLogLogP16<ErtlMLE>,
    16,
    hll,
    mergeable
);
hll_battery!(
    hll_hip_p12_satisfies_the_hip_error_model,
    HyperLogLogHIPP12,
    12,
    hll_hip,
    not_mergeable
);
hll_battery!(
    hll_hip_p14_satisfies_the_hip_error_model,
    HyperLogLogHIPP14,
    14,
    hll_hip,
    not_mergeable
);
hll_battery!(
    hll_hip_p16_satisfies_the_hip_error_model,
    HyperLogLogHIPP16,
    16,
    hll_hip,
    not_mergeable
);

/// The portable wire type across every variant tag and precision.
///
/// `HllVariant` selects the *wire tag*, not the estimator: `HllSketch::estimate`
/// runs the same register-based Classic formula for `Regular`, `Datafusion` and
/// `Hip` alike. So all three are held to the register model — checking the
/// `Hip` tag against HIP's tighter constant would be asserting a property this
/// type does not implement.
#[test]
fn portable_hll_variants_and_precisions_satisfy_the_register_error_model() {
    const N: usize = 200_000;
    let mut tally = Tally::default();
    let mut context = Vec::new();

    for (v, variant) in [HllVariant::Regular, HllVariant::Datafusion, HllVariant::Hip]
        .into_iter()
        .enumerate()
    {
        for (p, precision) in [12u32, 14, 16].into_iter().enumerate() {
            let spec = CardinalityConfidenceSpec::hll(precision, Z);
            // A distinct stream per (variant, precision). Reusing one stream
            // across the three variants would feed three estimators the *same*
            // registers, whose errors are then near-perfectly correlated — nine
            // readings of at most three independent experiments.
            let seed = 2001 + (v * 3 + p) as u64;
            let stream = uniform_u64(N, u64::MAX / 4, seed);
            let truth: std::collections::HashSet<u64> = stream.iter().copied().collect();

            let mut hll = HllSketch::new(variant, precision);
            for k in &stream {
                hll.update(k.to_be_bytes().as_slice());
            }
            spec.tally_into(&mut tally, hll.estimate(), truth.len());

            // Merging a second sketch built over the SAME identities is a
            // register-wise max with itself: the estimate must not move at all.
            let mut other = HllSketch::new(variant, precision);
            for k in &truth {
                other.update((*k).to_be_bytes().as_slice());
            }
            let before = hll.estimate();
            hll.merge(&other).expect("merge");
            assert_eq!(
                hll.estimate(),
                before,
                "{variant:?} p{precision}: merging identical identities moved the estimate"
            );

            // Disjoint shards over the same stream merge by register-wise max,
            // so the result is the *same* sketch as the single pass and its
            // estimate is the *same number*. That is an equality, not a second
            // confidence-band reading: scoring it into the tally, as this test
            // used to, counted one experiment twice.
            let mut left = HllSketch::new(variant, precision);
            let mut right = HllSketch::new(variant, precision);
            for (i, k) in stream.iter().enumerate() {
                if i % 2 == 0 {
                    left.update(k.to_be_bytes().as_slice());
                } else {
                    right.update(k.to_be_bytes().as_slice());
                }
            }
            left.merge(&right).expect("shard merge");
            assert_eq!(
                left.estimate(),
                before,
                "{variant:?} p{precision}: an even/odd shard merge must reproduce the \
                 single pass exactly (registers combine by maximum)"
            );

            context.push(format!(
                "{variant:?}/p{precision} sigma={:.5} tol={:.5} stream_seed={seed}",
                spec.sigma_rel(),
                spec.tolerance()
            ));
        }
    }

    tally.assert_independent_binomial(
        "portable HllSketch / cardinality confidence band",
        CardinalityConfidenceSpec::hll(12, Z).per_check_failure(),
        &format!(
            "n={N} unique byte keys; one trial = one (variant, precision) over its own \
             stream seed; {}",
            context.join("; ")
        ),
    );
}

/// A larger precision must actually buy accuracy. A fixed percentage band
/// cannot see this — it passes identically at p12 and p16, so it would not
/// notice a precision parameter that never reached the register array.
///
/// The measured quantity is the RSE itself, estimated as the root-mean-square
/// relative error over eight disjoint identity blocks, so it is compared with
/// `1.04/sqrt(m)` directly rather than through a single draw from it.
#[test]
fn hll_accuracy_improves_with_precision_as_the_error_model_predicts() {
    // Comfortably inside the raw-estimator regime for every precision here
    // (n/m is 244, 61 and 15 at p12, p14 and p16), so `1.04/sqrt(m)` is the
    // applicable model at all three.
    const N: u64 = 1_000_000;
    const BLOCKS: u64 = 6;

    macro_rules! measured_rse {
        ($ty:ty) => {{
            let mut sq = 0.0f64;
            for b in 0..BLOCKS {
                let mut s = <$ty>::new();
                for i in 0..N {
                    s.insert(&DataInput::U64(b * N + i));
                }
                let rel = (s.estimate() as f64 - N as f64) / N as f64;
                sq += rel * rel;
            }
            (sq / BLOCKS as f64).sqrt()
        }};
    }

    let errors = [
        (12u32, measured_rse!(HyperLogLogP12<Classic>)),
        (14, measured_rse!(HyperLogLogP14<Classic>)),
        (16, measured_rse!(HyperLogLogP16<Classic>)),
    ];

    for (precision, rse) in &errors {
        let predicted = CardinalityConfidenceSpec::hll(*precision, Z).sigma_rel();
        assert!(
            *rse <= predicted * 2.0,
            "p{precision}: measured RSE {rse:.5} over {BLOCKS} disjoint blocks of {N} \
             identities exceeds twice the predicted 1.04/sqrt(m) = {predicted:.5}"
        );
    }
    // Sixteen times the registers must deliver at least twice the accuracy;
    // the model predicts 4x.
    assert!(
        errors[0].1 >= errors[2].1 * 2.0,
        "raising precision from p12 to p16 moved the measured RSE only from {:.5} to {:.5}; \
         1.04/sqrt(m) predicts a 4x improvement, so precision is not reaching the registers",
        errors[0].1,
        errors[2].1
    );
}

/// The Classic estimator's accuracy cliff at the linear-counting switchover.
///
/// `HyperLogLogImpl<Classic>::estimate` follows the original 2007 paper: it
/// uses linear counting while the raw estimate is at or below `2.5m`, and the
/// bias-corrected indicator above it. The two branches do not meet smoothly,
/// so just around the switchover the relative error is several times
/// `1.04/sqrt(m)` — this is the discontinuity HLL++ later removed with
/// empirical bias correction, and it is a property of the estimator as
/// shipped, not a defect introduced here.
///
/// Documented empirical regression, deliberately **not** presented as the
/// register theorem: no theoretical constant covers this band.
///
/// Band source: RMS relative error over 6 disjoint identity blocks at
/// `n = 2.5m`, measured as 2.1x the asymptotic RSE at p12, 3.5x at p14 and
/// 6.3x at p16. The ceiling below is 10x, which pins the cliff without
/// pretending it is not there; the floor asserts it is still a cliff, so that
/// a future bias correction makes this test fail loudly and get updated rather
/// than silently keeping a stale claim.
#[test]
fn hll_classic_switchover_band_stays_within_the_documented_empirical_band() {
    const BLOCKS: u64 = 6;

    macro_rules! rse_at {
        ($ty:ty, $n:expr) => {{
            let n: u64 = $n;
            let mut sq = 0.0f64;
            for b in 0..BLOCKS {
                let mut s = <$ty>::new();
                for i in 0..n {
                    s.insert(&DataInput::U64(b * 20_000_000 + i));
                }
                let rel = (s.estimate() as f64 - n as f64) / n as f64;
                sq += rel * rel;
            }
            (sq / BLOCKS as f64).sqrt()
        }};
    }

    let measured = [
        (
            12u32,
            rse_at!(HyperLogLogP12<Classic>, (2.5 * 4096.0) as u64),
        ),
        (14, rse_at!(HyperLogLogP14<Classic>, (2.5 * 16384.0) as u64)),
        (16, rse_at!(HyperLogLogP16<Classic>, (2.5 * 65536.0) as u64)),
    ];

    for (precision, rse) in &measured {
        let asymptotic = CardinalityConfidenceSpec::hll(*precision, Z).sigma_rel();
        let ratio = rse / asymptotic;
        assert!(
            ratio <= 10.0,
            "p{precision} at n = 2.5m: RMS relative error {rse:.5} is {ratio:.2}x the \
             asymptotic 1.04/sqrt(m) = {asymptotic:.5}, past the documented 10x band for the \
             linear-counting switchover"
        );
        assert!(
            ratio >= 1.5,
            "p{precision} at n = 2.5m: RMS relative error {rse:.5} is only {ratio:.2}x the \
             asymptotic {asymptotic:.5}. The switchover cliff this test documents appears to \
             be gone — if the estimator gained bias correction, delete this test and widen \
             CHECKPOINTS to cover the band under the register model instead of leaving a \
             stale claim here"
        );
    }
}

#[test]
fn set_aggregator_union_is_exact() {
    let mut agg = SetAggregator::new();
    let mut expected = std::collections::HashSet::new();
    let stream = uniform_u64(20_000, 500, 2002);
    for k in stream {
        let s = format!("member-{k}");
        expected.insert(s.clone());
        agg.update(&s);
    }
    let mut other = SetAggregator::new();
    for k in ["extra-a", "extra-b"] {
        expected.insert((*k).to_string());
        other.update(k);
    }
    agg.merge(&other).expect("merge");
    assert_eq!(
        agg.values.len(),
        expected.len(),
        "SetAggregator cardinality must be exact"
    );
    for k in &expected {
        assert!(agg.values.contains(k), "missing member {k}");
    }
}

hll_battery!(
    hll_classic_custom_p10_satisfies_its_register_error_model,
    HyperLogLogImpl<Classic, HllBucketListP10>,
    10,
    hll,
    mergeable,
    CUSTOM_CHECKPOINTS_P10
);
hll_battery!(
    hll_ertl_mle_custom_p10_satisfies_the_cramer_rao_error_model,
    HyperLogLogImpl<ErtlMLE, HllBucketListP10>,
    10,
    hll,
    mergeable,
    CUSTOM_CHECKPOINTS_P10
);
hll_battery!(
    hll_hip_custom_p10_satisfies_the_hip_error_model,
    HyperLogLogHIPImpl<HllBucketListP10>,
    10,
    hll_hip,
    not_mergeable,
    CUSTOM_CHECKPOINTS_P10
);
hll_battery!(
    hll_classic_custom_p13_satisfies_its_register_error_model,
    HyperLogLogImpl<Classic, HllBucketListP13>,
    13,
    hll,
    mergeable,
    CUSTOM_CHECKPOINTS_P13
);
hll_battery!(
    hll_ertl_mle_custom_p13_satisfies_the_cramer_rao_error_model,
    HyperLogLogImpl<ErtlMLE, HllBucketListP13>,
    13,
    hll,
    mergeable,
    CUSTOM_CHECKPOINTS_P13
);
hll_battery!(
    hll_hip_custom_p13_satisfies_the_hip_error_model,
    HyperLogLogHIPImpl<HllBucketListP13>,
    13,
    hll_hip,
    not_mergeable,
    CUSTOM_CHECKPOINTS_P13
);
hll_battery!(
    hll_classic_custom_p18_satisfies_its_register_error_model,
    HyperLogLogImpl<Classic, HllBucketListP18>,
    18,
    hll,
    mergeable,
    CUSTOM_CHECKPOINTS_P18
);
hll_battery!(
    hll_ertl_mle_custom_p18_satisfies_the_cramer_rao_error_model,
    HyperLogLogImpl<ErtlMLE, HllBucketListP18>,
    18,
    hll,
    mergeable,
    CUSTOM_CHECKPOINTS_P18
);
hll_battery!(
    hll_hip_custom_p18_satisfies_the_hip_error_model,
    HyperLogLogHIPImpl<HllBucketListP18>,
    18,
    hll_hip,
    not_mergeable,
    CUSTOM_CHECKPOINTS_P18
);

#[test]
fn custom_precision_accuracy_improves_with_precision_as_the_error_model_predicts() {
    const N: u64 = 200_000;
    const BASE: u64 = IDENTITY_NAMESPACE_STRIDE * 41;

    let mut p10 = HyperLogLogImpl::<Classic, HllBucketListP10>::new();
    let mut p13 = HyperLogLogImpl::<Classic, HllBucketListP13>::new();
    let mut p18 = HyperLogLogImpl::<Classic, HllBucketListP18>::new();
    for k in 0..N {
        let d = DataInput::U64(BASE + k);
        p10.insert(&d);
        p13.insert(&d);
        p18.insert(&d);
    }

    let relative = |estimate: usize| (estimate as f64 - N as f64).abs() / N as f64;
    let (e10, e13, e18) = (
        relative(p10.estimate()),
        relative(p13.estimate()),
        relative(p18.estimate()),
    );
    assert!(
        e18 <= e10,
        "p18 relative error {e18:.5} exceeded p10 at {e10:.5} over {N} distinct identities"
    );
    assert!(
        e18 <= e13,
        "p18 relative error {e18:.5} exceeded p13 at {e13:.5} over {N} distinct identities"
    );
}

#[test]
fn a_custom_precision_merge_reproduces_the_single_pass_registers_for_every_estimator() {
    const N: u64 = 120_000;
    const BASE: u64 = IDENTITY_NAMESPACE_STRIDE * 42;

    let mut classic_single = HyperLogLogImpl::<Classic, HllBucketListP13>::new();
    let mut classic_even = HyperLogLogImpl::<Classic, HllBucketListP13>::new();
    let mut classic_odd = HyperLogLogImpl::<Classic, HllBucketListP13>::new();
    let mut ertl_single = HyperLogLogImpl::<ErtlMLE, HllBucketListP13>::new();
    let mut ertl_even = HyperLogLogImpl::<ErtlMLE, HllBucketListP13>::new();
    let mut ertl_odd = HyperLogLogImpl::<ErtlMLE, HllBucketListP13>::new();
    for k in 0..N {
        let d = DataInput::U64(BASE + k);
        classic_single.insert(&d);
        ertl_single.insert(&d);
        if k % 2 == 0 {
            classic_even.insert(&d);
            ertl_even.insert(&d);
        } else {
            classic_odd.insert(&d);
            ertl_odd.insert(&d);
        }
    }

    classic_even.merge(&classic_odd);
    assert_eq!(
        classic_even.registers_as_slice(),
        classic_single.registers_as_slice(),
        "Classic p13 shard merge must reproduce the single pass register for register"
    );
    assert_eq!(
        classic_even.estimate(),
        classic_single.estimate(),
        "Classic p13 identical registers must give an identical estimate"
    );

    ertl_even.merge(&ertl_odd);
    assert_eq!(
        ertl_even.registers_as_slice(),
        ertl_single.registers_as_slice(),
        "ErtlMLE p13 shard merge must reproduce the single pass register for register"
    );
    assert_eq!(
        ertl_even.estimate(),
        ertl_single.estimate(),
        "ErtlMLE p13 identical registers must give an identical estimate"
    );
}

#[test]
fn a_set_aggregator_delta_describes_the_change_and_survives_the_wire() {
    use asap_sketchlib::{DeltaResult, MessagePackCodec};
    use std::collections::HashSet;

    let mut before = SetAggregator::new();
    for key in ["web", "api", "db", "cache"] {
        before.update(key);
    }
    let mut after = SetAggregator::new();
    for key in ["web", "api", "queue"] {
        after.update(key);
    }

    let added: HashSet<String> = after.values.difference(&before.values).cloned().collect();
    let removed: HashSet<String> = before.values.difference(&after.values).cloned().collect();
    let delta = DeltaResult {
        added: added.clone(),
        removed: removed.clone(),
    };

    assert_eq!(
        delta.added,
        HashSet::from(["queue".to_string()]),
        "only the arriving key is added"
    );
    assert_eq!(
        delta.removed,
        HashSet::from(["db".to_string(), "cache".to_string()]),
        "both departing keys are removed"
    );

    let bytes = delta.to_msgpack().expect("encode");
    let decoded = DeltaResult::from_msgpack(&bytes).expect("decode");
    assert_eq!(decoded.added, added, "added set survived the wire");
    assert_eq!(decoded.removed, removed, "removed set survived the wire");

    let mut replayed = before.clone();
    for key in &decoded.removed {
        replayed.values.remove(key);
    }
    for key in &decoded.added {
        replayed.update(key);
    }
    assert_eq!(
        replayed.values, after.values,
        "applying the decoded delta must reproduce the later snapshot"
    );

    let empty = DeltaResult {
        added: HashSet::new(),
        removed: HashSet::new(),
    };
    let round_tripped = DeltaResult::from_msgpack(&empty.to_msgpack().expect("encode empty"))
        .expect("decode empty");
    assert!(
        round_tripped.added.is_empty() && round_tripped.removed.is_empty(),
        "an empty delta must stay empty across the wire"
    );
}
