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

use common::conformance::assert_cardinality_bound;
use common::specs::CardinalityConfidenceSpec;
use common::streams::uniform_u64;
use common::variants::{hyperloglog_variants, portable_hll_variants};

use asap_sketchlib::{
    Classic, DataInput, HyperLogLogP12, HyperLogLogP14, HyperLogLogP16, SetAggregator,
};

/// Gaussian quantile for every cardinality band below. `z = 4` is a two-sided
/// failure probability of 6.3e-5 per check; with a few dozen checks per
/// battery the binomial acceptance rule then tolerates zero failures, which is
/// the intent — an estimator four standard errors out is broken, not unlucky.
const Z: f64 = 4.0;

#[test]
fn every_hyperloglog_instantiation_satisfies_its_own_cardinality_error_model() {
    assert_cardinality_bound(hyperloglog_variants);
}

#[test]
fn every_portable_hll_instantiation_satisfies_the_register_error_model() {
    assert_cardinality_bound(portable_hll_variants);
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
             CHECKPOINT_MULTIPLIERS to cover the band under the register model instead of \
             leaving a stale claim here"
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
