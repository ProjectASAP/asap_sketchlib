//! E2E cardinality pipelines on synthetic unique streams: core HLL variants
//! (+ shard merge), the portable wire HLL, and SetAggregator exactness.

mod common;

use common::{assert_between, uniform_u64};

use asap_sketchlib::message_pack_format::portable::hll::{HllSketch, HllVariant};
use asap_sketchlib::{DataInput, HyperLogLog, HyperLogLogHIP, SetAggregator};

#[test]
fn hll_variants_checkpoints_and_shard_merge() {
    // Checkpoints span the linear-counting regime (below the register count)
    // and the estimator regime above it.
    let checkpoints = [10u64, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000];
    let mut classic = HyperLogLog::<asap_sketchlib::Classic>::new();
    let mut ertl = HyperLogLog::<asap_sketchlib::ErtlMLE>::new();
    let mut hip = HyperLogLogHIP::new();
    // Even/odd shard split for the merge leg of the test.
    let mut classic_even = HyperLogLog::<asap_sketchlib::Classic>::new();
    let mut classic_odd = HyperLogLog::<asap_sketchlib::Classic>::new();
    let mut ertl_even = HyperLogLog::<asap_sketchlib::ErtlMLE>::new();
    let mut ertl_odd = HyperLogLog::<asap_sketchlib::ErtlMLE>::new();

    let mut seen = 0u64;
    for &target in &checkpoints {
        while seen < target {
            let d = DataInput::U64(seen);
            classic.insert(&d);
            ertl.insert(&d);
            hip.insert(&d);
            if seen % 2 == 0 {
                classic_even.insert(&d);
                ertl_even.insert(&d);
            } else {
                classic_odd.insert(&d);
                ertl_odd.insert(&d);
            }
            seen += 1;
        }
        let t = target as f64;
        for (label, est) in [
            ("Classic", classic.estimate() as f64),
            ("ErtlMLE", ertl.estimate() as f64),
            ("HIP", hip.estimate() as f64),
        ] {
            assert_between(est, t * 0.98, t * 1.02, &format!("{label} @ {target}"));
        }

        let mut classic_merged = classic_even.clone();
        classic_merged.merge(&classic_odd);
        assert_between(
            classic_merged.estimate() as f64,
            t * 0.98,
            t * 1.02,
            &format!("Classic shard-merge @ {target}"),
        );

        let mut ertl_merged = ertl_even.clone();
        ertl_merged.merge(&ertl_odd);
        assert_between(
            ertl_merged.estimate() as f64,
            t * 0.98,
            t * 1.02,
            &format!("ErtlMLE shard-merge @ {target}"),
        );
    }
}

#[test]
fn portable_hll_precisions_over_byte_keys() {
    for (precision, tol) in [(12u32, 0.03f64), (14u32, 0.02f64)] {
        let mut hll = HllSketch::new(HllVariant::Regular, precision);
        let stream = uniform_u64(200_000, u64::MAX / 4, 2001 + precision as u64);
        let mut truth = std::collections::HashSet::new();
        for k in stream {
            truth.insert(k);
            hll.update(k.to_be_bytes().as_slice());
        }
        let t = truth.len() as f64;
        let est = hll.estimate();
        assert_between(
            est,
            t * (1.0 - tol),
            t * (1.0 + tol),
            &format!("portable HLL p{precision}"),
        );

        // Merge a second sketch over the SAME identities (byte-identical
        // encodings): registers max, so distinct count must not change.
        let mut other = HllSketch::new(HllVariant::Regular, precision);
        for k in &truth {
            other.update((*k).to_be_bytes().as_slice());
        }
        hll.merge(&other).expect("merge");
        let rem = hll.estimate();
        assert_between(
            rem,
            t * (1.0 - tol),
            t * (1.0 + tol),
            &format!("portable HLL p{precision} after duplicate-merge"),
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
