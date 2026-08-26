//! E2E cardinality pipelines on synthetic unique streams: core HLL variants
//! (+ shard merge), the portable wire HLL, SetAggregator exactness, and a
//! mixed HashSketchEnsemble layer.

mod common;

use common::{assert_between, uniform_u64};

use asap_sketchlib::message_pack_format::portable::hll::{HllSketch, HllVariant};
use asap_sketchlib::{
    CountMin, DataInput, EnsembleSketch, FastPath, HashSketchEnsemble, HyperLogLog, HyperLogLogHIP,
    SetAggregator, Vector2D,
};

#[test]
fn hll_variants_checkpoints_and_shard_merge() {
    let checkpoints = [10_000u64, 100_000, 1_000_000];
    let mut classic = HyperLogLog::<asap_sketchlib::Classic>::new();
    let mut ertl = HyperLogLog::<asap_sketchlib::ErtlMLE>::new();
    let mut hip = HyperLogLogHIP::new();
    // Even/odd shard split for the merge leg of the test.
    let mut classic_even = HyperLogLog::<asap_sketchlib::Classic>::new();
    let mut classic_odd = HyperLogLog::<asap_sketchlib::Classic>::new();

    let mut seen = 0u64;
    for &target in &checkpoints {
        while seen < target {
            let d = DataInput::U64(seen);
            classic.insert(&d);
            ertl.insert(&d);
            hip.insert(&d);
            if seen % 2 == 0 {
                classic_even.insert(&d);
            } else {
                classic_odd.insert(&d);
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
    }

    classic_even.merge(&classic_odd);
    let merged = classic_even.estimate() as f64;
    assert_between(
        merged,
        1_000_000.0 * 0.98,
        1_000_000.0 * 1.02,
        "Classic shard-merge @ 1e6",
    );
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

#[test]
fn ensemble_layer_mixed_cms_and_hll() {
    let cms = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 4096);
    let ertl = HyperLogLog::<asap_sketchlib::ErtlMLE>::new();
    let mut ens: HashSketchEnsemble =
        HashSketchEnsemble::new(vec![EnsembleSketch::from(cms), EnsembleSketch::from(ertl)])
            .expect("ensemble");

    // Dominant head: 6000 copies of key 0; tail: 15k uniform over 3000 keys.
    let mut distinct = std::collections::HashSet::new();
    let mut truth_hot0 = 0i64;
    for k in common::uniform_u64(15_000, 3000, 2103) {
        ens.insert(&DataInput::I64(k as i64));
        distinct.insert(k as i64);
    }
    for _ in 0..6000 {
        ens.insert(&DataInput::I64(0));
        distinct.insert(0);
        truth_hot0 += 1;
    }

    // CMS cell: one-sided frequency estimate for the dominant key.
    let cm_est = ens.estimate(0, &DataInput::I64(0)).expect("cms estimate");
    assert!(
        cm_est >= truth_hot0 as f64 && cm_est <= truth_hot0 as f64 * 3.0,
        "ensemble CMS estimate {cm_est} vs true {truth_hot0} (must be one-sided)"
    );

    // HLL cell: shared-hash cardinality within 3%.
    let card = ens.cardinality(1).expect("hll cardinality");
    let t = distinct.len() as f64;
    assert_between(card, t * 0.97, t * 1.03, "ensemble HLL cardinality");
}
