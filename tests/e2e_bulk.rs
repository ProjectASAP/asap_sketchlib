//! Dedicated harness E2E for KLL bulk_update vs loop.
//! Independent from the main e2e_quantiles suite — uses the same
//! seeded generators and NumericTruth rank bands to prove bulk is
//! exactly equivalent to repeated update across every distribution.

mod common;

use asap_sketchlib::{DataInput, KLL, KLLDynamic};
use common::{NumericTruth, assert_in_rank_band, exponential_f64, log_uniform_f64, normal_f64};

const QS: [f64; 5] = [0.1, 0.25, 0.5, 0.75, 0.9];
const RANK_TOL: f64 = 0.03;

fn bulk_vs_loop_seeded_impl<F>(label: &str, values: Vec<f64>, make: F)
where
    F: Fn() -> KLL,
{
    let truth = NumericTruth::new(values.clone());

    // Loop path
    let mut via_loop = make();
    for v in &values {
        via_loop.update(v);
    }

    // Bulk path — same seed, same input slice
    let mut via_bulk = make();
    via_bulk.bulk_update(&values);

    // Byte-identical for seeded KLL
    assert_eq!(
        via_loop.serialize_to_bytes().unwrap(),
        via_bulk.serialize_to_bytes().unwrap(),
        "{label}: seeded bulk vs loop bytes diverged"
    );
    assert_eq!(
        via_loop.count(),
        via_bulk.count(),
        "{label}: count diverged"
    );

    // Both satisfy harness rank bands — proves bulk doesn't break accuracy
    for &q in &QS {
        assert_in_rank_band(
            via_loop.quantile(q),
            &truth,
            q,
            RANK_TOL,
            &format!("{label} loop"),
        );
        assert_in_rank_band(
            via_bulk.quantile(q),
            &truth,
            q,
            RANK_TOL,
            &format!("{label} bulk"),
        );
        // And bulk vs loop quantiles are identical (seeded determinism)
        assert_eq!(
            via_loop.quantile(q).to_bits(),
            via_bulk.quantile(q).to_bits(),
            "{label} q={q}: bulk vs loop quantile bits differ"
        );
    }
}

#[test]
fn kll_bulk_vs_loop_normal() {
    let vals = normal_f64(20_000, 500.0, 80.0, 9001)
        .into_iter()
        .filter(|v| v.is_finite() && *v > 0.0)
        .collect::<Vec<_>>();
    bulk_vs_loop_seeded_impl("KLL normal 20k", vals, || KLL::init_with_seed(200, 8, 42));
}

#[test]
fn kll_bulk_vs_loop_uniform() {
    let vals2 = {
        let mut v = Vec::new();
        for x in common::uniform_u64(20_000, 1_000_000, 7002) {
            v.push(x as f64);
        }
        v
    };
    bulk_vs_loop_seeded_impl("KLL uniform 20k", vals2, || KLL::init_with_seed(200, 8, 43));
    // Also tiny sequential edge (no compaction)
    let seq10: Vec<f64> = (0..10).map(|i| i as f64 * 1.7 + 11.0).collect();
    bulk_vs_loop_seeded_impl("KLL seq 10", seq10, || KLL::init_with_seed(200, 8, 44));
}

#[test]
fn kll_bulk_vs_loop_exponential() {
    let vals = exponential_f64(20_000, 1e-3, 3007);
    bulk_vs_loop_seeded_impl("KLL exponential 20k", vals, || {
        KLL::init_with_seed(200, 8, 45)
    });
}

#[test]
fn kll_bulk_vs_loop_adversarial_log_uniform() {
    // DDSketch-adversarial gamma, but KLL must also stay within rank bands
    let alpha = 0.01;
    let gamma = (1.0 + alpha) / (1.0 - alpha);
    let vals = log_uniform_f64(15_000, gamma, 5..40, 3005);
    bulk_vs_loop_seeded_impl("KLL log_uniform 15k", vals, || {
        KLL::init_with_seed(200, 8, 46)
    });
}

#[test]
fn kll_bulk_empty_and_single_are_noop() {
    let mut sk = KLL::init_with_seed(200, 8, 99);
    for &v in &[1.0, 2.0, 3.0] {
        sk.update(&v);
    }
    let cnt_before = sk.count();
    let q_before = sk.quantile(0.5);
    let bytes_before = sk.serialize_to_bytes().unwrap();
    sk.bulk_update(&[]);
    assert_eq!(sk.count(), cnt_before);
    assert_eq!(sk.quantile(0.5).to_bits(), q_before.to_bits());
    assert_eq!(sk.serialize_to_bytes().unwrap(), bytes_before);

    // Single-element bulk must equal single update
    let mut a = KLL::init_with_seed(200, 8, 100);
    let mut b = KLL::init_with_seed(200, 8, 100);
    a.update(&42.0);
    b.bulk_update(&[42.0]);
    assert_eq!(
        a.serialize_to_bytes().unwrap(),
        b.serialize_to_bytes().unwrap()
    );
}

#[test]
fn kll_bulk_data_input_batch_matches_loop() {
    let vals = normal_f64(5_000, 100.0, 20.0, 8001);
    let di_loop: Vec<DataInput> = vals.iter().map(|v| DataInput::F64(*v)).collect();
    let di_bulk = di_loop.clone();

    let mut via_loop = KLL::init_with_seed(200, 8, 55);
    for v in &di_loop {
        via_loop.update_data_input(v).unwrap();
    }
    let mut via_bulk = KLL::init_with_seed(200, 8, 55);
    via_bulk.bulk_update_data_input(&di_bulk).unwrap();
    assert_eq!(
        via_loop.serialize_to_bytes().unwrap(),
        via_bulk.serialize_to_bytes().unwrap(),
        "DataInput bulk vs loop bytes diverged"
    );

    // Non-numeric stops on first error, prefix preserved
    let bad = vec![
        DataInput::F64(1.0),
        DataInput::String("x".into()),
        DataInput::F64(2.0),
    ];
    let mut sk = KLL::init_with_seed(200, 8, 56);
    assert!(sk.bulk_update_data_input(&bad).is_err());
    assert_eq!(sk.count(), 1);

    let empty: Vec<DataInput> = vec![];
    let mut sk2 = KLL::init_with_seed(200, 8, 57);
    sk2.bulk_update_data_input(&empty).unwrap();
    assert_eq!(sk2.count(), 0);
}

#[test]
fn kll_dynamic_bulk_vs_loop_with_tolerance() {
    // KLLDynamic is wall-clock seeded (non-deterministic), so we only check
    // count within 5% and rank bands, not byte-identical.
    let vals = normal_f64(10_000, 0.0, 100.0, 9002);
    let truth = NumericTruth::new(vals.clone());

    let mut via_loop = KLLDynamic::<f64>::init_kll(200);
    for v in &vals {
        via_loop.update(v);
    }
    let mut via_bulk = KLLDynamic::<f64>::init_kll(200);
    via_bulk.bulk_update(&vals);

    let ca = via_loop.count() as f64;
    let cb = via_bulk.count() as f64;
    assert!(
        (ca - cb).abs() / ca < 0.05,
        "KLLDynamic count loop {ca} vs bulk {cb}"
    );

    for &q in &QS {
        assert_in_rank_band(via_loop.quantile(q), &truth, q, RANK_TOL, "KLLDynamic loop");
        assert_in_rank_band(via_bulk.quantile(q), &truth, q, RANK_TOL, "KLLDynamic bulk");
    }
}
