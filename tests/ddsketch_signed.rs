use asap_sketchlib::DDSketch;

// Negative and zero observations are counted.
#[test]
fn signed_samples_and_zero_are_retained() {
    let mut core = DDSketch::new(0.01);
    for v in [-20.0, -2.0, 0.0, 0.0, 2.0, 20.0] {
        core.add(&v);
    }
    assert_eq!(core.get_count(), 6);
    assert_eq!(core.get_value_at_quantile(0.5), Some(0.0));
    assert!(core.get_value_at_quantile(0.0).unwrap() < -19.0);
    assert!(core.get_value_at_quantile(1.0).unwrap() > 19.0);
}

// Signed snapshots survive ASAPv1 and merge.
#[test]
fn signed_merge_and_serialization() {
    let mut c = DDSketch::new(0.01);
    for v in [-4.0, 0.0, 4.0] {
        c.add(&v);
    }
    let mut c2 = DDSketch::deserialize_from_bytes(&c.serialize_to_bytes().unwrap()).unwrap();
    c2.merge(&c).unwrap();
    assert_eq!(c2.get_count(), 6);
    assert_eq!(c2.get_value_at_quantile(0.5), Some(0.0));
}

#[test]
fn signed_order_statistics_obey_relative_bound() {
    let mut values: Vec<f64> = (-300i32..=300)
        .map(|i| {
            if i == 0 {
                0.
            } else {
                (i as f64).signum() * 10f64.powf(i.abs() as f64 / 50. - 3.)
            }
        })
        .collect();
    values.sort_by(f64::total_cmp);
    let mut core = DDSketch::new(0.01);
    for v in &values {
        core.try_add(v).unwrap();
    }
    for i in 0..=1000 {
        let q = i as f64 / 1000.;
        let ci = ((q * values.len() as f64).ceil() as usize).max(1) - 1;
        let (got, want) = (core.get_value_at_quantile(q).unwrap(), values[ci]);
        assert!(
            (got - want).abs() <= 0.010000001 * want.abs(),
            "q={q}, got={got}, want={want}"
        );
    }
}

#[test]
fn zero_only_special_values_and_octo_flush() {
    use asap_sketchlib::sketch_framework::octo::DdWorkerSketch;
    let mut core = DDSketch::new(0.01);
    for v in [0., -0., 0.] {
        core.try_add(&v).unwrap();
    }
    for v in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(core.try_add(&v).is_err());
    }
    assert_eq!(core.get_count(), 3);
    let mut worker = DdWorkerSketch::new(0.01);
    let mut replay = DDSketch::new(0.01);
    for v in [-10., -10., -10., -0.1, 0., 0., 0.1, 10.] {
        worker.add_emit_delta(v, 3, &mut |delta| replay.apply_delta(delta));
    }
    worker.flush(&mut |delta| replay.apply_delta(delta));
    assert_eq!(worker.held_back(), 0);
    assert_eq!(replay.get_count(), 8);
    assert!(replay.get_value_at_quantile(0.1).unwrap() < 0.);
    assert_eq!(replay.get_value_at_quantile(0.6), Some(0.));
    assert!(replay.get_value_at_quantile(0.9).unwrap() > 0.);
}

#[test]
fn tiny_values_are_counted_in_zero_bucket_and_signed_serde_survives() {
    let mut core = DDSketch::new(0.01);
    let tiny = f64::MIN_POSITIVE / 2.;
    for v in [-tiny, tiny, 0.] {
        core.try_add(&v).unwrap();
    }
    assert_eq!(core.get_count(), 3);
    assert_eq!(core.zero_count(), 3);
    core.try_add(&-3.).unwrap();
    let decoded: DDSketch = rmp_serde::from_slice(&rmp_serde::to_vec(&core).unwrap()).unwrap();
    assert_eq!(decoded.get_count(), 4);
    assert_eq!(decoded.sum(), core.sum());
    assert_eq!(decoded.min(), core.min());
    assert_eq!(decoded.max(), core.max());
}
