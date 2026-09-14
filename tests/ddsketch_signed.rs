use asap_sketchlib::message_pack_format::MessagePackCodec;
use asap_sketchlib::{DDSketch, DdSketch};

// Both public implementations must count negative and zero observations.
#[test]
fn signed_samples_and_zero_are_retained() {
    let mut portable = DdSketch::new(0.01);
    let mut core = DDSketch::new(0.01);
    for v in [-20.0, -2.0, 0.0, 0.0, 2.0, 20.0] {
        portable.update(v);
        core.add(&v);
    }
    assert_eq!(portable.total_count(), 6);
    assert_eq!(core.get_count(), 6);
    assert_eq!(portable.quantile(0.5), Some(0.0));
    assert_eq!(core.get_value_at_quantile(0.5), Some(0.0));
    assert!(portable.quantile(0.0).unwrap() < -19.0);
    assert!(portable.quantile(1.0).unwrap() > 19.0);
}

// Signed snapshots must survive both codecs and merge with positive-only state.
#[test]
fn signed_merge_and_serialization() {
    let mut p = DdSketch::new(0.01);
    let mut c = DDSketch::new(0.01);
    for v in [-4.0, 0.0, 4.0] {
        p.update(v);
        c.add(&v);
    }
    let mut p2 = DdSketch::from_msgpack(&p.to_msgpack().unwrap()).unwrap();
    let mut c2 = DDSketch::deserialize_from_bytes(&c.serialize_to_bytes().unwrap()).unwrap();
    p2.merge(&p).unwrap();
    c2.merge(&c).unwrap();
    assert_eq!(p2.total_count(), 6);
    assert_eq!(c2.get_count(), 6);
    assert_eq!(p2.quantile_interpolated(0.5), Some(0.0));
    assert_eq!(c2.get_value_at_quantile(0.5), Some(0.0));
}

#[test]
fn signed_delta_and_proto_round_trips() {
    use prost::Message;
    let mut base = DdSketch::new(0.01);
    for v in [-100., -1., 0., 1., 100.] {
        base.update(v);
    }
    let mut current = base.clone();
    for v in [-1000., -0.1, 0., 0., 0.1, 1000.] {
        current.update(v);
    }
    let mut protobuf = base.clone();
    protobuf
        .apply_delta_bytes(&current.compute_delta(&base, 1))
        .unwrap();
    let mut msgpack = base;
    msgpack
        .apply_delta_msgpack_bytes(&current.compute_delta_msgpack(&msgpack, 1))
        .unwrap();
    let bytes = current.to_proto().encode_to_vec();
    let snapshot = DdSketch::from_proto(
        asap_sketchlib::proto::sketchlib::DdSketchState::decode(bytes.as_slice()).unwrap(),
    );
    for restored in [protobuf, msgpack, snapshot] {
        assert_eq!(restored.total_count(), 11);
        for q in [0., 0.1, 0.5, 0.9, 1.] {
            assert!((restored.quantile(q).unwrap() - current.quantile(q).unwrap()).abs() < 1e-9);
        }
    }
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
    let mut portable = DdSketch::new(0.01);
    let mut core = DDSketch::new(0.01);
    for v in &values {
        portable.try_update(*v).unwrap();
        core.try_add(v).unwrap();
    }
    for i in 0..=1000 {
        let q = i as f64 / 1000.;
        let pi = (q * (values.len() - 1) as f64).floor() as usize;
        let ci = ((q * values.len() as f64).ceil() as usize).max(1) - 1;
        for (got, want) in [
            (portable.quantile(q).unwrap(), values[pi]),
            (core.get_value_at_quantile(q).unwrap(), values[ci]),
        ] {
            assert!(
                (got - want).abs() <= 0.010000001 * want.abs(),
                "q={q}, got={got}, want={want}"
            );
        }
    }
}

#[test]
fn zero_only_special_values_and_octo_flush() {
    use asap_sketchlib::sketch_framework::octo::DdWorkerSketch;
    let mut portable = DdSketch::new(0.01);
    let mut core = DDSketch::new(0.01);
    for v in [0., -0., 0.] {
        portable.try_update(v).unwrap();
        core.try_add(&v).unwrap();
    }
    for v in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(portable.try_update(v).is_err());
        assert!(core.try_add(&v).is_err());
    }
    assert_eq!(portable.total_count(), 3);
    assert_eq!(core.get_count(), 3);
    assert_eq!(portable.quantile_interpolated(0.9), Some(0.));
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
    let mut portable = DdSketch::new(0.01);
    let tiny = f64::MIN_POSITIVE / 2.;
    for v in [-tiny, tiny, 0.] {
        core.try_add(&v).unwrap();
        portable.try_update(v).unwrap();
    }
    assert_eq!(core.get_count(), 3);
    assert_eq!(core.zero_count(), 3);
    assert_eq!(portable.total_count(), 3);
    assert_eq!(portable.quantile_interpolated(0.5), Some(0.));
    let named: DdSketch =
        rmp_serde::from_slice(&rmp_serde::to_vec_named(&portable).unwrap()).unwrap();
    assert_eq!(named.total_count(), 3);
    assert_eq!(named.quantile(0.5), Some(0.));
    core.try_add(&-3.).unwrap();
    let decoded: DDSketch = rmp_serde::from_slice(&rmp_serde::to_vec(&core).unwrap()).unwrap();
    assert_eq!(decoded.get_count(), 4);
    assert_eq!(decoded.sum(), core.sum());
    assert_eq!(decoded.min(), core.min());
    assert_eq!(decoded.max(), core.max());
}

#[test]
fn cross_zero_interpolation_is_not_a_relative_error_certificate() {
    let mut sketch = DdSketch::new(0.01);
    sketch.update(-1.);
    sketch.update(1.001);
    let exact = (-1. + 1.001) / 2.;
    let estimated = sketch.quantile_interpolated(0.5).unwrap();
    // Both magnitudes land in the same bucket, so their representatives cancel.
    assert_eq!(estimated, 0.);
    assert!((estimated - exact).abs() > 0.01 * exact.abs());
}
