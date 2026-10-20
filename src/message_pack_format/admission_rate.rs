//! The optional ASAPv1 metadata key `admission_rate`
//! (`docs/asapv1_wire_format.md` Section 2): the probability `p` with which a
//! producer admitted each update. HLL, Count-Min, Count Sketch and DDSketch
//! carry it, as the last key of the map, and only when `p < 1`.

use serde::de::Error;
use serde::{Deserialize, Deserializer};

use super::wire_key::exact;

/// The rate of a sketch that admits every update.
pub(crate) const fn admit_all() -> f64 {
    1.0
}

/// The key's value for a sketch holding `p`: absent at `p = 1`. Fails when `p`
/// is outside `(0, 1]`.
pub(crate) fn to_key(p: f64) -> Result<Option<f64>, String> {
    if !(p > 0.0 && p <= 1.0) {
        return Err(format!("admission_rate {p} is not in (0, 1]"));
    }
    Ok((p < 1.0).then_some(p))
}

/// The rate a key's value names: `1` when the key is absent. Fails unless
/// `0 < p < 1`.
pub(crate) fn from_key(key: Option<f64>) -> Result<f64, String> {
    match key {
        None => Ok(admit_all()),
        Some(p) if p > 0.0 && p < 1.0 => Ok(p),
        Some(p) => Err(format!("admission_rate {p} is not in (0, 1)")),
    }
}

/// Checks that two states admit at the same rate.
pub(crate) fn check_equal(ours: f64, theirs: f64) -> Result<(), String> {
    if ours != theirs {
        return Err(format!("admission_rate {theirs} differs from {ours}"));
    }
    Ok(())
}

/// Checks that a sketch nested in another kind's state admits every update.
pub(crate) fn check_admits_all(p: f64) -> Result<(), String> {
    if p != admit_all() {
        return Err(format!(
            "a nested sketch carries no admission_rate, but this one holds {p}"
        ));
    }
    Ok(())
}

/// Reads a present `admission_rate` key, from msgpack float64 alone.
pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<f64>, D::Error> {
    exact(deserializer).map(Some)
}

/// Reads a sketch's rate from its derived serde form: inside `(0, 1]`.
pub(crate) fn deserialize_rate<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<f64, D::Error> {
    let p = f64::deserialize(deserializer)?;
    to_key(p).map_err(D::Error::custom)?;
    Ok(p)
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use crate::input::HydraCounter;
    use crate::message_pack_format::envelope;
    use crate::sketch_framework::eh_bucket_sketch::wire::{SketchState, rebuild_sketch};
    use crate::sketches::CountSketchDelta;
    use crate::*;

    type Hll = HyperLogLog<Classic>;
    type HllRegisterDelta = HyperLogLogDelta<Classic, HllRegistersP14>;
    type Cms = CountMinSketch<Vector2D<i64>, RegularPath>;
    type CmsDelta = CountMinSketchDelta<i64, RegularPath>;
    type Cs = CountSketch<Vector2D<i64>, RegularPath>;
    type CsDelta = CountSketchDelta<i64, RegularPath>;

    type Encoded = Result<Vec<u8>, rmp_serde::encode::Error>;

    /// Every kind that carries the key, state and delta, encoded at rate `p`.
    fn encode_all(p: f64) -> Vec<(&'static str, Encoded)> {
        let dd_delta = DdSketchDelta::new(0.01, vec![0], vec![1], vec![], vec![], 0).unwrap();
        vec![
            ("HLL", Hll::default().with_admission_rate(p).to_asapv1()),
            (
                "HLL HIP",
                HyperLogLogHip::default().with_admission_rate(p).to_asapv1(),
            ),
            (
                "HLL delta",
                HllRegisterDelta::new(vec![0], vec![1])
                    .unwrap()
                    .with_admission_rate(p)
                    .to_asapv1(),
            ),
            (
                "CMS",
                Cms::with_dimensions(2, 3)
                    .with_admission_rate(p)
                    .to_asapv1(),
            ),
            (
                "CMS delta",
                CmsDelta::new(2, 3, vec![0], vec![1])
                    .unwrap()
                    .with_admission_rate(p)
                    .to_asapv1(),
            ),
            (
                "CS",
                Cs::with_dimensions(2, 4).with_admission_rate(p).to_asapv1(),
            ),
            (
                "CS delta",
                CsDelta::new(2, 4, vec![0], vec![1])
                    .unwrap()
                    .with_admission_rate(p)
                    .to_asapv1(),
            ),
            (
                "DDSketch",
                DdSketch::new(0.01).with_admission_rate(p).to_asapv1(),
            ),
            (
                "DDSketch delta",
                dd_delta.with_admission_rate(p).to_asapv1(),
            ),
        ]
    }

    /// Whether `bytes` decode as the kind `label` names.
    fn decodes(label: &str, bytes: &[u8]) -> bool {
        match label {
            "HLL" => Hll::from_asapv1(bytes).is_ok(),
            "HLL HIP" => HyperLogLogHip::from_asapv1(bytes).is_ok(),
            "HLL delta" => HllRegisterDelta::from_asapv1(bytes).is_ok(),
            "CMS" => Cms::from_asapv1(bytes).is_ok(),
            "CMS delta" => CmsDelta::from_asapv1(bytes).is_ok(),
            "CS" => Cs::from_asapv1(bytes).is_ok(),
            "CS delta" => CsDelta::from_asapv1(bytes).is_ok(),
            "DDSketch" => DdSketch::from_asapv1(bytes).is_ok(),
            "DDSketch delta" => DdSketchDelta::from_asapv1(bytes).is_ok(),
            _ => unreachable!("{label}"),
        }
    }

    /// Decodes `bytes` as the kind `label` names and checks the rate `0.5` came
    /// back: a state reports it; a delta merges into a `0.5` state alone.
    fn assert_rate_survives_decode(label: &str, bytes: &[u8]) {
        let merges = |at_half: Result<(), String>, at_one: Result<(), String>| {
            assert!(at_half.is_ok() && at_one.is_err(), "{label}");
        };
        match label {
            "HLL" => assert_eq!(Hll::from_asapv1(bytes).unwrap().admission_rate(), 0.5),
            "HLL HIP" => assert_eq!(
                HyperLogLogHip::from_asapv1(bytes).unwrap().admission_rate(),
                0.5
            ),
            "CMS" => assert_eq!(Cms::from_asapv1(bytes).unwrap().admission_rate(), 0.5),
            "CS" => assert_eq!(Cs::from_asapv1(bytes).unwrap().admission_rate(), 0.5),
            "DDSketch" => assert_eq!(DdSketch::from_asapv1(bytes).unwrap().admission_rate(), 0.5),
            "HLL delta" => {
                let delta = HllRegisterDelta::from_asapv1(bytes).unwrap();
                merges(
                    Hll::default().with_admission_rate(0.5).merge_delta(&delta),
                    Hll::default().merge_delta(&delta),
                );
            }
            "CMS delta" => {
                let delta = CmsDelta::from_asapv1(bytes).unwrap();
                let state = || Cms::with_dimensions(2, 3);
                merges(
                    state().with_admission_rate(0.5).merge_delta(&delta),
                    state().merge_delta(&delta),
                );
            }
            "CS delta" => {
                let delta = CsDelta::from_asapv1(bytes).unwrap();
                let state = || Cs::with_dimensions(2, 4);
                merges(
                    state().with_admission_rate(0.5).merge_delta(&delta),
                    state().merge_delta(&delta),
                );
            }
            "DDSketch delta" => {
                let delta = DdSketchDelta::from_asapv1(bytes).unwrap();
                let state = || DdSketch::new(0.01);
                merges(
                    state().with_admission_rate(0.5).merge_delta(&delta),
                    state().merge_delta(&delta),
                );
            }
            _ => unreachable!("{label}"),
        }
    }

    /// The msgpack encoding of the key followed by `value`, already encoded.
    fn key_then(value: &[u8]) -> Vec<u8> {
        let key = b"admission_rate";
        [&[0xa0 | key.len() as u8][..], key, value].concat()
    }

    fn float64(value: f64) -> Vec<u8> {
        [&[0xcb][..], &value.to_be_bytes()].concat()
    }

    #[test]
    fn every_carrier_closes_its_map_with_the_key_only_below_one() {
        for ((label, unsampled), (_, sampled)) in encode_all(1.0).into_iter().zip(encode_all(0.5)) {
            let (unsampled, sampled) = (unsampled.unwrap(), sampled.unwrap());
            let (_, plain, _) = envelope::split(&unsampled).unwrap();
            let (_, keyed, _) = envelope::split(&sampled).unwrap();
            let want = [&[plain[0] + 1][..], &plain[1..], &key_then(&float64(0.5))].concat();
            assert_eq!(keyed, want.as_slice(), "{label}");
            assert_rate_survives_decode(label, &sampled);
        }
    }

    #[test]
    fn every_carrier_refuses_to_encode_a_rate_outside_zero_to_one() {
        for p in [0.0, -0.5, 1.5, f64::INFINITY, f64::NAN] {
            for (label, encoded) in encode_all(p) {
                assert!(encoded.is_err(), "{label} at {p}");
            }
        }
    }

    #[test]
    fn every_carrier_decodes_only_a_float64_rate_strictly_inside_zero_to_one() {
        let float32 = [&[0xca][..], &0.5f32.to_be_bytes()].concat();
        let refused = [
            float64(1.0),
            float64(0.0),
            float64(-0.5),
            float64(1.5),
            float64(f64::NAN),
            float32,
            vec![0x00],
            vec![0xc0],
        ];
        for (label, encoded) in encode_all(0.5) {
            let encoded = encoded.unwrap();
            let (kind_id, metadata, payload) = envelope::split(&encoded).unwrap();
            let tail = key_then(&float64(0.5));
            let head = &metadata[..metadata.len() - tail.len()];
            for value in &refused {
                let metadata = [head, &key_then(value)].concat();
                let bytes = envelope::encode(kind_id, &metadata, payload);
                assert!(!decodes(label, &bytes), "{label} with {value:02x?}");
            }
        }
    }

    #[test]
    fn states_merge_only_at_equal_rates() {
        let panics = |merge: &mut dyn FnMut()| catch_unwind(AssertUnwindSafe(merge)).is_err();

        let hll = Hll::default().with_admission_rate(0.5);
        assert!(!panics(&mut || hll.clone().merge(&hll)));
        assert!(panics(&mut || Hll::default().merge(&hll)));

        let cms = Cms::with_dimensions(2, 3).with_admission_rate(0.5);
        assert!(!panics(&mut || cms.clone().merge(&cms)));
        assert!(panics(&mut || Cms::with_dimensions(2, 3).merge(&cms)));
        assert!(!panics(&mut || cms.clone().merge_max(&cms)));
        assert!(panics(&mut || Cms::with_dimensions(2, 3).merge_max(&cms)));

        let cs = Cs::with_dimensions(2, 4).with_admission_rate(0.5);
        assert!(!panics(&mut || cs.clone().merge(&cs)));
        assert!(panics(&mut || Cs::with_dimensions(2, 4).merge(&cs)));

        let dd = DdSketch::new(0.01).with_admission_rate(0.5);
        assert!(dd.clone().merge(&dd).is_ok());
        assert!(DdSketch::new(0.01).merge(&dd).is_err());

        let plain_cm = || CountMinSketch::<Vector2D<i32>, FastPath>::with_dimensions(2, 4);
        let sampled_cm = plain_cm().with_admission_rate(0.5);
        let err = EhBucketSketch::CountMinSketch(plain_cm())
            .merge(&EhBucketSketch::CountMinSketch(sampled_cm.clone()));
        assert!(err.unwrap_err().contains("admission_rate"));
        let err =
            EhBucketSketch::DdSketch(DdSketch::new(0.01)).merge(&EhBucketSketch::DdSketch(dd));
        assert!(err.unwrap_err().contains("admission_rate"));
        let err = HydraCounter::CountMinSketch(plain_cm())
            .merge(&HydraCounter::CountMinSketch(sampled_cm));
        assert!(err.unwrap_err().contains("admission_rate"));
    }

    #[test]
    fn a_delta_merges_only_at_the_state_rate_and_a_refused_one_changes_nothing() {
        let delta = HllRegisterDelta::new(vec![0], vec![1]).unwrap();
        let mut hll = Hll::default().with_admission_rate(0.5);
        assert!(hll.merge_delta(&delta).is_err());
        assert_eq!(hll.registers_as_slice()[0], 0);
        assert!(hll.merge_delta(&delta.with_admission_rate(0.5)).is_ok());
        assert_eq!(hll.registers_as_slice()[0], 1);

        let delta = CmsDelta::new(2, 3, vec![0], vec![1]).unwrap();
        let mut cms = Cms::with_dimensions(2, 3).with_admission_rate(0.5);
        assert!(cms.merge_delta(&delta).is_err());
        assert_eq!(cms.as_storage().as_slice()[0], 0);
        assert!(cms.merge_delta(&delta.with_admission_rate(0.5)).is_ok());
        assert_eq!(cms.as_storage().as_slice()[0], 1);

        let delta = CsDelta::new(2, 4, vec![0], vec![1]).unwrap();
        let mut cs = Cs::with_dimensions(2, 4);
        assert!(
            cs.merge_delta(&delta.clone().with_admission_rate(0.5))
                .is_err()
        );
        assert_eq!(cs.as_storage().as_slice()[0], 0);
        assert!(cs.merge_delta(&delta).is_ok());
        assert_eq!(cs.as_storage().as_slice()[0], 1);

        let delta = DdSketchDelta::new(0.01, vec![0], vec![1], vec![], vec![], 0).unwrap();
        let mut dd = DdSketch::new(0.01).with_admission_rate(0.5);
        assert!(dd.merge_delta(&delta).is_err());
        assert_eq!(dd.get_count(), 0);
        assert!(dd.merge_delta(&delta.with_admission_rate(0.5)).is_ok());
        assert_eq!(dd.get_count(), 1);
    }

    #[test]
    fn a_sketch_nested_in_another_kind_holds_no_rate() {
        let cm = || CountMinSketch::<Vector2D<i32>, FastPath>::with_dimensions(2, 4);
        let cs = || CountSketch::<Vector2D<i32>, FastPath>::with_dimensions(2, 4);
        let hll = HyperLogLog::<Ertl>::default;
        let dd = || DdSketch::new(0.01);

        let lists = |p: f64| {
            [
                EhBucketSketch::CountMinSketch(cm().with_admission_rate(p)),
                EhBucketSketch::CountSketch(cs().with_admission_rate(p)),
                EhBucketSketch::DdSketch(dd().with_admission_rate(p)),
                EhBucketSketch::Hll(hll().with_admission_rate(p)),
            ]
        };
        for (plain, sampled) in lists(1.0).iter().zip(lists(0.5).iter()) {
            assert!(plain.to_asapv1().is_ok());
            assert!(sampled.to_asapv1().is_err());
        }
        for (p, rebuilds) in [(1.0, true), (0.5, false)] {
            let bytes = cm().with_admission_rate(p).to_asapv1().unwrap();
            let (kind_id, descriptor, state) = envelope::split(&bytes).unwrap();
            let triple = SketchState {
                kind_id: kind_id.to_vec(),
                descriptor: descriptor.to_vec(),
                state: state.to_vec(),
            };
            assert_eq!(rebuild_sketch(&triple).is_ok(), rebuilds, "{p}");
        }

        let counters = |p: f64| {
            [
                HydraCounter::CountMinSketch(cm().with_admission_rate(p)),
                HydraCounter::CountSketch(cs().with_admission_rate(p)),
                HydraCounter::Hll(hll().with_admission_rate(p)),
            ]
        };
        for (plain, sampled) in counters(1.0).into_iter().zip(counters(0.5)) {
            let grid = |counter| Hydra::with_schema(1, 2, ["k"], counter).unwrap();
            assert!(grid(plain.clone()).to_asapv1().is_ok());
            assert!(grid(sampled.clone()).to_asapv1().is_err());
            let mut one_cell = grid(plain);
            one_cell.sketches.as_mut_slice()[1] = sampled;
            assert!(one_cell.to_asapv1().is_err());
        }

        let mut cms_heap = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(2, 4, 3);
        assert!(cms_heap.to_asapv1().is_ok());
        *cms_heap.count_min_sketch_mut() = cms_heap.count_min_sketch().clone().with_admission_rate(0.5);
        assert!(cms_heap.to_asapv1().is_err());

        let mut cs_heap = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(2, 4, 3);
        assert!(cs_heap.to_asapv1().is_ok());
        *cs_heap.count_sketch_mut() = cs_heap.count_sketch().clone().with_admission_rate(0.5);
        assert!(cs_heap.to_asapv1().is_err());

        let mut elastic: ElasticSketch = ElasticSketch::with_heavy_and_light(4, 2, 4);
        assert!(elastic.to_asapv1().is_ok());
        elastic.light = elastic.light.clone().with_admission_rate(0.5);
        assert!(elastic.to_asapv1().is_err());
    }
}
