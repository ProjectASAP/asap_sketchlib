//! Sketch enums used by the exponential histogram framework.

use crate::asapv1::admission_rate;
use crate::{DataInput, Vector2D};
use serde::{Deserialize, Serialize};

use super::super::sketches::*;
use super::UnivMon;
use crate::sketches::countsketch_l2_norm::CountSketchWithL2Norm;

pub(crate) mod wire;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Norm choices used by EH merge policies.
pub enum SketchNorm {
    /// L1-based merge policy.
    L1,
    /// L2-based merge policy.
    L2,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Erased sketch variants supported by exponential histograms.
pub enum EhBucketSketch {
    /// Count-Min sketch.
    CountMinSketch(CountMinSketch<Vector2D<i32>, FastPath>),
    /// CocoSketch.
    CocoSketch(CocoSketch),
    /// CountSketchWithL2Norm sketch.
    CountSketchWithL2Norm(CountSketchWithL2Norm),
    /// Count Sketch.
    CountSketch(CountSketch<Vector2D<i32>, FastPath>),
    /// DDSketch.
    DdSketch(DdSketch),
    /// Elastic sketch.
    ElasticSketch(ElasticSketch),
    /// HyperLogLog.
    Hll(HyperLogLog<Ertl>),
    /// KLL sketch.
    Kll(Kll),
    #[cfg(feature = "experimental")]
    Uniform(UniformSampling),
    // LOCHER(LocherSketch),
    /// UnivMon sketch.
    UnivMon(UnivMon),
}

impl EhBucketSketch {
    /// Returns whether this sketch supports the requested merge norm.
    pub fn supports_norm(&self, norm: SketchNorm) -> bool {
        match self {
            EhBucketSketch::CountSketchWithL2Norm(_) | EhBucketSketch::UnivMon(_) => {
                norm == SketchNorm::L2
            }
            EhBucketSketch::CountMinSketch(_)
            | EhBucketSketch::CocoSketch(_)
            | EhBucketSketch::CountSketch(_)
            | EhBucketSketch::DdSketch(_)
            | EhBucketSketch::ElasticSketch(_)
            | EhBucketSketch::Hll(_)
            | EhBucketSketch::Kll(_) => norm == SketchNorm::L1,
            #[cfg(feature = "experimental")]
            EhBucketSketch::Uniform(_) => norm == SketchNorm::L1,
        }
    }

    pub(crate) fn eh_l2_mass(&self) -> Option<f64> {
        match self {
            EhBucketSketch::CountSketchWithL2Norm(sketch) => Some(sketch.l2_squared()),
            EhBucketSketch::UnivMon(sketch) => {
                let l2 = sketch.estimate_l2();
                Some(l2 * l2)
            }
            _ => None,
        }
    }

    /// Insert a value into the sketch
    pub fn update(&mut self, val: &DataInput) {
        match self {
            EhBucketSketch::CountMinSketch(sketch) => sketch.update(val),
            EhBucketSketch::CocoSketch(sketch) => match val {
                DataInput::Str(s) => sketch.update_weighted(s, 1),
                DataInput::String(s) => sketch.update_weighted(s.as_str(), 1),
                _ => {}
            },
            EhBucketSketch::CountSketchWithL2Norm(sketch) => sketch.update_weighted(val, 1),
            EhBucketSketch::CountSketch(sketch) => sketch.update(val),
            EhBucketSketch::DdSketch(sketch) => {
                let _ = sketch.add_input(val);
            }
            EhBucketSketch::ElasticSketch(sketch) => match val {
                DataInput::String(s) => sketch.update(s.to_string()),
                DataInput::I32(i) => sketch.update(i.to_string()),
                DataInput::I64(i) => sketch.update(i.to_string()),
                DataInput::U32(u) => sketch.update(u.to_string()),
                DataInput::U64(u) => sketch.update(u.to_string()),
                DataInput::F32(f) => sketch.update(f.to_string()),
                DataInput::F64(f) => sketch.update(f.to_string()),
                DataInput::Str(s) => sketch.update(s.to_string()),
                DataInput::Bytes(items) => {
                    let s = String::from_utf8_lossy(items).to_string();
                    sketch.update(s)
                }
                DataInput::I8(i) => sketch.update(i.to_string()),
                DataInput::I16(i) => sketch.update(i.to_string()),
                DataInput::I128(i) => sketch.update(i.to_string()),
                DataInput::Isize(i) => sketch.update(i.to_string()),
                DataInput::U8(u) => sketch.update(u.to_string()),
                DataInput::U16(u) => sketch.update(u.to_string()),
                DataInput::U128(u) => sketch.update(u.to_string()),
                DataInput::Usize(u) => sketch.update(u.to_string()),
            },
            EhBucketSketch::Hll(sketch) => sketch.update(val),
            EhBucketSketch::Kll(sketch) => {
                let _ = sketch.update_data_input(val);
            }
            #[cfg(feature = "experimental")]
            EhBucketSketch::Uniform(sketch) => {
                let _ = sketch.update_input(val);
            }
            EhBucketSketch::UnivMon(sketch) => sketch.update_weighted(val, 1),
        }
    }

    /// Merge another sketch of the same type into this one
    pub fn merge(&mut self, other: &EhBucketSketch) -> Result<(), String> {
        match (self, other) {
            (EhBucketSketch::CountMinSketch(s), EhBucketSketch::CountMinSketch(o)) => {
                admission_rate::check_equal(s.admission_rate(), o.admission_rate())?;
                s.merge(o);
                Ok(())
            }
            (EhBucketSketch::CocoSketch(s), EhBucketSketch::CocoSketch(o)) => {
                s.merge(o);
                Ok(())
            }
            (
                EhBucketSketch::CountSketchWithL2Norm(s),
                EhBucketSketch::CountSketchWithL2Norm(o),
            ) => {
                s.merge(o);
                Ok(())
            }
            (EhBucketSketch::CountSketch(s), EhBucketSketch::CountSketch(o)) => {
                admission_rate::check_equal(s.admission_rate(), o.admission_rate())?;
                s.merge(o);
                Ok(())
            }
            (EhBucketSketch::DdSketch(s), EhBucketSketch::DdSketch(o)) => s.merge(o),
            (EhBucketSketch::ElasticSketch(s), EhBucketSketch::ElasticSketch(o)) => {
                s.merge(o);
                Ok(())
            }
            (EhBucketSketch::Hll(s), EhBucketSketch::Hll(o)) => {
                admission_rate::check_equal(s.admission_rate(), o.admission_rate())?;
                s.merge(o);
                Ok(())
            }
            (EhBucketSketch::Kll(s), EhBucketSketch::Kll(o)) => {
                s.merge(o);
                Ok(())
            }
            #[cfg(feature = "experimental")]
            (EhBucketSketch::Uniform(s), EhBucketSketch::Uniform(o)) => {
                s.merge(o).map_err(String::from)
            }
            (EhBucketSketch::UnivMon(s), EhBucketSketch::UnivMon(o)) => {
                s.merge(o);
                Ok(())
            }
            _ => Err("Cannot merge sketches of different types".to_string()),
        }
    }

    /// Queries the sketch using a sketch-specific interpretation of `key`.
    pub fn query(&self, key: &DataInput) -> Result<f64, &'static str> {
        match (self, key) {
            (EhBucketSketch::CountMinSketch(count_min), _) => Ok(count_min.estimate(key) as f64),
            (EhBucketSketch::CocoSketch(coco), DataInput::Str(s)) => Ok(coco.estimate(s) as f64),
            (EhBucketSketch::CocoSketch(coco), DataInput::String(s)) => {
                Ok(coco.estimate(s.as_str()) as f64)
            }
            (EhBucketSketch::CountSketchWithL2Norm(count_univ), _) => Ok(count_univ.estimate(key)),
            (EhBucketSketch::CountSketch(count_sketch), _) => Ok(count_sketch.estimate(key)),
            (EhBucketSketch::DdSketch(dd), DataInput::I32(i)) => dd
                .get_value_at_quantile(*i as f64)
                .ok_or("DDSketch has no samples"),
            (EhBucketSketch::DdSketch(dd), DataInput::I64(i)) => dd
                .get_value_at_quantile(*i as f64)
                .ok_or("DDSketch has no samples"),
            (EhBucketSketch::DdSketch(dd), DataInput::U32(u)) => dd
                .get_value_at_quantile(*u as f64)
                .ok_or("DDSketch has no samples"),
            (EhBucketSketch::DdSketch(dd), DataInput::U64(u)) => dd
                .get_value_at_quantile(*u as f64)
                .ok_or("DDSketch has no samples"),
            (EhBucketSketch::DdSketch(dd), DataInput::F32(f)) => dd
                .get_value_at_quantile(*f as f64)
                .ok_or("DDSketch has no samples"),
            (EhBucketSketch::DdSketch(dd), DataInput::F64(f)) => dd
                .get_value_at_quantile(*f)
                .ok_or("DDSketch has no samples"),
            (EhBucketSketch::DdSketch(dd), DataInput::Str(cmd)) => match *cmd {
                "count" => Ok(dd.get_count() as f64),
                "min" => dd.min().ok_or("DDSketch has no samples"),
                "max" => dd.max().ok_or("DDSketch has no samples"),
                _ => Err("Unsupported command for DDSketch"),
            },
            (EhBucketSketch::DdSketch(dd), DataInput::String(cmd)) => match cmd.as_str() {
                "count" => Ok(dd.get_count() as f64),
                "min" => dd.min().ok_or("DDSketch has no samples"),
                "max" => dd.max().ok_or("DDSketch has no samples"),
                _ => Err("Unsupported command for DDSketch"),
            },
            (EhBucketSketch::ElasticSketch(elastic), DataInput::String(s)) => {
                Ok(elastic.estimate(s.clone()) as f64)
            }
            (EhBucketSketch::Hll(hll_df_modified), _) => Ok(hll_df_modified.estimate() as f64),
            (EhBucketSketch::Kll(kll), DataInput::I32(i)) => Ok(kll.quantile(*i as f64)),
            (EhBucketSketch::Kll(kll), DataInput::I64(i)) => Ok(kll.quantile(*i as f64)),
            (EhBucketSketch::Kll(kll), DataInput::U32(u)) => Ok(kll.quantile(*u as f64)),
            (EhBucketSketch::Kll(kll), DataInput::U64(u)) => Ok(kll.quantile(*u as f64)),
            (EhBucketSketch::Kll(kll), DataInput::F32(f)) => Ok(kll.quantile(*f as f64)),
            (EhBucketSketch::Kll(kll), DataInput::F64(f)) => Ok(kll.quantile(*f)),
            #[cfg(feature = "experimental")]
            (EhBucketSketch::Uniform(sampler), DataInput::U64(idx)) => sampler
                .sample_at(*idx as usize)
                .ok_or("Sample index out of bounds"),
            #[cfg(feature = "experimental")]
            (EhBucketSketch::Uniform(sampler), DataInput::U32(idx)) => sampler
                .sample_at(*idx as usize)
                .ok_or("Sample index out of bounds"),
            #[cfg(feature = "experimental")]
            (EhBucketSketch::Uniform(sampler), DataInput::I64(idx)) if *idx >= 0 => sampler
                .sample_at(*idx as usize)
                .ok_or("Sample index out of bounds"),
            #[cfg(feature = "experimental")]
            (EhBucketSketch::Uniform(sampler), DataInput::I32(idx)) if *idx >= 0 => sampler
                .sample_at(*idx as usize)
                .ok_or("Sample index out of bounds"),
            #[cfg(feature = "experimental")]
            (EhBucketSketch::Uniform(sampler), DataInput::Str(cmd)) => match *cmd {
                "len" => Ok(sampler.len() as f64),
                "total_seen" => Ok(sampler.total_seen() as f64),
                _ => Err("Unsupported command for UniformSampling"),
            },
            #[cfg(feature = "experimental")]
            (EhBucketSketch::Uniform(sampler), DataInput::String(cmd)) => match cmd.as_str() {
                "len" => Ok(sampler.len() as f64),
                "total_seen" => Ok(sampler.total_seen() as f64),
                _ => Err("Unsupported command for UniformSampling"),
            },
            (EhBucketSketch::UnivMon(um), DataInput::Str(cmd)) => match *cmd {
                "cardinality" | "card" => Ok(um.estimate_cardinality()),
                "l1" => Ok(um.estimate_l1()),
                "l2" => Ok(um.estimate_l2()),
                "entropy" => Ok(um.estimate_entropy()),
                _ => Err("Unsupported command for UnivMon"),
            },
            (EhBucketSketch::UnivMon(um), DataInput::String(cmd)) => match cmd.as_str() {
                "cardinality" | "card" => Ok(um.estimate_cardinality()),
                "l1" => Ok(um.estimate_l1()),
                "l2" => Ok(um.estimate_l2()),
                "entropy" => Ok(um.estimate_entropy()),
                _ => Err("Unsupported command for UnivMon"),
            },
            _ => Err("Parameter type and Sketch Type Mismatched"),
        }
    }

    /// Get the type of sketch as a string
    pub fn sketch_type(&self) -> &'static str {
        match self {
            EhBucketSketch::CountMinSketch(_) => "CountMinSketch",
            EhBucketSketch::CocoSketch(_) => "Coco",
            EhBucketSketch::CountSketchWithL2Norm(_) => "CountSketchWithL2Norm",
            EhBucketSketch::CountSketch(_) => "CountSketch",
            EhBucketSketch::DdSketch(_) => "DDSketch",
            EhBucketSketch::ElasticSketch(_) => "Elastic",
            EhBucketSketch::Hll(_) => "HLL",
            EhBucketSketch::Kll(_) => "KLL",
            #[cfg(feature = "experimental")]
            EhBucketSketch::Uniform(_) => "UniformSampling",
            EhBucketSketch::UnivMon(_) => "UnivMon",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_routes_to_count_sketch_with_l2_norm_and_univmon() {
        let key = DataInput::I64(7);

        let mut count_l2hh =
            EhBucketSketch::CountSketchWithL2Norm(CountSketchWithL2Norm::with_dimensions(5, 1024));
        for _ in 0..9 {
            count_l2hh.update(&key);
        }
        let l2hh_est = count_l2hh.query(&key).expect("query CountSketchWithL2Norm");
        assert!(
            l2hh_est >= 9.0,
            "expected CountSketchWithL2Norm estimate >= 9, got {l2hh_est}"
        );

        let mut um = EhBucketSketch::UnivMon(UnivMon::default());
        for _ in 0..6 {
            um.update(&key);
        }
        match um {
            EhBucketSketch::UnivMon(ref u) => assert_eq!(u.total_weight, 6),
            _ => panic!("expected UnivMon chapter variant"),
        }
    }

    #[test]
    fn count_sketch_update_and_query_round_trip() {
        let mut cs = EhBucketSketch::CountSketch(CountSketch::<Vector2D<i32>, FastPath>::default());
        let key = DataInput::I64(11);
        cs.update(&key);
        let est = cs.query(&key).expect("query CountSketch");
        assert!(est >= 1.0, "expected CountSketch estimate >= 1, got {est}");
    }

    #[test]
    fn ddsketch_update_and_quantile_query_round_trip() {
        let mut dd = EhBucketSketch::DdSketch(DdSketch::new(0.01));
        dd.update(&DataInput::F64(10.0));
        dd.update(&DataInput::F64(20.0));
        dd.update(&DataInput::F64(30.0));

        let q50 = dd.query(&DataInput::F64(0.5)).expect("query DDSketch q50");
        assert!((10.0..=30.0).contains(&q50), "unexpected q50 {q50}");
    }

    /// `EhBucketSketch` derives its serde form from the variants it holds, so a
    /// round trip of a nested DDSketch must carry the sketch's whole state.
    #[test]
    fn nested_ddsketch_survives_a_serde_round_trip() {
        let mut dd = EhBucketSketch::DdSketch(DdSketch::new(0.01));
        for v in [10.0f64, 20.0, 30.0, 40.0] {
            dd.update(&DataInput::F64(v));
        }
        let before = match &dd {
            EhBucketSketch::DdSketch(inner) => {
                (inner.get_count(), inner.sum(), inner.min(), inner.max())
            }
            _ => panic!("expected the DDSketch variant"),
        };

        let bytes = rmp_serde::to_vec(&dd).expect("encode");
        let restored: EhBucketSketch = rmp_serde::from_slice(&bytes).expect("decode");
        let after = match &restored {
            EhBucketSketch::DdSketch(inner) => {
                (inner.get_count(), inner.sum(), inner.min(), inner.max())
            }
            _ => panic!("expected the DDSketch variant"),
        };
        assert_eq!(after, before, "the nested sketch lost its running state");

        let q50 = restored
            .query(&DataInput::F64(0.5))
            .expect("query the restored DDSketch");
        assert!((10.0..=40.0).contains(&q50), "unexpected q50 {q50}");
    }

    #[test]
    fn supports_norm_whitelist_is_enforced() {
        let cm =
            EhBucketSketch::CountMinSketch(CountMinSketch::<Vector2D<i32>, FastPath>::default());
        assert!(cm.supports_norm(SketchNorm::L1));
        assert!(!cm.supports_norm(SketchNorm::L2));

        let count_l2hh =
            EhBucketSketch::CountSketchWithL2Norm(CountSketchWithL2Norm::with_dimensions(5, 1024));
        assert!(count_l2hh.supports_norm(SketchNorm::L2));
        assert!(!count_l2hh.supports_norm(SketchNorm::L1));

        let cs = EhBucketSketch::CountSketch(CountSketch::<Vector2D<i32>, FastPath>::default());
        assert!(cs.supports_norm(SketchNorm::L1));
        assert!(!cs.supports_norm(SketchNorm::L2));

        let dd = EhBucketSketch::DdSketch(DdSketch::new(0.01));
        assert!(dd.supports_norm(SketchNorm::L1));
        assert!(!dd.supports_norm(SketchNorm::L2));

        let um = EhBucketSketch::UnivMon(UnivMon::default());
        assert!(um.supports_norm(SketchNorm::L2));
        assert!(!um.supports_norm(SketchNorm::L1));
    }
}
