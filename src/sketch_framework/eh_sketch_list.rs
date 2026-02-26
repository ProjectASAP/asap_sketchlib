use crate::{SketchInput, Vector2D};
use serde::{Deserialize, Serialize};

use super::super::sketches::*;
use super::UnivMon;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SketchNorm {
    L1,
    L2,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EHSketchList {
    CM(CountMin<Vector2D<i32>, FastPath>),
    COCO(Coco),
    COUNTL2HH(CountL2HH),
    CS(Count<Vector2D<i32>, FastPath>),
    DDS(DDSketch),
    ELASTIC(Elastic),
    HLL(HyperLogLog<DataFusion>),
    KLL(KLL),
    UNIFORM(UniformSampling),
    // LOCHER(LocherSketch),
    UNIVMON(UnivMon),
}

/// this should be a temporary function
/// modify KLL to remove this function
pub fn iv_to_f64(i: &SketchInput) -> f64 {
    match i {
        SketchInput::I32(x) => *x as f64,
        SketchInput::I64(x) => *x as f64,
        SketchInput::U32(x) => *x as f64,
        SketchInput::U64(x) => *x as f64,
        SketchInput::F32(x) => *x as f64,
        SketchInput::F64(f) => *f,
        SketchInput::Str(_) => todo!(),
        SketchInput::String(_) => todo!(),
        SketchInput::Bytes(_) => todo!(),
        SketchInput::I8(_) => todo!(),
        SketchInput::I16(_) => todo!(),
        SketchInput::I128(_) => todo!(),
        SketchInput::ISIZE(_) => todo!(),
        SketchInput::U8(_) => todo!(),
        SketchInput::U16(_) => todo!(),
        SketchInput::U128(_) => todo!(),
        SketchInput::USIZE(_) => todo!(),
    }
}

impl EHSketchList {
    pub fn supports_norm(&self, norm: SketchNorm) -> bool {
        match self {
            EHSketchList::COUNTL2HH(_) | EHSketchList::UNIVMON(_) => norm == SketchNorm::L2,
            EHSketchList::CM(_)
            | EHSketchList::CS(_)
            | EHSketchList::DDS(_)
            | EHSketchList::COCO(_)
            | EHSketchList::ELASTIC(_)
            | EHSketchList::HLL(_)
            | EHSketchList::KLL(_)
            | EHSketchList::UNIFORM(_) => norm == SketchNorm::L1,
        }
    }

    pub(crate) fn eh_l2_mass(&self) -> Option<f64> {
        match self {
            EHSketchList::COUNTL2HH(sketch) => Some(sketch.get_l2_sqr()),
            EHSketchList::UNIVMON(sketch) => {
                let l2 = sketch.calc_l2();
                Some(l2 * l2)
            }
            _ => None,
        }
    }

    /// Insert a value into the sketch
    pub fn insert(&mut self, val: &SketchInput) {
        match self {
            EHSketchList::CM(sketch) => sketch.insert(val),
            EHSketchList::COCO(sketch) => match val {
                SketchInput::Str(s) => sketch.insert(s, 1),
                SketchInput::String(s) => sketch.insert(s.as_str(), 1),
                _ => {}
            },
            EHSketchList::COUNTL2HH(sketch) => sketch.fast_insert_with_count(val, 1),
            EHSketchList::CS(sketch) => sketch.insert(val),
            EHSketchList::DDS(sketch) => {
                let _ = sketch.add_input(val);
            }
            EHSketchList::ELASTIC(sketch) => match val {
                SketchInput::String(s) => sketch.insert(s.to_string()),
                SketchInput::I32(i) => sketch.insert(i.to_string()),
                SketchInput::I64(i) => sketch.insert(i.to_string()),
                SketchInput::U32(u) => sketch.insert(u.to_string()),
                SketchInput::U64(u) => sketch.insert(u.to_string()),
                SketchInput::F32(f) => sketch.insert(f.to_string()),
                SketchInput::F64(f) => sketch.insert(f.to_string()),
                SketchInput::Str(s) => sketch.insert(s.to_string()),
                SketchInput::Bytes(items) => {
                    let s = String::from_utf8_lossy(items).to_string();
                    sketch.insert(s)
                }
                SketchInput::I8(_) => todo!(),
                SketchInput::I16(_) => todo!(),
                SketchInput::I128(_) => todo!(),
                SketchInput::ISIZE(_) => todo!(),
                SketchInput::U8(_) => todo!(),
                SketchInput::U16(_) => todo!(),
                SketchInput::U128(_) => todo!(),
                SketchInput::USIZE(_) => todo!(),
            },
            EHSketchList::HLL(sketch) => sketch.insert(val),
            EHSketchList::KLL(sketch) => {
                let _ = sketch.update(val);
            }
            EHSketchList::UNIFORM(sketch) => {
                let _ = sketch.update_input(val);
            }
            EHSketchList::UNIVMON(sketch) => sketch.insert(val, 1),
            // EHSketchList::LOCHER(sketch) => {
            //     // Locher requires a String
            //     if let SketchInput::String(s) = val {
            //         sketch.insert(s, 1);
            //     }
            // }
        }
    }

    /// Merge another sketch of the same type into this one
    pub fn merge(&mut self, other: &EHSketchList) -> Result<(), &'static str> {
        match (self, other) {
            (EHSketchList::CM(s), EHSketchList::CM(o)) => {
                s.merge(o);
                Ok(())
            }
            (EHSketchList::COCO(s), EHSketchList::COCO(o)) => {
                s.merge(o);
                Ok(())
            }
            (EHSketchList::COUNTL2HH(s), EHSketchList::COUNTL2HH(o)) => {
                s.merge(o);
                Ok(())
            }
            (EHSketchList::CS(s), EHSketchList::CS(o)) => {
                s.merge(o);
                Ok(())
            }
            (EHSketchList::DDS(s), EHSketchList::DDS(o)) => {
                s.merge(o);
                Ok(())
            }
            // (Bucket::ELASTIC(s), Bucket::ELASTIC(o)) => {
            //     s.merge(o);
            //     Ok(())
            // }, // not yet
            (EHSketchList::HLL(s), EHSketchList::HLL(o)) => {
                s.merge(o);
                Ok(())
            }
            (EHSketchList::KLL(s), EHSketchList::KLL(o)) => {
                s.merge(o);
                Ok(())
            }
            (EHSketchList::UNIFORM(s), EHSketchList::UNIFORM(o)) => s.merge(o),
            (EHSketchList::UNIVMON(s), EHSketchList::UNIVMON(o)) => {
                s.merge(o);
                Ok(())
            }
            // (Bucket::LOCHER(s), Bucket::LOCHER(o)) => {
            //     s.merge(o);
            //     Ok(())
            // }, // not yet
            _ => Err("Cannot merge sketches of different types"),
        }
    }

    pub fn query(&self, key: &SketchInput) -> Result<f64, &'static str> {
        match (self, key) {
            (EHSketchList::CM(count_min), _) => Ok(count_min.estimate(key) as f64),
            (EHSketchList::COCO(coco), SketchInput::Str(s)) => Ok(coco.clone().estimate(s) as f64),
            (EHSketchList::COCO(coco), SketchInput::String(s)) => {
                Ok(coco.clone().estimate(s.as_str()) as f64)
            }
            (EHSketchList::COUNTL2HH(count_univ), _) => Ok(count_univ.fast_get_est(key)),
            (EHSketchList::CS(count_sketch), _) => Ok(count_sketch.estimate(key)),
            (EHSketchList::DDS(dd), SketchInput::I32(i)) => dd
                .get_value_at_quantile(*i as f64)
                .ok_or("DDSketch has no samples"),
            (EHSketchList::DDS(dd), SketchInput::I64(i)) => dd
                .get_value_at_quantile(*i as f64)
                .ok_or("DDSketch has no samples"),
            (EHSketchList::DDS(dd), SketchInput::U32(u)) => dd
                .get_value_at_quantile(*u as f64)
                .ok_or("DDSketch has no samples"),
            (EHSketchList::DDS(dd), SketchInput::U64(u)) => dd
                .get_value_at_quantile(*u as f64)
                .ok_or("DDSketch has no samples"),
            (EHSketchList::DDS(dd), SketchInput::F32(f)) => dd
                .get_value_at_quantile(*f as f64)
                .ok_or("DDSketch has no samples"),
            (EHSketchList::DDS(dd), SketchInput::F64(f)) => dd
                .get_value_at_quantile(*f)
                .ok_or("DDSketch has no samples"),
            (EHSketchList::DDS(dd), SketchInput::Str(cmd)) => match *cmd {
                "count" => Ok(dd.get_count() as f64),
                "min" => dd.min().ok_or("DDSketch has no samples"),
                "max" => dd.max().ok_or("DDSketch has no samples"),
                _ => Err("Unsupported command for DDSketch"),
            },
            (EHSketchList::DDS(dd), SketchInput::String(cmd)) => match cmd.as_str() {
                "count" => Ok(dd.get_count() as f64),
                "min" => dd.min().ok_or("DDSketch has no samples"),
                "max" => dd.max().ok_or("DDSketch has no samples"),
                _ => Err("Unsupported command for DDSketch"),
            },
            (EHSketchList::ELASTIC(elastic), SketchInput::String(s)) => {
                Ok(elastic.clone().query(s.clone()) as f64)
            }
            (EHSketchList::HLL(hll_df_modified), _) => Ok(hll_df_modified.estimate() as f64),
            (EHSketchList::KLL(kll), SketchInput::I32(i)) => Ok(kll.quantile(*i as f64)),
            (EHSketchList::KLL(kll), SketchInput::I64(i)) => Ok(kll.quantile(*i as f64)),
            (EHSketchList::KLL(kll), SketchInput::U32(u)) => Ok(kll.quantile(*u as f64)),
            (EHSketchList::KLL(kll), SketchInput::U64(u)) => Ok(kll.quantile(*u as f64)),
            (EHSketchList::KLL(kll), SketchInput::F32(f)) => Ok(kll.quantile(*f as f64)),
            (EHSketchList::KLL(kll), SketchInput::F64(f)) => Ok(kll.quantile(*f)),
            (EHSketchList::UNIFORM(sampler), SketchInput::U64(idx)) => sampler
                .sample_at(*idx as usize)
                .ok_or("Sample index out of bounds"),
            (EHSketchList::UNIFORM(sampler), SketchInput::U32(idx)) => sampler
                .sample_at(*idx as usize)
                .ok_or("Sample index out of bounds"),
            (EHSketchList::UNIFORM(sampler), SketchInput::I64(idx)) if *idx >= 0 => sampler
                .sample_at(*idx as usize)
                .ok_or("Sample index out of bounds"),
            (EHSketchList::UNIFORM(sampler), SketchInput::I32(idx)) if *idx >= 0 => sampler
                .sample_at(*idx as usize)
                .ok_or("Sample index out of bounds"),
            (EHSketchList::UNIFORM(sampler), SketchInput::Str(cmd)) => match *cmd {
                "len" => Ok(sampler.len() as f64),
                "total_seen" => Ok(sampler.total_seen() as f64),
                _ => Err("Unsupported command for UniformSampling"),
            },
            (EHSketchList::UNIFORM(sampler), SketchInput::String(cmd)) => match cmd.as_str() {
                "len" => Ok(sampler.len() as f64),
                "total_seen" => Ok(sampler.total_seen() as f64),
                _ => Err("Unsupported command for UniformSampling"),
            },
            (EHSketchList::UNIVMON(um), SketchInput::Str(cmd)) => match *cmd {
                "cardinality" | "card" => Ok(um.calc_card()),
                "l1" => Ok(um.calc_l1()),
                "l2" => Ok(um.calc_l2()),
                "entropy" => Ok(um.calc_entropy()),
                _ => Err("Unsupported command for UnivMon"),
            },
            (EHSketchList::UNIVMON(um), SketchInput::String(cmd)) => match cmd.as_str() {
                "cardinality" | "card" => Ok(um.calc_card()),
                "l1" => Ok(um.calc_l1()),
                "l2" => Ok(um.calc_l2()),
                "entropy" => Ok(um.calc_entropy()),
                _ => Err("Unsupported command for UnivMon"),
            },
            // (EHSketchList::LOCHER(locher_sketch), SketchInput::Str(s)) => Ok(locher_sketch.estimate(*s)),
            _ => Err("Parameter type and Sketch Type Mismatched"),
        }
    }

    /// Get the type of sketch as a string
    pub fn sketch_type(&self) -> &'static str {
        match self {
            EHSketchList::CM(_) => "CountMin",
            EHSketchList::COCO(_) => "Coco",
            EHSketchList::COUNTL2HH(_) => "CountL2HH",
            EHSketchList::CS(_) => "CountSketch",
            EHSketchList::DDS(_) => "DDSketch",
            EHSketchList::ELASTIC(_) => "Elastic",
            EHSketchList::HLL(_) => "HLL",
            EHSketchList::KLL(_) => "KLL",
            EHSketchList::UNIFORM(_) => "UniformSampling",
            EHSketchList::UNIVMON(_) => "UnivMon",
            // EHSketchList::LOCHER(_) => "Locher",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_routes_to_countl2hh_and_univmon() {
        let key = SketchInput::I64(7);

        let mut count_l2hh = EHSketchList::COUNTL2HH(CountL2HH::with_dimensions(5, 1024));
        for _ in 0..9 {
            count_l2hh.insert(&key);
        }
        let l2hh_est = count_l2hh.query(&key).expect("query COUNTL2HH");
        assert!(
            l2hh_est >= 9.0,
            "expected COUNTL2HH estimate >= 9, got {l2hh_est}"
        );

        let mut um = EHSketchList::UNIVMON(UnivMon::default());
        for _ in 0..6 {
            um.insert(&key);
        }
        match um {
            EHSketchList::UNIVMON(ref u) => assert_eq!(u.bucket_size, 6),
            _ => panic!("expected UnivMon chapter variant"),
        }
    }

    #[test]
    fn count_sketch_insert_and_query_round_trip() {
        let mut cs = EHSketchList::CS(Count::<Vector2D<i32>, FastPath>::default());
        let key = SketchInput::I64(11);
        cs.insert(&key);
        let est = cs.query(&key).expect("query CountSketch");
        assert!(est >= 1.0, "expected CountSketch estimate >= 1, got {est}");
    }

    #[test]
    fn ddsketch_insert_and_quantile_query_round_trip() {
        let mut dd = EHSketchList::DDS(DDSketch::new(0.01));
        dd.insert(&SketchInput::F64(10.0));
        dd.insert(&SketchInput::F64(20.0));
        dd.insert(&SketchInput::F64(30.0));

        let q50 = dd
            .query(&SketchInput::F64(0.5))
            .expect("query DDSketch q50");
        assert!(q50 >= 10.0 && q50 <= 30.0, "unexpected q50 {q50}");
    }

    #[test]
    fn supports_norm_whitelist_is_enforced() {
        let cm = EHSketchList::CM(CountMin::<Vector2D<i32>, FastPath>::default());
        assert!(cm.supports_norm(SketchNorm::L1));
        assert!(!cm.supports_norm(SketchNorm::L2));

        let count_l2hh = EHSketchList::COUNTL2HH(CountL2HH::with_dimensions(5, 1024));
        assert!(count_l2hh.supports_norm(SketchNorm::L2));
        assert!(!count_l2hh.supports_norm(SketchNorm::L1));

        let cs = EHSketchList::CS(Count::<Vector2D<i32>, FastPath>::default());
        assert!(cs.supports_norm(SketchNorm::L1));
        assert!(!cs.supports_norm(SketchNorm::L2));

        let dd = EHSketchList::DDS(DDSketch::new(0.01));
        assert!(dd.supports_norm(SketchNorm::L1));
        assert!(!dd.supports_norm(SketchNorm::L2));

        let um = EHSketchList::UNIVMON(UnivMon::default());
        assert!(um.supports_norm(SketchNorm::L2));
        assert!(!um.supports_norm(SketchNorm::L1));
    }
}
