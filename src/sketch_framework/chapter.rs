use crate::SketchInput;
use serde::{Deserialize, Serialize};

use super::super::sketches::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(deserialize = "'de: 'a"))]
pub enum Chapter<'a> {
    CM(CountMin),
    #[serde(borrow)]
    COCO(Coco<'a>),
    CU(CountL2HH),
    ELASTIC(Elastic),
    HLL(HllDf),
    KLL(KLL),
    UNIFORM(UniformSampling),
    // LOCHER(LocherSketch),
    // UNIVMON(UnivMon),
}

// impl L2HH {
//     /// Creates a count-based L2 heavy hitter sketch with the requested dimensions.
//     pub fn count_with_dimensions(rows: usize, cols: usize) -> Self {
//         L2HH::COUNT(VectorCount::with_dimensions(rows, cols))
//     }

//     /// Inserts a single observation.
//     pub fn insert(&mut self, value: &SketchInput) {
//         let _ = self.update(value, 1);
//     }

//     /// Inserts an observation with an explicit weight and returns the current estimate.
//     pub fn update(&mut self, value: &SketchInput, weight: i64) -> f64 {
//         match self {
//             L2HH::COUNT(sketch) => {
//                 sketch.insert_with_count(value, weight);
//                 sketch.estimate(value)
//             }
//         }
//     }

//     /// Returns the estimated frequency for the provided value.
//     pub fn estimate(&self, value: &SketchInput) -> f64 {
//         match self {
//             L2HH::COUNT(sketch) => sketch.estimate(value),
//         }
//     }

//     /// Provides an optional query interface for compatibility with older code.
//     pub fn query(&self, value: &SketchInput) -> Option<f64> {
//         Some(self.estimate(value))
//     }

//     /// Approximates the L2 norm of the sketch.
//     pub fn get_l2(&self) -> f64 {
//         match self {
//             L2HH::COUNT(sketch) => sketch.l2(),
//         }
//     }

//     /// Merges another L2 heavy hitter sketch into this one.
//     pub fn merge(&mut self, other: &L2HH) {
//         match (self, other) {
//             (L2HH::COUNT(left), L2HH::COUNT(right)) => left.merge(right),
//         }
//     }
// }

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

impl<'a> Chapter<'a> {
    /// Insert a value into the sketch
    pub fn insert(&mut self, val: &SketchInput<'a>) {
        match self {
            Chapter::CM(sketch) => sketch.insert_cm(val),
            Chapter::COCO(sketch) => sketch.insert(val, 1),
            Chapter::CU(sketch) => sketch.fast_insert_with_count(val, 1),
            Chapter::ELASTIC(sketch) => match val {
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
            Chapter::HLL(sketch) => sketch.insert(val),
            Chapter::KLL(sketch) => sketch.update(iv_to_f64(val)),
            Chapter::UNIFORM(sketch) => {
                let _ = sketch.update_input(val);
            } // Chapter::LOCHER(sketch) => {
              //     // Locher requires a String
              //     if let SketchInput::String(s) = val {
              //         sketch.insert(s, 1);
              //     }
              // }
              // Chapter::UNIVMON(sketch) => {
              //     // UnivMon requires update with key, value, bottom_layer_num
              //     // Using default bottom_layer_num of 0
              //     if let SketchInput::Str(s) = val {
              //         sketch.update(s, 1, 0);
              //     } else if let SketchInput::String(s) = val {
              //         sketch.update(s.as_str(), 1, 0);
              //     }
              // }
        }
    }

    /// Merge another sketch of the same type into this one
    pub fn merge(&mut self, other: &Chapter<'a>) -> Result<(), &'static str> {
        match (self, other) {
            (Chapter::CM(s), Chapter::CM(o)) => {
                s.merge(o);
                Ok(())
            }
            (Chapter::COCO(s), Chapter::COCO(o)) => {
                s.merge(o);
                Ok(())
            }
            (Chapter::CU(s), Chapter::CU(o)) => {
                s.merge(o);
                Ok(())
            }
            // (Bucket::ELASTIC(s), Bucket::ELASTIC(o)) => {
            //     s.merge(o);
            //     Ok(())
            // }, // not yet
            (Chapter::HLL(s), Chapter::HLL(o)) => {
                s.merge(o);
                Ok(())
            }
            (Chapter::KLL(s), Chapter::KLL(o)) => {
                s.merge(o);
                Ok(())
            }
            (Chapter::UNIFORM(s), Chapter::UNIFORM(o)) => s.merge(o),
            // (Bucket::LOCHER(s), Bucket::LOCHER(o)) => {
            //     s.merge(o);
            //     Ok(())
            // }, // not yet
            // (Chapter::UNIVMON(s), Chapter::UNIVMON(o)) => {
            //     s.merge_with(o);
            //     Ok(())
            // }
            _ => Err("Cannot merge sketches of different types"),
        }
    }

    pub fn query(&self, key: &SketchInput<'a>) -> Result<f64, &'static str> {
        match (self, key) {
            (Chapter::CM(count_min), _) => Ok(count_min.get_est(key) as f64),
            (Chapter::COCO(coco), _) => Ok(coco.clone().estimate(key.clone()) as f64),
            (Chapter::CU(count_univ), _) => Ok(count_univ.fast_get_est(key) as f64),
            (Chapter::ELASTIC(elastic), SketchInput::String(s)) => {
                Ok(elastic.clone().query(s.clone()) as f64)
            }
            (Chapter::HLL(hll_df_modified), _) => Ok(hll_df_modified.get_est() as f64),
            (Chapter::KLL(kll), SketchInput::I32(i)) => Ok(kll.quantile(*i as f64)),
            (Chapter::KLL(kll), SketchInput::I64(i)) => Ok(kll.quantile(*i as f64)),
            (Chapter::KLL(kll), SketchInput::U32(u)) => Ok(kll.quantile(*u as f64)),
            (Chapter::KLL(kll), SketchInput::U64(u)) => Ok(kll.quantile(*u as f64)),
            (Chapter::KLL(kll), SketchInput::F32(f)) => Ok(kll.quantile(*f as f64)),
            (Chapter::KLL(kll), SketchInput::F64(f)) => Ok(kll.quantile(*f)),
            (Chapter::UNIFORM(sampler), SketchInput::U64(idx)) => sampler
                .sample_at(*idx as usize)
                .ok_or("Sample index out of bounds"),
            (Chapter::UNIFORM(sampler), SketchInput::U32(idx)) => sampler
                .sample_at(*idx as usize)
                .ok_or("Sample index out of bounds"),
            (Chapter::UNIFORM(sampler), SketchInput::I64(idx)) if *idx >= 0 => sampler
                .sample_at(*idx as usize)
                .ok_or("Sample index out of bounds"),
            (Chapter::UNIFORM(sampler), SketchInput::I32(idx)) if *idx >= 0 => sampler
                .sample_at(*idx as usize)
                .ok_or("Sample index out of bounds"),
            (Chapter::UNIFORM(sampler), SketchInput::Str(cmd)) => match *cmd {
                "len" => Ok(sampler.len() as f64),
                "total_seen" => Ok(sampler.total_seen() as f64),
                _ => Err("Unsupported command for UniformSampling"),
            },
            (Chapter::UNIFORM(sampler), SketchInput::String(cmd)) => match cmd.as_str() {
                "len" => Ok(sampler.len() as f64),
                "total_seen" => Ok(sampler.total_seen() as f64),
                _ => Err("Unsupported command for UniformSampling"),
            },
            // (Chapter::LOCHER(locher_sketch), SketchInput::Str(s)) => Ok(locher_sketch.estimate(*s)),
            _ => Err("Parameter type and Sketch Type Mismatched"),
        }
    }

    /// Get the type of sketch as a string
    pub fn sketch_type(&self) -> &'static str {
        match self {
            Chapter::CM(_) => "CountMin",
            Chapter::COCO(_) => "Coco",
            Chapter::CU(_) => "CountUniv",
            Chapter::ELASTIC(_) => "Elastic",
            Chapter::HLL(_) => "HLL",
            Chapter::KLL(_) => "KLL",
            Chapter::UNIFORM(_) => "UniformSampling",
            // Chapter::LOCHER(_) => "Locher",
            // Chapter::UNIVMON(_) => "UnivMon",
        }
    }
}
