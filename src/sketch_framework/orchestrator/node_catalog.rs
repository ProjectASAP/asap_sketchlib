//! Sketch enum taxonomy and shared orchestration utilities.
//! Defines capability-grouped sketch enums and hash fast-path helpers used by the sketch orchestrator.

use crate::{
    CANONICAL_HASH_SEED, Coco, Count, CountMin, DDSketch, DataFusion, DefaultMatrixI32,
    DefaultMatrixI64, DefaultMatrixI128, FastPath, FixedMatrix, HyperLogLog, HyperLogLogHIP, KLL,
    MatrixHashMode, MatrixHashType, QuickMatrixI32, QuickMatrixI64, QuickMatrixI128, Regular,
    RegularPath, SketchInput, UnivMon, Vector2D, hash_for_matrix_seeded_with_mode,
    hash_mode_for_matrix, hash64_seeded, hydra::MultiHeadHydra, input::HydraQuery,
    sketch_framework::Hydra,
};

pub trait OrchestratorSketch {
    fn insert(&mut self, val: &SketchInput);
    fn query(&self, val: &SketchInput) -> Result<f64, &'static str>;
}

pub trait HashReuseSketch: OrchestratorSketch {
    fn hash_domain(&self) -> HashDomain;
    fn insert_with_hash_value(&mut self, hash: &HashValue, val: &SketchInput);
    fn query_with_hash_value(&self, hash: &HashValue) -> Result<f64, &'static str>;
}

pub enum OrchestratorInsert<'a> {
    Sketch(&'a SketchInput<'a>),
    Hydra {
        key: &'a str,
        value: &'a SketchInput<'a>,
        count: Option<i32>,
    },
    UnivMon {
        key: &'a SketchInput<'a>,
        value: i64,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum UnivMonQuery {
    Cardinality,
    L1Norm,
    L2Norm,
    Entropy,
}

pub enum OrchestratorQuery<'a> {
    Sketch(&'a SketchInput<'a>),
    Hydra {
        key: Vec<&'a str>,
        query: HydraQuery<'a>,
    },
    UnivMon(UnivMonQuery),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HashDomain {
    Matrix {
        rows: usize,
        cols: usize,
        mode: MatrixHashMode,
        seed_idx: usize,
    },
    FastPath64 {
        seed_idx: usize,
    },
}

impl HashDomain {
    pub(crate) fn hash_for_input(&self, input: &SketchInput) -> HashValue {
        match *self {
            HashDomain::Matrix {
                rows,
                mode,
                seed_idx,
                ..
            } => HashValue::Matrix(hash_for_matrix_seeded_with_mode(
                seed_idx, mode, rows, input,
            )),
            HashDomain::FastPath64 { seed_idx } => {
                HashValue::Fast64(hash64_seeded(seed_idx, input))
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum HashValue {
    Matrix(MatrixHashType),
    Fast64(u64),
}

impl From<MatrixHashType> for HashValue {
    fn from(value: MatrixHashType) -> Self {
        HashValue::Matrix(value)
    }
}

impl From<u64> for HashValue {
    fn from(value: u64) -> Self {
        HashValue::Fast64(value)
    }
}

pub enum FreqSketch {
    CountMin(CountMin<Vector2D<i32>, FastPath>),
    CountMinRegular(CountMin<Vector2D<i32>, RegularPath>),
    CountMinV2I64Fast(CountMin<Vector2D<i64>, FastPath>),
    CountMinV2I64Regular(CountMin<Vector2D<i64>, RegularPath>),
    CountMinV2I128Fast(CountMin<Vector2D<i128>, FastPath>),
    CountMinV2I128Regular(CountMin<Vector2D<i128>, RegularPath>),
    Count(Count<Vector2D<i32>, FastPath>),
    CountRegular(Count<Vector2D<i32>, RegularPath>),
    CountV2I64Fast(Count<Vector2D<i64>, FastPath>),
    CountV2I64Regular(Count<Vector2D<i64>, RegularPath>),
    CountV2I128Fast(Count<Vector2D<i128>, FastPath>),
    CountV2I128Regular(Count<Vector2D<i128>, RegularPath>),
    CountMinFixed(CountMin<FixedMatrix, FastPath>),
    CountMinFixedRegular(CountMin<FixedMatrix, RegularPath>),
    CountFixed(Count<FixedMatrix, FastPath>),
    CountFixedRegular(Count<FixedMatrix, RegularPath>),
    CountMinDefaultI32Fast(CountMin<DefaultMatrixI32, FastPath>),
    CountMinDefaultI32Regular(CountMin<DefaultMatrixI32, RegularPath>),
    CountMinDefaultI64Fast(CountMin<DefaultMatrixI64, FastPath>),
    CountMinDefaultI64Regular(CountMin<DefaultMatrixI64, RegularPath>),
    CountMinDefaultI128Fast(CountMin<DefaultMatrixI128, FastPath>),
    CountMinDefaultI128Regular(CountMin<DefaultMatrixI128, RegularPath>),
    CountMinQuickI32Fast(CountMin<QuickMatrixI32, FastPath>),
    CountMinQuickI32Regular(CountMin<QuickMatrixI32, RegularPath>),
    CountMinQuickI64Fast(CountMin<QuickMatrixI64, FastPath>),
    CountMinQuickI64Regular(CountMin<QuickMatrixI64, RegularPath>),
    CountMinQuickI128Fast(CountMin<QuickMatrixI128, FastPath>),
    CountMinQuickI128Regular(CountMin<QuickMatrixI128, RegularPath>),
    CountDefaultI32Fast(Count<DefaultMatrixI32, FastPath>),
    CountDefaultI32Regular(Count<DefaultMatrixI32, RegularPath>),
    CountDefaultI64Fast(Count<DefaultMatrixI64, FastPath>),
    CountDefaultI64Regular(Count<DefaultMatrixI64, RegularPath>),
    CountDefaultI128Fast(Count<DefaultMatrixI128, FastPath>),
    CountDefaultI128Regular(Count<DefaultMatrixI128, RegularPath>),
    CountQuickI32Fast(Count<QuickMatrixI32, FastPath>),
    CountQuickI32Regular(Count<QuickMatrixI32, RegularPath>),
    CountQuickI64Fast(Count<QuickMatrixI64, FastPath>),
    CountQuickI64Regular(Count<QuickMatrixI64, RegularPath>),
    CountQuickI128Fast(Count<QuickMatrixI128, FastPath>),
    CountQuickI128Regular(Count<QuickMatrixI128, RegularPath>),
}

pub enum CardinalitySketch {
    HllDf(HyperLogLog<DataFusion>),
    HllRegular(HyperLogLog<Regular>),
    HllHip(HyperLogLogHIP),
}

pub enum QuantileSketch {
    Kll(KLL),
    Dd(DDSketch),
}

pub enum SubpopulationSketch {
    Hydra(Hydra),
    MultiHydra(MultiHeadHydra),
}

pub enum SubquerySketch {
    Coco(Coco),
}

pub enum GSumSketch {
    UnivMon(UnivMon),
}

pub enum OrchestratedSketch {
    Freq(FreqSketch),
    Cardinality(CardinalitySketch),
    Quantile(QuantileSketch),
    Subpopulation(SubpopulationSketch),
    Subquery(SubquerySketch),
    GSum(GSumSketch),
}

impl FreqSketch {
    pub fn sketch_type(&self) -> &'static str {
        match self {
            FreqSketch::CountMin(_)
            | FreqSketch::CountMinRegular(_)
            | FreqSketch::CountMinV2I64Fast(_)
            | FreqSketch::CountMinV2I64Regular(_)
            | FreqSketch::CountMinV2I128Fast(_)
            | FreqSketch::CountMinV2I128Regular(_)
            | FreqSketch::CountMinFixed(_)
            | FreqSketch::CountMinFixedRegular(_)
            | FreqSketch::CountMinDefaultI32Fast(_)
            | FreqSketch::CountMinDefaultI32Regular(_)
            | FreqSketch::CountMinDefaultI64Fast(_)
            | FreqSketch::CountMinDefaultI64Regular(_)
            | FreqSketch::CountMinDefaultI128Fast(_)
            | FreqSketch::CountMinDefaultI128Regular(_)
            | FreqSketch::CountMinQuickI32Fast(_)
            | FreqSketch::CountMinQuickI32Regular(_)
            | FreqSketch::CountMinQuickI64Fast(_)
            | FreqSketch::CountMinQuickI64Regular(_)
            | FreqSketch::CountMinQuickI128Fast(_)
            | FreqSketch::CountMinQuickI128Regular(_) => "CountMin",
            FreqSketch::Count(_)
            | FreqSketch::CountRegular(_)
            | FreqSketch::CountV2I64Fast(_)
            | FreqSketch::CountV2I64Regular(_)
            | FreqSketch::CountV2I128Fast(_)
            | FreqSketch::CountV2I128Regular(_)
            | FreqSketch::CountFixed(_)
            | FreqSketch::CountFixedRegular(_)
            | FreqSketch::CountDefaultI32Fast(_)
            | FreqSketch::CountDefaultI32Regular(_)
            | FreqSketch::CountDefaultI64Fast(_)
            | FreqSketch::CountDefaultI64Regular(_)
            | FreqSketch::CountDefaultI128Fast(_)
            | FreqSketch::CountDefaultI128Regular(_)
            | FreqSketch::CountQuickI32Fast(_)
            | FreqSketch::CountQuickI32Regular(_)
            | FreqSketch::CountQuickI64Fast(_)
            | FreqSketch::CountQuickI64Regular(_)
            | FreqSketch::CountQuickI128Fast(_)
            | FreqSketch::CountQuickI128Regular(_) => "Count",
        }
    }

    pub fn hash_domain(&self) -> Option<HashDomain> {
        match self {
            FreqSketch::CountMin(sketch) => Some(HashDomain::Matrix {
                rows: sketch.rows(),
                cols: sketch.cols(),
                mode: hash_mode_for_matrix(sketch.rows(), sketch.cols()),
                seed_idx: 0,
            }),
            FreqSketch::CountMinV2I64Fast(sketch) => Some(HashDomain::Matrix {
                rows: sketch.rows(),
                cols: sketch.cols(),
                mode: hash_mode_for_matrix(sketch.rows(), sketch.cols()),
                seed_idx: 0,
            }),
            FreqSketch::CountMinV2I128Fast(sketch) => Some(HashDomain::Matrix {
                rows: sketch.rows(),
                cols: sketch.cols(),
                mode: hash_mode_for_matrix(sketch.rows(), sketch.cols()),
                seed_idx: 0,
            }),
            FreqSketch::Count(sketch) => Some(HashDomain::Matrix {
                rows: sketch.rows(),
                cols: sketch.cols(),
                mode: hash_mode_for_matrix(sketch.rows(), sketch.cols()),
                seed_idx: 0,
            }),
            FreqSketch::CountV2I64Fast(sketch) => Some(HashDomain::Matrix {
                rows: sketch.rows(),
                cols: sketch.cols(),
                mode: hash_mode_for_matrix(sketch.rows(), sketch.cols()),
                seed_idx: 0,
            }),
            FreqSketch::CountV2I128Fast(sketch) => Some(HashDomain::Matrix {
                rows: sketch.rows(),
                cols: sketch.cols(),
                mode: hash_mode_for_matrix(sketch.rows(), sketch.cols()),
                seed_idx: 0,
            }),
            FreqSketch::CountMinFixed(sketch) => Some(HashDomain::Matrix {
                rows: sketch.rows(),
                cols: sketch.cols(),
                mode: hash_mode_for_matrix(sketch.rows(), sketch.cols()),
                seed_idx: 0,
            }),
            FreqSketch::CountFixed(sketch) => Some(HashDomain::Matrix {
                rows: sketch.rows(),
                cols: sketch.cols(),
                mode: hash_mode_for_matrix(sketch.rows(), sketch.cols()),
                seed_idx: 0,
            }),
            FreqSketch::CountMinDefaultI32Fast(sketch) => Some(HashDomain::Matrix {
                rows: sketch.rows(),
                cols: sketch.cols(),
                mode: hash_mode_for_matrix(sketch.rows(), sketch.cols()),
                seed_idx: 0,
            }),
            FreqSketch::CountMinDefaultI64Fast(sketch) => Some(HashDomain::Matrix {
                rows: sketch.rows(),
                cols: sketch.cols(),
                mode: hash_mode_for_matrix(sketch.rows(), sketch.cols()),
                seed_idx: 0,
            }),
            FreqSketch::CountMinDefaultI128Fast(sketch) => Some(HashDomain::Matrix {
                rows: sketch.rows(),
                cols: sketch.cols(),
                mode: hash_mode_for_matrix(sketch.rows(), sketch.cols()),
                seed_idx: 0,
            }),
            FreqSketch::CountMinQuickI32Fast(sketch) => Some(HashDomain::Matrix {
                rows: sketch.rows(),
                cols: sketch.cols(),
                mode: hash_mode_for_matrix(sketch.rows(), sketch.cols()),
                seed_idx: 0,
            }),
            FreqSketch::CountMinQuickI64Fast(sketch) => Some(HashDomain::Matrix {
                rows: sketch.rows(),
                cols: sketch.cols(),
                mode: hash_mode_for_matrix(sketch.rows(), sketch.cols()),
                seed_idx: 0,
            }),
            FreqSketch::CountMinQuickI128Fast(sketch) => Some(HashDomain::Matrix {
                rows: sketch.rows(),
                cols: sketch.cols(),
                mode: hash_mode_for_matrix(sketch.rows(), sketch.cols()),
                seed_idx: 0,
            }),
            FreqSketch::CountDefaultI32Fast(sketch) => Some(HashDomain::Matrix {
                rows: sketch.rows(),
                cols: sketch.cols(),
                mode: hash_mode_for_matrix(sketch.rows(), sketch.cols()),
                seed_idx: 0,
            }),
            FreqSketch::CountDefaultI64Fast(sketch) => Some(HashDomain::Matrix {
                rows: sketch.rows(),
                cols: sketch.cols(),
                mode: hash_mode_for_matrix(sketch.rows(), sketch.cols()),
                seed_idx: 0,
            }),
            FreqSketch::CountDefaultI128Fast(sketch) => Some(HashDomain::Matrix {
                rows: sketch.rows(),
                cols: sketch.cols(),
                mode: hash_mode_for_matrix(sketch.rows(), sketch.cols()),
                seed_idx: 0,
            }),
            FreqSketch::CountQuickI32Fast(sketch) => Some(HashDomain::Matrix {
                rows: sketch.rows(),
                cols: sketch.cols(),
                mode: hash_mode_for_matrix(sketch.rows(), sketch.cols()),
                seed_idx: 0,
            }),
            FreqSketch::CountQuickI64Fast(sketch) => Some(HashDomain::Matrix {
                rows: sketch.rows(),
                cols: sketch.cols(),
                mode: hash_mode_for_matrix(sketch.rows(), sketch.cols()),
                seed_idx: 0,
            }),
            FreqSketch::CountQuickI128Fast(sketch) => Some(HashDomain::Matrix {
                rows: sketch.rows(),
                cols: sketch.cols(),
                mode: hash_mode_for_matrix(sketch.rows(), sketch.cols()),
                seed_idx: 0,
            }),
            _ => None,
        }
    }

    pub fn insert(&mut self, val: &SketchInput) {
        match self {
            FreqSketch::CountMin(sketch) => sketch.insert(val),
            FreqSketch::CountMinRegular(sketch) => sketch.insert(val),
            FreqSketch::CountMinV2I64Fast(sketch) => sketch.insert(val),
            FreqSketch::CountMinV2I64Regular(sketch) => sketch.insert(val),
            FreqSketch::CountMinV2I128Fast(sketch) => sketch.insert(val),
            FreqSketch::CountMinV2I128Regular(sketch) => sketch.insert(val),
            FreqSketch::Count(sketch) => sketch.insert(val),
            FreqSketch::CountRegular(sketch) => sketch.insert(val),
            FreqSketch::CountV2I64Fast(sketch) => sketch.insert(val),
            FreqSketch::CountV2I64Regular(sketch) => sketch.insert(val),
            FreqSketch::CountV2I128Fast(sketch) => sketch.insert(val),
            FreqSketch::CountV2I128Regular(sketch) => sketch.insert(val),
            FreqSketch::CountMinFixed(sketch) => sketch.insert(val),
            FreqSketch::CountMinFixedRegular(sketch) => sketch.insert(val),
            FreqSketch::CountFixed(sketch) => sketch.insert(val),
            FreqSketch::CountFixedRegular(sketch) => sketch.insert(val),
            FreqSketch::CountMinDefaultI32Fast(sketch) => sketch.insert(val),
            FreqSketch::CountMinDefaultI32Regular(sketch) => sketch.insert(val),
            FreqSketch::CountMinDefaultI64Fast(sketch) => sketch.insert(val),
            FreqSketch::CountMinDefaultI64Regular(sketch) => sketch.insert(val),
            FreqSketch::CountMinDefaultI128Fast(sketch) => sketch.insert(val),
            FreqSketch::CountMinDefaultI128Regular(sketch) => sketch.insert(val),
            FreqSketch::CountMinQuickI32Fast(sketch) => sketch.insert(val),
            FreqSketch::CountMinQuickI32Regular(sketch) => sketch.insert(val),
            FreqSketch::CountMinQuickI64Fast(sketch) => sketch.insert(val),
            FreqSketch::CountMinQuickI64Regular(sketch) => sketch.insert(val),
            FreqSketch::CountMinQuickI128Fast(sketch) => sketch.insert(val),
            FreqSketch::CountMinQuickI128Regular(sketch) => sketch.insert(val),
            FreqSketch::CountDefaultI32Fast(sketch) => sketch.insert(val),
            FreqSketch::CountDefaultI32Regular(sketch) => sketch.insert(val),
            FreqSketch::CountDefaultI64Fast(sketch) => sketch.insert(val),
            FreqSketch::CountDefaultI64Regular(sketch) => sketch.insert(val),
            FreqSketch::CountDefaultI128Fast(sketch) => sketch.insert(val),
            FreqSketch::CountDefaultI128Regular(sketch) => sketch.insert(val),
            FreqSketch::CountQuickI32Fast(sketch) => sketch.insert(val),
            FreqSketch::CountQuickI32Regular(sketch) => sketch.insert(val),
            FreqSketch::CountQuickI64Fast(sketch) => sketch.insert(val),
            FreqSketch::CountQuickI64Regular(sketch) => sketch.insert(val),
            FreqSketch::CountQuickI128Fast(sketch) => sketch.insert(val),
            FreqSketch::CountQuickI128Regular(sketch) => sketch.insert(val),
        }
    }

    pub fn query(&self, val: &SketchInput) -> Result<f64, &'static str> {
        match self {
            FreqSketch::CountMin(cm) => Ok(cm.estimate(val) as f64),
            FreqSketch::CountMinRegular(cm) => Ok(cm.estimate(val) as f64),
            FreqSketch::CountMinV2I64Fast(cm) => Ok(cm.estimate(val) as f64),
            FreqSketch::CountMinV2I64Regular(cm) => Ok(cm.estimate(val) as f64),
            FreqSketch::CountMinV2I128Fast(cm) => Ok(cm.estimate(val) as f64),
            FreqSketch::CountMinV2I128Regular(cm) => Ok(cm.estimate(val) as f64),
            FreqSketch::Count(cs) => Ok(cs.estimate(val)),
            FreqSketch::CountRegular(cs) => Ok(cs.estimate(val)),
            FreqSketch::CountV2I64Fast(cs) => Ok(cs.estimate(val)),
            FreqSketch::CountV2I64Regular(cs) => Ok(cs.estimate(val)),
            FreqSketch::CountV2I128Fast(cs) => Ok(cs.estimate(val)),
            FreqSketch::CountV2I128Regular(cs) => Ok(cs.estimate(val)),
            FreqSketch::CountMinFixed(cm) => Ok(cm.estimate(val) as f64),
            FreqSketch::CountMinFixedRegular(cm) => Ok(cm.estimate(val) as f64),
            FreqSketch::CountFixed(cs) => Ok(cs.estimate(val)),
            FreqSketch::CountFixedRegular(cs) => Ok(cs.estimate(val)),
            FreqSketch::CountMinDefaultI32Fast(cm) => Ok(cm.estimate(val) as f64),
            FreqSketch::CountMinDefaultI32Regular(cm) => Ok(cm.estimate(val) as f64),
            FreqSketch::CountMinDefaultI64Fast(cm) => Ok(cm.estimate(val) as f64),
            FreqSketch::CountMinDefaultI64Regular(cm) => Ok(cm.estimate(val) as f64),
            FreqSketch::CountMinDefaultI128Fast(cm) => Ok(cm.estimate(val) as f64),
            FreqSketch::CountMinDefaultI128Regular(cm) => Ok(cm.estimate(val) as f64),
            FreqSketch::CountMinQuickI32Fast(cm) => Ok(cm.estimate(val) as f64),
            FreqSketch::CountMinQuickI32Regular(cm) => Ok(cm.estimate(val) as f64),
            FreqSketch::CountMinQuickI64Fast(cm) => Ok(cm.estimate(val) as f64),
            FreqSketch::CountMinQuickI64Regular(cm) => Ok(cm.estimate(val) as f64),
            FreqSketch::CountMinQuickI128Fast(cm) => Ok(cm.estimate(val) as f64),
            FreqSketch::CountMinQuickI128Regular(cm) => Ok(cm.estimate(val) as f64),
            FreqSketch::CountDefaultI32Fast(cs) => Ok(cs.estimate(val)),
            FreqSketch::CountDefaultI32Regular(cs) => Ok(cs.estimate(val)),
            FreqSketch::CountDefaultI64Fast(cs) => Ok(cs.estimate(val)),
            FreqSketch::CountDefaultI64Regular(cs) => Ok(cs.estimate(val)),
            FreqSketch::CountDefaultI128Fast(cs) => Ok(cs.estimate(val)),
            FreqSketch::CountDefaultI128Regular(cs) => Ok(cs.estimate(val)),
            FreqSketch::CountQuickI32Fast(cs) => Ok(cs.estimate(val)),
            FreqSketch::CountQuickI32Regular(cs) => Ok(cs.estimate(val)),
            FreqSketch::CountQuickI64Fast(cs) => Ok(cs.estimate(val)),
            FreqSketch::CountQuickI64Regular(cs) => Ok(cs.estimate(val)),
            FreqSketch::CountQuickI128Fast(cs) => Ok(cs.estimate(val)),
            FreqSketch::CountQuickI128Regular(cs) => Ok(cs.estimate(val)),
        }
    }

    pub fn query_with_hash_value(&self, hash: &HashValue) -> Result<f64, &'static str> {
        match (self, hash) {
            (FreqSketch::CountMin(cm), HashValue::Matrix(h)) => {
                Ok(cm.fast_estimate_with_hash(h) as f64)
            }
            (FreqSketch::CountMinV2I64Fast(cm), HashValue::Matrix(h)) => {
                Ok(cm.fast_estimate_with_hash(h) as f64)
            }
            (FreqSketch::CountMinV2I128Fast(cm), HashValue::Matrix(h)) => {
                Ok(cm.fast_estimate_with_hash(h) as f64)
            }
            (FreqSketch::Count(cs), HashValue::Matrix(h)) => Ok(cs.fast_estimate_with_hash(h)),
            (FreqSketch::CountV2I64Fast(cs), HashValue::Matrix(h)) => {
                Ok(cs.fast_estimate_with_hash(h))
            }
            (FreqSketch::CountV2I128Fast(cs), HashValue::Matrix(h)) => {
                Ok(cs.fast_estimate_with_hash(h))
            }
            (FreqSketch::CountMinFixed(cm), HashValue::Matrix(h)) => {
                Ok(cm.fast_estimate_with_hash(h) as f64)
            }
            (FreqSketch::CountMinDefaultI32Fast(cm), HashValue::Matrix(h)) => {
                Ok(cm.fast_estimate_with_hash(h) as f64)
            }
            (FreqSketch::CountMinDefaultI64Fast(cm), HashValue::Matrix(h)) => {
                Ok(cm.fast_estimate_with_hash(h) as f64)
            }
            (FreqSketch::CountMinDefaultI128Fast(cm), HashValue::Matrix(h)) => {
                Ok(cm.fast_estimate_with_hash(h) as f64)
            }
            (FreqSketch::CountMinQuickI32Fast(cm), HashValue::Matrix(h)) => {
                Ok(cm.fast_estimate_with_hash(h) as f64)
            }
            (FreqSketch::CountMinQuickI64Fast(cm), HashValue::Matrix(h)) => {
                Ok(cm.fast_estimate_with_hash(h) as f64)
            }
            (FreqSketch::CountMinQuickI128Fast(cm), HashValue::Matrix(h)) => {
                Ok(cm.fast_estimate_with_hash(h) as f64)
            }
            (FreqSketch::CountFixed(cs), HashValue::Matrix(h)) => Ok(cs.fast_estimate_with_hash(h)),
            (FreqSketch::CountDefaultI32Fast(cs), HashValue::Matrix(h)) => {
                Ok(cs.fast_estimate_with_hash(h))
            }
            (FreqSketch::CountDefaultI64Fast(cs), HashValue::Matrix(h)) => {
                Ok(cs.fast_estimate_with_hash(h))
            }
            (FreqSketch::CountDefaultI128Fast(cs), HashValue::Matrix(h)) => {
                Ok(cs.fast_estimate_with_hash(h))
            }
            (FreqSketch::CountQuickI32Fast(cs), HashValue::Matrix(h)) => {
                Ok(cs.fast_estimate_with_hash(h))
            }
            (FreqSketch::CountQuickI64Fast(cs), HashValue::Matrix(h)) => {
                Ok(cs.fast_estimate_with_hash(h))
            }
            (FreqSketch::CountQuickI128Fast(cs), HashValue::Matrix(h)) => {
                Ok(cs.fast_estimate_with_hash(h))
            }
            _ => Err("Hash value type not supported"),
        }
    }

    pub fn try_insert_with_hash_value(&mut self, hash: &HashValue, _val: &SketchInput) -> bool {
        match (self, hash) {
            (FreqSketch::CountMin(cm), HashValue::Matrix(h)) => {
                cm.fast_insert_with_hash_value(h);
                true
            }
            (FreqSketch::CountMinV2I64Fast(cm), HashValue::Matrix(h)) => {
                cm.fast_insert_with_hash_value(h);
                true
            }
            (FreqSketch::CountMinV2I128Fast(cm), HashValue::Matrix(h)) => {
                cm.fast_insert_with_hash_value(h);
                true
            }
            (FreqSketch::Count(cs), HashValue::Matrix(h)) => {
                cs.fast_insert_with_hash_value(h);
                true
            }
            (FreqSketch::CountV2I64Fast(cs), HashValue::Matrix(h)) => {
                cs.fast_insert_with_hash_value(h);
                true
            }
            (FreqSketch::CountV2I128Fast(cs), HashValue::Matrix(h)) => {
                cs.fast_insert_with_hash_value(h);
                true
            }
            (FreqSketch::CountMinFixed(cm), HashValue::Matrix(h)) => {
                cm.fast_insert_with_hash_value(h);
                true
            }
            (FreqSketch::CountMinDefaultI32Fast(cm), HashValue::Matrix(h)) => {
                cm.fast_insert_with_hash_value(h);
                true
            }
            (FreqSketch::CountMinDefaultI64Fast(cm), HashValue::Matrix(h)) => {
                cm.fast_insert_with_hash_value(h);
                true
            }
            (FreqSketch::CountMinDefaultI128Fast(cm), HashValue::Matrix(h)) => {
                cm.fast_insert_with_hash_value(h);
                true
            }
            (FreqSketch::CountMinQuickI32Fast(cm), HashValue::Matrix(h)) => {
                cm.fast_insert_with_hash_value(h);
                true
            }
            (FreqSketch::CountMinQuickI64Fast(cm), HashValue::Matrix(h)) => {
                cm.fast_insert_with_hash_value(h);
                true
            }
            (FreqSketch::CountMinQuickI128Fast(cm), HashValue::Matrix(h)) => {
                cm.fast_insert_with_hash_value(h);
                true
            }
            (FreqSketch::CountFixed(cs), HashValue::Matrix(h)) => {
                cs.fast_insert_with_hash_value(h);
                true
            }
            (FreqSketch::CountDefaultI32Fast(cs), HashValue::Matrix(h)) => {
                cs.fast_insert_with_hash_value(h);
                true
            }
            (FreqSketch::CountDefaultI64Fast(cs), HashValue::Matrix(h)) => {
                cs.fast_insert_with_hash_value(h);
                true
            }
            (FreqSketch::CountDefaultI128Fast(cs), HashValue::Matrix(h)) => {
                cs.fast_insert_with_hash_value(h);
                true
            }
            (FreqSketch::CountQuickI32Fast(cs), HashValue::Matrix(h)) => {
                cs.fast_insert_with_hash_value(h);
                true
            }
            (FreqSketch::CountQuickI64Fast(cs), HashValue::Matrix(h)) => {
                cs.fast_insert_with_hash_value(h);
                true
            }
            (FreqSketch::CountQuickI128Fast(cs), HashValue::Matrix(h)) => {
                cs.fast_insert_with_hash_value(h);
                true
            }
            _ => false,
        }
    }

    pub fn insert_with_hash_only(&mut self, hash: &HashValue) -> Result<(), &'static str> {
        match (self, hash) {
            (FreqSketch::CountMin(cm), HashValue::Matrix(h)) => {
                cm.fast_insert_with_hash_value(h);
                Ok(())
            }
            (FreqSketch::CountMinV2I64Fast(cm), HashValue::Matrix(h)) => {
                cm.fast_insert_with_hash_value(h);
                Ok(())
            }
            (FreqSketch::CountMinV2I128Fast(cm), HashValue::Matrix(h)) => {
                cm.fast_insert_with_hash_value(h);
                Ok(())
            }
            (FreqSketch::Count(cs), HashValue::Matrix(h)) => {
                cs.fast_insert_with_hash_value(h);
                Ok(())
            }
            (FreqSketch::CountV2I64Fast(cs), HashValue::Matrix(h)) => {
                cs.fast_insert_with_hash_value(h);
                Ok(())
            }
            (FreqSketch::CountV2I128Fast(cs), HashValue::Matrix(h)) => {
                cs.fast_insert_with_hash_value(h);
                Ok(())
            }
            (FreqSketch::CountMinFixed(cm), HashValue::Matrix(h)) => {
                cm.fast_insert_with_hash_value(h);
                Ok(())
            }
            (FreqSketch::CountMinDefaultI32Fast(cm), HashValue::Matrix(h)) => {
                cm.fast_insert_with_hash_value(h);
                Ok(())
            }
            (FreqSketch::CountMinDefaultI64Fast(cm), HashValue::Matrix(h)) => {
                cm.fast_insert_with_hash_value(h);
                Ok(())
            }
            (FreqSketch::CountMinDefaultI128Fast(cm), HashValue::Matrix(h)) => {
                cm.fast_insert_with_hash_value(h);
                Ok(())
            }
            (FreqSketch::CountMinQuickI32Fast(cm), HashValue::Matrix(h)) => {
                cm.fast_insert_with_hash_value(h);
                Ok(())
            }
            (FreqSketch::CountMinQuickI64Fast(cm), HashValue::Matrix(h)) => {
                cm.fast_insert_with_hash_value(h);
                Ok(())
            }
            (FreqSketch::CountMinQuickI128Fast(cm), HashValue::Matrix(h)) => {
                cm.fast_insert_with_hash_value(h);
                Ok(())
            }
            (FreqSketch::CountFixed(cs), HashValue::Matrix(h)) => {
                cs.fast_insert_with_hash_value(h);
                Ok(())
            }
            (FreqSketch::CountDefaultI32Fast(cs), HashValue::Matrix(h)) => {
                cs.fast_insert_with_hash_value(h);
                Ok(())
            }
            (FreqSketch::CountDefaultI64Fast(cs), HashValue::Matrix(h)) => {
                cs.fast_insert_with_hash_value(h);
                Ok(())
            }
            (FreqSketch::CountDefaultI128Fast(cs), HashValue::Matrix(h)) => {
                cs.fast_insert_with_hash_value(h);
                Ok(())
            }
            (FreqSketch::CountQuickI32Fast(cs), HashValue::Matrix(h)) => {
                cs.fast_insert_with_hash_value(h);
                Ok(())
            }
            (FreqSketch::CountQuickI64Fast(cs), HashValue::Matrix(h)) => {
                cs.fast_insert_with_hash_value(h);
                Ok(())
            }
            (FreqSketch::CountQuickI128Fast(cs), HashValue::Matrix(h)) => {
                cs.fast_insert_with_hash_value(h);
                Ok(())
            }
            _ => Err("Hash value type not supported"),
        }
    }
}

impl CardinalitySketch {
    pub fn sketch_type(&self) -> &'static str {
        "HLL"
    }

    pub fn hash_domain(&self) -> Option<HashDomain> {
        Some(HashDomain::FastPath64 {
            seed_idx: CANONICAL_HASH_SEED,
        })
    }

    pub fn insert(&mut self, val: &SketchInput) {
        match self {
            CardinalitySketch::HllDf(sketch) => sketch.insert(val),
            CardinalitySketch::HllRegular(sketch) => sketch.insert(val),
            CardinalitySketch::HllHip(sketch) => sketch.insert(val),
        }
    }

    pub fn query(&self, _val: &SketchInput) -> Result<f64, &'static str> {
        match self {
            CardinalitySketch::HllDf(hll_df) => Ok(hll_df.estimate() as f64),
            CardinalitySketch::HllRegular(hll) => Ok(hll.estimate() as f64),
            CardinalitySketch::HllHip(hll) => Ok(hll.estimate() as f64),
        }
    }

    pub fn query_with_hash_value(&self, _hash: &HashValue) -> Result<f64, &'static str> {
        match self {
            CardinalitySketch::HllDf(hll_df) => Ok(hll_df.estimate() as f64),
            CardinalitySketch::HllRegular(hll) => Ok(hll.estimate() as f64),
            CardinalitySketch::HllHip(hll) => Ok(hll.estimate() as f64),
        }
    }

    pub fn try_insert_with_hash_value(&mut self, hash: &HashValue) -> bool {
        match (self, hash) {
            (CardinalitySketch::HllDf(hll_df), HashValue::Fast64(h)) => {
                hll_df.insert_with_hash(*h);
                true
            }
            (CardinalitySketch::HllRegular(hll_df), HashValue::Fast64(h)) => {
                hll_df.insert_with_hash(*h);
                true
            }
            (CardinalitySketch::HllHip(hll_df), HashValue::Fast64(h)) => {
                hll_df.insert_with_hash(*h);
                true
            }
            _ => false,
        }
    }

    pub fn insert_with_hash_only(&mut self, hash: &HashValue) -> Result<(), &'static str> {
        match (self, hash) {
            (CardinalitySketch::HllDf(hll_df), HashValue::Fast64(h)) => {
                hll_df.insert_with_hash(*h);
                Ok(())
            }
            (CardinalitySketch::HllRegular(hll_df), HashValue::Fast64(h)) => {
                hll_df.insert_with_hash(*h);
                Ok(())
            }
            (CardinalitySketch::HllHip(hll_df), HashValue::Fast64(h)) => {
                hll_df.insert_with_hash(*h);
                Ok(())
            }
            _ => Err("Hash value type not supported"),
        }
    }
}

impl QuantileSketch {
    pub fn sketch_type(&self) -> &'static str {
        match self {
            QuantileSketch::Kll(_) => "KLL",
            QuantileSketch::Dd(_) => "DDSketch",
        }
    }

    pub fn hash_domain(&self) -> Option<HashDomain> {
        None
    }

    pub fn insert(&mut self, val: &SketchInput) {
        match self {
            QuantileSketch::Kll(sketch) => {
                let _ = sketch.update(val);
            }
            QuantileSketch::Dd(sketch) => {
                let _ = sketch.add_input(val);
            }
        }
    }

    pub fn query(&self, _val: &SketchInput) -> Result<f64, &'static str> {
        match self {
            QuantileSketch::Kll(_) => Err("KLL requires quantile/CDF queries"),
            QuantileSketch::Dd(_) => Err("DDSketch requires quantile/CDF queries"),
        }
    }

    pub fn query_with_hash_value(&self, _hash: &HashValue) -> Result<f64, &'static str> {
        Err("Hash value type not supported")
    }

    pub fn try_insert_with_hash_value(&mut self, _hash: &HashValue) -> bool {
        false
    }

    pub fn insert_with_hash_only(&mut self, _hash: &HashValue) -> Result<(), &'static str> {
        Err("Hash value type not supported")
    }
}

impl SubpopulationSketch {
    pub fn sketch_type(&self) -> &'static str {
        match self {
            SubpopulationSketch::Hydra(_) => "Hydra",
            SubpopulationSketch::MultiHydra(_) => "MultiHydra",
        }
    }

    pub fn hash_domain(&self) -> Option<HashDomain> {
        None
    }

    pub fn insert(&mut self, _val: &SketchInput) {
        match self {
            SubpopulationSketch::Hydra(_) | SubpopulationSketch::MultiHydra(_) => {}
        }
    }

    pub fn query(&self, _val: &SketchInput) -> Result<f64, &'static str> {
        match self {
            SubpopulationSketch::Hydra(_) | SubpopulationSketch::MultiHydra(_) => {
                Err("Hydra requires HydraQuery")
            }
        }
    }

    pub fn query_with_hash_value(&self, _hash: &HashValue) -> Result<f64, &'static str> {
        Err("Hash value type not supported")
    }

    pub fn try_insert_with_hash_value(&mut self, hash: &HashValue, val: &SketchInput) -> bool {
        let _ = (hash, val);
        false
    }

    pub fn insert_with_hash_only(&mut self, _hash: &HashValue) -> Result<(), &'static str> {
        Err("Hash value type not supported")
    }
}

impl SubquerySketch {
    pub fn sketch_type(&self) -> &'static str {
        match self {
            SubquerySketch::Coco(_) => "Coco",
        }
    }

    pub fn hash_domain(&self) -> Option<HashDomain> {
        None
    }

    pub fn insert(&mut self, val: &SketchInput) {
        match (self, val) {
            (SubquerySketch::Coco(sketch), SketchInput::Str(key)) => sketch.insert(key, 1),
            (SubquerySketch::Coco(sketch), SketchInput::String(key)) => {
                sketch.insert(key.as_str(), 1)
            }
            (SubquerySketch::Coco(sketch), SketchInput::Bytes(bytes)) => {
                if let Ok(key) = std::str::from_utf8(bytes) {
                    sketch.insert(key, 1);
                }
            }
            _ => {}
        }
    }

    pub fn query(&self, _val: &SketchInput) -> Result<f64, &'static str> {
        Err("Subquery requires a subquery-specific query type")
    }

    pub fn query_with_hash_value(&self, _hash: &HashValue) -> Result<f64, &'static str> {
        Err("Hash value type not supported")
    }

    pub fn try_insert_with_hash_value(&mut self, _hash: &HashValue, _val: &SketchInput) -> bool {
        false
    }

    pub fn insert_with_hash_only(&mut self, _hash: &HashValue) -> Result<(), &'static str> {
        Err("Hash value type not supported")
    }
}

impl GSumSketch {
    pub fn sketch_type(&self) -> &'static str {
        "UnivMon"
    }

    pub fn hash_domain(&self) -> Option<HashDomain> {
        None
    }

    pub fn insert(&mut self, val: &SketchInput) {
        match self {
            GSumSketch::UnivMon(sketch) => sketch.insert(val, 1),
        }
    }

    pub fn query(&self, _val: &SketchInput) -> Result<f64, &'static str> {
        Err("UnivMon requires a query type")
    }

    pub fn query_with_hash_value(&self, _hash: &HashValue) -> Result<f64, &'static str> {
        Err("Hash value type not supported")
    }

    pub fn try_insert_with_hash_value(&mut self, _hash: &HashValue, _val: &SketchInput) -> bool {
        false
    }

    pub fn insert_with_hash_only(&mut self, _hash: &HashValue) -> Result<(), &'static str> {
        Err("Hash value type not supported")
    }
}

impl OrchestratedSketch {
    pub fn sketch_type(&self) -> &'static str {
        match self {
            OrchestratedSketch::Freq(sketch) => sketch.sketch_type(),
            OrchestratedSketch::Cardinality(sketch) => sketch.sketch_type(),
            OrchestratedSketch::Quantile(sketch) => sketch.sketch_type(),
            OrchestratedSketch::Subpopulation(sketch) => sketch.sketch_type(),
            OrchestratedSketch::Subquery(sketch) => sketch.sketch_type(),
            OrchestratedSketch::GSum(sketch) => sketch.sketch_type(),
        }
    }

    pub fn supports_hash_reuse(&self) -> bool {
        self.hash_domain().is_some() || matches!(self, OrchestratedSketch::Cardinality(_))
    }

    pub fn hash_domain(&self) -> Option<HashDomain> {
        match self {
            OrchestratedSketch::Freq(sketch) => sketch.hash_domain(),
            OrchestratedSketch::Cardinality(sketch) => sketch.hash_domain(),
            OrchestratedSketch::Quantile(sketch) => sketch.hash_domain(),
            OrchestratedSketch::Subpopulation(sketch) => sketch.hash_domain(),
            OrchestratedSketch::Subquery(sketch) => sketch.hash_domain(),
            OrchestratedSketch::GSum(sketch) => sketch.hash_domain(),
        }
    }

    pub fn insert(&mut self, val: &SketchInput) {
        match self {
            OrchestratedSketch::Freq(sketch) => sketch.insert(val),
            OrchestratedSketch::Cardinality(sketch) => sketch.insert(val),
            OrchestratedSketch::Quantile(sketch) => sketch.insert(val),
            OrchestratedSketch::Subpopulation(sketch) => sketch.insert(val),
            OrchestratedSketch::Subquery(sketch) => sketch.insert(val),
            OrchestratedSketch::GSum(sketch) => sketch.insert(val),
        }
    }

    pub fn insert_with_request(
        &mut self,
        req: &OrchestratorInsert<'_>,
    ) -> Result<(), &'static str> {
        match req {
            OrchestratorInsert::Sketch(val) => {
                self.insert(val);
                Ok(())
            }
            OrchestratorInsert::Hydra { key, value, count } => match self {
                OrchestratedSketch::Subpopulation(SubpopulationSketch::Hydra(h)) => {
                    h.update(key, value, *count);
                    Ok(())
                }
                _ => Err("Input type not supported by sketch"),
            },
            OrchestratorInsert::UnivMon { key, value } => match self {
                OrchestratedSketch::GSum(GSumSketch::UnivMon(um)) => {
                    um.insert(key, *value);
                    Ok(())
                }
                _ => Err("Input type not supported by sketch"),
            },
        }
    }

    pub fn insert_with_hash_value(&mut self, hash: &HashValue, val: &SketchInput) {
        if self.try_insert_with_hash_value(hash, val) {
            return;
        }
        self.insert(val);
    }

    pub fn query(&self, val: &SketchInput) -> Result<f64, &'static str> {
        match self {
            OrchestratedSketch::Freq(sketch) => sketch.query(val),
            OrchestratedSketch::Cardinality(sketch) => sketch.query(val),
            OrchestratedSketch::Quantile(sketch) => sketch.query(val),
            OrchestratedSketch::Subpopulation(sketch) => sketch.query(val),
            OrchestratedSketch::Subquery(sketch) => sketch.query(val),
            OrchestratedSketch::GSum(sketch) => sketch.query(val),
        }
    }

    pub fn query_with_request(&self, req: &OrchestratorQuery<'_>) -> Result<f64, &'static str> {
        match (self, req) {
            (_, OrchestratorQuery::Sketch(val)) => self.query(val),
            (
                OrchestratedSketch::Subpopulation(SubpopulationSketch::Hydra(h)),
                OrchestratorQuery::Hydra { key, query },
            ) => Ok(h.query_key(key.clone(), query)),
            (OrchestratedSketch::GSum(GSumSketch::UnivMon(um)), OrchestratorQuery::UnivMon(q)) => {
                match q {
                    UnivMonQuery::Cardinality => Ok(um.calc_card()),
                    UnivMonQuery::L1Norm => Ok(um.calc_l1()),
                    UnivMonQuery::L2Norm => Ok(um.calc_l2()),
                    UnivMonQuery::Entropy => Ok(um.calc_entropy()),
                }
            }
            _ => Err("Query type not supported by sketch"),
        }
    }

    pub fn query_with_hash_value(&self, hash: &HashValue) -> Result<f64, &'static str> {
        match self {
            OrchestratedSketch::Freq(sketch) => sketch.query_with_hash_value(hash),
            OrchestratedSketch::Cardinality(sketch) => sketch.query_with_hash_value(hash),
            OrchestratedSketch::Quantile(sketch) => sketch.query_with_hash_value(hash),
            OrchestratedSketch::Subpopulation(sketch) => sketch.query_with_hash_value(hash),
            OrchestratedSketch::Subquery(sketch) => sketch.query_with_hash_value(hash),
            OrchestratedSketch::GSum(sketch) => sketch.query_with_hash_value(hash),
        }
    }

    pub fn try_insert_with_hash_value(&mut self, hash: &HashValue, val: &SketchInput) -> bool {
        match self {
            OrchestratedSketch::Freq(sketch) => sketch.try_insert_with_hash_value(hash, val),
            OrchestratedSketch::Cardinality(sketch) => sketch.try_insert_with_hash_value(hash),
            OrchestratedSketch::Quantile(sketch) => sketch.try_insert_with_hash_value(hash),
            OrchestratedSketch::Subpopulation(sketch) => {
                sketch.try_insert_with_hash_value(hash, val)
            }
            OrchestratedSketch::Subquery(sketch) => sketch.try_insert_with_hash_value(hash, val),
            OrchestratedSketch::GSum(sketch) => sketch.try_insert_with_hash_value(hash, val),
        }
    }

    pub fn insert_with_hash_only(&mut self, hash: &HashValue) -> Result<(), &'static str> {
        match self {
            OrchestratedSketch::Freq(sketch) => sketch.insert_with_hash_only(hash),
            OrchestratedSketch::Cardinality(sketch) => sketch.insert_with_hash_only(hash),
            OrchestratedSketch::Quantile(sketch) => sketch.insert_with_hash_only(hash),
            OrchestratedSketch::Subpopulation(sketch) => sketch.insert_with_hash_only(hash),
            OrchestratedSketch::Subquery(sketch) => sketch.insert_with_hash_only(hash),
            OrchestratedSketch::GSum(sketch) => sketch.insert_with_hash_only(hash),
        }
    }
}
