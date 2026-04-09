#[cfg(feature = "experimental")]
pub mod coco;
#[cfg(feature = "experimental")]
pub use coco::Coco;
#[cfg(feature = "experimental")]
pub use coco::CocoBucket;

pub mod count;
pub use count::Count;
pub use count::CountL2HH;

pub mod mode;
pub use mode::{FastPath, RegularPath};

pub mod countmin;
pub use crate::MatrixStorage;
pub use countmin::{CountMin, QUICKSTART_COL_NUM, QUICKSTART_ROW_NUM};

#[cfg(feature = "experimental")]
pub mod elastic;
#[cfg(feature = "experimental")]
pub use elastic::Elastic;
#[cfg(feature = "experimental")]
pub use elastic::HeavyBucket;

pub mod hll;
pub use hll::{
    ErtlMLE, HyperLogLog, HyperLogLogHIP, HyperLogLogHIPP12, HyperLogLogHIPP14, HyperLogLogHIPP16,
    HyperLogLogP12, HyperLogLogP14, HyperLogLogP16, Classic,
};

pub mod kll;
pub use kll::KLL;

pub mod kll_dynamic;
pub use kll_dynamic::KLLDynamic;

#[cfg(feature = "experimental")]
pub mod kmv;
#[cfg(feature = "experimental")]
pub use kmv::KMV;

#[cfg(feature = "experimental")]
pub mod uniform;
#[cfg(feature = "experimental")]
pub use uniform::UniformSampling;

pub mod ddsketch;
pub use ddsketch::DDSketch;

pub mod cms_heap;
pub use cms_heap::CMSHeap;

pub mod cs_heap;
pub use cs_heap::CSHeap;

pub mod octo_delta;
pub use octo_delta::{CM_PROMASK, COUNT_PROMASK, CmDelta, CountDelta, HLL_PROMASK, HllDelta};

pub mod fold_cms;
pub use fold_cms::{FoldCMS, FoldCell, FoldEntry};

pub mod fold_cs;
pub use fold_cs::FoldCS;
