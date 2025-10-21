use super::Chapter;
use crate::{SketchInput, TopKHeap};

pub struct UM<'a> {
    pub k: usize,
    pub row: usize,
    pub col: usize,
    pub layer: usize,
    pub layers: Vec<Chapter<'a>>,
    pub hh_layers: Vec<TopKHeap>,
    pub pool_idx: i64,
    pub heap_update: i32,
    pub bucket_size: usize,
}