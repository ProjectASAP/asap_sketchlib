use crate::utils::LASTSTATE;

use super::CountMin;
use super::utils::{SketchInput, hash_it};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct HeavyCounter {
    pub key: String, // flow id?
    pub vote_pos: i32,
    pub vote_neg: i32,
    pub flag: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HeavyBucket {
    pub flow_id: String,
    pub vote_pos: i32,
    pub vote_neg: i32,
    pub eviction: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Elastic {
    pub heavy: Vec<HeavyBucket>,
    pub light: CountMin,
    pub bktlen: i32,
}

impl HeavyBucket {
    pub fn new() -> Self {
        HeavyBucket {
            flow_id: "".to_string(),
            vote_pos: 0,
            vote_neg: 0,
            eviction: false,
        }
    }

    pub fn evict(&mut self, id: String) -> () {
        self.flow_id = id;
        self.vote_pos = 1;
        self.vote_neg = 1;
        self.eviction = true;
    }
}

impl Default for Elastic {
    fn default() -> Self {
        Self::new()
    }
}

impl Elastic {
    pub fn new() -> Self {
        Elastic::init_with_length(8)
    }

    pub fn init_with_length(l: i32) -> Self {
        let mut heavy = Vec::with_capacity(l as usize);
        for _ in 0..l {
            heavy.push(HeavyBucket::new());
        }
        let light = CountMin::default();
        Elastic {
            heavy: heavy,
            light: light,
            bktlen: l,
        }
    }

    pub fn insert(&mut self, id: String) {
        // let hash = hash_it(LASTSTATE, &id);
        let hash = hash_it(LASTSTATE, &SketchInput::String(id.clone()));
        let idx = hash as usize % self.bktlen as usize;
        let heavy_bkt = &mut self.heavy[idx];
        if heavy_bkt.flow_id == "" && heavy_bkt.vote_neg == 0 && heavy_bkt.vote_pos == 0 {
            // empty
            heavy_bkt.flow_id = id;
            heavy_bkt.vote_pos += 1;
        } else if id == heavy_bkt.flow_id {
            // matched
            heavy_bkt.vote_pos += 1;
        } else if id != heavy_bkt.flow_id {
            heavy_bkt.vote_neg += 1;
            if heavy_bkt.vote_neg / heavy_bkt.vote_pos < 8 {
                // self.light.insert_cm(&id);
                self.light.insert_cm(&SketchInput::String(id));
            } else {
                let vote = heavy_bkt.vote_pos;
                heavy_bkt.evict(id);
                for _ in 0..vote {
                    // self.light. insert_cm(&to_evict);
                    self.light
                        .insert_cm(&SketchInput::String(heavy_bkt.flow_id.clone()));
                }
            }
        }
    }

    pub fn query(&mut self, id: String) -> i32 {
        // let hash = hash_it(LASTSTATE, &id);
        let hash = hash_it(LASTSTATE, &SketchInput::String(id.clone()));
        let idx = hash as usize % self.bktlen as usize;
        let heavy_bkt = &self.heavy[idx];
        if id == heavy_bkt.flow_id {
            if heavy_bkt.eviction {
                // let light_result = self.light.get_est(&id) as i32;
                let light_result = self.light.get_est(&SketchInput::String(id)) as i32;
                let heavy_result = heavy_bkt.vote_pos;
                return light_result + heavy_result;
            } else {
                return heavy_bkt.vote_pos;
            }
        } else {
            // return self.light.get_est(&id) as i32;
            return self.light.get_est(&SketchInput::String(id)) as i32;
        }
    }
}
