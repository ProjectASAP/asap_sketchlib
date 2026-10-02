//! The per-cell KLL type of [`crate::message_pack_format::portable::hydra_kll`].

use serde::{Deserialize, Serialize};

use crate::sketches::kll::KLL;

/// One `HydraKllSketch` cell: a `KLL<f64>` with its `k`.
#[derive(Clone)]
pub struct KllSketch {
    pub k: u16,
    pub(crate) backend: KLL<f64>,
}

impl KllSketch {
    pub fn new(k: u16) -> Self {
        Self {
            k,
            backend: KLL::init_kll(k as i32),
        }
    }

    /// [`KllSketch::new`] with an explicit compaction-coin seed.
    pub fn with_seed(k: u16, seed: u64) -> Self {
        Self {
            k,
            backend: KLL::init_kll_with_seed(k as i32, seed),
        }
    }

    /// The cell's ASAPv1 KLL bytes.
    pub fn sketch_bytes(&self) -> Vec<u8> {
        self.backend.serialize_to_bytes().unwrap()
    }

    pub fn update(&mut self, value: f64) {
        self.backend.update(&value);
    }

    pub fn count(&self) -> u64 {
        self.backend.count() as u64
    }

    /// Estimate the value at the given quantile `q ∈ [0, 1]`.
    pub fn quantile(&self, q: f64) -> f64 {
        if self.count() == 0 {
            return 0.0;
        }
        self.backend.quantile(q)
    }

    /// Merge another cell into self in place. Both operands must have
    /// identical `k`.
    pub fn merge(
        &mut self,
        other: &KllSketch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.k != other.k {
            return Err(format!("KllSketch k mismatch: self={}, other={}", self.k, other.k).into());
        }
        self.backend.merge(&other.backend);
        Ok(())
    }
}

impl std::fmt::Debug for KllSketch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KllSketch")
            .field("k", &self.k)
            .field("sketch_n", &self.count())
            .finish()
    }
}

/// Wire DTO for one cell of
/// [`crate::message_pack_format::portable::hydra_kll::HydraKllSketchWire`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KllSketchData {
    pub k: u16,
    pub sketch_bytes: Vec<u8>,
}
