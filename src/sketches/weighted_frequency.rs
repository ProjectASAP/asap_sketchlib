//! Float64 weighted CMS/CountSketch with typed candidate identities.
//! Independent of Planner types, execution scopes and batch/runtime interfaces.
//! The versioned snapshot preserves the representation extracted from ASAPPlanner.
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::BinaryHeap};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WeightedFrequencyError {
    Invalid(String),
    Update(String),
}
impl std::fmt::Display for WeightedFrequencyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) | Self::Update(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for WeightedFrequencyError {}
type Error = WeightedFrequencyError;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FrequencyIdentity {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Utf8(String),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Candidate {
    identity: Vec<FrequencyIdentity>,
    key: Vec<u8>,
    score: f64,
}
impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Candidate {}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // The weakest candidate is the root of this bounded min-heap.
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| other.key.cmp(&self.key))
    }
}
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrequencyAlgorithm {
    Cms,
    CountSketch,
}
/// Weighted frequency estimates with bounded candidate retention.
///
/// CMS accepts nonnegative finite weights; CountSketch accepts signed finite
/// weights and requires positive odd depth. Floating identities must be finite;
/// signed zeros denote the same identity. The heap ranks estimated scores, not
/// absolute magnitudes, and does not establish candidate completeness.
///
/// ```
/// use asap_sketchlib::{FrequencyAlgorithm, FrequencyIdentity, WeightedFrequency};
/// let mut state = WeightedFrequency::new(FrequencyAlgorithm::Cms, 1024, 5, 16)?;
/// state.update(&[FrequencyIdentity::Utf8("api".into())], 0.125)?;
/// state.update(&[FrequencyIdentity::Utf8("api".into())], 0.25)?;
/// assert_eq!(state.topk(1)[0].1, 0.375);
/// # Ok::<(), asap_sketchlib::WeightedFrequencyError>(())
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WeightedFrequency {
    algorithm: FrequencyAlgorithm,
    width: usize,
    depth: usize,
    capacity: usize,
    cells: Vec<f64>,
    candidates: BinaryHeap<Candidate>,
}
impl WeightedFrequency {
    pub fn algorithm(&self) -> FrequencyAlgorithm {
        self.algorithm
    }
    pub fn shape(&self) -> (usize, usize, usize) {
        (self.width, self.depth, self.capacity)
    }
    pub fn new(
        algorithm: FrequencyAlgorithm,
        width: usize,
        depth: usize,
        capacity: usize,
    ) -> Result<Self, Error> {
        let len = width
            .checked_mul(depth)
            .filter(|_| {
                width > 0
                    && depth > 0
                    && capacity > 0
                    && (algorithm != FrequencyAlgorithm::CountSketch || depth % 2 == 1)
            })
            .ok_or_else(|| Error::Invalid("invalid weighted frequency dimensions".into()))?;
        let mut cells = Vec::new();
        cells
            .try_reserve_exact(len)
            .map_err(|_| Error::Invalid("weighted frequency allocation failed".into()))?;
        cells.resize(len, 0.0);
        Ok(Self {
            algorithm,
            width,
            depth,
            capacity,
            cells,
            candidates: BinaryHeap::new(),
        })
    }
    /// Decode only this kernel's versioned Float64 representation. Integer CMS
    /// wire frames are different representations and are not accepted here.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        use bincode::Options;
        let bytes = bytes
            .strip_prefix(b"ASAP-WFREQ-1\0")
            .ok_or_else(|| Error::Invalid("weighted frequency format/version mismatch".into()))?;
        let mut state: Self = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(bytes.len() as u64)
            .reject_trailing_bytes()
            .deserialize(bytes)
            .map_err(|e| Error::Invalid(e.to_string()))?;
        if state.width == 0
            || state.depth == 0
            || state.capacity == 0
            || (state.algorithm == FrequencyAlgorithm::CountSketch && state.depth % 2 == 0)
            || state.width.checked_mul(state.depth) != Some(state.cells.len())
            || state
                .cells
                .iter()
                .any(|v| !v.is_finite() || (state.algorithm == FrequencyAlgorithm::Cms && *v < 0.0))
            || state.candidates.len() > state.capacity
        {
            return Err(Error::Invalid("invalid weighted frequency state".into()));
        }
        for candidate in &state.candidates {
            if candidate
                .identity
                .iter()
                .any(|v| matches!(v, FrequencyIdentity::Float64(n) if !n.is_finite()))
                || bincode::serialize(&candidate.identity)
                    .map_err(|e| Error::Invalid(e.to_string()))?
                    != candidate.key
            {
                return Err(Error::Invalid("invalid weighted frequency identity".into()));
            }
        }
        state.retain(state.candidates.iter().cloned().collect());
        Ok(state)
    }
    fn indexes<'a>(&'a self, key: &'a [u8]) -> impl Iterator<Item = (usize, f64)> + 'a {
        (0..self.depth).map(move |row| {
            let bucket =
                (xxhash_rust::xxh64::xxh64(key, 2 * row as u64) % self.width as u64) as usize;
            let sign = if self.algorithm == FrequencyAlgorithm::CountSketch
                && xxhash_rust::xxh64::xxh64(key, 2 * row as u64 + 1) & 1 != 0
            {
                -1.0
            } else {
                1.0
            };
            (row * self.width + bucket, sign)
        })
    }
    fn estimate(&self, key: &[u8]) -> f64 {
        let mut estimates = self
            .indexes(key)
            .map(|(i, sign)| self.cells[i] * sign)
            .collect::<Vec<_>>();
        match self.algorithm {
            FrequencyAlgorithm::Cms => estimates.into_iter().fold(f64::INFINITY, f64::min),
            FrequencyAlgorithm::CountSketch => {
                let middle = estimates.len() / 2;
                *estimates.select_nth_unstable_by(middle, f64::total_cmp).1
            }
        }
    }
    fn retain(&mut self, mut candidates: Vec<Candidate>) {
        candidates.sort_by(|a, b| a.key.cmp(&b.key));
        candidates.dedup_by(|a, b| a.key == b.key);
        self.candidates.clear();
        for mut candidate in candidates {
            candidate.score = self.estimate(&candidate.key);
            self.candidates.push(candidate);
            if self.candidates.len() > self.capacity {
                self.candidates.pop();
            }
        }
    }
    pub fn update(&mut self, values: &[FrequencyIdentity], weight: f64) -> Result<(), Error> {
        if !weight.is_finite() || (self.algorithm == FrequencyAlgorithm::Cms && weight < 0.0) {
            return Err(Error::Update(
                "weighted frequency requires finite weights; CMS additionally requires nonnegative weights".into(),
            ));
        }
        let identity = values
            .iter()
            .map(|value| match value {
                FrequencyIdentity::Float64(v) if !v.is_finite() => Err(Error::Invalid(
                    "non-finite weighted frequency identity".into(),
                )),
                FrequencyIdentity::Float64(v) if *v == 0.0 => Ok(FrequencyIdentity::Float64(0.0)),
                value => Ok(value.clone()),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let key = bincode::serialize(&identity).map_err(|e| Error::Update(e.to_string()))?;
        let indexes = self.indexes(&key).collect::<Vec<_>>();
        if indexes
            .iter()
            .any(|&(i, sign)| !(self.cells[i] + sign * weight).is_finite())
        {
            return Err(Error::Update("weighted frequency sum overflow".into()));
        }
        for (i, sign) in indexes {
            self.cells[i] += sign * weight;
        }
        let mut candidates = self.candidates.iter().cloned().collect::<Vec<_>>();
        candidates.push(Candidate {
            identity,
            key,
            score: 0.0,
        });
        self.retain(candidates);
        Ok(())
    }
    /// Return retained candidates in descending estimated-score order.
    /// Candidate retention alone does not guarantee top-k membership completeness.
    pub fn topk(&self, n: usize) -> Vec<(Vec<FrequencyIdentity>, f64)> {
        let mut candidates = self.candidates.iter().collect::<Vec<_>>();
        candidates.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.key.cmp(&b.key)));
        candidates
            .into_iter()
            .take(n)
            .map(|c| (c.identity.clone(), c.score))
            .collect()
    }
    /// Merge compatible states without modifying either input.
    pub fn merge(&self, other: &Self) -> Result<Self, Error> {
        if self.algorithm != other.algorithm || self.shape() != other.shape() {
            return Err(Error::Invalid("weighted frequency shape mismatch".into()));
        }
        let mut result = self.clone();
        for (value, rhs) in result.cells.iter_mut().zip(&other.cells) {
            *value += rhs;
            if !value.is_finite() {
                return Err(Error::Update("weighted frequency merge overflow".into()));
            }
        }
        result.retain(
            self.candidates
                .iter()
                .chain(&other.candidates)
                .cloned()
                .collect(),
        );
        Ok(result)
    }
    pub fn approx_memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.cells.capacity() * 8
            + self
                .candidates
                .iter()
                .map(|c| {
                    std::mem::size_of::<Candidate>()
                        + c.key.capacity()
                        + c.identity
                            .iter()
                            .map(|v| {
                                std::mem::size_of::<FrequencyIdentity>()
                                    + if let FrequencyIdentity::Utf8(s) = v {
                                        s.capacity()
                                    } else {
                                        0
                                    }
                            })
                            .sum::<usize>()
                })
                .sum::<usize>()
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = b"ASAP-WFREQ-1\0".to_vec();
        bytes.extend(bincode::serialize(self).expect("serializable frequency state"));
        bytes
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    // Signed fractional updates and merges retain numeric ranking, not magnitude ranking.
    #[test]
    fn count_sketch_signed_updates_roundtrip_and_merge() {
        let mut left = WeightedFrequency::new(FrequencyAlgorithm::CountSketch, 4096, 5, 8).unwrap();
        left.update(&[FrequencyIdentity::Int64(1)], -10.5).unwrap();
        left.update(&[FrequencyIdentity::Null], 0.125).unwrap();
        left.update(&[FrequencyIdentity::Null], -0.0625).unwrap();
        let mut right =
            WeightedFrequency::new(FrequencyAlgorithm::CountSketch, 4096, 5, 8).unwrap();
        right.update(&[FrequencyIdentity::Null], 0.25).unwrap();
        let merged = left.merge(&right).unwrap();
        let decoded = WeightedFrequency::from_bytes(&merged.to_bytes()).unwrap();
        let rows = decoded.topk(2);
        assert!(matches!(rows[0].0[0], FrequencyIdentity::Null));
        assert!(matches!(rows[0].1, 0.3125));
        assert!(matches!(rows[1].1, -10.5));
        assert!(
            left.merge(&WeightedFrequency::new(FrequencyAlgorithm::Cms, 4096, 5, 8).unwrap())
                .is_err()
        );
        let before = left.to_bytes();
        for weight in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(left.update(&[FrequencyIdentity::Null], weight).is_err());
            assert_eq!(left.to_bytes(), before);
        }
    }

    // CountSketch uses sign-corrected median: one corrupted row cannot dominate five rows.
    #[test]
    fn count_sketch_median_and_odd_depth_contract() {
        assert!(WeightedFrequency::new(FrequencyAlgorithm::CountSketch, 8, 2, 2).is_err());
        let mut state = WeightedFrequency::new(FrequencyAlgorithm::CountSketch, 16, 5, 8).unwrap();
        state
            .update(&[FrequencyIdentity::Int64(4)], -0.375)
            .unwrap();
        let key = state.candidates.peek().unwrap().key.clone();
        let indexes = state.indexes(&key).collect::<Vec<_>>();
        for &(index, sign) in &indexes {
            assert_eq!(state.cells[index] * sign, -0.375);
        }
        state.cells[indexes[0].0] += 1000.0;
        assert_eq!(state.estimate(&key), -0.375);
        let mut invalid = state.clone();
        invalid.depth = 4;
        invalid.cells.truncate(64);
        assert!(WeightedFrequency::from_bytes(&invalid.to_bytes()).is_err());
    }

    // Invalid rates must not mutate state; typed keys cannot collide by formatting.
    #[test]
    fn fractional_updates_typed_identities_and_invalid_weights() {
        let mut state = WeightedFrequency::new(FrequencyAlgorithm::Cms, 4096, 5, 8).unwrap();
        state.update(&[FrequencyIdentity::Int64(1)], 0.125).unwrap();
        state.update(&[FrequencyIdentity::Int64(1)], 0.125).unwrap();
        state
            .update(&[FrequencyIdentity::Utf8("1".into())], 0.5)
            .unwrap();
        state.update(&[FrequencyIdentity::Null], 0.75).unwrap();
        let before = state.to_bytes();
        let decoded = WeightedFrequency::from_bytes(&before).unwrap();
        assert_eq!(decoded.topk(8).len(), 3);
        assert!(WeightedFrequency::from_bytes(b"old integer state").is_err());
        for weight in [-1.0, f64::INFINITY, f64::NAN] {
            assert!(state.update(&[FrequencyIdentity::Null], weight).is_err());
            assert_eq!(state.to_bytes(), before);
        }
        let rows = state.topk(8);
        assert_eq!(rows.len(), 3);
        assert!(matches!(rows[0].0[0], FrequencyIdentity::Null));
        assert!(matches!(rows[1].0[0], FrequencyIdentity::Utf8(_)));
        assert!(matches!(rows[2].1, 0.25));
    }
    // Merge uses the same Float64 state representation and rejects other shapes.
    #[test]
    fn compatible_merge_preserves_fractional_weights() {
        let mut left = WeightedFrequency::new(FrequencyAlgorithm::Cms, 4096, 5, 8).unwrap();
        let mut right = left.clone();
        left.update(&[FrequencyIdentity::Int64(7)], 0.125).unwrap();
        right.update(&[FrequencyIdentity::Int64(7)], 0.25).unwrap();
        let merged = left.merge(&right).unwrap();
        assert!(matches!(merged.topk(1)[0].1, 0.375));
        assert!(
            left.merge(&WeightedFrequency::new(FrequencyAlgorithm::Cms, 32, 5, 8).unwrap())
                .is_err()
        );
    }
    // Extraction must preserve already-produced typed snapshots byte for byte.
    #[test]
    fn planner_v1_snapshots_remain_compatible() {
        for (algorithm, bytes) in [
            (
                FrequencyAlgorithm::Cms,
                include_bytes!("../../tests/fixtures/weighted_frequency/cms_v1.bin").as_slice(),
            ),
            (
                FrequencyAlgorithm::CountSketch,
                include_bytes!("../../tests/fixtures/weighted_frequency/count_sketch_v1.bin")
                    .as_slice(),
            ),
        ] {
            let mut state = WeightedFrequency::new(algorithm, 8, 3, 4).unwrap();
            state.update(&[FrequencyIdentity::Int64(1)], 0.125).unwrap();
            state
                .update(&[FrequencyIdentity::Utf8("1".into())], 0.5)
                .unwrap();
            state.update(&[FrequencyIdentity::Null], 0.75).unwrap();
            assert_eq!(state.to_bytes(), bytes);
            let decoded = WeightedFrequency::from_bytes(bytes).unwrap();
            assert_eq!(decoded.topk(4), state.topk(4));
            assert_eq!(decoded.to_bytes(), bytes);
        }
    }
    // Numeric zero signs share an identity; tuple and type boundaries remain distinct.
    #[test]
    fn typed_tuples_zero_and_rejected_inputs() {
        let mut state = WeightedFrequency::new(FrequencyAlgorithm::Cms, 4096, 5, 8).unwrap();
        state
            .update(&[FrequencyIdentity::Float64(-0.0)], 0.25)
            .unwrap();
        state
            .update(&[FrequencyIdentity::Float64(0.0)], 0.5)
            .unwrap();
        assert_eq!(
            state.topk(8),
            vec![(vec![FrequencyIdentity::Float64(0.0)], 0.75)]
        );
        state
            .update(
                &[
                    FrequencyIdentity::Utf8("ab".into()),
                    FrequencyIdentity::Utf8("c".into()),
                ],
                1.0,
            )
            .unwrap();
        state
            .update(
                &[
                    FrequencyIdentity::Utf8("a".into()),
                    FrequencyIdentity::Utf8("bc".into()),
                ],
                2.0,
            )
            .unwrap();
        assert_eq!(state.topk(8).len(), 3);
        let before = state.to_bytes();
        assert!(
            state
                .update(&[FrequencyIdentity::Float64(f64::NAN)], 1.0)
                .is_err()
        );
        assert_eq!(state.to_bytes(), before);
        let mut trailing = before.clone();
        trailing.push(0);
        assert!(WeightedFrequency::from_bytes(&trailing).is_err());
    }
    // Overflow errors are atomic, and candidate storage never exceeds its configured bound.
    #[test]
    fn overflow_and_candidate_capacity() {
        for algorithm in [FrequencyAlgorithm::Cms, FrequencyAlgorithm::CountSketch] {
            let mut state = WeightedFrequency::new(algorithm, 16, 3, 2).unwrap();
            state
                .update(&[FrequencyIdentity::Int64(1)], f64::MAX)
                .unwrap();
            let before = state.to_bytes();
            assert!(
                state
                    .update(&[FrequencyIdentity::Int64(1)], f64::MAX)
                    .is_err()
            );
            assert!(state.merge(&state).is_err());
            assert_eq!(state.to_bytes(), before);
            let mut bounded = WeightedFrequency::new(algorithm, 4096, 5, 2).unwrap();
            for i in 0..20 {
                bounded
                    .update(&[FrequencyIdentity::Int64(i)], i as f64)
                    .unwrap();
            }
            assert_eq!(bounded.topk(100).len(), 2);
        }
        for (width, depth, capacity) in [(0, 3, 1), (1, 0, 1), (1, 3, 0), (usize::MAX, 3, 1)] {
            assert!(
                WeightedFrequency::new(FrequencyAlgorithm::Cms, width, depth, capacity).is_err()
            );
        }
    }
}
