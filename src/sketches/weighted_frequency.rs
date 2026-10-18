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
/// Canonical identity tuple and its hash key: floating identities must be
/// finite, and signed zeros share one identity.
/// The identity is allocated at its exact length, so a retained candidate
/// holds no spare slots.
fn normalize(values: &[FrequencyIdentity]) -> Result<(Vec<FrequencyIdentity>, Vec<u8>), Error> {
    let mut identity = Vec::with_capacity(values.len());
    for value in values {
        identity.push(match value {
            FrequencyIdentity::Float64(v) if !v.is_finite() => {
                return Err(Error::Invalid(
                    "non-finite weighted frequency identity".into(),
                ));
            }
            FrequencyIdentity::Float64(v) if *v == 0.0 => FrequencyIdentity::Float64(0.0),
            value => value.clone(),
        });
    }
    let key = bincode::serialize(&identity).map_err(|e| Error::Update(e.to_string()))?;
    Ok((identity, key))
}
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrequencyAlgorithm {
    Cms,
    CountSketch,
}
/// Weighted frequency estimates with bounded candidate retention.
///
/// CMS accepts nonnegative finite weights; CountSketch accepts signed finite
/// weights and requires positive odd depth. Each row hashes with its own seed,
/// so rows are pairwise independent at any depth. Floating identities must be
/// finite; signed zeros denote the same identity. Capacity 0 retains no
/// candidates: the state is then a plain frequency sketch read by
/// [`WeightedFrequency::estimate`]. The heap ranks estimated scores, not
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
    /// Checks a shape without allocating it: `width` and `depth` positive,
    /// `width * depth` representable, and an odd CountSketch `depth` (the
    /// median needs an odd row count). [`Self::new`] accepts exactly these
    /// shapes, so a caller can reject a configuration before building it.
    pub fn check_shape(
        algorithm: FrequencyAlgorithm,
        width: usize,
        depth: usize,
    ) -> Result<(), Error> {
        width
            .checked_mul(depth)
            .filter(|_| {
                width > 0
                    && depth > 0
                    && (algorithm != FrequencyAlgorithm::CountSketch || depth % 2 == 1)
            })
            .map(|_| ())
            .ok_or_else(|| Error::Invalid("invalid weighted frequency dimensions".into()))
    }
    /// Bytes a state of shape `width × depth` holds once it retains
    /// `capacity` candidates whose identity tuples are shaped like `identity`:
    /// the struct, the `width × depth` 8-byte cells, the candidate heap's
    /// slots, and per retained candidate its identity tuple (with a `Utf8`
    /// value's bytes) and its encoded hash key. Only the identity's shape
    /// matters (its variants and string lengths), not its values.
    ///
    /// The candidate heap is rebuilt on every update by pushing up to
    /// `capacity + 1` candidates, so it holds `max(4, (capacity + 1)
    /// rounded up to a power of two)` slots, and during an update the
    /// previous heap's slots are held as well. A memory model calls this to
    /// price a top-k frequency state before building one.
    pub fn estimated_memory_bytes(
        width: usize,
        depth: usize,
        capacity: usize,
        identity: &[FrequencyIdentity],
    ) -> usize {
        let cells = width.saturating_mul(depth).saturating_mul(size_of::<f64>());
        if capacity == 0 {
            return size_of::<Self>() + cells;
        }
        let heap_slots =
            crate::common::heap::vec_slots_after_pushes(0, capacity + 1, size_of::<Candidate>());
        let identity_bytes = size_of_val(identity)
            + identity
                .iter()
                .map(|value| match value {
                    FrequencyIdentity::Utf8(text) => text.len(),
                    _ => 0,
                })
                .sum::<usize>();
        let key_bytes = bincode::serialized_size(identity).map_or(0, |bytes| bytes as usize);
        size_of::<Self>()
            + cells
            + heap_slots * size_of::<Candidate>()
            + capacity * (identity_bytes + key_bytes)
    }
    pub fn new(
        algorithm: FrequencyAlgorithm,
        width: usize,
        depth: usize,
        capacity: usize,
    ) -> Result<Self, Error> {
        Self::check_shape(algorithm, width, depth)?;
        let len = width * depth;
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
        if Self::check_shape(state.algorithm, state.width, state.depth).is_err()
            || state.width * state.depth != state.cells.len()
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
    fn estimate_key(&self, key: &[u8]) -> f64 {
        self.estimate_key_with(key, &mut Vec::new())
    }
    /// [`Self::estimate_key`] with a caller-owned buffer for the CountSketch
    /// row estimates, so scoring many candidates allocates once.
    fn estimate_key_with(&self, key: &[u8], rows: &mut Vec<f64>) -> f64 {
        match self.algorithm {
            FrequencyAlgorithm::Cms => self
                .indexes(key)
                .map(|(i, _)| self.cells[i])
                .fold(f64::INFINITY, f64::min),
            FrequencyAlgorithm::CountSketch => {
                rows.clear();
                rows.extend(self.indexes(key).map(|(i, sign)| self.cells[i] * sign));
                let middle = rows.len() / 2;
                *rows.select_nth_unstable_by(middle, f64::total_cmp).1
            }
        }
    }
    fn retain(&mut self, mut candidates: Vec<Candidate>) {
        candidates.sort_by(|a, b| a.key.cmp(&b.key));
        candidates.dedup_by(|a, b| a.key == b.key);
        self.candidates.clear();
        let mut rows = Vec::with_capacity(self.depth);
        for mut candidate in candidates {
            candidate.score = self.estimate_key_with(&candidate.key, &mut rows);
            self.candidates.push(candidate);
            if self.candidates.len() > self.capacity {
                self.candidates.pop();
            }
        }
    }
    /// Checks a weight [`Self::update`] would accept: finite, and nonnegative
    /// for CMS. Lets a caller reject an input before it reaches the state.
    pub fn check_weight(&self, weight: f64) -> Result<(), Error> {
        if !weight.is_finite() || (self.algorithm == FrequencyAlgorithm::Cms && weight < 0.0) {
            return Err(Error::Update(
                "weighted frequency requires finite weights; CMS additionally requires nonnegative weights".into(),
            ));
        }
        Ok(())
    }
    /// Empties the counters and candidates in place, keeping the algorithm,
    /// shape and allocation, so a state can be reused for the next window.
    pub fn clear(&mut self) {
        self.cells.fill(0.0);
        self.candidates.clear();
    }
    pub fn update(&mut self, values: &[FrequencyIdentity], weight: f64) -> Result<(), Error> {
        self.check_weight(weight)?;
        let (identity, key) = normalize(values)?;
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
        if self.capacity == 0 {
            return Ok(());
        }
        // Every retained score is re-read, since this update may have moved
        // cells they share. The candidates move into the rescoring instead of
        // being copied, and a key already retained is not added twice.
        let mut candidates = std::mem::take(&mut self.candidates).into_vec();
        if !candidates.iter().any(|candidate| candidate.key == key) {
            candidates.push(Candidate {
                identity,
                key,
                score: 0.0,
            });
        }
        self.retain(candidates);
        Ok(())
    }
    /// Point estimate for one identity tuple: the minimum over rows for CMS, the
    /// sign-corrected median over rows for CountSketch.
    pub fn estimate(&self, values: &[FrequencyIdentity]) -> Result<f64, Error> {
        Ok(self.estimate_key(&normalize(values)?.1))
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
        assert_eq!(state.estimate_key(&key), -0.375);
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
    // A point estimate reads the same sign-corrected median the heap scores use,
    // and capacity 0 is a frequency sketch without candidate retention.
    #[test]
    fn point_estimate_and_heapless_capacity() {
        for algorithm in [FrequencyAlgorithm::Cms, FrequencyAlgorithm::CountSketch] {
            let mut left = WeightedFrequency::new(algorithm, 4096, 5, 0).unwrap();
            let mut right = left.clone();
            left.update(&[FrequencyIdentity::Int64(7)], 0.125).unwrap();
            right.update(&[FrequencyIdentity::Int64(7)], 0.25).unwrap();
            right
                .update(&[FrequencyIdentity::Float64(-0.0)], 0.5)
                .unwrap();
            assert!(left.topk(8).is_empty());
            let merged = left.merge(&right).unwrap();
            assert_eq!(
                merged.estimate(&[FrequencyIdentity::Int64(7)]).unwrap(),
                0.375
            );
            // Signed zeros denote one identity, as in `update`.
            assert_eq!(
                merged.estimate(&[FrequencyIdentity::Float64(0.0)]).unwrap(),
                0.5
            );
            assert_eq!(merged.estimate(&[FrequencyIdentity::Null]).unwrap(), 0.0);
            assert!(
                merged
                    .estimate(&[FrequencyIdentity::Float64(f64::NAN)])
                    .is_err()
            );
            let decoded = WeightedFrequency::from_bytes(&merged.to_bytes()).unwrap();
            assert_eq!(decoded.shape(), (4096, 5, 0));
            assert_eq!(
                decoded.estimate(&[FrequencyIdentity::Int64(7)]).unwrap(),
                0.375
            );
        }
        let mut heap = WeightedFrequency::new(FrequencyAlgorithm::CountSketch, 64, 3, 2).unwrap();
        heap.update(&[FrequencyIdentity::Int64(1)], -2.5).unwrap();
        assert_eq!(
            heap.estimate(&[FrequencyIdentity::Int64(1)]).unwrap(),
            heap.topk(1)[0].1
        );
    }

    // Each row hashes with its own seed, so deep shapes stay pairwise independent:
    // no row geometry limit, and two rows rarely share a key's column.
    #[test]
    fn deep_rows_hash_independently() {
        let (width, depth) = (30_000, 15);
        let state =
            WeightedFrequency::new(FrequencyAlgorithm::CountSketch, width, depth, 0).unwrap();
        let keys = (0..2_000i64)
            .map(|i| bincode::serialize(&[FrequencyIdentity::Int64(i)]).unwrap())
            .collect::<Vec<_>>();
        let columns = keys
            .iter()
            .map(|key| {
                state
                    .indexes(key)
                    .enumerate()
                    .map(|(row, (index, _))| index - row * width)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        for a in 0..depth {
            for b in a + 1..depth {
                let shared = columns.iter().filter(|c| c[a] == c[b]).count();
                // Expected 2000 / 30000 < 1 per pair.
                assert!(shared <= 5, "rows {a} and {b} share {shared} columns");
            }
        }
    }

    // A caller can check a shape and a weight before building or updating:
    // the checks accept exactly what `new` and `update` accept.
    #[test]
    fn shape_and_weight_checks_match_construction_and_update() {
        use FrequencyAlgorithm::{Cms, CountSketch};
        for (algorithm, width, depth) in [
            (Cms, 16, 4),
            (Cms, 16, 5),
            (CountSketch, 16, 5),
            (CountSketch, 16, 4),
            (Cms, 0, 3),
            (CountSketch, 16, 0),
            (Cms, usize::MAX, 3),
        ] {
            assert_eq!(
                WeightedFrequency::check_shape(algorithm, width, depth).is_ok(),
                WeightedFrequency::new(algorithm, width, depth, 0).is_ok(),
                "{algorithm:?} {width}x{depth}"
            );
        }
        assert!(WeightedFrequency::check_shape(CountSketch, 16, 4).is_err());
        for algorithm in [Cms, CountSketch] {
            let state = WeightedFrequency::new(algorithm, 16, 3, 2).unwrap();
            for weight in [0.5, -0.5, 0.0, f64::NAN, f64::INFINITY] {
                assert_eq!(
                    state.check_weight(weight).is_ok(),
                    state
                        .clone()
                        .update(&[FrequencyIdentity::Null], weight)
                        .is_ok(),
                    "{algorithm:?} {weight}"
                );
            }
            assert_eq!(state.check_weight(-0.5).is_ok(), algorithm == CountSketch);
        }
    }

    // `clear` empties the counters and candidates in place and keeps the shape,
    // so a cleared state equals a fresh one byte for byte.
    #[test]
    fn clear_empties_in_place_and_keeps_the_shape() {
        for algorithm in [FrequencyAlgorithm::Cms, FrequencyAlgorithm::CountSketch] {
            let fresh = WeightedFrequency::new(algorithm, 64, 3, 4).unwrap();
            let mut state = fresh.clone();
            state.update(&[FrequencyIdentity::Int64(1)], 2.5).unwrap();
            state.clear();
            assert_eq!(state.to_bytes(), fresh.to_bytes());
            assert!(state.topk(4).is_empty());
            assert_eq!(state.estimate(&[FrequencyIdentity::Int64(1)]).unwrap(), 0.0);
        }
    }

    // Under heavy collisions every update re-reads each retained score, and a
    // key updated again stays one candidate: the retained scores always equal
    // the point estimates.
    #[test]
    fn retained_scores_track_every_update() {
        for algorithm in [FrequencyAlgorithm::Cms, FrequencyAlgorithm::CountSketch] {
            let mut state = WeightedFrequency::new(algorithm, 8, 3, 3).unwrap();
            for step in 0..60i64 {
                let key = [FrequencyIdentity::Int64(step % 7)];
                state.update(&key, 0.5 + (step % 3) as f64).unwrap();
                let rows = state.topk(8);
                assert!(rows.len() <= 3);
                for (identity, score) in &rows {
                    assert_eq!(*score, state.estimate(identity).unwrap(), "step {step}");
                }
                let mut keys = rows.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>();
                keys.dedup();
                assert_eq!(keys.len(), rows.len());
            }
        }
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
        for (width, depth, capacity) in [(0, 3, 1), (1, 0, 1), (usize::MAX, 3, 1)] {
            assert!(
                WeightedFrequency::new(FrequencyAlgorithm::Cms, width, depth, capacity).is_err()
            );
        }
    }
}
