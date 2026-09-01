use std::collections::{HashMap, HashSet};
use std::hash::Hash;

pub struct FreqTruth<K> {
    counts: HashMap<K, i64>,
}

impl<K> Default for FreqTruth<K> {
    fn default() -> Self {
        Self { counts: HashMap::new() }
    }
}

impl<K> FreqTruth<K>
where K: Eq + Hash + Clone + Ord {
    pub fn from_data(data: &[K]) -> Self
    where K: Copy {
        let mut truth = Self::default();
        for &key in data {
            truth.observe(key);
        }
        truth
    }

    /// `from_data` for a key that is not `Copy`, such as `String`.
    pub fn from_data_cloned(data: &[K]) -> Self {
        let mut truth = Self::default();
        for key in data {
            truth.observe(key.clone());
        }
        truth
    }

    pub fn observe(&mut self, key: K) {
        *self.counts.entry(key).or_insert(0) += 1;
    }
    pub fn observe_weighted(&mut self, key: K, weight: i64) {
        *self.counts.entry(key).or_insert(0) += weight;
    }
    pub fn get(&self, key: K) -> i64 {
        self.counts.get(&key).copied().unwrap_or(0)
    }
    pub fn total(&self) -> i64 {
        self.counts.values().sum()
    }
    pub fn distinct(&self) -> usize {
        self.counts.len()
    }
    pub fn f2(&self) -> f64 {
        self.counts
            .values()
            .map(|c| (*c as f64) * (*c as f64))
            .sum()
    }
    pub fn l2_norm(&self) -> f64 {
        self.f2().sqrt()
    }

    pub fn entropy(&self, base_bits: bool) -> f64 {
        let total = self.total() as f64;
        -self
            .counts
            .values()
            .filter(|c| **c > 0)
            .map(|c| {
                let p = *c as f64 / total;
                p * if base_bits { p.log2() } else { p.ln() }
            })
            .sum::<f64>()
    }

    pub fn top_k(&self, k: usize) -> Vec<(K, i64)> {
        let mut values: Vec<_> = self
            .counts
            .iter()
            .map(|(key, count)| (key.clone(), *count))
            .collect();
        values.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        values.truncate(k);
        values
    }

    pub fn pairs(&self) -> Vec<(K, i64)> {
        self.counts
            .iter()
            .map(|(key, count)| (key.clone(), *count))
            .collect()
    }
}

pub fn freq_truth<K: Eq + Hash + Copy + Ord>(data: &[K]) -> FreqTruth<K> {
    FreqTruth::from_data(data)
}

pub struct NumericTruth {
    sorted: Vec<f64>,
}

impl NumericTruth {
    pub fn from_data<T: Copy + Into<f64>>(data: &[T]) -> Self {
        Self::new(data.iter().copied().map(Into::into).collect())
    }

    pub fn new(mut values: Vec<f64>) -> Self {
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        Self { sorted: values }
    }

    pub fn len(&self) -> usize {
        self.sorted.len()
    }
    pub fn is_empty(&self) -> bool {
        self.sorted.is_empty()
    }
    pub fn min(&self) -> f64 {
        self.sorted[0]
    }
    pub fn max(&self) -> f64 {
        self.sorted[self.sorted.len() - 1]
    }

    pub fn quantile(&self, q: f64) -> f64 {
        let n = self.sorted.len();
        let idx = ((q.clamp(0.0, 1.0) * n as f64).ceil() as usize).clamp(1, n);
        self.sorted[idx - 1]
    }

    pub fn cdf(&self, x: f64) -> f64 {
        self.sorted.iter().filter(|v| **v <= x).count() as f64 / self.sorted.len() as f64
    }

    pub fn sorted(&self) -> &[f64] {
        &self.sorted
    }

    pub fn count_of(&self, x: f64) -> f64 {
        let hi = self
            .sorted
            .partition_point(|v| v.total_cmp(&x) != std::cmp::Ordering::Greater);
        let lo = self
            .sorted
            .partition_point(|v| v.total_cmp(&x) == std::cmp::Ordering::Less);
        (hi - lo) as f64
    }

    pub fn rank_interval(&self, x: f64) -> (f64, f64) {
        let n = self.sorted.len() as f64;
        let excl = self.sorted.partition_point(|v| *v < x) as f64 / n;
        let incl = self.sorted.partition_point(|v| *v <= x) as f64 / n;
        (excl, incl)
    }

    pub fn quantile_band(&self, q: f64, tol: f64) -> (f64, f64) {
        (
            self.quantile((q - tol).clamp(0.0, 1.0)),
            self.quantile((q + tol).clamp(0.0, 1.0)),
        )
    }
}

pub struct CardinalityTruth {
    distinct: usize,
}

impl CardinalityTruth {
    pub fn from_data<T: Eq + Hash>(data: &[T]) -> Self {
        Self {
            distinct: data.iter().collect::<HashSet<_>>().len(),
        }
    }
    pub fn distinct(&self) -> usize {
        self.distinct
    }
}

pub struct MembershipTruth<T> {
    members: HashSet<T>,
}

impl<T: Copy + Eq + Hash> MembershipTruth<T> {
    pub fn from_data(data: &[T]) -> Self {
        Self {
            members: data.iter().copied().collect(),
        }
    }
    pub fn contains(&self, value: &T) -> bool {
        self.members.contains(value)
    }
    pub fn len(&self) -> usize {
        self.members.len()
    }
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
}
