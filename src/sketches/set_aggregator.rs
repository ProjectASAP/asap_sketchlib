//! Exact set of string keys, and the change between two of its snapshots.
//!
//! [`SetAggregator`] is not a sketch: it keeps every distinct key it sees, so
//! its cardinality and membership are exact. [`DeltaResult`] records the keys
//! one snapshot added and removed relative to an earlier one. Both serialize
//! to ASAPv1 (`docs/asapv1_wire_format.md` §3.21, §3.22).

use std::collections::HashSet;

mod wire;

/// Set aggregator for tracking a set of unique string keys.
#[derive(Debug, Clone)]
pub struct SetAggregator {
    pub values: HashSet<String>,
}

impl SetAggregator {
    pub fn new() -> Self {
        Self {
            values: HashSet::new(),
        }
    }

    /// Insert a key into the set.
    pub fn update(&mut self, key: &str) {
        self.values.insert(key.to_string());
    }

    /// Merge another SetAggregator into self in place (set union).
    pub fn merge(
        &mut self,
        other: &SetAggregator,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for v in &other.values {
            self.values.insert(v.clone());
        }
        Ok(())
    }

    /// Merge from references, returning a new aggregator.
    pub fn merge_refs(
        inputs: &[&SetAggregator],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if inputs.is_empty() {
            return Err("SetAggregator::merge_refs called with empty input".into());
        }
        let mut merged = SetAggregator::new();
        for s in inputs {
            merged.merge(s)?;
        }
        Ok(merged)
    }
}

impl Default for SetAggregator {
    fn default() -> Self {
        Self::new()
    }
}

/// The keys a later set snapshot added and removed relative to an earlier
/// one. The two sets must be disjoint; `serialize_to_bytes` refuses a key
/// held in both.
#[derive(Debug, Clone)]
pub struct DeltaResult {
    pub added: HashSet<String>,
    pub removed: HashSet<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_creation() {
        let sa = SetAggregator::new();
        assert!(sa.values.is_empty());
    }

    #[test]
    fn test_insert() {
        let mut sa = SetAggregator::new();
        sa.update("web");
        sa.update("api");
        sa.update("web"); // duplicate
        assert_eq!(sa.values.len(), 2);
        assert!(sa.values.contains("web"));
        assert!(sa.values.contains("api"));
    }

    #[test]
    fn test_merge() {
        let mut sa1 = SetAggregator::new();
        let mut sa2 = SetAggregator::new();

        sa1.update("web");
        sa1.update("api");
        sa2.update("api"); // duplicate
        sa2.update("db");

        sa1.merge(&sa2).unwrap();
        assert_eq!(sa1.values.len(), 3);
        assert!(sa1.values.contains("web"));
        assert!(sa1.values.contains("api"));
        assert!(sa1.values.contains("db"));
    }
}
