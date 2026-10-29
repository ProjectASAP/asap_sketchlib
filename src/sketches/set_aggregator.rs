//! Set aggregator: an exact set of string keys.
//!
//! `SetAggregator` is not a sketch — it is a plain `HashSet<String>` with an
//! ASAPv1 form (§3.21); [`SetAggregatorDelta`] is its delta (§3.22).

use crate::common::heap::hash_map_bytes;
use std::collections::HashSet;

mod delta;
mod wire;

pub use delta::SetAggregatorDelta;

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
        crate::analytics::ops::add(1);
        self.values.insert(key.to_string());
    }

    /// True when `key` is in the set.
    pub fn contains(&self, key: &str) -> bool {
        crate::analytics::ops::add(1);
        self.values.contains(key)
    }

    /// Bytes this set holds: the struct, the hash table and each key's bytes.
    pub fn memory_bytes(&self) -> usize {
        size_of::<Self>()
            + hash_map_bytes::<String>(self.values.capacity())
            + self.values.iter().map(String::capacity).sum::<usize>()
    }

    /// Merge another SetAggregator into self in place (set union).
    pub fn merge(
        &mut self,
        other: &SetAggregator,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for v in &other.values {
            crate::analytics::ops::add(2);
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
