//! The delta of a [`super::SetAggregator`] between two snapshots.

use std::collections::HashSet;

/// The string keys added to and removed from a set between two consecutive
/// observations (ASAPv1 kind `08 80`, §3.22).
#[derive(Debug, Clone)]
pub struct SetAggregatorDelta {
    pub added: HashSet<String>,
    pub removed: HashSet<String>,
}
