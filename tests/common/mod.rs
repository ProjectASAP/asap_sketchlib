//! Shared infrastructure for the E2E sketch test suites.

// Which helpers are "used" varies by feature flags and per-suite coverage.
#![allow(dead_code, unused_imports)]

pub mod assertions;
pub mod specs;
pub mod storage;
pub mod streams;
pub mod truth;

pub use assertions::{assert_between, assert_in_rank_band, assert_rel_close};
pub use truth::{CardinalityTruth, FreqTruth, MembershipTruth, NumericTruth};
