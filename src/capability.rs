//! Family-capability table: per sketch algorithm, the statistics one state
//! answers, how merge inputs may relate, and each statistic's error after a
//! merge. A row lists the family's own estimates, not an inner sketch's
//! reached through an accessor, and not an exact aggregate kept in a side
//! field next to the sketch (DDSketch's sum, min and max; a Space-Saving
//! total): those belong to a separate exact operator (#250). A total the
//! sketch derives from its own counters (Count-Min's, UnivMon's) stays. The names are the ASAPPlanner's, which reads this table
//! directly.
//!
//! The enums are exhaustive on purpose: a downstream `match` without a
//! wildcard stops compiling when a family or statistic is added, instead of
//! skipping it.

/// A public sketch family: one row of the table.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum SketchAlgorithm {
    /// `CountMinSketch` over every storage and path, and `CountMinF64`.
    /// Total is exact unless Nitro sampling is enabled.
    CountMinSketch,
    CountSketch,
    CountSketchWithL2Norm,
    /// `CountMinSketchWithHeap`.
    CountMinSketchWithHeap,
    CountSketchWithHeap,
    /// `CountMinSketchWithSpaceSaving`.
    CountMinSketchWithSpaceSaving,
    CountSketchWithSpaceSaving,
    /// `CountMinSketchFold`.
    CountMinSketchFold,
    CountSketchFold,
    /// `HyperLogLog` (Classic or Ertl) at any precision.
    HLL,
    /// `HyperLogLogHip` at any precision.
    HLLHip,
    KMV,
    KLL,
    KLLDynamic,
    DDSketch,
    SpaceSaving,
    /// `ElasticSketch`.
    ElasticSketch,
    /// `CocoSketch`.
    CocoSketch,
    Bloom,
    SetAggregator,
    UnivMon,
    UnivMonTwoTier,
    /// Quantile and rank are `None` when `ordered_samples` is 0.
    UnivMonQ,
    /// `Hydra` over Count-Min cells, per subpopulation.
    HydraCountMinSketch,
    /// `Hydra` over Count Sketch cells, per subpopulation.
    HydraCountSketch,
    /// `Hydra` over KLL cells, per subpopulation.
    HydraKLL,
    /// `Hydra` over HyperLogLog cells, per subpopulation.
    HydraHLL,
    /// `Hydra` over UnivMon cells, per subpopulation.
    HydraUnivMon,
    #[cfg(feature = "experimental")]
    CountMinSketchHLL,
    #[cfg(feature = "experimental")]
    MicroCountMinSketch,
    #[cfg(feature = "experimental")]
    UniformSampling,
}

/// The kind of a statistic a sketch state answers, without its arguments.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum StatisticKind {
    /// Total update weight, read from the sketch's own counters: a
    /// Count-Min row's sum, UnivMon's L1, a Hydra Count-Min's group total or
    /// a Hydra UnivMon cell's L1.
    Total,
    ItemFrequency,
    Quantile,
    /// The quantile interpolated between the order statistics at rank
    /// `q·(n − 1)`.
    QuantileInterpolated,
    /// Absolute on the KLLs and UnivMonQ, normalized to `[0, 1]` on Hydra.
    Rank,
    Distinct,
    /// L2 norm of the item frequencies.
    FrequencyL2,
    /// Entropy of the item frequencies.
    FrequencyEntropy,
    /// Sum of `g` over the item frequencies.
    GSum,
    TopK,
    Membership,
    /// Distinct values under one key.
    KeyDistinct,
    /// Summed frequency of the items sharing a partial key.
    PartialKeySum,
    /// Uniform sample of the values.
    Sample,
}

/// How the inputs of a merge may relate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MergeInputRule {
    /// A shared update counts twice.
    Disjoint,
    /// The merge is idempotent.
    MayOverlap,
}

/// The error of a statistic read from a merged state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MergedError {
    /// Exact for the updates the sketch admitted.
    Exact,
    /// As one state built over the union, given equal parameters: the merge
    /// adds no error of its own. Whatever guarantee one state has, the merged
    /// state keeps; a family with none (HydraKLL) gains none.
    AsIfBuiltOverUnion,
    /// The merge is allowed, but the merged readout has no guarantee.
    Approximate,
}

/// One row of the table.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FamilyCapability {
    /// `None` when the family has no merge.
    pub merge_inputs: Option<MergeInputRule>,
    pub readouts: &'static [(StatisticKind, MergedError)],
}

impl FamilyCapability {
    /// Whether one state answers `statistic`.
    pub fn answers(&self, statistic: StatisticKind) -> bool {
        self.readout(statistic).is_some()
    }

    /// `None` when the family has no merge or does not answer `statistic`.
    pub fn merged_error(&self, statistic: StatisticKind) -> Option<MergedError> {
        self.merge_inputs.and(self.readout(statistic))
    }

    fn readout(&self, statistic: StatisticKind) -> Option<MergedError> {
        self.readouts
            .iter()
            .find(|(answered, _)| *answered == statistic)
            .map(|&(_, error)| error)
    }
}

impl SketchAlgorithm {
    /// Every family, each once.
    pub const ALL: &'static [SketchAlgorithm] = &[
        SketchAlgorithm::CountMinSketch,
        SketchAlgorithm::CountSketch,
        SketchAlgorithm::CountSketchWithL2Norm,
        SketchAlgorithm::CountMinSketchWithHeap,
        SketchAlgorithm::CountSketchWithHeap,
        SketchAlgorithm::CountMinSketchWithSpaceSaving,
        SketchAlgorithm::CountSketchWithSpaceSaving,
        SketchAlgorithm::CountMinSketchFold,
        SketchAlgorithm::CountSketchFold,
        SketchAlgorithm::HLL,
        SketchAlgorithm::HLLHip,
        SketchAlgorithm::KMV,
        SketchAlgorithm::KLL,
        SketchAlgorithm::KLLDynamic,
        SketchAlgorithm::DDSketch,
        SketchAlgorithm::SpaceSaving,
        SketchAlgorithm::ElasticSketch,
        SketchAlgorithm::CocoSketch,
        SketchAlgorithm::Bloom,
        SketchAlgorithm::SetAggregator,
        SketchAlgorithm::UnivMon,
        SketchAlgorithm::UnivMonTwoTier,
        SketchAlgorithm::UnivMonQ,
        SketchAlgorithm::HydraCountMinSketch,
        SketchAlgorithm::HydraCountSketch,
        SketchAlgorithm::HydraKLL,
        SketchAlgorithm::HydraHLL,
        SketchAlgorithm::HydraUnivMon,
        #[cfg(feature = "experimental")]
        SketchAlgorithm::CountMinSketchHLL,
        #[cfg(feature = "experimental")]
        SketchAlgorithm::MicroCountMinSketch,
        #[cfg(feature = "experimental")]
        SketchAlgorithm::UniformSampling,
    ];

    /// This family's row of the capability table.
    pub fn capability(self) -> FamilyCapability {
        use MergeInputRule::{Disjoint, MayOverlap};
        use MergedError::{Approximate as A, AsIfBuiltOverUnion as U, Exact as E};
        use StatisticKind::*;
        const UNIVMON: &[(StatisticKind, MergedError)] = &[
            (Total, E),
            (FrequencyL2, U),
            (Distinct, U),
            (FrequencyEntropy, A),
            (GSum, A),
        ];
        const FREQUENCY_TOP_K: &[(StatisticKind, MergedError)] = &[(ItemFrequency, U), (TopK, A)];
        let (merge_inputs, readouts): (_, &'static [(StatisticKind, MergedError)]) = match self {
            Self::CountMinSketch => (Some(Disjoint), &[(ItemFrequency, U), (Total, U)]),
            Self::CountSketch => (Some(Disjoint), &[(ItemFrequency, U)]),
            Self::CountSketchWithL2Norm => {
                (Some(Disjoint), &[(ItemFrequency, U), (FrequencyL2, U)])
            }
            Self::CountMinSketchWithHeap
            | Self::CountSketchWithHeap
            | Self::CountMinSketchWithSpaceSaving
            | Self::CountSketchWithSpaceSaving
            | Self::CountMinSketchFold
            | Self::CountSketchFold => (Some(Disjoint), FREQUENCY_TOP_K),
            Self::HLL => (Some(MayOverlap), &[(Distinct, U)]),
            Self::HLLHip => (None, &[(Distinct, A)]),
            Self::KMV => (Some(MayOverlap), &[(Distinct, U)]),
            Self::KLL | Self::KLLDynamic => (Some(Disjoint), &[(Quantile, U), (Rank, U)]),
            Self::DDSketch => (Some(Disjoint), &[(Quantile, U), (QuantileInterpolated, U)]),
            Self::SpaceSaving => (Some(Disjoint), &[(ItemFrequency, A), (TopK, A)]),
            Self::ElasticSketch => (Some(Disjoint), &[(ItemFrequency, A), (TopK, A)]),
            Self::CocoSketch => (Some(Disjoint), &[(ItemFrequency, A), (PartialKeySum, A)]),
            Self::Bloom => (Some(MayOverlap), &[(Membership, U)]),
            Self::SetAggregator => (Some(MayOverlap), &[(Distinct, E), (Membership, E)]),
            Self::UnivMon | Self::UnivMonTwoTier => (Some(Disjoint), UNIVMON),
            Self::UnivMonQ => (
                Some(Disjoint),
                // Quantile and rank hold only when occurrence source ids are
                // unique across the merged states, which the sketch does not
                // check; distinct count has no proof for UnivMon-Q's strata;
                // G-sum reads the same rebuilt candidates as UnivMon's.
                &[
                    (ItemFrequency, U),
                    (FrequencyL2, U),
                    (Distinct, A),
                    (Quantile, A),
                    (Rank, A),
                    (GSum, A),
                    (FrequencyEntropy, A),
                    (TopK, A),
                ],
            ),
            Self::HydraCountMinSketch => (Some(Disjoint), &[(ItemFrequency, U), (Total, U)]),
            Self::HydraCountSketch => (Some(Disjoint), &[(ItemFrequency, U)]),
            Self::HydraKLL => (Some(Disjoint), &[(Quantile, U), (Rank, U)]),
            Self::HydraHLL => (Some(MayOverlap), &[(Distinct, U)]),
            Self::HydraUnivMon => (
                Some(Disjoint),
                &[
                    (Total, U),
                    (Distinct, U),
                    (FrequencyL2, U),
                    (FrequencyEntropy, A),
                ],
            ),
            #[cfg(feature = "experimental")]
            Self::CountMinSketchHLL => (Some(MayOverlap), &[(KeyDistinct, U)]),
            #[cfg(feature = "experimental")]
            Self::MicroCountMinSketch => (Some(Disjoint), &[(ItemFrequency, A)]),
            #[cfg(feature = "experimental")]
            Self::UniformSampling => (Some(Disjoint), &[(Sample, A)]),
        };
        FamilyCapability {
            merge_inputs,
            readouts,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SketchAlgorithm;

    /// No wildcard: a new variant fails to compile until listed here.
    fn expected_index(family: SketchAlgorithm) -> usize {
        match family {
            SketchAlgorithm::CountMinSketch => 0,
            SketchAlgorithm::CountSketch => 1,
            SketchAlgorithm::CountSketchWithL2Norm => 2,
            SketchAlgorithm::CountMinSketchWithHeap => 3,
            SketchAlgorithm::CountSketchWithHeap => 4,
            SketchAlgorithm::CountMinSketchWithSpaceSaving => 5,
            SketchAlgorithm::CountSketchWithSpaceSaving => 6,
            SketchAlgorithm::CountMinSketchFold => 7,
            SketchAlgorithm::CountSketchFold => 8,
            SketchAlgorithm::HLL => 9,
            SketchAlgorithm::HLLHip => 10,
            SketchAlgorithm::KMV => 11,
            SketchAlgorithm::KLL => 12,
            SketchAlgorithm::KLLDynamic => 13,
            SketchAlgorithm::DDSketch => 14,
            SketchAlgorithm::SpaceSaving => 15,
            SketchAlgorithm::ElasticSketch => 16,
            SketchAlgorithm::CocoSketch => 17,
            SketchAlgorithm::Bloom => 18,
            SketchAlgorithm::SetAggregator => 19,
            SketchAlgorithm::UnivMon => 20,
            SketchAlgorithm::UnivMonTwoTier => 21,
            SketchAlgorithm::UnivMonQ => 22,
            SketchAlgorithm::HydraCountMinSketch => 23,
            SketchAlgorithm::HydraCountSketch => 24,
            SketchAlgorithm::HydraKLL => 25,
            SketchAlgorithm::HydraHLL => 26,
            SketchAlgorithm::HydraUnivMon => 27,
            #[cfg(feature = "experimental")]
            SketchAlgorithm::CountMinSketchHLL => 28,
            #[cfg(feature = "experimental")]
            SketchAlgorithm::MicroCountMinSketch => 29,
            #[cfg(feature = "experimental")]
            SketchAlgorithm::UniformSampling => 30,
        }
    }

    #[test]
    fn all_lists_every_family_exactly_once() {
        let expected_len = if cfg!(feature = "experimental") {
            31
        } else {
            28
        };
        assert_eq!(SketchAlgorithm::ALL.len(), expected_len);
        for (position, &family) in SketchAlgorithm::ALL.iter().enumerate() {
            assert_eq!(expected_index(family), position, "{family:?}");
        }
    }
}
