//! Family-capability table: per sketch algorithm, the statistics one state
//! answers, how merge inputs may relate, and each statistic's error after a
//! merge. A row lists the family's own estimates, not an inner sketch's
//! reached through an accessor, and not an exact aggregate (total, sum,
//! min, max) kept next to the sketch: those belong to a separate exact
//! operator (#250). The names are the ASAPPlanner's, which reads this table
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
    Cms,
    CountSketch,
    CountSketchWithL2Norm,
    /// `CountMinSketchWithHeap`.
    CmsWithHeap,
    CountSketchWithHeap,
    /// `CountMinSketchWithSpaceSaving`.
    CmsWithSpaceSaving,
    CountSketchWithSpaceSaving,
    /// `CountMinSketchFold`.
    CmsFold,
    CountSketchFold,
    /// `HyperLogLog` (Classic or Ertl) at any precision.
    Hll,
    /// `HyperLogLogHip` at any precision.
    HllHip,
    Kmv,
    Kll,
    KllDynamic,
    DDSketch,
    SpaceSaving,
    /// `ElasticSketch`.
    Elastic,
    /// `CocoSketch`.
    Coco,
    Bloom,
    SetAggregator,
    UnivMon,
    UnivMonTwoTier,
    /// Quantile and rank are `None` when `ordered_samples` is 0.
    UnivMonQ,
    /// `Hydra` over Count-Min cells, per subpopulation.
    HydraCms,
    /// `Hydra` over Count Sketch cells, per subpopulation.
    HydraCountSketch,
    /// `Hydra` over KLL cells, per subpopulation.
    HydraKll,
    /// `Hydra` over HyperLogLog cells, per subpopulation.
    HydraHll,
    /// `Hydra` over UnivMon cells, per subpopulation.
    HydraUnivMon,
    #[cfg(feature = "experimental")]
    CmsHll,
    #[cfg(feature = "experimental")]
    MicroCm,
    #[cfg(feature = "experimental")]
    UniformSampling,
}

/// The kind of a statistic a sketch state answers, without its arguments.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum StatisticKind {
    /// Estimated total update weight of one subpopulation of a Hydra.
    Total,
    ItemFrequency,
    Quantile,
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
    /// state keeps; a family with none (HydraKll) gains none.
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
        SketchAlgorithm::Cms,
        SketchAlgorithm::CountSketch,
        SketchAlgorithm::CountSketchWithL2Norm,
        SketchAlgorithm::CmsWithHeap,
        SketchAlgorithm::CountSketchWithHeap,
        SketchAlgorithm::CmsWithSpaceSaving,
        SketchAlgorithm::CountSketchWithSpaceSaving,
        SketchAlgorithm::CmsFold,
        SketchAlgorithm::CountSketchFold,
        SketchAlgorithm::Hll,
        SketchAlgorithm::HllHip,
        SketchAlgorithm::Kmv,
        SketchAlgorithm::Kll,
        SketchAlgorithm::KllDynamic,
        SketchAlgorithm::DDSketch,
        SketchAlgorithm::SpaceSaving,
        SketchAlgorithm::Elastic,
        SketchAlgorithm::Coco,
        SketchAlgorithm::Bloom,
        SketchAlgorithm::SetAggregator,
        SketchAlgorithm::UnivMon,
        SketchAlgorithm::UnivMonTwoTier,
        SketchAlgorithm::UnivMonQ,
        SketchAlgorithm::HydraCms,
        SketchAlgorithm::HydraCountSketch,
        SketchAlgorithm::HydraKll,
        SketchAlgorithm::HydraHll,
        SketchAlgorithm::HydraUnivMon,
        #[cfg(feature = "experimental")]
        SketchAlgorithm::CmsHll,
        #[cfg(feature = "experimental")]
        SketchAlgorithm::MicroCm,
        #[cfg(feature = "experimental")]
        SketchAlgorithm::UniformSampling,
    ];

    /// This family's row of the capability table.
    pub fn capability(self) -> FamilyCapability {
        use MergeInputRule::{Disjoint, MayOverlap};
        use MergedError::{Approximate as A, AsIfBuiltOverUnion as U, Exact as E};
        use StatisticKind::*;
        const UNIVMON: &[(StatisticKind, MergedError)] = &[
            (FrequencyL2, U),
            (Distinct, U),
            (FrequencyEntropy, A),
            (GSum, A),
        ];
        const FREQUENCY_TOP_K: &[(StatisticKind, MergedError)] = &[(ItemFrequency, U), (TopK, A)];
        let (merge_inputs, readouts): (_, &'static [(StatisticKind, MergedError)]) = match self {
            Self::Cms => (Some(Disjoint), &[(ItemFrequency, U)]),
            Self::CountSketch => (Some(Disjoint), &[(ItemFrequency, U)]),
            Self::CountSketchWithL2Norm => {
                (Some(Disjoint), &[(ItemFrequency, U), (FrequencyL2, U)])
            }
            Self::CmsWithHeap
            | Self::CountSketchWithHeap
            | Self::CmsWithSpaceSaving
            | Self::CountSketchWithSpaceSaving
            | Self::CmsFold
            | Self::CountSketchFold => (Some(Disjoint), FREQUENCY_TOP_K),
            Self::Hll => (Some(MayOverlap), &[(Distinct, U)]),
            Self::HllHip => (None, &[(Distinct, A)]),
            Self::Kmv => (Some(MayOverlap), &[(Distinct, U)]),
            Self::Kll | Self::KllDynamic => (Some(Disjoint), &[(Quantile, U), (Rank, U)]),
            Self::DDSketch => (Some(Disjoint), &[(Quantile, U)]),
            Self::SpaceSaving => (Some(Disjoint), &[(ItemFrequency, A), (TopK, A)]),
            Self::Elastic => (Some(Disjoint), &[(ItemFrequency, A), (TopK, A)]),
            Self::Coco => (Some(Disjoint), &[(ItemFrequency, A), (PartialKeySum, A)]),
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
            Self::HydraCms => (Some(Disjoint), &[(ItemFrequency, U), (Total, U)]),
            Self::HydraCountSketch => (Some(Disjoint), &[(ItemFrequency, U)]),
            Self::HydraKll => (Some(Disjoint), &[(Quantile, U), (Rank, U)]),
            Self::HydraHll => (Some(MayOverlap), &[(Distinct, U)]),
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
            Self::CmsHll => (Some(MayOverlap), &[(KeyDistinct, U)]),
            #[cfg(feature = "experimental")]
            Self::MicroCm => (Some(Disjoint), &[(ItemFrequency, A)]),
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
            SketchAlgorithm::Cms => 0,
            SketchAlgorithm::CountSketch => 1,
            SketchAlgorithm::CountSketchWithL2Norm => 2,
            SketchAlgorithm::CmsWithHeap => 3,
            SketchAlgorithm::CountSketchWithHeap => 4,
            SketchAlgorithm::CmsWithSpaceSaving => 5,
            SketchAlgorithm::CountSketchWithSpaceSaving => 6,
            SketchAlgorithm::CmsFold => 7,
            SketchAlgorithm::CountSketchFold => 8,
            SketchAlgorithm::Hll => 9,
            SketchAlgorithm::HllHip => 10,
            SketchAlgorithm::Kmv => 11,
            SketchAlgorithm::Kll => 12,
            SketchAlgorithm::KllDynamic => 13,
            SketchAlgorithm::DDSketch => 14,
            SketchAlgorithm::SpaceSaving => 15,
            SketchAlgorithm::Elastic => 16,
            SketchAlgorithm::Coco => 17,
            SketchAlgorithm::Bloom => 18,
            SketchAlgorithm::SetAggregator => 19,
            SketchAlgorithm::UnivMon => 20,
            SketchAlgorithm::UnivMonTwoTier => 21,
            SketchAlgorithm::UnivMonQ => 22,
            SketchAlgorithm::HydraCms => 23,
            SketchAlgorithm::HydraCountSketch => 24,
            SketchAlgorithm::HydraKll => 25,
            SketchAlgorithm::HydraHll => 26,
            SketchAlgorithm::HydraUnivMon => 27,
            #[cfg(feature = "experimental")]
            SketchAlgorithm::CmsHll => 28,
            #[cfg(feature = "experimental")]
            SketchAlgorithm::MicroCm => 29,
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
