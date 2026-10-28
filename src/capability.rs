//! Capability table: per sketch family, the statistics one state answers,
//! how merge inputs may relate, and each statistic's error after a merge.
//! A row lists the family's own readouts, not an inner sketch's reached
//! through an accessor.

/// A public sketch family: one row of the table.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SketchFamily {
    /// `CountMinSketch` over every storage and path, and `CountMinF64`.
    CountMin,
    CountSketch,
    CountSketchWithL2Norm,
    /// `CountMinSketchWithHeap`.
    CountMinWithHeap,
    CountSketchWithHeap,
    /// `CountMinSketchWithSpaceSaving`.
    CountMinWithSpaceSaving,
    CountSketchWithSpaceSaving,
    /// `CountMinSketchFold`.
    CountMinFold,
    CountSketchFold,
    /// `HyperLogLog` (Classic or Ertl) at any precision.
    Hll,
    /// `HyperLogLogHip` at any precision.
    HllHip,
    Kmv,
    Kll,
    KllDynamic,
    DdSketch,
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
    HydraCountMin,
    /// `Hydra` over Count Sketch cells, per subpopulation.
    HydraCountSketch,
    /// `Hydra` over KLL cells, per subpopulation.
    HydraKll,
    /// `Hydra` over HyperLogLog cells, per subpopulation.
    HydraHll,
    /// `Hydra` over UnivMon cells, per subpopulation.
    HydraUnivMon,
    #[cfg(feature = "experimental")]
    CountMinHll,
    #[cfg(feature = "experimental")]
    MicroCm,
    #[cfg(feature = "experimental")]
    UniformSampling,
}

/// A statistic a sketch state answers.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Statistic {
    /// Total update weight.
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
    Sum,
    Min,
    Max,
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
pub enum MergeInputs {
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
    /// As one state built over the union, given equal parameters.
    AsIfBuiltOverUnion,
    NoGuarantee,
}

/// One row of the table.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Capability {
    /// `None` when the family has no merge.
    pub merge: Option<MergeInputs>,
    pub readouts: &'static [(Statistic, MergedError)],
}

impl Capability {
    /// Whether one state answers `statistic`.
    pub fn answers(&self, statistic: Statistic) -> bool {
        self.readout(statistic).is_some()
    }

    /// `None` when the family has no merge or does not answer `statistic`.
    pub fn merged_error(&self, statistic: Statistic) -> Option<MergedError> {
        self.merge.and(self.readout(statistic))
    }

    fn readout(&self, statistic: Statistic) -> Option<MergedError> {
        self.readouts
            .iter()
            .find(|(answered, _)| *answered == statistic)
            .map(|&(_, error)| error)
    }
}

impl SketchFamily {
    /// Every family, each once.
    pub const ALL: &'static [SketchFamily] = &[
        SketchFamily::CountMin,
        SketchFamily::CountSketch,
        SketchFamily::CountSketchWithL2Norm,
        SketchFamily::CountMinWithHeap,
        SketchFamily::CountSketchWithHeap,
        SketchFamily::CountMinWithSpaceSaving,
        SketchFamily::CountSketchWithSpaceSaving,
        SketchFamily::CountMinFold,
        SketchFamily::CountSketchFold,
        SketchFamily::Hll,
        SketchFamily::HllHip,
        SketchFamily::Kmv,
        SketchFamily::Kll,
        SketchFamily::KllDynamic,
        SketchFamily::DdSketch,
        SketchFamily::SpaceSaving,
        SketchFamily::Elastic,
        SketchFamily::Coco,
        SketchFamily::Bloom,
        SketchFamily::SetAggregator,
        SketchFamily::UnivMon,
        SketchFamily::UnivMonTwoTier,
        SketchFamily::UnivMonQ,
        SketchFamily::HydraCountMin,
        SketchFamily::HydraCountSketch,
        SketchFamily::HydraKll,
        SketchFamily::HydraHll,
        SketchFamily::HydraUnivMon,
        #[cfg(feature = "experimental")]
        SketchFamily::CountMinHll,
        #[cfg(feature = "experimental")]
        SketchFamily::MicroCm,
        #[cfg(feature = "experimental")]
        SketchFamily::UniformSampling,
    ];

    /// This family's row of the capability table.
    pub fn capability(self) -> Capability {
        use MergeInputs::{Disjoint, MayOverlap};
        use MergedError::{AsIfBuiltOverUnion as U, Exact as E, NoGuarantee as N};
        use Statistic::*;
        const UNIVMON: &[(Statistic, MergedError)] = &[
            (Total, E),
            (FrequencyL2, U),
            (Distinct, U),
            (FrequencyEntropy, N),
            (GSum, N),
        ];
        const FREQUENCY_TOP_K: &[(Statistic, MergedError)] = &[(ItemFrequency, U), (TopK, N)];
        let (merge, readouts): (_, &'static [(Statistic, MergedError)]) = match self {
            Self::CountMin => (Some(Disjoint), &[(ItemFrequency, U), (Total, E)]),
            Self::CountSketch => (Some(Disjoint), &[(ItemFrequency, U)]),
            Self::CountSketchWithL2Norm => {
                (Some(Disjoint), &[(ItemFrequency, U), (FrequencyL2, U)])
            }
            Self::CountMinWithHeap
            | Self::CountSketchWithHeap
            | Self::CountMinWithSpaceSaving
            | Self::CountSketchWithSpaceSaving
            | Self::CountMinFold
            | Self::CountSketchFold => (Some(Disjoint), FREQUENCY_TOP_K),
            Self::Hll => (Some(MayOverlap), &[(Distinct, U)]),
            Self::HllHip => (None, &[(Distinct, N)]),
            Self::Kmv => (Some(MayOverlap), &[(Distinct, U)]),
            Self::Kll | Self::KllDynamic => (Some(Disjoint), &[(Quantile, U), (Rank, U)]),
            Self::DdSketch => (
                Some(Disjoint),
                &[(Quantile, U), (Total, E), (Sum, E), (Min, E), (Max, E)],
            ),
            Self::SpaceSaving => (Some(Disjoint), &[(ItemFrequency, N), (TopK, N), (Total, E)]),
            Self::Elastic => (Some(Disjoint), &[(ItemFrequency, N), (TopK, N)]),
            Self::Coco => (
                Some(Disjoint),
                &[(ItemFrequency, N), (PartialKeySum, N), (Total, E)],
            ),
            Self::Bloom => (Some(MayOverlap), &[(Membership, U)]),
            Self::SetAggregator => (Some(MayOverlap), &[(Distinct, E), (Membership, E)]),
            Self::UnivMon | Self::UnivMonTwoTier => (Some(Disjoint), UNIVMON),
            Self::UnivMonQ => (
                Some(Disjoint),
                &[
                    (ItemFrequency, U),
                    (Total, E),
                    (FrequencyL2, U),
                    (Distinct, U),
                    (Quantile, U),
                    (Rank, U),
                    (Min, E),
                    (Max, E),
                    (GSum, U),
                    (FrequencyEntropy, N),
                    (TopK, N),
                ],
            ),
            Self::HydraCountMin => (Some(Disjoint), &[(ItemFrequency, U), (Total, U)]),
            Self::HydraCountSketch => (Some(Disjoint), &[(ItemFrequency, U)]),
            Self::HydraKll => (Some(Disjoint), &[(Quantile, U), (Rank, U)]),
            Self::HydraHll => (Some(MayOverlap), &[(Distinct, U)]),
            Self::HydraUnivMon => (
                Some(Disjoint),
                &[
                    (Total, U),
                    (Distinct, U),
                    (FrequencyL2, U),
                    (FrequencyEntropy, N),
                ],
            ),
            #[cfg(feature = "experimental")]
            Self::CountMinHll => (Some(MayOverlap), &[(KeyDistinct, U)]),
            #[cfg(feature = "experimental")]
            Self::MicroCm => (Some(Disjoint), &[(ItemFrequency, N)]),
            #[cfg(feature = "experimental")]
            Self::UniformSampling => (Some(Disjoint), &[(Sample, N), (Total, E)]),
        };
        Capability { merge, readouts }
    }
}

#[cfg(test)]
mod tests {
    use super::SketchFamily;

    /// No wildcard: a new variant fails to compile until listed here.
    fn expected_index(family: SketchFamily) -> usize {
        match family {
            SketchFamily::CountMin => 0,
            SketchFamily::CountSketch => 1,
            SketchFamily::CountSketchWithL2Norm => 2,
            SketchFamily::CountMinWithHeap => 3,
            SketchFamily::CountSketchWithHeap => 4,
            SketchFamily::CountMinWithSpaceSaving => 5,
            SketchFamily::CountSketchWithSpaceSaving => 6,
            SketchFamily::CountMinFold => 7,
            SketchFamily::CountSketchFold => 8,
            SketchFamily::Hll => 9,
            SketchFamily::HllHip => 10,
            SketchFamily::Kmv => 11,
            SketchFamily::Kll => 12,
            SketchFamily::KllDynamic => 13,
            SketchFamily::DdSketch => 14,
            SketchFamily::SpaceSaving => 15,
            SketchFamily::Elastic => 16,
            SketchFamily::Coco => 17,
            SketchFamily::Bloom => 18,
            SketchFamily::SetAggregator => 19,
            SketchFamily::UnivMon => 20,
            SketchFamily::UnivMonTwoTier => 21,
            SketchFamily::UnivMonQ => 22,
            SketchFamily::HydraCountMin => 23,
            SketchFamily::HydraCountSketch => 24,
            SketchFamily::HydraKll => 25,
            SketchFamily::HydraHll => 26,
            SketchFamily::HydraUnivMon => 27,
            #[cfg(feature = "experimental")]
            SketchFamily::CountMinHll => 28,
            #[cfg(feature = "experimental")]
            SketchFamily::MicroCm => 29,
            #[cfg(feature = "experimental")]
            SketchFamily::UniformSampling => 30,
        }
    }

    #[test]
    fn all_lists_every_family_exactly_once() {
        let expected_len = if cfg!(feature = "experimental") {
            31
        } else {
            28
        };
        assert_eq!(SketchFamily::ALL.len(), expected_len);
        for (position, &family) in SketchFamily::ALL.iter().enumerate() {
            assert_eq!(expected_index(family), position, "{family:?}");
        }
    }
}
