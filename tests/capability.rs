//! The capability table: row invariants, the decided cells, and the merge column.

use std::collections::HashSet;

use MergeInputRule::{Disjoint as D, MayOverlap as O};
use MergedError::{Approximate as A, AsIfBuiltOverUnion as U, Exact as E};
use SketchAlgorithm as F;
use StatisticKind::*;
use asap_sketchlib::input::{HydraCounter, HydraQuery};
use asap_sketchlib::*;

type Readouts = &'static [(StatisticKind, MergedError)];

const UNIVMON: Readouts = &[
    (Total, E),
    (FrequencyL2, U),
    (Distinct, U),
    (FrequencyEntropy, A),
    (GSum, A),
];
const FREQ_TOP_K: Readouts = &[(ItemFrequency, U), (TopK, A)];
const QUANTILE_RANK: Readouts = &[(Quantile, U), (Rank, U)];

#[rustfmt::skip]
const DECIDED: &[(SketchAlgorithm, Option<MergeInputRule>, Readouts)] = &[
    (F::Cms, Some(D), &[(ItemFrequency, U), (Total, E)]),
    (F::CountSketch, Some(D), &[(ItemFrequency, U)]),
    (F::CountSketchWithL2Norm, Some(D), &[(ItemFrequency, U), (FrequencyL2, U)]),
    (F::CmsWithHeap, Some(D), FREQ_TOP_K),
    (F::CountSketchWithHeap, Some(D), FREQ_TOP_K),
    (F::CmsWithSpaceSaving, Some(D), FREQ_TOP_K),
    (F::CountSketchWithSpaceSaving, Some(D), FREQ_TOP_K),
    (F::CmsFold, Some(D), FREQ_TOP_K),
    (F::CountSketchFold, Some(D), FREQ_TOP_K),
    (F::Hll, Some(O), &[(Distinct, U)]),
    (F::HllHip, None, &[(Distinct, A)]),
    (F::Kmv, Some(O), &[(Distinct, U)]),
    (F::Kll, Some(D), QUANTILE_RANK),
    (F::KllDynamic, Some(D), QUANTILE_RANK),
    (F::DDSketch, Some(D), &[(Quantile, U)]),
    (F::SpaceSaving, Some(D), &[(ItemFrequency, A), (TopK, A)]),
    (F::Elastic, Some(D), &[(ItemFrequency, A), (TopK, A)]),
    (F::Coco, Some(D), &[(ItemFrequency, A), (PartialKeySum, A)]),
    (F::Bloom, Some(O), &[(Membership, U)]),
    (F::SetAggregator, Some(O), &[(Distinct, E), (Membership, E)]),
    (F::UnivMon, Some(D), UNIVMON),
    (F::UnivMonTwoTier, Some(D), UNIVMON),
    (F::UnivMonQ, Some(D), &[
        (ItemFrequency, U), (FrequencyL2, U), (Distinct, A), (Quantile, A), (Rank, A),
        (GSum, A), (FrequencyEntropy, A), (TopK, A),
    ]),
    (F::HydraCms, Some(D), &[(ItemFrequency, U), (Total, U)]),
    (F::HydraCountSketch, Some(D), &[(ItemFrequency, U)]),
    (F::HydraKll, Some(D), QUANTILE_RANK),
    (F::HydraHll, Some(O), &[(Distinct, U)]),
    (F::HydraUnivMon, Some(D), &[(Total, U), (Distinct, U), (FrequencyL2, U), (FrequencyEntropy, A)]),
    #[cfg(feature = "experimental")]
    (F::CmsHll, Some(O), &[(KeyDistinct, U)]),
    #[cfg(feature = "experimental")]
    (F::MicroCm, Some(D), &[(ItemFrequency, A)]),
    #[cfg(feature = "experimental")]
    (F::UniformSampling, Some(D), &[(Sample, A)]),
];

#[test]
fn every_row_is_well_formed() {
    for &family in SketchAlgorithm::ALL {
        let row = family.capability();
        assert!(!row.readouts.is_empty(), "{family:?}");
        let mut seen = HashSet::new();
        for &(statistic, error) in row.readouts {
            assert!(
                seen.insert(statistic),
                "{family:?} lists {statistic:?} twice"
            );
            assert!(row.answers(statistic));
            assert_eq!(row.merged_error(statistic), row.merge_inputs.map(|_| error));
        }
    }
    assert_eq!(F::HllHip.capability().merged_error(Distinct), None);
    assert_eq!(F::Cms.capability().merged_error(Quantile), None);
}

#[test]
fn the_table_holds_the_decided_cells() {
    let families: Vec<_> = DECIDED.iter().map(|row| row.0).collect();
    assert_eq!(families, SketchAlgorithm::ALL);
    for &(family, merge, readouts) in DECIDED {
        let row = family.capability();
        assert_eq!(row.merge_inputs, merge, "{family:?}");
        assert_eq!(row.readouts.len(), readouts.len(), "{family:?}");
        assert!(
            readouts.iter().all(|pair| row.readouts.contains(pair)),
            "{family:?}"
        );
    }
}

/// Readouts after `update`, before and after merging the state with a clone.
fn self_merge<S: Clone>(
    mut state: S,
    update: impl FnOnce(&mut S),
    merge: impl Fn(&mut S, &S),
    read: impl Fn(&S) -> Vec<(StatisticKind, f64)>,
) -> Vec<(StatisticKind, f64, f64)> {
    update(&mut state);
    let before = read(&state);
    let copy = state.clone();
    merge(&mut state, &copy);
    before
        .into_iter()
        .zip(read(&state))
        .map(|((s, b), (_, a))| (s, b, a))
        .collect()
}

fn hydra(cell: HydraCounter) -> Hydra {
    Hydra::with_schema(2, 16, ["g"], cell).unwrap()
}

const K: DataInput = DataInput::U64(1);
const G: [Option<&str>; 1] = [Some("x")];

/// Total and ItemFrequency on Disjoint rows (and UnivMon's L2, which a self
/// merge also doubles), the cheap readouts on MayOverlap rows.
#[rustfmt::skip]
fn probe(family: SketchAlgorithm) -> Vec<(StatisticKind, f64, f64)> {
    let add3 = |h: &mut Hydra| h.update(&["x"], &K, Some(3)).unwrap();
    let merge = |a: &mut Hydra, b: &Hydra| a.merge(b).unwrap();
    let query = |h: &Hydra, q: HydraQuery| h.query_key(&G, &q).unwrap();
    let keys = |n: u64| (0..n).map(DataInput::U64);
    match family {
        F::Cms => self_merge(CountMinSketch::<Vector2D<i64>, FastPath>::with_dimensions(3, 64), |s| s.update_weighted(&K, 3),
            |a, b| a.merge(b), |s| vec![(Total, s.total()), (ItemFrequency, s.estimate(&K) as f64)]),
        F::CountSketch => self_merge(CountSketch::<Vector2D<i64>, FastPath>::with_dimensions(3, 64), |s| s.update_weighted(&K, 3),
            |a, b| a.merge(b), |s| vec![(ItemFrequency, s.estimate(&K))]),
        F::CountSketchWithL2Norm => self_merge(CountSketchWithL2Norm::<DefaultXxHasher>::with_dimensions(3, 64),
            |s| s.update_weighted(&K, 3), |a, b| a.merge(b), |s| vec![(ItemFrequency, s.estimate(&K))]),
        F::CmsWithHeap => self_merge(CountMinSketchWithHeap::<Vector2D<i64>, FastPath>::new(3, 64, 4),
            |s| s.update_weighted(&K, 3), |a, b| a.merge(b), |s| vec![(ItemFrequency, s.estimate(&K) as f64)]),
        F::CountSketchWithHeap => self_merge(CountSketchWithHeap::<Vector2D<i64>, FastPath>::new(3, 64, 4),
            |s| s.update_weighted(&K, 3), |a, b| a.merge(b), |s| vec![(ItemFrequency, s.estimate(&K))]),
        F::CmsWithSpaceSaving => self_merge(CountMinSketchWithSpaceSaving::<Vector2D<i64>>::new(3, 64, 4),
            |s| s.update_weighted(&K, 3), |a, b| a.merge(b), |s| vec![(ItemFrequency, s.estimate(&K) as f64)]),
        F::CountSketchWithSpaceSaving => self_merge(CountSketchWithSpaceSaving::<Vector2D<i64>>::new(3, 64, 4),
            |s| s.update_weighted(&K, 3), |a, b| a.merge(b), |s| vec![(ItemFrequency, s.estimate(&K))]),
        F::CmsFold => self_merge(CountMinSketchFold::<DefaultXxHasher>::new(3, 64, 2, 4), |s| s.update_weighted(&K, 3),
            |a, b| a.merge_same_level(b), |s| vec![(ItemFrequency, s.estimate(&K) as f64)]),
        F::CountSketchFold => self_merge(CountSketchFold::<DefaultXxHasher>::new(3, 64, 2, 4), |s| s.update_weighted(&K, 3),
            |a, b| a.merge_same_level(b), |s| vec![(ItemFrequency, s.estimate(&K) as f64)]),
        F::Hll => self_merge(HyperLogLog::<Ertl>::new(), |s| keys(50).for_each(|k| s.update(&k)),
            |a, b| a.merge(b), |s| vec![(Distinct, s.estimate() as f64)]),
        F::Kmv => self_merge(Kmv::<DefaultXxHasher>::new(16), |s| keys(50).for_each(|k| s.update(&k)),
            |a, b| a.merge(b), |s| vec![(Distinct, s.estimate())]),
        F::SpaceSaving => self_merge(SpaceSaving::<DefaultXxHasher>::with_capacity(4), |s| s.update_weighted(&K, 3),
            |a, b| a.merge(b), |s| vec![(ItemFrequency, s.estimate(&K) as f64)]),
        F::Elastic => self_merge(ElasticSketch::<DefaultXxHasher>::with_heavy_buckets(8), |s| s.update_weighted("a".into(), 3),
            |a, b| a.merge(b), |s| vec![(ItemFrequency, s.estimate("a".into()) as f64)]),
        F::Coco => self_merge(CocoSketch::<DefaultXxHasher>::with_dimensions(3, 64), |s| s.update_weighted("a", 3),
            |a, b| a.merge(b), |s| vec![(ItemFrequency, s.estimate("a") as f64)]),
        F::Bloom => self_merge(Bloom::<RegularPath>::with_capacity(100, 0.01), |s| s.update(&K),
            |a, b| a.merge(b), |s| vec![(Membership, s.contains(&K) as u8 as f64)]),
        F::SetAggregator => self_merge(SetAggregator::new(), |s| s.update("a"), |a, b| a.merge(b).unwrap(),
            |s| vec![(Distinct, s.values.len() as f64), (Membership, s.values.contains("a") as u8 as f64)]),
        F::UnivMon => self_merge(UnivMon::new(4, 3, 64, 4), |s| s.update_weighted(&K, 3),
            |a, b| a.merge(b), |s| vec![(Total, s.estimate_l1()), (FrequencyL2, s.estimate_l2())]),
        F::UnivMonTwoTier => self_merge(UnivMonTwoTier::with_defaults(), |s| s.update_weighted(&K, 3),
            |a, b| a.merge(b), |s| vec![(Total, s.estimate_l1())]),
        F::UnivMonQ => self_merge(UnivMonQ::<DefaultXxHasher>::default(), |s| (0..3).for_each(|_| s.update(&3.0f64)),
            |a, b| a.merge(b).unwrap(), |s| vec![(ItemFrequency, s.estimate_frequency(3.0) as f64)]),
        F::HydraCms => self_merge(hydra(HydraCounter::CountMinSketch(CountMinSketch::with_dimensions(3, 64))), add3, merge,
            |s| vec![(Total, s.query_group_total(&G).unwrap()), (ItemFrequency, s.query_frequency(&G, &K).unwrap())]),
        F::HydraCountSketch => self_merge(hydra(HydraCounter::CountSketch(CountSketch::with_dimensions(3, 64))), add3, merge,
            |s| vec![(ItemFrequency, s.query_frequency(&G, &K).unwrap())]),
        F::HydraHll => self_merge(hydra(HydraCounter::Hll(HyperLogLog::<Ertl>::new())), add3, merge,
            |s| vec![(Distinct, query(s, HydraQuery::Cardinality))]),
        F::HydraUnivMon => self_merge(hydra(HydraCounter::UnivMon(UnivMon::new(4, 3, 64, 4))), add3, merge,
            |s| vec![(Total, query(s, HydraQuery::L1Norm))]),
        #[cfg(feature = "experimental")]
        F::CmsHll => self_merge(CountMinHll::<DefaultXxHasher>::with_dimensions(3, 64, 10),
            |s| keys(20).for_each(|v| s.update(&K, &v)), |a, b| a.merge(b).unwrap(), |s| vec![(KeyDistinct, s.estimate(&K))]),
        #[cfg(feature = "experimental")]
        F::MicroCm => self_merge(MicroCm::default(), |s| (0..3).for_each(|_| s.update(&K)),
            |a, b| a.merge(b).unwrap(), |s| vec![(ItemFrequency, s.estimate(&K))]),
        _ => vec![],
    }
}

/// A MayOverlap merge of a state with itself changes no readout; a Disjoint
/// one doubles Total, ItemFrequency and a single item's L2 norm.
#[test]
fn self_merge_follows_the_merge_column() {
    for &family in SketchAlgorithm::ALL {
        let row = family.capability();
        let Some(merge) = row.merge_inputs else {
            continue;
        };
        let probes = probe(family);
        for &(statistic, before, after) in &probes {
            assert!(row.answers(statistic), "{family:?} {statistic:?}");
            let expected = if merge == O { before } else { 2.0 * before };
            assert!(before > 0.0, "{family:?} {statistic:?} read 0");
            assert_eq!(after, expected, "{family:?} {statistic:?}");
        }
        let probed: HashSet<_> = probes.iter().map(|p| p.0).collect();
        for &(statistic, _) in row.readouts {
            let required = merge == O || matches!(statistic, Total | ItemFrequency);
            let cheap = !matches!(statistic, TopK | PartialKeySum | Sample);
            if required && cheap {
                assert!(
                    probed.contains(&statistic),
                    "{family:?} {statistic:?} unprobed"
                );
            }
        }
    }
}
