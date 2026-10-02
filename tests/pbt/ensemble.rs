//! Property tests for `HashSketchEnsemble`: several sketches fed from one
//! shared hashing layer.
//!
//! The oracle is a bare sketch of the member's own type fed the same stream,
//! so both sides of every law are real implementations and no model stands
//! between them. The layer hashes once from a layout cached at construction;
//! a bare fast-path sketch hashes from its own storage on every insert. The
//! laws hold the two to the same cells while one hash serves families that
//! read different parts of it: Count-Min the packed columns, Count the
//! columns and the sign bit, HyperLogLog the low 64 bits. Matrix members of
//! one layer may differ in width where the layout is the same, and each
//! decodes the shared hash at its own width.
//!
//! A layer holding a matrix sketch hashes at seed index 0, and an HLL member
//! of it therefore diverges from a bare HLL, which hashes at
//! `CANONICAL_HASH_SEED`. The bare-HLL equality covers HLL members of layers
//! with no matrix sketch, where the layer hashes at the canonical seed too.
//! A layer takes its layout at construction and never changes it, so a push
//! mid-stream moves no member the layer already holds.

use crate::support::keys;
use asap_sketchlib::{
    Classic, Count, CountMin, DataInput, DefaultXxHasher, EnsembleSketch, ErtlMLE, FastPath,
    HashSketchEnsemble, HyperLogLog, HyperLogLogHIP, Vector2D,
};
use proptest::prelude::*;

type CmFast = CountMin<Vector2D<i32>, FastPath>;
type CsFast = Count<Vector2D<i32>, FastPath>;
type Layer = HashSketchEnsemble<DefaultXxHasher>;

/// The member families one layer accepts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    CountMin,
    Count,
    Ertl,
    Classic,
    Hip,
}

impl Kind {
    fn answers_frequency(self) -> bool {
        matches!(self, Kind::CountMin | Kind::Count)
    }

    /// The label the layer reports for the family.
    fn label(self) -> &'static str {
        match self {
            Kind::CountMin => "CountMin",
            Kind::Count => "Count",
            Kind::Ertl | Kind::Classic | Kind::Hip => "HLL",
        }
    }
}

/// A sketch of a member's type living outside any layer.
enum Bare {
    CountMin(Box<CmFast>),
    Count(Box<CsFast>),
    Ertl(Box<HyperLogLog<ErtlMLE>>),
    Classic(Box<HyperLogLog<Classic>>),
    Hip(Box<HyperLogLogHIP>),
}

impl Bare {
    fn new(kind: Kind, rows: usize, cols: usize) -> Self {
        match kind {
            Kind::CountMin => Bare::CountMin(Box::new(CmFast::with_dimensions(rows, cols))),
            Kind::Count => Bare::Count(Box::new(CsFast::with_dimensions(rows, cols))),
            Kind::Ertl => Bare::Ertl(Box::default()),
            Kind::Classic => Bare::Classic(Box::default()),
            Kind::Hip => Bare::Hip(Box::default()),
        }
    }

    fn insert(&mut self, value: &DataInput) {
        match self {
            Bare::CountMin(s) => s.insert(value),
            Bare::Count(s) => s.insert(value),
            Bare::Ertl(s) => s.insert(value),
            Bare::Classic(s) => s.insert(value),
            Bare::Hip(s) => s.insert(value),
        }
    }

    /// One estimate per probe for a frequency sketch, one cardinality for an
    /// HLL: everything the family answers.
    fn answers(&self, probes: &[u64]) -> Vec<f64> {
        match self {
            Bare::CountMin(s) => probes
                .iter()
                .map(|k| f64::from(s.estimate(&DataInput::U64(*k))))
                .collect(),
            Bare::Count(s) => probes
                .iter()
                .map(|k| s.estimate(&DataInput::U64(*k)))
                .collect(),
            Bare::Ertl(s) => vec![s.estimate() as f64],
            Bare::Classic(s) => vec![s.estimate() as f64],
            Bare::Hip(s) => vec![s.estimate() as f64],
        }
    }
}

/// The width member `index` takes from a layer's pool of widths.
fn cols_at(pool: &[usize], index: usize) -> usize {
    pool[index % pool.len()]
}

fn member(kind: Kind, rows: usize, cols: usize) -> EnsembleSketch {
    match kind {
        Kind::CountMin => CmFast::with_dimensions(rows, cols).into(),
        Kind::Count => CsFast::with_dimensions(rows, cols).into(),
        Kind::Ertl => HyperLogLog::<ErtlMLE>::default().into(),
        Kind::Classic => HyperLogLog::<Classic>::default().into(),
        Kind::Hip => HyperLogLogHIP::default().into(),
    }
}

fn layer_of(kinds: &[Kind], rows: usize, pool: &[usize]) -> Layer {
    Layer::new(
        kinds
            .iter()
            .enumerate()
            .map(|(i, k)| member(*k, rows, cols_at(pool, i)))
            .collect(),
    )
    .expect("widths of one layout share a layer")
}

fn bare_of(kinds: &[Kind], rows: usize, pool: &[usize]) -> Vec<Bare> {
    kinds
        .iter()
        .enumerate()
        .map(|(i, k)| Bare::new(*k, rows, cols_at(pool, i)))
        .collect()
}

/// The same answers, read through the layer.
fn layer_answers(layer: &Layer, index: usize, kind: Kind, probes: &[u64]) -> Vec<f64> {
    if kind.answers_frequency() {
        probes
            .iter()
            .map(|k| {
                layer
                    .estimate(index, &DataInput::U64(*k))
                    .expect("a frequency member")
            })
            .collect()
    } else {
        vec![layer.cardinality(index).expect("a cardinality member")]
    }
}

fn feed(layer: &mut Layer, bare: &mut [Bare], stream: &[u64]) {
    for k in stream {
        let input = DataInput::U64(*k);
        layer.insert(&input);
        for sketch in bare.iter_mut() {
            sketch.insert(&input);
        }
    }
}

/// Every key the stream carried, and four the stream may have missed.
fn probes(stream: &[u64]) -> Vec<u64> {
    let mut p = stream.to_vec();
    p.sort_unstable();
    p.dedup();
    p.extend(0..4);
    p
}

fn kind() -> impl Strategy<Value = Kind> {
    prop_oneof![
        Just(Kind::CountMin),
        Just(Kind::Count),
        Just(Kind::Ertl),
        Just(Kind::Classic),
        Just(Kind::Hip),
    ]
}

fn matrix_kind() -> impl Strategy<Value = Kind> {
    prop_oneof![Just(Kind::CountMin), Just(Kind::Count)]
}

fn hll_kind() -> impl Strategy<Value = Kind> {
    prop_oneof![Just(Kind::Ertl), Just(Kind::Classic), Just(Kind::Hip)]
}

/// Member lists carrying both matrix families and all three HLL variants at
/// once, in an order and with duplicates the draw chooses.
fn mixed_members(extra: usize) -> impl Strategy<Value = Vec<Kind>> {
    prop::collection::vec(kind(), 0..extra)
        .prop_map(|mut v| {
            v.extend([
                Kind::CountMin,
                Kind::Count,
                Kind::Ertl,
                Kind::Classic,
                Kind::Hip,
            ]);
            v
        })
        .prop_shuffle()
}

/// HLL-only lists: the layer has no matrix member, so it hashes at the seed a
/// bare HLL hashes at.
fn hll_members(extra: usize) -> impl Strategy<Value = Vec<Kind>> {
    prop::collection::vec(hll_kind(), 0..extra)
        .prop_map(|mut v| {
            v.extend([Kind::Ertl, Kind::Classic, Kind::Hip]);
            v
        })
        .prop_shuffle()
}

/// A row count and the widths that share one hash layout with it: the three
/// layouts are packing into 64 bits, into 128, and one hash per row. Members
/// take widths from the pool in turn, so one layer holds matrix members of
/// several widths that all decode the same shared hash. The pools mix
/// powers of two with widths that are not, which fold a column differently.
fn geometry() -> impl Strategy<Value = (usize, Vec<usize>)> {
    prop_oneof![
        (
            1usize..6,
            prop::collection::vec(prop_oneof![Just(2usize), Just(8), Just(60), Just(64)], 1..4)
        ),
        (
            Just(8usize),
            prop::collection::vec(prop_oneof![Just(512usize), Just(1024)], 1..3)
        ),
        (
            Just(20usize),
            prop::collection::vec(prop_oneof![Just(64usize), Just(1000), Just(1024)], 1..3)
        ),
    ]
}

proptest! {
    // ===== A member holds what a bare sketch of its type holds =====

    #[test]
    fn a_frequency_member_answers_what_a_bare_sketch_fed_the_same_stream_answers(
        (rows, pool) in geometry(),
        members in mixed_members(3),
        stream in keys(120),
    ) {
        let mut layer = layer_of(&members, rows, &pool);
        let mut bare = bare_of(&members, rows, &pool);
        feed(&mut layer, &mut bare, &stream);

        let probes = probes(&stream);
        for (i, kind) in members.iter().enumerate() {
            if !kind.answers_frequency() {
                continue;
            }
            prop_assert_eq!(
                layer_answers(&layer, i, *kind, &probes),
                bare[i].answers(&probes),
                "member {} ({:?}) of {:?} at {} rows over widths {:?}",
                i, kind, members, rows, pool
            );
        }
    }

    #[test]
    fn a_member_of_a_matrix_free_layer_answers_what_a_bare_hll_answers(
        members in hll_members(3),
        stream in keys(120),
    ) {
        let mut layer = layer_of(&members, 1, &[1]);
        let mut bare = bare_of(&members, 1, &[1]);
        feed(&mut layer, &mut bare, &stream);

        for (i, kind) in members.iter().enumerate() {
            prop_assert_eq!(
                layer_answers(&layer, i, *kind, &[]),
                bare[i].answers(&[]),
                "member {} ({:?}) of {:?}", i, kind, members
            );
        }
    }

    // ===== The hash a caller computes is the hash the layer inserts =====

    #[test]
    fn the_prehashed_path_holds_every_member_where_the_plain_path_holds_it(
        (rows, pool) in geometry(),
        members in mixed_members(3),
        stream in keys(120),
    ) {
        let mut plain = layer_of(&members, rows, &pool);
        let mut prehashed = layer_of(&members, rows, &pool);

        let hashes: Vec<_> = stream
            .iter()
            .map(|k| prehashed.hash_input(&DataInput::U64(*k)))
            .collect();
        for k in &stream {
            plain.insert(&DataInput::U64(*k));
        }
        prehashed.bulk_insert_with_hashes(&hashes);

        let probes = probes(&stream);
        for (i, kind) in members.iter().enumerate() {
            prop_assert_eq!(
                layer_answers(&prehashed, i, *kind, &probes),
                layer_answers(&plain, i, *kind, &probes),
                "member {} ({:?}) fed by hash", i, kind
            );
            if !kind.answers_frequency() {
                continue;
            }
            for k in &probes {
                let hash = plain.hash_input(&DataInput::U64(*k));
                prop_assert_eq!(
                    plain.estimate_with_hash(i, &hash).expect("a frequency member"),
                    plain.estimate(i, &DataInput::U64(*k)).expect("a frequency member"),
                    "member {} ({:?}) queried by hash for {}", i, kind, k
                );
            }
        }
    }

    // ===== An insert reaches the members it names and no others =====

    #[test]
    fn an_insert_at_reaches_the_named_members_and_leaves_the_rest_empty(
        (rows, pool) in geometry(),
        members in mixed_members(3),
        picks in prop::collection::vec(0usize..8, 0..8),
        stream in keys(120),
    ) {
        let mut chosen: Vec<usize> = picks.iter().map(|p| p % members.len()).collect();
        chosen.sort_unstable();
        chosen.dedup();

        let mut named = layer_of(&members, rows, &pool);
        let mut every = layer_of(&members, rows, &pool);
        let empty = layer_of(&members, rows, &pool);
        let values: Vec<DataInput> = stream.iter().map(|k| DataInput::U64(*k)).collect();
        named.bulk_insert_at(&chosen, &values);
        every.bulk_insert(&values);

        let probes = probes(&stream);
        for (i, kind) in members.iter().enumerate() {
            let expected = if chosen.contains(&i) { &every } else { &empty };
            prop_assert_eq!(
                layer_answers(&named, i, *kind, &probes),
                layer_answers(expected, i, *kind, &probes),
                "member {} ({:?}) under insert_at {:?}", i, kind, chosen
            );
        }
    }

    // ===== A push mid-stream moves no member the layer already holds =====

    #[test]
    fn a_push_mid_stream_moves_no_member_and_a_taken_one_holds_only_the_rest(
        (rows, pool) in geometry(),
        (members, extra) in prop_oneof![
            (mixed_members(2), kind()),
            (hll_members(2), matrix_kind()),
        ],
        prefix in keys(60),
        suffix in keys(60),
    ) {
        // A layer takes its layout at construction: a matrix sketch pushed
        // into a layer holding none is refused, and a layer whose layout is
        // already fixed takes a member of that layout.
        let width = cols_at(&pool, members.len());
        let mut pushed = layer_of(&members, rows, &pool);
        let mut untouched = layer_of(&members, rows, &pool);
        let mut late = Bare::new(extra, rows, width);

        for k in &prefix {
            let input = DataInput::U64(*k);
            pushed.insert(&input);
            untouched.insert(&input);
        }
        let taken = pushed.push(member(extra, rows, width)).is_ok();
        prop_assert_eq!(
            pushed.len(),
            members.len() + usize::from(taken),
            "a refused {:?} was added to {:?} anyway", extra, members
        );
        for k in &suffix {
            let input = DataInput::U64(*k);
            pushed.insert(&input);
            untouched.insert(&input);
            late.insert(&input);
        }

        let whole: Vec<u64> = prefix.iter().chain(&suffix).copied().collect();
        let probes = probes(&whole);
        for (i, kind) in members.iter().enumerate() {
            prop_assert_eq!(
                layer_answers(&pushed, i, *kind, &probes),
                layer_answers(&untouched, i, *kind, &probes),
                "member {} ({:?}) moved by a {:?} pushed mid-stream", i, kind, extra
            );
        }

        if taken && extra.answers_frequency() {
            let i = members.len();
            prop_assert_eq!(
                layer_answers(&pushed, i, extra, &probes),
                late.answers(&probes),
                "the pushed {:?} at {} rows over {} holds another stream or hashes elsewhere",
                extra, rows, width
            );
        }
    }

    // ===== A matrix-free layer takes no matrix member, fed or not =====

    #[test]
    fn a_matrix_free_layer_refuses_a_matrix_member_whether_or_not_it_has_been_fed(
        (rows, pool) in geometry(),
        members in hll_members(2),
        alien in matrix_kind(),
        stream in keys(120),
    ) {
        let width = cols_at(&pool, members.len());
        let mut layer = layer_of(&members, rows, &pool);
        let mut untouched = layer_of(&members, rows, &pool);

        prop_assert!(
            layer.push(member(alien, rows, width)).is_err(),
            "a layer of {:?} took a {:?} before any insert", members, alien
        );
        prop_assert_eq!(layer.len(), members.len());

        for k in &stream {
            let input = DataInput::U64(*k);
            layer.insert(&input);
            untouched.insert(&input);
        }
        prop_assert!(
            layer.push(member(alien, rows, width)).is_err(),
            "a layer of {:?} took a {:?} after a stream", members, alien
        );
        prop_assert_eq!(layer.len(), members.len());

        // The layer still hashes at the canonical seed, so the stream it
        // holds is what a layer that saw no push holds.
        let probes = probes(&stream);
        for (i, kind) in members.iter().enumerate() {
            prop_assert_eq!(
                layer_answers(&layer, i, *kind, &probes),
                layer_answers(&untouched, i, *kind, &probes),
                "member {} ({:?}) moved by a refused {:?}", i, kind, alien
            );
        }

        // An HLL member carries no layout, so it is taken either way.
        prop_assert!(layer.push(member(Kind::Hip, rows, width)).is_ok());
        prop_assert_eq!(layer.len(), members.len() + 1);
    }

    // ===== A layer answers for the family it holds, and only for it =====

    #[test]
    fn a_member_answers_its_own_family_names_it_and_refuses_the_other(
        (rows, pool) in geometry(),
        members in mixed_members(3),
        stream in keys(60),
    ) {
        let mut layer = layer_of(&members, rows, &pool);
        for k in &stream {
            layer.insert(&DataInput::U64(*k));
        }

        let key = DataInput::U64(7);
        for (i, kind) in members.iter().enumerate() {
            prop_assert_eq!(
                layer.get(i).expect("a member at every index").sketch_type(),
                kind.label()
            );
            prop_assert_eq!(
                layer.estimate(i, &key).is_ok(),
                kind.answers_frequency(),
                "member {} ({:?}) answers the wrong question", i, kind
            );
            prop_assert_eq!(
                layer.cardinality(i).is_ok(),
                !kind.answers_frequency(),
                "member {} ({:?}) answers the wrong question", i, kind
            );
        }

        let past = layer.len();
        prop_assert_eq!(layer.len(), members.len());
        prop_assert!(layer.get(past).is_none());
        prop_assert!(layer.estimate(past, &key).is_err());
        prop_assert!(layer.cardinality(past).is_err());
        prop_assert!(
            layer
                .estimate_with_hash(past, &layer.hash_input(&key))
                .is_err()
        );
    }

    // ===== A matrix member of another layout is refused =====

    #[test]
    fn a_matrix_member_of_another_row_count_is_refused_and_the_layer_is_unchanged(
        (rows, pool) in geometry(),
        members in mixed_members(2),
        alien in matrix_kind(),
        alien_rows in 1usize..21,
        stream in keys(60),
    ) {
        let alien_rows = if alien_rows == rows { rows % 20 + 1 } else { alien_rows };
        let width = cols_at(&pool, 0);

        let mut sketches: Vec<EnsembleSketch> = members
            .iter()
            .enumerate()
            .map(|(i, k)| member(*k, rows, cols_at(&pool, i)))
            .collect();
        sketches.push(member(alien, alien_rows, width));
        prop_assert!(
            Layer::new(sketches).is_err(),
            "a layer took {:?} at {} rows beside members at {} rows", alien, alien_rows, rows
        );

        let mut layer = layer_of(&members, rows, &pool);
        for k in &stream {
            layer.insert(&DataInput::U64(*k));
        }
        let probes = probes(&stream);
        let before: Vec<Vec<f64>> = members
            .iter()
            .enumerate()
            .map(|(i, kind)| layer_answers(&layer, i, *kind, &probes))
            .collect();

        prop_assert!(layer.push(member(alien, alien_rows, width)).is_err());
        prop_assert_eq!(layer.len(), members.len());
        for (i, kind) in members.iter().enumerate() {
            prop_assert_eq!(
                layer_answers(&layer, i, *kind, &probes),
                before[i].clone(),
                "member {} ({:?}) moved by a refused push", i, kind
            );
        }
    }
}
