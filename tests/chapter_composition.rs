// use sketchlib_rust::{
//     HllDf,
//     common::SketchInput,
//     sketch_framework::Chapter,
//     sketches::{
//         coco::Coco, countmin::CountMin, kll::KLL, locher::LocherSketch, uniform::UniformSampling,
//         univmon::UnivMon,
//     },
// };

// fn sketch_input_str(value: &'static str) -> SketchInput<'static> {
//     SketchInput::Str(value)
// }

// #[test]
// fn chapter_countmin_reports_inserted_frequency() {
//     // exercise Chapter::insert/query pathway for CountMin sketches
//     let mut chapter = Chapter::CM(CountMin::init_cm_with_row_col(3, 64));
//     let input = sketch_input_str("count::key");
//     for _ in 0..12 {
//         chapter.insert(&input);
//     }
//     let estimate = chapter
//         .query(&input)
//         .expect("countmin chapter should query") as u64;
//     assert!(
//         estimate >= 12,
//         "expected count >= 12 for inserted key, got {}",
//         estimate
//     );
// }

// #[test]
// fn chapter_kll_reports_quantile_estimates() {
//     // verify Chapter::KLL integrates update/query for numerical values
//     let mut chapter = Chapter::KLL(KLL::init_kll(200));
//     let samples = [10.0, 20.0, 30.0, 40.0, 50.0];
//     for value in samples {
//         chapter.insert(&SketchInput::F64(value));
//     }

//     let p60 = chapter
//         .query(&SketchInput::F64(35.0))
//         .expect("kll chapter query");
//     assert!(
//         (p60 - 0.6).abs() < 1e-6,
//         "expected ~0.6 quantile, got {}",
//         p60
//     );
// }

// #[test]
// fn chapter_uniform_sampling_tracks_length_and_samples() {
//     // uniform sampler variant should expose size and total_seen via Chapter::query
//     let mut chapter = Chapter::UNIFORM(UniformSampling::with_seed(0.4, 0xC0FFEE));
//     for value in 0..25 {
//         chapter.insert(&SketchInput::I32(value));
//     }

//     let len = chapter.query(&SketchInput::Str("len")).expect("len query") as usize;
//     let total_seen = chapter
//         .query(&SketchInput::Str("total_seen"))
//         .expect("total_seen query");

//     assert!(len > 0, "expected sampler to store at least one value");
//     assert_eq!(total_seen, 25.0);

//     if len > 0 {
//         let first_sample = chapter
//             .query(&SketchInput::U32(0))
//             .expect("sample-at query");
//         assert!(
//             first_sample >= 0.0 && first_sample < 25.0,
//             "sample should be within inserted range"
//         );
//     }
// }

// #[test]
// fn chapter_locher_estimates_heavy_hitters() {
//     // ensure Locher sketch integration returns a sizable estimate after inserts
//     let mut chapter = Chapter::LOCHER(LocherSketch::new(3, 32, 6));
//     for _ in 0..40 {
//         chapter.insert(&SketchInput::String("endpoint=/search".into()));
//     }

//     let estimate = chapter
//         .query(&SketchInput::Str("endpoint=/search"))
//         .expect("locher query");
//     assert!(
//         estimate >= 20.0,
//         "expected locher to report a noticeable frequency, got {}",
//         estimate
//     );
// }

// #[test]
// fn chapter_coco_supports_partial_queries() {
//     // coco variant should let queries aggregate counts via substring matches
//     let mut chapter = Chapter::COCO(Coco::init_with_size(32, 5));
//     let key = SketchInput::String("region=us-west|id=42".into());
//     chapter.insert(&key);
//     chapter.insert(&key);

//     let estimate = chapter
//         .query(&SketchInput::Str("us-west"))
//         .expect("coco query");
//     assert_eq!(estimate, 2.0);
// }

// #[test]
// fn chapter_hll_exposes_cardinality_estimate() {
//     // HLL variant should expose an approximate distinct count via Chapter::query
//     let mut chapter = Chapter::HLL(HllDf::new());
//     for value in 0..5_000u64 {
//         chapter.insert(&SketchInput::U64(value));
//     }

//     let estimate = chapter
//         .query(&SketchInput::Str("ignored"))
//         .expect("hll query");
//     let error = (estimate - 5_000.0).abs() / 5_000.0;
//     assert!(
//         error < 0.05,
//         "expected HLL error < 5%, estimate {}, error {}",
//         estimate,
//         error
//     );
// }

// #[test]
// fn chapter_univmon_updates_cardinality_state() {
//     // UnivMon variant lacks direct query support, so validate the internal sketch after inserts
//     let mut chapter = Chapter::UNIVMON(UnivMon::init_univmon(16, 3, 32, 4, 0));
//     for _ in 0..30 {
//         chapter.insert(&SketchInput::Str("flow"));
//     }

//     if let Chapter::UNIVMON(sketch) = chapter {
//         assert!(sketch.calc_card() >= 1.0);
//         assert!(sketch.calc_l1() > 0.0);
//     } else {
//         panic!("expected UnivMon variant");
//     }
// }
