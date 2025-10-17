use querysimulation::{sketches::Coco, utils::SketchInput};
use std::fs;
use std::path::Path;

fn main() {
    println!("=== Coco Sketch Test ===\n");

    basic_test();
    basic_test_with_udf();

    let _ = test_performance_with_metric_data();
    let _ = test_performance_with_metric_data_truncated();
}

pub fn basic_test() {
    println!("--- Basic Test ---");
    let mut sketch = Coco::default();
    let full_str1 = "128.0.0.1"; //.to_string();
    let full_str2 = "128.0.10.10"; //.to_string();
    let full_str3 = "8.8.8.8"; //.to_string();
    let partial1 = "128.0"; //.to_string();
    let partial2 = "8.8"; //.to_string();
    // sketch.insert("128.0.0.1".to_string(), 10);
    // sketch.insert("128.0.10.10".to_string(), 20);
    // sketch.insert("8.8.8.8".to_string(), 15);
    sketch.insert(&SketchInput::Str(full_str1), 10);
    sketch.insert(&SketchInput::Str(full_str2), 20);
    sketch.insert(&SketchInput::Str(full_str3), 15);
    // let p1 = "128.0".to_string();
    // let p2 = "8.8".to_string();
    let p1 = SketchInput::Str(partial1);
    let p2 = SketchInput::Str(partial2);
    // sketch.debug();
    // println!("partial key {} get: {}, while should be 30", p1.clone(), sketch.estimate(p1));
    // println!("partial key {} 8.8 get: {}, while should be 15", p2.clone(), sketch.estimate(p2));
    println!(
        "partial key 128.0 get: {}, while should be 30",
        sketch.estimate(p1)
    );
    println!(
        "partial key 8.8 get: {}, while should be 15",
        sketch.estimate(p2)
    );
}

pub fn basic_test_with_udf() {
    println!("\n\n--- Basic Test With UDF ---");
    let mut sketch = Coco::default();
    let full_str1 = "128.0.0.1"; //.to_string();
    let full_str2 = "128.0.10.10"; //.to_string();
    let full_str3 = "8.8.8.8"; //.to_string();
    let partial1 = "128.0"; //.to_string();
    let partial2 = "8.8"; //.to_string();
    // sketch.insert("128.0.0.1".to_string(), 10);
    // sketch.insert("128.0.10.10".to_string(), 20);
    // sketch.insert("8.8.8.8".to_string(), 15);
    sketch.insert(&SketchInput::Str(full_str1), 10);
    sketch.insert(&SketchInput::Str(full_str2), 20);
    sketch.insert(&SketchInput::Str(full_str3), 15);
    // let p1 = "128.0".to_string();
    // let p2 = "8.8".to_string();
    let p1 = SketchInput::Str(partial1);
    let p2 = SketchInput::Str(partial2);
    // sketch.debug();
    // println!("partial key {} get: {}, while should be 30", p1.clone(), sketch.estimate(p1));
    // println!("partial key {} 8.8 get: {}, while should be 15", p2.clone(), sketch.estimate(p2));
    println!(
        "partial key 128.0 get: {}, while should be 30",
        sketch.estimate_with_udf(p1, custom_partial_key)
    );
    println!(
        "partial key 8.8 get: {}, while should be 15",
        sketch.estimate_with_udf(p2, custom_partial_key)
    );
}

// many cases are just place holders
pub fn custom_partial_key(full: &SketchInput, partial: &SketchInput) -> bool {
    match (full, partial) {
        (SketchInput::I32(f), SketchInput::I32(p)) => f % p == 0,
        (SketchInput::I64(f), SketchInput::I64(p)) => f % p == 0,
        (SketchInput::U32(f), SketchInput::U32(p)) => f % p == 0,
        (SketchInput::U64(f), SketchInput::U64(p)) => f % p == 0,
        (SketchInput::F32(f), SketchInput::F32(p)) => f > p,
        (SketchInput::F64(f), SketchInput::F64(p)) => f > p,
        (SketchInput::Str(f), SketchInput::Str(p)) => (*f).contains(*p),
        (SketchInput::String(f), SketchInput::String(p)) => {
            // println!("here: f => {}  p => {}, result => {}", *f, *p, (*f).contains(*p));
            (*f).contains(p)
        }
        (SketchInput::Bytes(f), SketchInput::Bytes(p)) => {
            let mut res = true;
            for byte in *p {
                res = res && (*f).contains(byte)
            }
            res
        }
        _ => false,
    }
}

pub fn test_performance_with_metric_data() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n\n--- Metric Data Test ---");
    let file_location = "testdata/sample_input.txt";
    if !Path::new("testdata").exists() {
        fs::create_dir_all("testdata")?;
        println!("Create Dir ./testdata");
    }
    // this is a bunch of records, as a large string
    let all_lines = fs::read_to_string(file_location)?;

    ////// the following requires crate: prom_text_format_parser
    ////// just leave it here in case it can be useful in the future
    // let scrape = Scrape::parse(&all_lines).unwrap();
    // for metric in scrape.metrics {
    //     if metric.name == "fake_machine_metric" {
    //         for sample in metric.samples {
    //             sample.labels.to_string();
    //             // println!("labels: |{}|, value: |{}|", sample.labels.contains(&Label::new("machineid".to_string(), "machine_1242".to_string())), sample.value.value.as_f64() as u64);
    //         }
    //     }
    // }
    //////

    let lines: Vec<&str> = all_lines.split("\n").collect();
    println!("first line: {}", lines[0]);
    let mut sketch = Coco::default();
    let mut baseline = Vec::new(); // order matters
    for line in lines {
        if !line.contains("#") {
            let pos = line.chars().position(|c| c == '}').unwrap();
            let series = &line[0..pos + 1];
            let value = line[pos + 1..].trim().parse::<f64>().unwrap();
            //// for debugging only
            // println!("seires: |{}| ==> value: |{}|", series, value);
            // sketch.insert(&SketchInput::Str(series), value as u64);
            sketch.insert(&SketchInput::Str(series), value as u64);
            baseline.push(value as u64);
        }
    }
    // sketch.debug();
    let mut avg_err = 0.0;
    let mut very_close_amount = 0;
    let mut min_err = f64::MAX;
    let mut min_err_est = 0;
    let mut min_err_truth = 0;
    let mut min_err_idx = 0;
    for i in 0..baseline.len() {
        // wow... to have the get_est inside a loop, I need a different lifetime for the get estimate function
        // that is, the partial key has life time just valid for one iteration
        // the sketch has a life time across all evaluation
        // the var created at each iteration cannot live across different iteration
        // and certainly cannot be used if the lifetime of the partial key is the same with that of sketch
        let machine_id = format!("machine_{}", i);
        let partial_key = SketchInput::Str(machine_id.as_str());
        let result = sketch.estimate_with_udf(partial_key, custom_partial_key);
        // println!("est: {} with actual: {}", result, baseline[i]);
        let err_rate = (result as f64 - baseline[i] as f64).abs() / baseline[i] as f64;
        // if result != 0 {
        //     println!("idx: {}", i);
        //     println!("error rate: {}, result: {}, exp: {}", err_rate, result, baseline[i]);
        // }
        if err_rate < 0.05 {
            very_close_amount += 1;
        }
        if err_rate < min_err {
            min_err_est = result;
            min_err_truth = baseline[i];
            min_err = err_rate;
            min_err_idx = i;
        }
        avg_err += err_rate;
    }
    avg_err = avg_err * 100.0 / baseline.len() as f64;
    println!("actually... the average error is: {:.2} %", avg_err);
    println!(
        "the very close (less than 5% difference) result count is: {} out of {}",
        very_close_amount,
        baseline.len()
    );
    println!(
        "min error occurs at idx {}, where the est is: {} and true value is: {}",
        min_err_idx, min_err_est, min_err_truth
    );

    println!("\n>>> (meaningless) sub query estimate ");
    println!(">>> test for basic correctness");
    println!(">>> i.e., the error is how sub query is, not the implementation");
    let real_sub_query = "machine_";
    let partial_key = SketchInput::Str(real_sub_query);
    let result = sketch.estimate_with_udf(partial_key, custom_partial_key);
    let truth_value_exp: u64 = baseline.iter().sum::<u64>();
    let err_rate =
        ((result as f64 - truth_value_exp as f64).abs() / truth_value_exp as f64) * 100.0;
    println!("error rate for sub query machine_ is: {:.2} %", err_rate);
    println!("query estimate: {}", result);
    println!("expected value: {}", truth_value_exp);
    Ok(())
}

pub fn test_performance_with_metric_data_truncated() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n\n--- Metric Data Test Truncated Version ---");
    let file_location = "testdata/sample_input.txt";
    if !Path::new("testdata").exists() {
        fs::create_dir_all("testdata")?;
        println!("Create Dir ./testdata");
    }
    // this is a bunch of records, as a large string
    let all_lines = fs::read_to_string(file_location)?;

    ////// the following requires crate: prom_text_format_parser
    ////// just leave it here in case it can be useful in the future
    // let scrape = Scrape::parse(&all_lines).unwrap();
    // for metric in scrape.metrics {
    //     if metric.name == "fake_machine_metric" {
    //         for sample in metric.samples {
    //             sample.labels.to_string();
    //             // println!("labels: |{}|, value: |{}|", sample.labels.contains(&Label::new("machineid".to_string(), "machine_1242".to_string())), sample.value.value.as_f64() as u64);
    //         }
    //     }
    // }
    //////

    let lines: Vec<&str> = all_lines.split("\n").collect();
    let truncated_lines = &lines[..102];
    println!("first line: {}", lines[0]);
    let mut sketch = Coco::default();
    let mut baseline = Vec::new(); // order matters
    for line in truncated_lines {
        if !line.contains("#") {
            let pos = line.chars().position(|c| c == '}').unwrap();
            let series = &line[0..pos + 1];
            let value = line[pos + 1..].trim().parse::<f64>().unwrap();
            //// for debugging only
            // println!("seires: |{}| ==> value: |{}|", series, value);
            // sketch.insert(&SketchInput::Str(series), value as u64);
            sketch.insert(&SketchInput::Str(series), value as u64);
            baseline.push(value as u64);
        }
    }
    // sketch.debug();
    let mut avg_err = 0.0;
    let mut very_close_amount = 0;
    let mut min_err = f64::MAX;
    let mut min_err_est = 0;
    let mut min_err_truth = 0;
    let mut min_err_idx = 0;
    for i in 0..baseline.len() {
        // wow... to have the get_est inside a loop, I need a different lifetime for the get estimate function
        // that is, the partial key has life time just valid for one iteration
        // the sketch has a life time across all evaluation
        // the var created at each iteration cannot live across different iteration
        // and certainly cannot be used if the lifetime of the partial key is the same with that of sketch
        let machine_id = format!("machine_{}", i);
        let partial_key = SketchInput::Str(machine_id.as_str());
        let result = sketch.estimate_with_udf(partial_key, custom_partial_key);
        // println!("est: {} with actual: {}", result, baseline[i]);
        let err_rate = (result as f64 - baseline[i] as f64).abs() / baseline[i] as f64;
        // if result != 0 {
        //     println!("idx: {}", i);
        //     println!("error rate: {}, result: {}, exp: {}", err_rate, result, baseline[i]);
        // }
        if err_rate < 0.05 {
            very_close_amount += 1;
        }
        if err_rate < min_err {
            min_err_est = result;
            min_err_truth = baseline[i];
            min_err = err_rate;
            min_err_idx = i;
        }
        avg_err += err_rate;
    }
    avg_err = avg_err * 100.0 / baseline.len() as f64;
    println!("actually... the average error is: {:.2} %", avg_err);
    println!(
        "the very close (less than 5% difference) result count is: {} out of {}",
        very_close_amount,
        baseline.len()
    );
    println!(
        "min error occurs at idx {}, where the est is: {} and true value is: {}",
        min_err_idx, min_err_est, min_err_truth
    );

    println!("\n>>> (meaningless) sub query estimate ");
    println!(">>> test for basic correctness");
    println!(">>> i.e., the error is how sub query is, not the implementation");
    let real_sub_query = "machine_";
    let partial_key = SketchInput::Str(real_sub_query);
    let result = sketch.estimate_with_udf(partial_key, custom_partial_key);
    let truth_value_exp: u64 = baseline.iter().sum::<u64>();
    let err_rate =
        ((result as f64 - truth_value_exp as f64).abs() / truth_value_exp as f64) * 100.0;
    println!("error rate for sub query machine_ is: {:.2} %", err_rate);
    println!("query estimate: {}", result);
    println!("expected value: {}", truth_value_exp);
    Ok(())
}

pub fn gen_full_key(s: &str) -> [u8; 16] {
    let mut arr = [0u8; 16];
    let bytes = s.as_bytes();
    let len = bytes.len().min(16);
    arr[..len].copy_from_slice(&bytes[..len]);
    arr
}

pub fn is_partial() -> bool {
    true
}
