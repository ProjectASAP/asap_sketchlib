use rand::{Rng, SeedableRng, rngs::StdRng};
use sketchlib_rust::{Nitro, SketchInput, hash_it_to_128};
use std::time::Instant;

const BIT_MASK: usize = (1_usize << 11) - 1;
const TO_SHIFT: usize = 11_usize;

fn build_input() -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(0x5eed_c0de_1234_5678);
    (0..1_000_000_000).map(|_| rng.random::<u64>()).collect()
}

#[inline(never)]
fn skip_packet_update(inputs: &Vec<u64>, sketch: &mut Vec<u32>, nitro: &mut Nitro) {
    for val in inputs {
        if nitro.to_skip > 0 {
            nitro.to_skip -= 1;
        } else {
            let hashed = hash_it_to_128(0, &SketchInput::U64(*val));
            for r in 0..5 {
                let idx = (hashed >> (TO_SHIFT * r)) as usize & BIT_MASK;
                sketch[2048 * r + idx] += 1;
            }
            nitro.draw_geometric();
        }
    }
}

#[inline(never)]
fn skip_row_update(inputs: &Vec<u64>, sketch: &mut Vec<u32>, nitro: &mut Nitro) {
    for val in inputs {
        if nitro.to_skip >= 5 {
            nitro.to_skip -= 5;
        } else {
            let hashed = hash_it_to_128(0, &SketchInput::U64(*val));
            loop {
                let r = nitro.to_skip;
                let idx = (hashed >> (TO_SHIFT * r)) as usize & BIT_MASK;
                sketch[2048 * r + idx] += 1;
                nitro.draw_geometric();
                if nitro.to_skip >= 5 - r - 1 {
                    nitro.to_skip -= 5 - r - 1;
                    break;
                } else {
                    nitro.to_skip += r + 1;
                }
            }
        }
    }
}

#[inline(never)]
fn no_skip_update(inputs: &Vec<u64>, sketch: &mut Vec<u32>) {
    for val in inputs {
        let hashed = hash_it_to_128(0, &SketchInput::U64(*val));
        for r in 0..5 {
            let idx = (hashed >> (TO_SHIFT * r)) as usize & BIT_MASK;
            sketch[2048 * r + idx] += 1;
        }
    }
}

// To compile: % cargo build --release --example nitro_example 
// To check flamegraph: % samply record ./target/release/examples/nitro_example
// To perf: $ sudo perf stat -e cycles,instructions,branches,branch-misses ./target/release/examples/nitro_example
pub fn main() {
    let value_to_insert = build_input();
    let mut nitro_row = Nitro::init_nitro(0.01);
    let mut nitro_pkt = Nitro::init_nitro(0.01);
    nitro_row.draw_geometric();
    nitro_pkt.draw_geometric();

    // imitate the fast update logic for cms
    let mut sketch_row = vec![0; 5 * 2048];
    let mut sketch_pkt = vec![0; 5 * 2048];
    let mut sketch_no_skip = vec![0; 5 * 2048];

    // skip packet for update

    let start1 = Instant::now();
    skip_row_update(&value_to_insert, &mut sketch_row, &mut nitro_row);
    let duration1 = start1.elapsed();
    let time_us1 = duration1.as_micros();
    println!("time for skip row: {time_us1}us");

    let start2 = Instant::now();
    skip_packet_update(&value_to_insert, &mut sketch_pkt, &mut nitro_pkt);
    let duration2 = start2.elapsed();
    let time_us2 = duration2.as_micros();
    println!("time for skip packet: {time_us2}us");

    let start3 = Instant::now();
    no_skip_update(&value_to_insert, &mut sketch_no_skip);
    let duration3 = start3.elapsed();
    let time_us3 = duration3.as_micros();
    println!("time for no skip: {time_us3}us");
}
