use crate::common::streams::zipf_u64;

pub const SCHEMA: [&str; 3] = ["src_region", "dst_region", "status"];
pub const REGIONS: [&str; 4] = ["eu-west", "us-east", "apac", "sa-east"];
pub const STATUSES: [&str; 3] = ["200", "404", "500"];
pub const ENDPOINTS: [&str; 4] = ["/login", "/checkout", "/query", "/asset"];

pub struct Record {
    pub key: [&'static str; 3],
    pub endpoint: &'static str,
}

pub const H2_REGIONS: [&str; 2] = ["eu-west", "us-east"];
pub const H2_SERVICES: [&str; 2] = ["auth", "cart"];

pub fn h2_keys(n: usize, seed: u64) -> Vec<(&'static str, &'static str)> {
    let regions = zipf_u64(n, H2_REGIONS.len(), 0.6, seed);
    let services = zipf_u64(n, H2_SERVICES.len(), 0.6, seed + 1);
    (0..n)
        .map(|i| {
            (
                H2_REGIONS[regions[i] as usize],
                H2_SERVICES[services[i] as usize],
            )
        })
        .collect()
}
