# Float64 weighted frequency candidates

`WeightedFrequency` combines a Float64 CMS or CountSketch matrix with a bounded
candidate heap. It is independent of Planner schemas and execution runtimes.

- `FrequencyAlgorithm::Cms`: finite, nonnegative weights and minimum-row estimates.
- `FrequencyAlgorithm::CountSketch`: finite signed weights and sign-corrected
  median estimates; depth must be positive and odd.
- `FrequencyIdentity`: typed Null, Bool, Int64, finite Float64 and Utf8 tuple
  components. Float zero signs identify the same key; tuple boundaries and types
  participate in hashing.
- `update`, `topk`, `merge`: update weights, read descending estimated scores,
  and combine states with identical algorithm/width/depth/capacity. Merging leaves
  both inputs unchanged. Negative scores are ranked numerically, not by magnitude.
- `to_bytes` / `from_bytes`: versioned `ASAP-WFREQ-1` snapshots, compatible with
  the implementation extracted from ASAPPlanner. Integer-count sketch formats
  and cross-language interchange are separate contracts.

Heap capacity bounds retained candidates, not the number of distinct input keys.
Candidate retention and merging do not establish TopK membership completeness.
Callers own grouping, windows, memory budgets and any accuracy admission policy.

Tests include signed/fractional updates, type separation, median robustness,
shape rejection, overflow atomicity and pre-migration golden snapshots.
