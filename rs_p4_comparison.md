# SketchLib Comparison

This note extracts the practical takeaways from the SketchLib-P4 paper so we can port the useful ideas into the Rust implementation of SketchLib.

## P4 Optimizations

Below is a quick refresher on the optimizations highlighted in the P4 work and what each one tries to achieve.

### O1 — Combine Short Hash Calls

When multiple sketches only need small (for example, 16-bit) hash slices, their hash requests can be packed into a single wider hash call and then sliced. This reduces the number of hash pipeline invocations.

### O2 — Reuse Hash Calls Across Layers

Hierarchical sketches often repeat the same hash computations per layer. Caching a hash result and reusing it across layers removes redundant work.

### O3 — Avoid Sequential `if`

This is a P4-specific control-flow micro-optimization that avoids serial conditionals in the data plane pipeline. It does not translate directly to Rust.

### O4 — Update One Level Instead of Multiple Levels

Certain sketches (e.g., UnivMon variants) only need to update a single hierarchy level per event. Skipping unnecessary level updates reduces memory pressure and update latency.

### O5 — Remove Unnecessary SALU Operations

Another P4-specific change: streamline stateful ALU (SALU) usage to fit within switch hardware limits. There is no clear analogue in software implementations.

### O6 — Hash-Table plus Exact-Match for Duplicate Keys

Introduce a small duplicate-detection cache (hash table + exact match) to short-circuit costly sketch updates when the same key appears repeatedly.

## Rust SketchLib Implications

Only some of the P4 optimizations map cleanly to Rust:

- **Adoptable:** O1, O2, O4, and O6 can help the Rust version reduce CPU work and memory traffic.
- **P4-specific:** O3 and O5 rely on data-plane constraints and do not provide obvious benefits in the Rust context.

### Special Notes

- **Return Vec vs In Place:** if the updated hash function (where one hash for multiple rows) returns a Vec, there will be no speed up, but only the opposite: it's slower; hash in place with fewer hash operation can really help

## General API Considerations

- **P4 API:** The SketchLib-P4 API surfaces these optimizations so P4 users can compose efficient data-plane sketches.
- **Rust API:** We still need a comparable abstraction that lets callers opt into the adoptable optimizations without exposing hardware-specific details. Reusable hashing utilities is promising. General structure of sketches is likely feasible. Duplicate-key cache needs more consideration.
