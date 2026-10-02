# Signed DDSketch

`DDSketch` retains positive samples, negative samples, and zeros. The design follows [DataDog DDSketch](https://github.com/DataDog/sketches-go/blob/master/ddsketch/ddsketch.go): separate positive and negative magnitude stores, plus a zero count. Bucket indices can be negative in either store; an index's sign does not encode an observation's sign.

`try_add` returns an error for NaN, infinities, or magnitudes above the mapping's maximum. `add` ignores those errors. Magnitudes below the mapping's minimum are counted as zero, including subnormal inputs. Such nonzero tiny observations are outside the relative-error guarantee. Original sample values are still compressed into buckets, not retained individually.

Quantiles use the ceil(q*n) order-statistic convention and exact ingested extrema. For indexable nonzero order statistics the estimate has absolute error at most `alpha * abs(value)`, for either sign.

## Wire and OctoSketch

- ASAPv1: positive-only states use metadata version 1. States with negative samples or zeros use version 2, which appends negative counts, negative offset, and zero count.
- OctoSketch: `DdDelta` carries a `store: DdStore` discriminator, and worker residual keys are `(DdStore, i32)`. Promotion and flush preserve all three categories; partial counts need flushing before complete queries.
