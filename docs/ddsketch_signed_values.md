# Signed DDSketch

`DdSketch` retains positive samples, negative samples, and zeros. The design follows [DataDog DDSketch](https://github.com/DataDog/sketches-go/blob/master/ddsketch/ddsketch.go): separate positive and negative magnitude stores, plus a zero count. Bucket indices can be negative in either store; an index's sign does not encode an observation's sign.

`try_add` returns errors for NaN, infinities, or magnitudes above the mapping's maximum. `add` ignores those errors. Magnitudes below the mapping's minimum are counted as zero, including subnormal inputs. Such nonzero tiny observations are outside the relative-error guarantee. Original sample values are still compressed into buckets, not retained individually.

`DdSketch` answers the ceil(q*n) order statistic from its buckets, the extremes included; it keeps no ingested sum or extrema. For indexable nonzero order statistics the estimate has absolute error at most `alpha * abs(value)`, for either sign.

If interpolation combines endpoints x and y with weight t, the absolute error is bounded by `alpha * ((1-t)*abs(x) + t*abs(y))`, excluding zero-mapped tiny values. Opposite signs can cancel, so the interpolated result need not have relative error alpha. Dividing quantiles additionally requires a finite nonzero true denominator and valid relative-error bounds for both operands. This change does not establish those planner preconditions.

## Wire and downstream upgrade

- Native ASAPv1: every state is metadata version 1 with the five-field payload of kind `0x05 0x01` (both stores and the zero count); the retired eight-field `0x05 0x00` still decodes. `DdSketchDelta` (`0x05 0x81`) carries both stores and a zero-count increment.
- Octo: `DdSketchBucketDelta` now requires a `sign: DdSign` discriminator. Existing positive delta constructors should use `DdSign::Positive`. Worker residual keys are `(DdSign, i32)`. Both promotion and flush preserve all three categories; partial counts still need flushing before complete queries.

Go implementations of the ASAP wire formats and downstream dependency pins are not changed by this Rust library patch.
