# PBT Notes

The problem trying to solve (by introducing PBT) is phrased as follow:

- If I want to know the sketch algorithm is implemented correctly, what can I do, beyond unit test and e2e test.

## Mental Model

It's not trying to compare some input and output is paired correctly.
It's not trying to guarantee "something is absolutely correct".

Instead, a more intuitive way to think about it is:

- if the implementation is correct, this property is supposed to be held
  - and there are generated test cases to test that

## Example

### CMS

Some property for CMS can be:

- sum of counters in each row is equal to the total "weight" of inserted items, at any time
- if there is a sequence of insert and merge operations, order of operations doesn't matter

These are properties that hard to represent in unit test or not straightforward in unit test.

### HLL

For HIP estimator:

- the estimate is an accumulator that depends on insertion order
- the estimate is only updated when a register is really changed

It's hard to track when the register really changes in unit test.

## What It Has Found

- KLL: a NaN anywhere in the stream panicked every quantile query (fixed).
- KMV: `estimate` divides by a zero-shifted k-th minimum and returns `+inf`.
- `Cdf::quantile`: the binary search stops at an arbitrary copy of a repeated
  value, so `cdf(x) * n` and `rank(x)` disagree.
- The hash-sketch ensemble: pushing a matrix member re-seeds the HyperLogLogs
  already in that layer, so one key is counted twice.

## Current Limitation

The coverage (for configurable type) is not satisfiable at this moment.

Some laws state less than they look like they do: a generator that never
reaches the regime the law is about, or an assertion loose enough that a wrong
answer still fits.
