# Phase 1 Correctness Contract

## Goal
Verify that the current implementation returns correct results before large-scale benchmarking.

This phase checks correctness first, not performance tuning. Benchmark numbers only have meaning after all correctness checks in this contract pass.

## Dataset levels
- `smoke_points.csv`: fast debug loop for repeated local runs.
- `edge_points.csv`: adversarial cases that expose logic bugs.
- `chi_subset.csv` or `syn_subset.csv`: larger subset for near-realistic behavior and index usage.

## Query correctness definitions

### Point Query
Input: query point `(x, y)`.

Expected output:
- boolean flow: true iff an exact point exists
- list-returning flow: non-empty result iff an exact point exists; current repo returns at most one exact matching point

Oracle:
- brute-force linear scan over the full dataset.

### Range Query
Input: rectangle `(xl, yl, xh, yh)`.

Expected output:
- all points satisfying `xl <= x <= xh` and `yl <= y <= yh`.

Oracle:
- brute-force rectangle filter over the full dataset.

### kNN Query
Input: query point + `k`.

Expected output:
- exact top-`k` nearest points by Euclidean distance.

Oracle:
- brute-force full scan + sort by Euclidean distance.

## Local index contract

### Fallback path
If partition size `<= 100`:
- `linear_scan = true`
- spline/radix are not required
- correctness still must match brute-force

### Learned-index path
If partition size `> 100`:
- points sorted by `Y`
- spline built
- radix built
- `isBuild = true`

## Phase 1 test scope and intent
- Prove correctness behavior, not runtime speed.
- Validate local partition behavior before large Spark benchmarks.
- Ensure benchmark phase is gated by correctness pass.

## Paper mapping
- Section III.B: learned spatial index inside partition.
- Section IV.A/B/C: point, range, and kNN correctness semantics.

## Exit criteria for Phase 1
- Contract tests pass on smoke + edge + subset datasets.
- For each query type, actual output is parity-equal with brute-force oracle.
- Local index contract checks pass for both fallback and built modes.
