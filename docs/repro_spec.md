# LiLIS Reproduction Spec

## Fixed choices
- sort_key: Y
- local_model: taut_string_spline
- epsilon: 32
- radix_bits: 10
- partitioner_default: KD_TREE
- runtime: Spark RDD
- benchmark_driver: idnexbuild/BuildAll.java

## Not in main path
- Z-order local key
- regression / sklearn
- segment-based index
- Z-quantile partitioning
- join reproduction in week 1

## Immediate goal
- compile repo successfully
- run local build path
- verify point/range/kNN correctness later