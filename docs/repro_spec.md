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
## Not in main path
- src/main/java/main.java = local demo only
- src/test/java/Test.java = helper/test path only
- benchmark build path = idnexbuild/BuildAll.java
- preferred reproduce path = idnexbuild/KPIndexBuild.java

Luồng toàn dự án
Phase 0: làm cho repo chạy được, biết đường chạy nào là chính, khóa quyết định kỹ thuật.
Phase 1: chứng minh local learned index và query logic đúng.
Phase 2: kiểm soát partitioning và quality của partition.
Phase 3: kiểm chứng query engine đầy đủ.
Phase 4: đưa lên Spark cluster 2 máy.
Phase 5: evaluation và viết kết quả