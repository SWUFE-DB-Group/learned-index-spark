# Paper Mapping

## What has been reproduced so far
1. Spark-based runnable environment
2. Build benchmark path through BuildAll.java
3. Default LiLIS-K path through KPIndexBuild.java
4. Local learned index flow:
   - sort by Y
   - taut-string spline
   - radix table
   - epsilon = 32
   - radix bits = 10

## Mapping to paper
- Section III: LiLIS Design
- Section V.A.2: Baselines and Implementations
- RQ5: Index Cost
- Partial RQ2: Varying Partitioners

## Not reproduced yet
- Point query correctness
- Range query correctness
- kNN correctness
- Sedona-N baseline
- Distributed cluster evaluation
- RQ1, RQ3, RQ4