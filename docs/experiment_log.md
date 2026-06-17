# Experiment Protocol

## Main execution paths
- Main reproduce path: idnexbuild.KPIndexBuild
- Build benchmark orchestrator: idnexbuild.BuildAll
- Local demo path: main.java
- Helper/test path: src/test/java/Test.java

## Build benchmark input
- args[0]: dataset label
- args[1]: dataset CSV path

## Build benchmark output
- output file: results/<label>_build_results.txt
- each run must record:
  - label
  - driver
  - partitioner
  - input path
  - build_time_ms
  - timestamp

## Phase 1 correctness output
- output file: results/correctness_phase1.txt
- metrics:
  - point query passed / total
  - range query passed / total
  - knn query passed / total
  - fallback ratio

## Naming convention
- smoke dataset: smoke
- subset dataset: chi_subset / syn_subset
- full dataset: chi_full / syn_full