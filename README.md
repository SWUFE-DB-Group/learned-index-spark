#  LiLIS: Enhancing Big Spatial Data Processing with Lightweight Distributed Learned Index
![Version](https://img.shields.io/badge/version-1.0.0-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.0+-orange)

## About

[LiLIS](https://swufe-db-group.github.io/learned-index-spark/) is a distributed learning-based spatial data indexing system. Built on Apache Spark, it combines learned-search techniques with distributed computing to deliver high-performance spatial data indexing and querying, outperforming traditional spatial indexing methods by 2–3 orders of magnitude.

See more at [here](https://swufe-db-group.github.io/learned-index-spark/).

## 🚀 Key Features

- **Distributed, Learning-Based Indexing** – Optimizes spatial queries by integrating machine learning with distributed computing
- **High Query Efficiency** – Boosts performance by 2–3 orders of magnitude over traditional methods
- **Flexible & Scalable** – Supports various spatial queries including range, point, join, and KNN queries while seamlessly handling massive datasets

## 🛠️ Quick Start

### Prerequisites

```
✓ Apache Spark 3.0+
✓ Hadoop HDFS
✓ Java 8+
✓ Scala 2.12.x
✓ Maven 4.0
```

### Installation

```bash
# Clone 
git clone git@github.com:SWUFE-DB-Group/learned-index-spark.git
cd lilis

# Build with Maven
mvn clean package
```

## 🌐 Technical Overview

### Core Indexing Structures

LiLIS integrates several spatial indexing methods:

- **R-Tree**: Balanced tree for multidimensional data
- **KD-Tree**: Binary tree optimized for point data
- **Quadtree**: Recursive spatial partitioning
- **Grid Index**: Uniform grid-based partitioning

### Spatial Query Capabilities

The system supports a range of spatial queries:

- **Range Queries**: Retrieve objects within a geographic area
- **Point Queries**: Access data at specific coordinates
- **Nearest Neighbor (NN) & KNN Queries**: Find the closest spatial objects
- **Spatial Joins**: Merge datasets based on spatial relationships

## 🧪 Performance Evaluation

### Tested Datasets
- **Chicago Crimes**: 1.9GB, 7M entries
- **NYC Taxi**: 20GB, 300M entries 
- **Synthetic Dataset**: 3GB, 100M points

### Performance Highlights

#### 1. Query Performance Summary (seconds)

| Query Type | Dataset | LiLIS | Traditional Methods |
|------------|---------|-------|---------------------|
| Range | Chicago Crimes | 0.71 | 10.88-109.96 |
| Range | NYC Taxi | 0.13-0.34 | 262.18-446.68 |
| Range | Synthetic | 0.09-0.14 | 563.82-866.84 |
| Point | Chicago Crimes | 0.61 | 11.87-57.22 |
| Point | NYC Taxi | 0.07 | 380.22-493.62 |
| Point | Synthetic | 0.11 | 579.80-827.25 |
| KNN (K=10) | Chicago Crimes | 0.74 | 6.88-7.86 |
| KNN (K=10) | NYC Taxi | 0.61 | 314.24-790.99 |
| KNN (K=10) | Synthetic | 0.14 | 49.59-83.17 |
| Join | Chicago Crimes | 228.58 | 21492.01 |

#### 2. Index Construction Time (milliseconds)

| Partition Method | Chicago Crimes | NYC Taxi | Synthetic |
|------------------|----------------|----------|-----------|
| FixGrid | 18,671 | 41,455 | 101,873 |
| AdaptiveGrid | 21,133 | 48,797 | 115,524 |
| QuadTree | 23,850 | 53,733 | 139,423 |
| KDBTree | 21,484 | 50,520 | 107,025 |
| RTree | 20,827 | 44,934 | 105,876 |

#### 3. Optimal Partition Methods by Dataset and Query Type

| Dataset | Range Query | Point Query | KNN Query |
|---------|-------------|-------------|-----------|
| Chicago Crimes | KDB-tree/Quadtree | KDB-tree | KDB-tree |
| NYC Taxi | Quadtree/R-tree | R-tree | R-tree |
| Synthetic | KDB-tree/R-tree | R-tree | R-tree |

### Temporal Extension Support

LiLIS supports temporal data with exceptional performance improvements:

| Query Type | LiLIS (sec) | NoIndex (sec) | Improvement |
|------------|-------------|---------------|-------------|
| Time Point & Space Point | 110.95 | 15,697.36 | ~141× |
| Time Range & Space Range | 100.62 | 26,845.77 | ~267× |

