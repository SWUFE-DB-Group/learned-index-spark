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
cd learned-index-spark

# Build uber-jar (includes all dependencies except Spark)
./deploy/bin/build.sh
# or manually:
mvn clean package -DskipTests
```

Output: `target/LearnIndexSpark-1.0-SNAPSHOT.jar`

## Deployment (Spark Standalone Cluster)

### Cluster Requirements

| Component | Version |
|-----------|---------|
| Java | JDK 8 |
| Spark | 3.0.x (Standalone mode) |
| Hadoop/HDFS | 2.7+ or 3.x |
| Scala | 2.12 (bundled with Spark) |

### Step 1: Build

```bash
./deploy/bin/build.sh
```

### Step 2: Upload Data to HDFS

```bash
# Upload CSV file
./deploy/bin/upload-data.sh /path/to/points.csv

# Or specify HDFS target path
./deploy/bin/upload-data.sh /path/to/data.csv /user/hadoop/lilis-data
```

CSV format: first row as header, column 0 = x (longitude), column 1 = y (latitude), comma-separated.

### Step 3: Configure Cluster (Optional)

```bash
# Copy recommended Spark config
cp deploy/conf/spark-defaults.conf $SPARK_HOME/conf/
cp deploy/conf/log4j.properties $SPARK_HOME/conf/
```

Key parameters (via environment variables):

| Variable | Default | Description |
|----------|---------|-------------|
| `SPARK_MASTER` | `spark://master:7077` | Spark Master URL |
| `DRIVER_MEMORY` | `4g` | Driver memory |
| `EXECUTOR_MEMORY` | `8g` | Executor memory |
| `EXECUTOR_CORES` | `4` | Cores per executor |
| `NUM_EXECUTORS` | `4` | Number of executors |

### Step 4: Submit Jobs

```bash
# Run full system benchmark (generated data)
./deploy/bin/submit-benchmark.sh --generate 100000

# Run full system benchmark (HDFS data)
./deploy/bin/submit-benchmark.sh hdfs:///lilis/data/points.csv

# Run all partition strategies comparison
./deploy/bin/submit-all-benchmarks.sh --generate 50000

# Run specific partition benchmark
./deploy/bin/submit-all-benchmarks.sh --partition QuadTree hdfs:///lilis/data/points.csv

# Run custom main class
./deploy/bin/submit-query.sh benchmark.RTreeBenchmark --generate 100000
```

### Available Entry Classes

| Class | Description |
|-------|-------------|
| `SystemBenchmark` | Full system benchmark (partition + index + all queries) |
| `benchmark.BenchmarkRunner` | All partitions comparison with summary table |
| `benchmark.QuadTreeBenchmark` | QuadTree partition benchmark |
| `benchmark.KDBTreeBenchmark` | KDB-Tree partition benchmark |
| `benchmark.RTreeBenchmark` | R-Tree partition benchmark |
| `benchmark.FixGridBenchmark` | FixGrid partition benchmark |
| `benchmark.AdaptiveGridBenchmark` | AdaptiveGrid partition benchmark |
| `idnexbuild.BuildAll` | Build indexes with all partition methods |

### Monitoring

- Spark Web UI: `http://<master-ip>:8080`
- History Server: `http://<master-ip>:18080`
- Benchmark results: CSV files in working directory

For detailed deployment documentation, see [`deploy/DEPLOY.md`](deploy/DEPLOY.md).

## Project Structure

```
learned-index-spark/
├── src/main/java/
│   ├── benchmark/          # Benchmark test classes (per-partition)
│   ├── datatypes/          # Point, Rectangle
│   ├── index/              # Index building logic
│   ├── partitions/         # 5 spatial partition strategies
│   ├── query/              # Range, Point, KNN, Distance, Join queries
│   ├── spline/             # Spline index (CDF + Radix acceleration)
│   └── utils/              # HDFSPointReader, utilities
├── deploy/
│   ├── bin/                # Build & submit scripts
│   ├── conf/               # Spark configuration files
│   └── DEPLOY.md           # Detailed deployment guide
└── pom.xml
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

