#!/bin/bash
# =============================================================
# 一键运行所有分区策略的基准测试
# 用法:
#   ./submit-all-benchmarks.sh --generate 50000
#   ./submit-all-benchmarks.sh hdfs:///data/points.csv
#
# 或者只运行单个分区:
#   ./submit-all-benchmarks.sh --partition QuadTree --generate 50000
# =============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ========== 配置区域 ==========
SPARK_MASTER="${SPARK_MASTER:-spark://master:7077}"
DRIVER_MEMORY="${DRIVER_MEMORY:-4g}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-8g}"
EXECUTOR_CORES="${EXECUTOR_CORES:-4}"
NUM_EXECUTORS="${NUM_EXECUTORS:-4}"
# ==============================

JAR_FILE=$(find "$PROJECT_DIR/target" -name "LearnIndexSpark-*.jar" ! -name "*original*" 2>/dev/null | head -1)

if [ -z "$JAR_FILE" ]; then
    echo "[ERROR] 未找到 JAR 文件，请先运行 deploy/bin/build.sh"
    exit 1
fi

echo "========================================"
echo "  LiLIS All Partitions Benchmark"
echo "========================================"
echo "  Master: $SPARK_MASTER"
echo "  Args:   $@"
echo "========================================"
echo ""

spark-submit \
    --class benchmark.BenchmarkRunner \
    --master "$SPARK_MASTER" \
    --deploy-mode client \
    --driver-memory "$DRIVER_MEMORY" \
    --executor-memory "$EXECUTOR_MEMORY" \
    --executor-cores "$EXECUTOR_CORES" \
    --total-executor-cores $((NUM_EXECUTORS * EXECUTOR_CORES)) \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryoserializer.buffer.max=256m \
    --conf spark.network.timeout=600s \
    "$JAR_FILE" \
    "$@"
