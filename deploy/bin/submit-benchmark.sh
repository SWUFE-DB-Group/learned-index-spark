#!/bin/bash
# =============================================================
# LiLIS SystemBenchmark 提交脚本
# 用法:
#   ./submit-benchmark.sh                          # 使用生成数据 (50000点)
#   ./submit-benchmark.sh --generate 100000        # 使用生成数据 (自定义数量)
#   ./submit-benchmark.sh hdfs:///data/points.csv  # 使用 HDFS 数据
# =============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ========== 配置区域 (根据集群实际情况修改) ==========
SPARK_MASTER="${SPARK_MASTER:-spark://master:7077}"
DRIVER_MEMORY="${DRIVER_MEMORY:-4g}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-8g}"
EXECUTOR_CORES="${EXECUTOR_CORES:-4}"
NUM_EXECUTORS="${NUM_EXECUTORS:-4}"
# ====================================================

JAR_FILE=$(find "$PROJECT_DIR/target" -name "LearnIndexSpark-*.jar" ! -name "*original*" 2>/dev/null | head -1)

if [ -z "$JAR_FILE" ]; then
    echo "[ERROR] 未找到 JAR 文件，请先运行 deploy/bin/build.sh"
    exit 1
fi

echo "========================================"
echo "  LiLIS SystemBenchmark Submit"
echo "========================================"
echo "  Master:       $SPARK_MASTER"
echo "  JAR:          $JAR_FILE"
echo "  Driver Mem:   $DRIVER_MEMORY"
echo "  Executor Mem: $EXECUTOR_MEMORY"
echo "  Executor Cores: $EXECUTOR_CORES"
echo "  Num Executors:  $NUM_EXECUTORS"
echo "  Args:         $@"
echo "========================================"
echo ""

spark-submit \
    --class SystemBenchmark \
    --master "$SPARK_MASTER" \
    --deploy-mode client \
    --driver-memory "$DRIVER_MEMORY" \
    --executor-memory "$EXECUTOR_MEMORY" \
    --executor-cores "$EXECUTOR_CORES" \
    --total-executor-cores $((NUM_EXECUTORS * EXECUTOR_CORES)) \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryoserializer.buffer.max=256m \
    --conf spark.network.timeout=600s \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs:///spark-logs \
    "$JAR_FILE" \
    "$@"
