#!/bin/bash
# =============================================================
# LiLIS 通用任务提交脚本
# 用法:
#   ./submit-query.sh <MainClass> [args...]
#
# 示例:
#   ./submit-query.sh main
#   ./submit-query.sh idnexbuild.BuildAll /data/points.csv
#   ./submit-query.sh SystemBenchmark hdfs:///data/points.csv /output/report.txt
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

if [ $# -lt 1 ]; then
    echo "用法: $0 <MainClass> [args...]"
    echo ""
    echo "可用的 MainClass:"
    echo "  SystemBenchmark              - 系统基准测试 (完整流程)"
    echo "  main                         - 原始入口 (本地 JSON 数据)"
    echo "  idnexbuild.BuildAll          - 全部分区策略索引构建"
    echo "  idnexbuild.QPIndexBuild      - QuadTree 分区 + 索引"
    echo "  idnexbuild.KPIndexBuild      - KDB-Tree 分区 + 索引"
    echo "  idnexbuild.RPIndexBuild      - R-Tree 分区 + 索引"
    echo "  idnexbuild.FPIndexBuild      - FixGrid 分区 + 索引"
    echo "  idnexbuild.APIndexBuild      - AdaptiveGrid 分区 + 索引"
    exit 1
fi

MAIN_CLASS="$1"
shift

JAR_FILE=$(find "$PROJECT_DIR/target" -name "LearnIndexSpark-*.jar" ! -name "*original*" 2>/dev/null | head -1)

if [ -z "$JAR_FILE" ]; then
    echo "[ERROR] 未找到 JAR 文件，请先运行 deploy/bin/build.sh"
    exit 1
fi

echo "提交任务: $MAIN_CLASS"
echo "  Master: $SPARK_MASTER"
echo "  Args:   $@"
echo ""

spark-submit \
    --class "$MAIN_CLASS" \
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
