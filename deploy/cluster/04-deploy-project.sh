#!/bin/bash
# ============================================================
# Step 4: 部署 LiLIS 项目并提交运行
# 功能: 打包项目、上传数据到 HDFS、提交 Spark 任务
# 用法:
#   ./04-deploy-project.sh                      # 使用生成数据测试
#   ./04-deploy-project.sh /path/to/points.csv  # 上传数据并运行
# ============================================================

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/hosts.conf"

PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
MASTER_IP=$(grep "master$" "$SCRIPT_DIR/hosts.conf" | grep -v "^#" | awk '{print $1}')

SPARK_MASTER="spark://master:7077"
SPARK_INSTALL="$INSTALL_DIR/spark"
HADOOP_INSTALL="$INSTALL_DIR/hadoop"

DATA_FILE=${1:-""}
HDFS_DATA_PATH="/lilis/data"

echo "========================================"
echo "  LiLIS 项目部署与运行"
echo "========================================"
echo "  项目目录: $PROJECT_DIR"
echo "  Master:   $SPARK_MASTER"
echo "  数据文件: ${DATA_FILE:-'使用生成数据'}"
echo "========================================"
echo ""

# ========== 1. 打包项目 ==========
echo "[Step 1/4] 打包项目 (mvn package)..."
cd "$PROJECT_DIR"
if command -v mvn &>/dev/null; then
    mvn package -DskipTests -q
else
    echo "  [WARN] 本地无 Maven，尝试在 master 节点打包..."
    scp -rq "$PROJECT_DIR" $DEPLOY_USER@$MASTER_IP:/tmp/lilis-build/
    ssh $DEPLOY_USER@$MASTER_IP "cd /tmp/lilis-build && mvn package -DskipTests -q"
    scp -q $DEPLOY_USER@$MASTER_IP:/tmp/lilis-build/target/LearnIndexSpark-1.0-SNAPSHOT.jar "$PROJECT_DIR/target/"
fi

JAR_FILE="$PROJECT_DIR/target/LearnIndexSpark-1.0-SNAPSHOT.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo "  [ERROR] JAR 文件不存在: $JAR_FILE"
    exit 1
fi
echo "  JAR: $JAR_FILE ($(du -h "$JAR_FILE" | cut -f1))"

# ========== 2. 上传 JAR 到 Master ==========
echo ""
echo "[Step 2/4] 上传 JAR 到 master..."
ssh $DEPLOY_USER@$MASTER_IP "mkdir -p /opt/bigdata/lilis"
scp -q "$JAR_FILE" $DEPLOY_USER@$MASTER_IP:/opt/bigdata/lilis/
echo "  已上传到 master:/opt/bigdata/lilis/"

# ========== 3. 上传数据到 HDFS (如果提供了数据文件) ==========
echo ""
if [ -n "$DATA_FILE" ] && [ -f "$DATA_FILE" ]; then
    echo "[Step 3/4] 上传数据到 HDFS..."
    FILENAME=$(basename "$DATA_FILE")
    scp -q "$DATA_FILE" $DEPLOY_USER@$MASTER_IP:/tmp/
    ssh $DEPLOY_USER@$MASTER_IP bash -s <<UPLOAD
export HADOOP_HOME=$HADOOP_INSTALL
export PATH=\$HADOOP_HOME/bin:\$PATH
hdfs dfs -mkdir -p $HDFS_DATA_PATH
hdfs dfs -put -f /tmp/$FILENAME $HDFS_DATA_PATH/
echo "  HDFS 路径: hdfs:///$HDFS_DATA_PATH/$FILENAME"
hdfs dfs -ls $HDFS_DATA_PATH/
rm -f /tmp/$FILENAME
UPLOAD
    SUBMIT_DATA="hdfs:///$HDFS_DATA_PATH/$FILENAME"
else
    echo "[Step 3/4] 未提供数据文件，将使用生成数据测试"
    SUBMIT_DATA="--generate 50000"
fi

# ========== 4. 提交任务 ==========
echo ""
echo "[Step 4/4] 提交 Spark 任务..."
ssh $DEPLOY_USER@$MASTER_IP bash -s <<SUBMIT
set -e
export JAVA_HOME=\$(dirname \$(dirname \$(readlink -f \$(which java))))
export SPARK_HOME=$SPARK_INSTALL
export HADOOP_HOME=$HADOOP_INSTALL
export PATH=\$SPARK_HOME/bin:\$HADOOP_HOME/bin:\$PATH

echo "  提交 SystemBenchmark..."
echo "  参数: $SUBMIT_DATA"
echo ""

spark-submit \\
    --class SystemBenchmark \\
    --master $SPARK_MASTER \\
    --deploy-mode client \\
    --driver-memory 4g \\
    --executor-memory 12g \\
    --executor-cores 4 \\
    --total-executor-cores 24 \\
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \\
    --conf spark.kryoserializer.buffer.max=256m \\
    --conf spark.network.timeout=600s \\
    --conf spark.eventLog.enabled=true \\
    --conf spark.eventLog.dir=hdfs:///spark-logs \\
    /opt/bigdata/lilis/LearnIndexSpark-1.0-SNAPSHOT.jar \\
    $SUBMIT_DATA

echo ""
echo "  任务执行完成！"
SUBMIT

echo ""
echo "========================================"
echo "  LiLIS 项目部署与运行完成！"
echo "  Spark Web UI: http://master:8080"
echo "  HDFS Web UI:  http://master:9870"
echo "========================================"
