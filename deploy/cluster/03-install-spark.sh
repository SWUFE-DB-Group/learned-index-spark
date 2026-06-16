#!/bin/bash
# ============================================================
# Step 3: 部署 Spark Standalone 集群
# 功能: 下载 Spark、分发配置、启动 Master + Workers
# 用法: 在 master 节点执行
#   ./03-install-spark.sh
# ============================================================

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/hosts.conf"

MASTER_IP=$(grep "master$" "$SCRIPT_DIR/hosts.conf" | grep -v "^#" | awk '{print $1}')
WORKER_IPS=($(grep "worker$" "$SCRIPT_DIR/hosts.conf" | grep -v "^#" | awk '{print $1}'))
ALL_IPS=("$MASTER_IP" "${WORKER_IPS[@]}")

SPARK_TAR="spark-${SPARK_VERSION}-bin-hadoop3.2.tgz"
SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TAR}"
SPARK_INSTALL="$INSTALL_DIR/spark"

echo "========================================"
echo "  Spark Standalone 集群部署"
echo "  Spark $SPARK_VERSION"
echo "========================================"
echo "  Master: master ($MASTER_IP) :7077"
echo "  Workers: ${#WORKER_IPS[@]} 个节点"
echo "  Worker Memory: $SPARK_WORKER_MEMORY"
echo "  Worker Cores: $SPARK_WORKER_CORES"
echo "========================================"
echo ""

# ========== 1. 下载 Spark ==========
echo "[Step 1/4] 下载 Spark..."
if [ ! -f "/tmp/$SPARK_TAR" ]; then
    echo "  下载 $SPARK_URL ..."
    wget -q -P /tmp "$SPARK_URL"
else
    echo "  已存在 /tmp/$SPARK_TAR，跳过下载"
fi

# ========== 2. 分发并解压 ==========
echo ""
echo "[Step 2/4] 分发 Spark 到所有节点..."
for ip in "${ALL_IPS[@]}"; do
    echo "  分发到 $ip ..."
    scp -q /tmp/$SPARK_TAR $DEPLOY_USER@$ip:/tmp/
    ssh $DEPLOY_USER@$ip bash -s <<INSTALL
set -e
sudo rm -rf $SPARK_INSTALL
sudo mkdir -p $INSTALL_DIR
sudo tar -xzf /tmp/$SPARK_TAR -C $INSTALL_DIR
sudo mv $INSTALL_DIR/spark-${SPARK_VERSION}-bin-hadoop3.2 $SPARK_INSTALL
sudo chown -R $DEPLOY_USER:$DEPLOY_USER $SPARK_INSTALL
mkdir -p $SPARK_INSTALL/logs $SPARK_INSTALL/pids
rm -f /tmp/$SPARK_TAR
INSTALL
done

# ========== 3. 分发配置文件 ==========
echo ""
echo "[Step 3/4] 分发 Spark 配置..."
for ip in "${ALL_IPS[@]}"; do
    echo "  配置 $ip ..."
    scp -q "$SCRIPT_DIR/conf/spark/spark-env.sh" $DEPLOY_USER@$ip:$SPARK_INSTALL/conf/
    scp -q "$SCRIPT_DIR/conf/spark/workers" $DEPLOY_USER@$ip:$SPARK_INSTALL/conf/
    scp -q "$SCRIPT_DIR/conf/spark/spark-defaults.conf" $DEPLOY_USER@$ip:$SPARK_INSTALL/conf/

    # 更新 JAVA_HOME
    ssh $DEPLOY_USER@$ip bash -s <<FIXJAVA
JAVA_PATH=\$(dirname \$(dirname \$(readlink -f \$(which java))))
sed -i "s|^export JAVA_HOME=.*|export JAVA_HOME=\$JAVA_PATH|" $SPARK_INSTALL/conf/spark-env.sh
FIXJAVA
done

# ========== 4. 启动 Spark 集群 ==========
echo ""
echo "[Step 4/4] 启动 Spark 集群..."
ssh $DEPLOY_USER@$MASTER_IP bash -s <<START
set -e
export JAVA_HOME=\$(dirname \$(dirname \$(readlink -f \$(which java))))
export SPARK_HOME=$SPARK_INSTALL
export HADOOP_HOME=$INSTALL_DIR/hadoop
export PATH=\$SPARK_HOME/bin:\$SPARK_HOME/sbin:\$PATH

# 停止旧进程
\$SPARK_HOME/sbin/stop-all.sh 2>/dev/null || true
sleep 2

# 启动 Master
\$SPARK_HOME/sbin/start-master.sh
sleep 3

# 启动所有 Workers
\$SPARK_HOME/sbin/start-workers.sh

echo ""
echo "  Spark 进程:"
jps | grep -E "Master|Worker"

# 等待 workers 注册
sleep 5
echo ""
echo "  集群状态 (通过 Web UI 查看更多详情):"
echo "  Master UI: http://master:8080"
START

echo ""
echo "========================================"
echo "  Spark 集群部署完成！"
echo "  Master:    spark://master:7077"
echo "  Web UI:    http://master:8080"
echo "  Workers:   ${#WORKER_IPS[@]} 个"
echo "  验证: spark-submit --master spark://master:7077 --version"
echo "  下一步: ./04-deploy-project.sh"
echo "========================================"
