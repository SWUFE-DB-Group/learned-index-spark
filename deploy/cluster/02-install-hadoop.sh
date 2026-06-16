#!/bin/bash
# ============================================================
# Step 2: 部署 HDFS (Hadoop Distributed File System)
# 功能: 下载 Hadoop、分发配置、格式化 NameNode、启动 HDFS
# 用法: 在 master 节点以 root 或 DEPLOY_USER 执行
#   ./02-install-hadoop.sh
# ============================================================

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/hosts.conf"

MASTER_IP=$(grep "master$" "$SCRIPT_DIR/hosts.conf" | grep -v "^#" | awk '{print $1}')
WORKER_IPS=($(grep "worker$" "$SCRIPT_DIR/hosts.conf" | grep -v "^#" | awk '{print $1}'))
WORKER_HOSTS=($(grep "worker$" "$SCRIPT_DIR/hosts.conf" | grep -v "^#" | awk '{print $2}'))
ALL_IPS=("$MASTER_IP" "${WORKER_IPS[@]}")

HADOOP_TAR="hadoop-${HADOOP_VERSION}.tar.gz"
HADOOP_URL="https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_TAR}"
HADOOP_INSTALL="$INSTALL_DIR/hadoop"

echo "========================================"
echo "  HDFS 部署 (Hadoop $HADOOP_VERSION)"
echo "========================================"
echo "  NameNode: master ($MASTER_IP)"
echo "  DataNodes: ${WORKER_HOSTS[*]}"
echo "  安装目录: $HADOOP_INSTALL"
echo "  数据目录: $HDFS_DATA_DIR"
echo "========================================"
echo ""

# ========== 1. 下载 Hadoop ==========
echo "[Step 1/5] 下载 Hadoop..."
if [ ! -f "/tmp/$HADOOP_TAR" ]; then
    echo "  下载 $HADOOP_URL ..."
    wget -q -P /tmp "$HADOOP_URL"
else
    echo "  已存在 /tmp/$HADOOP_TAR，跳过下载"
fi

# ========== 2. 分发并解压到所有节点 ==========
echo ""
echo "[Step 2/5] 分发 Hadoop 到所有节点..."
for ip in "${ALL_IPS[@]}"; do
    echo "  分发到 $ip ..."
    scp -q /tmp/$HADOOP_TAR $DEPLOY_USER@$ip:/tmp/
    ssh $DEPLOY_USER@$ip bash -s <<INSTALL
set -e
sudo rm -rf $HADOOP_INSTALL
sudo mkdir -p $INSTALL_DIR
sudo tar -xzf /tmp/$HADOOP_TAR -C $INSTALL_DIR
sudo mv $INSTALL_DIR/hadoop-${HADOOP_VERSION} $HADOOP_INSTALL
sudo chown -R $DEPLOY_USER:$DEPLOY_USER $HADOOP_INSTALL
rm -f /tmp/$HADOOP_TAR
INSTALL
done

# ========== 3. 分发配置文件 ==========
echo ""
echo "[Step 3/5] 分发 Hadoop 配置文件..."
for ip in "${ALL_IPS[@]}"; do
    echo "  配置 $ip ..."
    scp -q "$SCRIPT_DIR/conf/hadoop/core-site.xml" $DEPLOY_USER@$ip:$HADOOP_INSTALL/etc/hadoop/
    scp -q "$SCRIPT_DIR/conf/hadoop/hdfs-site.xml" $DEPLOY_USER@$ip:$HADOOP_INSTALL/etc/hadoop/
    scp -q "$SCRIPT_DIR/conf/hadoop/workers" $DEPLOY_USER@$ip:$HADOOP_INSTALL/etc/hadoop/

    # 设置 JAVA_HOME in hadoop-env.sh
    ssh $DEPLOY_USER@$ip bash -s <<JAVAHOME
JAVA_PATH=\$(dirname \$(dirname \$(readlink -f \$(which java))))
sed -i "s|^# export JAVA_HOME=.*|export JAVA_HOME=\$JAVA_PATH|" $HADOOP_INSTALL/etc/hadoop/hadoop-env.sh
grep -q "^export JAVA_HOME" $HADOOP_INSTALL/etc/hadoop/hadoop-env.sh || echo "export JAVA_HOME=\$JAVA_PATH" >> $HADOOP_INSTALL/etc/hadoop/hadoop-env.sh
JAVAHOME
done

# ========== 4. 格式化 NameNode ==========
echo ""
echo "[Step 4/5] 格式化 NameNode..."
ssh $DEPLOY_USER@$MASTER_IP bash -s <<FORMAT
set -e
export JAVA_HOME=\$(dirname \$(dirname \$(readlink -f \$(which java))))
# 清理旧数据
rm -rf $HDFS_DATA_DIR/namenode/current
$HADOOP_INSTALL/bin/hdfs namenode -format -force -nonInteractive 2>&1 | tail -5
echo "  NameNode 格式化完成"
FORMAT

# ========== 5. 启动 HDFS ==========
echo ""
echo "[Step 5/5] 启动 HDFS..."
ssh $DEPLOY_USER@$MASTER_IP bash -s <<START
set -e
export JAVA_HOME=\$(dirname \$(dirname \$(readlink -f \$(which java))))
export HADOOP_HOME=$HADOOP_INSTALL
export PATH=\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PATH

# 停止旧进程 (如果有)
stop-dfs.sh 2>/dev/null || true
sleep 2

# 启动 HDFS
start-dfs.sh

echo ""
echo "  HDFS 进程:"
jps | grep -E "NameNode|DataNode|SecondaryNameNode"

# 创建常用目录
hdfs dfs -mkdir -p /spark-logs
hdfs dfs -mkdir -p /lilis/data
hdfs dfs -chmod 777 /spark-logs
hdfs dfs -chmod 777 /lilis

echo ""
echo "  HDFS 目录已创建: /spark-logs, /lilis/data"
START

echo ""
echo "========================================"
echo "  HDFS 部署完成！"
echo "  NameNode Web UI: http://master:9870"
echo "  验证: hdfs dfs -ls /"
echo "  下一步: ./03-install-spark.sh"
echo "========================================"
