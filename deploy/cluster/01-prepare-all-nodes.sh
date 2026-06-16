#!/bin/bash
# ============================================================
# Step 1: 所有节点基础环境准备
# 功能: 配置 hosts、安装 Java、创建用户、配置 SSH 免密
# 用法: 在 master 节点执行
#   chmod +x 01-prepare-all-nodes.sh
#   ./01-prepare-all-nodes.sh
# ============================================================

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/hosts.conf"

# 解析节点列表
MASTER_IP=$(grep "master$" "$SCRIPT_DIR/hosts.conf" | grep -v "^#" | awk '{print $1}')
MASTER_HOST=$(grep "master$" "$SCRIPT_DIR/hosts.conf" | grep -v "^#" | awk '{print $2}')
WORKER_IPS=($(grep "worker$" "$SCRIPT_DIR/hosts.conf" | grep -v "^#" | awk '{print $1}'))
WORKER_HOSTS=($(grep "worker$" "$SCRIPT_DIR/hosts.conf" | grep -v "^#" | awk '{print $2}'))
ALL_IPS=("$MASTER_IP" "${WORKER_IPS[@]}")
ALL_HOSTS=("$MASTER_HOST" "${WORKER_HOSTS[@]}")

echo "========================================"
echo "  LiLIS 集群环境准备"
echo "========================================"
echo "  Master: $MASTER_IP ($MASTER_HOST)"
echo "  Workers: ${WORKER_IPS[*]}"
echo "  Deploy User: $DEPLOY_USER"
echo "  Install Dir: $INSTALL_DIR"
echo "========================================"
echo ""

# ========== 1. 生成 /etc/hosts 内容 ==========
echo "[Step 1/5] 生成 hosts 文件内容..."
HOSTS_CONTENT=""
for i in "${!ALL_IPS[@]}"; do
    HOSTS_CONTENT+="${ALL_IPS[$i]}  ${ALL_HOSTS[$i]}"$'\n'
done

echo "将以下内容追加到所有节点的 /etc/hosts:"
echo "---"
echo "$HOSTS_CONTENT"
echo "---"
echo ""

# ========== 2. 在每个节点上执行准备工作 ==========
prepare_node() {
    local ip=$1
    local hostname=$2
    echo "[节点 $ip] 开始配置..."

    ssh root@$ip bash -s <<REMOTE_SCRIPT
set -e

# 设置主机名
hostnamectl set-hostname $hostname

# 追加 hosts (先清理旧条目)
for h in ${ALL_HOSTS[*]}; do
    sed -i "/\s$h\$/d" /etc/hosts
done
cat >> /etc/hosts <<'HOSTS'
$HOSTS_CONTENT
HOSTS

# 安装 Java 8
if ! java -version 2>&1 | grep -q "1.8"; then
    echo "  安装 JDK 8..."
    if command -v apt-get &>/dev/null; then
        apt-get update -qq && apt-get install -y -qq openjdk-8-jdk > /dev/null
    elif command -v yum &>/dev/null; then
        yum install -y -q java-1.8.0-openjdk-devel > /dev/null
    fi
fi
echo "  Java: \$(java -version 2>&1 | head -1)"

# 创建部署用户
if ! id $DEPLOY_USER &>/dev/null; then
    useradd -m -s /bin/bash $DEPLOY_USER
    echo "$DEPLOY_USER ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/$DEPLOY_USER
fi

# 创建安装目录
mkdir -p $INSTALL_DIR
chown $DEPLOY_USER:$DEPLOY_USER $INSTALL_DIR

# 创建数据目录
mkdir -p $HDFS_DATA_DIR/namenode $HDFS_DATA_DIR/datanode
chown -R $DEPLOY_USER:$DEPLOY_USER $HDFS_DATA_DIR

echo "  [节点 $ip] 配置完成"
REMOTE_SCRIPT
}

echo "[Step 2/5] 配置所有节点基础环境..."
for i in "${!ALL_IPS[@]}"; do
    prepare_node "${ALL_IPS[$i]}" "${ALL_HOSTS[$i]}"
done

# ========== 3. 配置 SSH 免密 ==========
echo ""
echo "[Step 3/5] 配置 $DEPLOY_USER 用户 SSH 免密登录..."

# 在 master 上为 deploy user 生成 key
ssh root@$MASTER_IP bash -s <<KEYGEN
su - $DEPLOY_USER -c '
if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa
fi
'
KEYGEN

# 获取公钥
PUB_KEY=$(ssh root@$MASTER_IP "cat /home/$DEPLOY_USER/.ssh/id_rsa.pub")

# 分发公钥到所有节点
for ip in "${ALL_IPS[@]}"; do
    ssh root@$ip bash -s <<DISTRIBUTE
su - $DEPLOY_USER -c '
mkdir -p ~/.ssh && chmod 700 ~/.ssh
echo "$PUB_KEY" >> ~/.ssh/authorized_keys
sort -u -o ~/.ssh/authorized_keys ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
'
DISTRIBUTE
    echo "  SSH key 已分发到 $ip"
done

# ========== 4. 配置 JAVA_HOME ==========
echo ""
echo "[Step 4/5] 配置环境变量..."

# 检测 Java 路径
JAVA_HOME_PATH=$(ssh root@$MASTER_IP "dirname \$(dirname \$(readlink -f \$(which java)))")

for ip in "${ALL_IPS[@]}"; do
    ssh root@$ip bash -s <<ENVSETUP
cat > /etc/profile.d/bigdata.sh <<'PROFILE'
export JAVA_HOME=$JAVA_HOME_PATH
export HADOOP_HOME=$INSTALL_DIR/hadoop
export SPARK_HOME=$INSTALL_DIR/spark
export PATH=\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$SPARK_HOME/bin:\$SPARK_HOME/sbin:\$PATH
PROFILE
source /etc/profile.d/bigdata.sh
ENVSETUP
done

# ========== 5. 验证 ==========
echo ""
echo "[Step 5/5] 验证连通性..."
for ip in "${ALL_IPS[@]}"; do
    RESULT=$(ssh root@$MASTER_IP "su - $DEPLOY_USER -c 'ssh -o StrictHostKeyChecking=no $ip hostname'" 2>/dev/null)
    echo "  $MASTER_HOST -> $ip: $RESULT"
done

echo ""
echo "========================================"
echo "  基础环境准备完成！"
echo "  下一步: ./02-install-hadoop.sh"
echo "========================================"
