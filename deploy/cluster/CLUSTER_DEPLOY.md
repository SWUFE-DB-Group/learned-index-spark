# LiLIS 集群完整部署指南 (1 Master + 6 Workers)

## 集群架构

```
┌────────────────────────────────────────────────────────────────┐
│                        Master 节点                              │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐           │
│  │ Spark Master│  │  NameNode   │  │ History Server│           │
│  │   :7077     │  │   :9000     │  │   :18080     │           │
│  └─────────────┘  └─────────────┘  └──────────────┘           │
└────────────────────────────────────────────────────────────────┘
         │                    │
         ▼                    ▼
┌─────────────┐  ┌─────────────┐       ┌─────────────┐
│  Worker 1   │  │  Worker 2   │  ...  │  Worker 6   │
│ Spark Worker│  │ Spark Worker│       │ Spark Worker│
│  DataNode   │  │  DataNode   │       │  DataNode   │
│ 16G / 8cores│  │ 16G / 8cores│       │ 16G / 8cores│
└─────────────┘  └─────────────┘       └─────────────┘
```

## 环境要求

| 组件 | 版本 | 说明 |
|------|------|------|
| OS | Ubuntu 18.04+ / CentOS 7+ | 所有节点 |
| Java | JDK 8 | 所有节点 |
| Hadoop | 3.3.6 | HDFS 分布式存储 |
| Spark | 3.0.3 | Standalone 模式 |
| Maven | 3.6+ | 仅打包机器 |

## 前置条件

1. 所有 7 台服务器之间网络互通
2. 有一台机器可以通过 SSH root 访问所有节点
3. 所有节点可以访问外网（下载软件包）或提前准备好离线安装包

## 快速部署步骤

### Step 0: 修改配置

编辑 `hosts.conf`，将占位 IP 替换为你的实际 IP：

```bash
vi deploy/cluster/hosts.conf
```

同时根据服务器硬件调整以下参数：
- `SPARK_WORKER_MEMORY` — 每个 Worker 分配的内存（建议为物理内存的 70-80%）
- `SPARK_WORKER_CORES` — 每个 Worker 使用的 CPU 核心数

### Step 1: 基础环境准备

```bash
chmod +x deploy/cluster/*.sh
./deploy/cluster/01-prepare-all-nodes.sh
```

此脚本完成：
- 配置所有节点的 `/etc/hosts`
- 安装 JDK 8
- 创建 hadoop 用户
- 配置 SSH 免密登录
- 设置环境变量

### Step 2: 部署 HDFS

```bash
./deploy/cluster/02-install-hadoop.sh
```

此脚本完成：
- 下载并分发 Hadoop 到所有节点
- 分发配置文件（core-site.xml, hdfs-site.xml, workers）
- 格式化 NameNode
- 启动 HDFS（NameNode + 6 个 DataNode）
- 创建 `/spark-logs` 和 `/lilis/data` 目录

验证：
```bash
# 查看 HDFS 状态
hdfs dfs -ls /
# Web UI
浏览器访问 http://master:9870
```

### Step 3: 部署 Spark

```bash
./deploy/cluster/03-install-spark.sh
```

此脚本完成：
- 下载并分发 Spark 到所有节点
- 配置 spark-env.sh、workers、spark-defaults.conf
- 启动 Spark Master + 6 个 Worker

验证：
```bash
# 查看进程
jps  # 应看到 Master
# Web UI
浏览器访问 http://master:8080
# 应显示 6 个 Worker, 96G 内存, 48 Cores
```

### Step 4: 部署项目并运行

```bash
# 使用生成数据测试
./deploy/cluster/04-deploy-project.sh

# 使用真实数据（CSV 文件）
./deploy/cluster/04-deploy-project.sh /path/to/your/points.csv
```

此脚本完成：
- Maven 打包项目为 uber-jar
- 上传 jar 到 master 节点
- 上传数据到 HDFS（如提供）
- 提交 Spark 任务运行 SystemBenchmark

## 手动操作参考

### 集群管理命令

```bash
# --- HDFS ---
# 启动 HDFS
$HADOOP_HOME/sbin/start-dfs.sh
# 停止 HDFS
$HADOOP_HOME/sbin/stop-dfs.sh
# 查看 HDFS 报告
hdfs dfsadmin -report

# --- Spark ---
# 启动 Spark 集群
$SPARK_HOME/sbin/start-all.sh
# 停止 Spark 集群
$SPARK_HOME/sbin/stop-all.sh
# 单独启动 Master
$SPARK_HOME/sbin/start-master.sh
# 单独启动所有 Workers
$SPARK_HOME/sbin/start-workers.sh
```

### 手动提交任务

```bash
# 基准测试 (全部分区策略)
spark-submit \
    --class benchmark.BenchmarkRunner \
    --master spark://master:7077 \
    --driver-memory 4g \
    --executor-memory 12g \
    --executor-cores 4 \
    --total-executor-cores 24 \
    /opt/bigdata/lilis/LearnIndexSpark-1.0-SNAPSHOT.jar \
    hdfs:///lilis/data/points.csv

# 单独运行某个分区策略
spark-submit \
    --class benchmark.QuadTreeBenchmark \
    --master spark://master:7077 \
    --driver-memory 4g \
    --executor-memory 12g \
    /opt/bigdata/lilis/LearnIndexSpark-1.0-SNAPSHOT.jar \
    hdfs:///lilis/data/points.csv

# 使用生成数据
spark-submit \
    --class SystemBenchmark \
    --master spark://master:7077 \
    --driver-memory 4g \
    --executor-memory 12g \
    /opt/bigdata/lilis/LearnIndexSpark-1.0-SNAPSHOT.jar \
    --generate 1000000
```

### 数据管理

```bash
# 上传数据到 HDFS
hdfs dfs -put /local/path/points.csv /lilis/data/

# 查看已上传的数据
hdfs dfs -ls /lilis/data/

# 查看数据前几行
hdfs dfs -head /lilis/data/points.csv

# 查看 HDFS 使用情况
hdfs dfs -du -h /lilis/
```

## 资源配置建议

根据服务器配置调整 `hosts.conf` 和 `conf/spark/spark-env.sh`：

| 服务器内存 | SPARK_WORKER_MEMORY | EXECUTOR_MEMORY | 说明 |
|-----------|--------------------|-----------------| -----|
| 16 GB | 12g | 10g | 预留 4G 给系统和 DataNode |
| 32 GB | 24g | 20g | 预留 8G |
| 64 GB | 48g | 40g | 预留 16G |

| 服务器 CPU | SPARK_WORKER_CORES | EXECUTOR_CORES | 说明 |
|-----------|--------------------|-----------------| -----|
| 4 cores | 3 | 3 | 预留 1 core 给系统 |
| 8 cores | 6 | 4 | 可运行多个 Executor |
| 16 cores | 12 | 4 | 每节点 3 个 Executor |

## Web UI 地址汇总

| 服务 | 地址 | 说明 |
|------|------|------|
| Spark Master | http://master:8080 | 集群状态、Worker 列表 |
| Spark History | http://master:18080 | 历史任务查看 |
| HDFS NameNode | http://master:9870 | 文件系统状态 |
| Spark Job UI | http://master:4040 | 运行中任务详情（任务运行时可用）|

## 常见问题

### Q: Worker 未注册到 Master
1. 检查防火墙: `sudo ufw status` 或 `sudo firewall-cmd --list-all`
2. 关闭防火墙或开放端口: 7077, 8080, 9000, 9870
3. 确认 `/etc/hosts` 中主机名解析正确

### Q: HDFS DataNode 未启动
1. 检查日志: `$HADOOP_HOME/logs/hadoop-*-datanode-*.log`
2. 确认数据目录权限: `ls -la /data/hdfs/datanode`
3. 如果重新格式化 NameNode，需要清除所有 DataNode 数据目录

### Q: spark-submit 报 Connection refused
1. 确认 Master 正在运行: `jps | grep Master`
2. 确认连接地址正确: `spark://master:7077`
3. 检查 master 节点是否有 7077 端口监听: `ss -tlnp | grep 7077`

### Q: 任务 OOM (Out of Memory)
1. 增大 executor 内存: `--executor-memory 16g`
2. 减少数据量验证
3. 检查是否存在数据倾斜（某些 partition 数据过多）

### Q: 离线安装（无外网）
提前在有网络的机器下载：
```bash
# Hadoop
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
# Spark
wget https://archive.apache.org/dist/spark/spark-3.0.3/spark-3.0.3-bin-hadoop3.2.tgz
```
然后将文件放到 `/tmp/` 目录下，脚本会自动跳过下载。
