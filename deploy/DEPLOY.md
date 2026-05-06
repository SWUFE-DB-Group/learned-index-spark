# LiLIS 集群部署指南

## 环境要求

| 组件 | 版本要求 | 说明 |
|------|---------|------|
| Java | JDK 8 | 所有节点 |
| Spark | 3.0.x | Standalone 模式 |
| Hadoop/HDFS | 2.7+ 或 3.x | 数据存储 |
| Maven | 3.6+ | 仅打包机器需要 |
| Scala | 2.12 | Spark 自带 |

## 集群架构

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Master    │     │  Worker-1   │     │  Worker-N   │
│  Spark UI   │────▶│  Executor   │ ... │  Executor   │
│  Port 7077  │     │  8G RAM     │     │  8G RAM     │
└─────────────┘     └─────────────┘     └─────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────┐
│                    HDFS 集群                              │
│              存储空间数据 (CSV/JSON)                       │
└─────────────────────────────────────────────────────────┘
```

## 快速开始

### 1. 打包

```bash
# 在开发机上执行
cd /path/to/learned-index-spark
./deploy/bin/build.sh
```

生成的 JAR 文件: `target/LearnIndexSpark-1.0-SNAPSHOT.jar`

### 2. 上传数据到 HDFS

```bash
# 上传 CSV 数据文件
./deploy/bin/upload-data.sh /path/to/your/points.csv

# 或指定 HDFS 目标路径
./deploy/bin/upload-data.sh /path/to/data.csv /user/hadoop/lilis-data
```

CSV 格式要求:
- 默认第一行为表头
- 默认第 0 列为 x 坐标 (经度)，第 1 列为 y 坐标 (纬度)
- 逗号分隔

### 3. 提交任务

```bash
# 运行 SystemBenchmark (使用 HDFS 数据)
./deploy/bin/submit-benchmark.sh hdfs:///lilis/data/points.csv

# 运行 SystemBenchmark (生成测试数据)
./deploy/bin/submit-benchmark.sh --generate 100000

# 运行其他任务入口
./deploy/bin/submit-query.sh idnexbuild.BuildAll /data/points.csv
```

### 4. 查看结果

- Spark Web UI: `http://<master-ip>:8080`
- History Server: `http://<master-ip>:18080`
- 报告文件: 默认输出到 driver 工作目录的 `benchmark_result_*.txt`

## 详细配置

### 集群参数调优

通过环境变量覆盖默认配置:

```bash
# 调整资源分配
export SPARK_MASTER="spark://your-master:7077"
export DRIVER_MEMORY="8g"
export EXECUTOR_MEMORY="16g"
export EXECUTOR_CORES="8"
export NUM_EXECUTORS="6"

./deploy/bin/submit-benchmark.sh hdfs:///data/points.csv
```

### Spark 配置文件

将 `deploy/conf/spark-defaults.conf` 复制到集群:

```bash
cp deploy/conf/spark-defaults.conf $SPARK_HOME/conf/
cp deploy/conf/log4j.properties $SPARK_HOME/conf/
```

或在提交时指定:

```bash
spark-submit --properties-file deploy/conf/spark-defaults.conf ...
```

### 推荐资源配置

| 集群规模 | Worker 数 | Executor Memory | Executor Cores | 适合数据量 |
|---------|-----------|-----------------|----------------|-----------|
| 小型 | 2~3 | 4g | 2 | < 100万点 |
| 中型 | 4~6 | 8g | 4 | 100万~1000万点 |
| 大型 | 8~10 | 16g | 8 | > 1000万点 |

### 关键 Spark 参数说明

| 参数 | 说明 | LiLIS 推荐值 |
|------|------|-------------|
| `spark.serializer` | 序列化器 | KryoSerializer (比默认快 10x) |
| `spark.default.parallelism` | 默认并行度 | 2~3 倍 CPU 总核数 |
| `spark.memory.fraction` | 执行+存储内存占比 | 0.8 |
| `spark.memory.storageFraction` | 存储占比 | 0.3 (索引构建后需缓存) |
| `spark.network.timeout` | 网络超时 | 600s (大数据集分区慢) |

## 数据准备

### CSV 格式示例

```csv
x,y
-87.697249,41.822730
-87.686513,41.830143
-87.625438,41.870533
```

### 多列 CSV (指定列偏移)

如果数据有多列，通过程序参数指定 x/y 列索引:
```csv
id,longitude,latitude,timestamp
1,-87.697249,41.822730,2024-01-01
```

在代码中使用:
```java
JavaRDD<Point> points = HDFSPointReader.readCSV(sc, "hdfs:///data/taxi.csv", 1, 2, true);
```

## 任务入口说明

| 类名 | 说明 | 参数 |
|------|------|------|
| `SystemBenchmark` | 完整系统基准测试 | `[--generate N]` 或 `<hdfs-path> [output-path]` |
| `idnexbuild.BuildAll` | 全部 5 种分区+索引 | `<数据路径>` |
| `idnexbuild.QPIndexBuild` | QuadTree 分区+索引 | `<数据路径>` |
| `idnexbuild.KPIndexBuild` | KDB-Tree 分区+索引 | `<数据路径>` |
| `idnexbuild.RPIndexBuild` | R-Tree 分区+索引 | `<数据路径>` |
| `idnexbuild.FPIndexBuild` | FixGrid 分区+索引 | `<数据路径>` |
| `idnexbuild.APIndexBuild` | AdaptiveGrid 分区+索引 | `<数据路径>` |

## HDFS 操作命令参考

```bash
# 查看 HDFS 文件
hdfs dfs -ls /lilis/data/

# 查看文件前几行
hdfs dfs -head /lilis/data/points.csv

# 删除文件
hdfs dfs -rm /lilis/data/old_file.csv

# 查看文件大小
hdfs dfs -du -h /lilis/data/

# 创建 Spark 日志目录
hdfs dfs -mkdir -p /spark-logs
```

## 常见问题

### Q: 提交任务时报 "Connection refused"
确认 Spark Master 正在运行:
```bash
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-workers.sh
```
检查 `SPARK_MASTER` 环境变量是否正确。

### Q: 报 "java.lang.OutOfMemoryError"
增大 executor 内存:
```bash
export EXECUTOR_MEMORY="16g"
```
或减少数据量验证后再扩大。

### Q: KryoSerializer 报序列化异常
某些类未注册到 Kryo，可在 `spark-defaults.conf` 中添加:
```
spark.kryo.registrationRequired  false
```

### Q: 任务运行很慢
1. 检查数据倾斜: 增加 `spark.default.parallelism`
2. 检查 Shuffle: 适当增大 `spark.reducer.maxSizeInFlight`
3. 检查 GC: 添加 `--conf spark.executor.extraJavaOptions="-XX:+UseG1GC"`

### Q: 找不到类 "ClassNotFoundException"
确认使用的是 shade 后的 JAR (不是 original-* 前缀的):
```bash
ls target/LearnIndexSpark-1.0-SNAPSHOT.jar        # 正确 (uber-jar)
ls target/original-LearnIndexSpark-1.0-SNAPSHOT.jar  # 错误 (仅项目代码)
```

### Q: Spark 版本不匹配
集群 Spark 版本需为 3.0.x，Scala 2.12。检查:
```bash
spark-submit --version
```

## 目录结构

```
deploy/
├── DEPLOY.md              # 本文档
├── bin/
│   ├── build.sh           # 打包脚本
│   ├── submit-benchmark.sh # 提交 SystemBenchmark
│   ├── submit-query.sh    # 提交自定义任务
│   └── upload-data.sh     # 上传数据到 HDFS
└── conf/
    ├── spark-defaults.conf # Spark 默认配置
    └── log4j.properties   # 日志配置
```
