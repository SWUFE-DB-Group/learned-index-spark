#!/bin/bash
# =============================================================
# 数据上传脚本 - 将本地 CSV 文件上传到 HDFS
# 用法:
#   ./upload-data.sh /path/to/local/data.csv              # 上传到 hdfs:///lilis/data/
#   ./upload-data.sh /path/to/local/data.csv /custom/path # 上传到指定 HDFS 路径
# =============================================================

set -e

HDFS_BASE_DIR="${HDFS_BASE_DIR:-/lilis/data}"

if [ $# -lt 1 ]; then
    echo "用法: $0 <本地文件路径> [HDFS目标路径]"
    echo ""
    echo "示例:"
    echo "  $0 ./data/chicago_crimes.csv"
    echo "  $0 ./data/points.csv /user/hadoop/spatial-data"
    echo "  $0 ./data/  # 上传整个目录"
    echo ""
    echo "默认 HDFS 目标: $HDFS_BASE_DIR"
    echo "可通过 HDFS_BASE_DIR 环境变量修改"
    exit 1
fi

LOCAL_PATH="$1"
HDFS_TARGET="${2:-$HDFS_BASE_DIR}"

if [ ! -e "$LOCAL_PATH" ]; then
    echo "[ERROR] 本地路径不存在: $LOCAL_PATH"
    exit 1
fi

# 检查 hdfs 命令
if ! command -v hdfs &> /dev/null; then
    echo "[ERROR] hdfs 命令未找到，请确认 Hadoop 环境已配置"
    echo "  export HADOOP_HOME=/path/to/hadoop"
    echo "  export PATH=\$HADOOP_HOME/bin:\$PATH"
    exit 1
fi

echo "========================================"
echo "  数据上传到 HDFS"
echo "========================================"
echo "  本地路径: $LOCAL_PATH"
echo "  HDFS 目标: $HDFS_TARGET"
echo ""

# 创建 HDFS 目录
echo "[1/3] 创建 HDFS 目录..."
hdfs dfs -mkdir -p "$HDFS_TARGET"

# 上传文件
echo "[2/3] 上传文件..."
if [ -d "$LOCAL_PATH" ]; then
    hdfs dfs -put -f "$LOCAL_PATH"/* "$HDFS_TARGET/"
else
    hdfs dfs -put -f "$LOCAL_PATH" "$HDFS_TARGET/"
fi

# 验证
echo "[3/3] 验证上传..."
echo ""
echo "HDFS 文件列表:"
hdfs dfs -ls "$HDFS_TARGET/"
echo ""

FILENAME=$(basename "$LOCAL_PATH")
if [ -d "$LOCAL_PATH" ]; then
    echo "========================================"
    echo "  上传成功! 目录已上传到: $HDFS_TARGET/"
    echo "========================================"
else
    HDFS_FULL_PATH="$HDFS_TARGET/$FILENAME"
    echo "========================================"
    echo "  上传成功!"
    echo "  HDFS 路径: $HDFS_FULL_PATH"
    echo "========================================"
    echo ""
    echo "使用示例:"
    echo "  ./submit-benchmark.sh $HDFS_FULL_PATH"
fi
