#!/bin/bash
# =============================================================
# LiLIS 项目打包脚本
# 生成 uber-jar 用于 spark-submit 部署
# =============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "========================================"
echo "  LiLIS Build Script"
echo "========================================"
echo ""
echo "项目目录: $PROJECT_DIR"
echo ""

cd "$PROJECT_DIR"

# 检查 Maven
if ! command -v mvn &> /dev/null; then
    if [ -d "$HOME/apache-maven-3.9.15" ]; then
        export PATH="$HOME/apache-maven-3.9.15/bin:$PATH"
    else
        echo "[ERROR] Maven 未找到，请安装 Maven 或设置 PATH"
        exit 1
    fi
fi

echo "[1/3] 清理旧构建..."
mvn clean -q

echo "[2/3] 编译打包 (跳过测试)..."
mvn package -DskipTests -q

echo "[3/3] 检查输出..."
JAR_FILE=$(find target -name "LearnIndexSpark-*.jar" ! -name "*original*" | head -1)

if [ -f "$JAR_FILE" ]; then
    JAR_SIZE=$(du -h "$JAR_FILE" | cut -f1)
    echo ""
    echo "========================================"
    echo "  构建成功!"
    echo "  JAR: $JAR_FILE"
    echo "  大小: $JAR_SIZE"
    echo "========================================"
    echo ""
    echo "下一步: 使用 deploy/bin/submit-benchmark.sh 提交到集群"
else
    echo "[ERROR] 未找到输出 JAR 文件"
    exit 1
fi
