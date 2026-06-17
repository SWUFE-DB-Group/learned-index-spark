#!/bin/bash
# =============================================================
# run_benchmark.sh — Chạy BuildAll N lần, tính trung bình
# Usage: bash run_benchmark.sh [dataset_label] [data_path] [runs]
# Ví dụ: bash run_benchmark.sh nyc_full data/nyc_full_xy.csv 10
# =============================================================

DATASET=${1:-nyc_full}
DATA_PATH=${2:-data/nyc_full_xy.csv}
RUNS=${3:-10}

RESULT_DIR="results/benchmark_runs"
RAW_FILE="$RESULT_DIR/${DATASET}_raw_runs.txt"
SUMMARY_FILE="$RESULT_DIR/${DATASET}_summary.txt"
JAR="target/LearnIndexSpark-1.0-SNAPSHOT-jar-with-dependencies.jar"

mkdir -p "$RESULT_DIR"
> "$RAW_FILE"

echo "=============================================="
echo "Dataset  : $DATASET"
echo "Data     : $DATA_PATH"
echo "Runs     : $RUNS"
echo "JAR      : $JAR"
echo "=============================================="
echo ""

for i in $(seq 1 $RUNS); do
    echo ">>> Run $i / $RUNS bắt đầu lúc $(date '+%H:%M:%S')"

    # Xóa file kết quả cũ để tránh append
    rm -f "results/${DATASET}_build_results.txt"

    spark-submit \
        --class idnexbuild.BuildAll \
        --master "local[*]" \
        --driver-memory 9g \
        --conf spark.default.parallelism=32 \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.rdd.compress=true \
        --conf spark.memory.fraction=0.8 \
        --conf spark.memory.storageFraction=0.3 \
        "$JAR" \
        "$DATASET" \
        "$DATA_PATH" \
        2>/dev/null

    # Đọc kết quả lần này
    if [ -f "results/${DATASET}_build_results.txt" ]; then
        COUNT=$(grep -c "Index build time" "results/${DATASET}_build_results.txt")
        if [ "$COUNT" -eq 5 ]; then
            echo "  [OK] Đủ 5 variants"
            echo "--- Run $i ---" >> "$RAW_FILE"
            cat "results/${DATASET}_build_results.txt" >> "$RAW_FILE"
        else
            echo "  [WARN] Chỉ có $COUNT variants — có thể OOM, bỏ qua run này"
            echo "--- Run $i SKIPPED (only $COUNT variants) ---" >> "$RAW_FILE"
        fi
    else
        echo "  [ERROR] Không có file kết quả — run $i thất bại"
        echo "--- Run $i FAILED ---" >> "$RAW_FILE"
    fi

    echo ""

    # Nghỉ 5 giây giữa các run để GC và JVM reset
    if [ $i -lt $RUNS ]; then
        sleep 5
    fi
done

echo "=============================================="
echo "Tất cả $RUNS runs xong. Đang tính trung bình..."
echo "=============================================="

# Tính trung bình bằng Python
python3 << PYEOF
import re
from collections import defaultdict

raw_file = "$RAW_FILE"
summary_file = "$SUMMARY_FILE"

variants = ['QPIndexBuild', 'KPIndexBuild', 'RPIndexBuild', 'FPIndexBuild', 'APIndexBuild']
times = defaultdict(list)
run_num = 0
skipped = 0

with open(raw_file) as f:
    content = f.read()

runs = re.split(r'--- Run \d+', content)
for run in runs:
    if 'SKIPPED' in run or 'FAILED' in run:
        skipped += 1
        continue
    for variant in variants:
        match = re.search(rf'build {variant}:\nIndex build time : (\d+)', run)
        if match:
            times[variant].append(int(match.group(1)))

lines = []
lines.append("=" * 50)
lines.append(f"BENCHMARK SUMMARY — $DATASET")
lines.append("=" * 50)
lines.append(f"Total runs attempted : $RUNS")
lines.append(f"Skipped/Failed runs  : {skipped}")
lines.append(f"Valid runs counted   : {max(len(v) for v in times.values()) if times else 0}")
lines.append("")
lines.append(f"{'Variant':<15} {'Min(ms)':<10} {'Max(ms)':<10} {'Avg(ms)':<10} {'StdDev':<10} {'Runs'}")
lines.append("-" * 65)

paper_ref = {
    'QPIndexBuild': None,
    'KPIndexBuild': 50000,
    'RPIndexBuild': None,
    'FPIndexBuild': None,
    'APIndexBuild': None,
}

for v in variants:
    t = times[v]
    if not t:
        lines.append(f"{'LiLIS-'+v[0]:<15} {'N/A':<10} {'N/A':<10} {'N/A':<10} {'N/A':<10} 0")
        continue
    import statistics
    mn = min(t)
    mx = max(t)
    avg = int(sum(t)/len(t))
    std = int(statistics.stdev(t)) if len(t) > 1 else 0
    label = 'LiLIS-' + v[0]
    ref = paper_ref[v]
    ref_str = f"  (paper: ~{ref//1000}s)" if ref else ""
    lines.append(f"{label:<15} {mn:<10} {mx:<10} {avg:<10} {std:<10} {len(t)}{ref_str}")

lines.append("")
lines.append("Thứ tự tốc độ (theo trung bình, nhanh → chậm):")
ranked = sorted([(v, int(sum(t)/len(t))) for v,t in times.items() if t], key=lambda x: x[1])
for rank, (v, avg) in enumerate(ranked, 1):
    label = 'LiLIS-' + v[0]
    lines.append(f"  {rank}. {label}: {avg:,}ms")

output = "\n".join(lines)
print(output)
with open(summary_file, 'w') as f:
    f.write(output)

print(f"\nDa luu ket qua vao: {summary_file}")
PYEOF

echo ""
echo "Raw data: $RAW_FILE"
echo "Summary : $SUMMARY_FILE"
