"""
Phase 2 - Step 1: CHI Data Partition Diagnostics (v2 - fixed outlier handling)
===============================================================================
Input:  data/chi_full_xy.csv  (X=longitude, Y=latitude, no header)
Output: results/phase2_partition_diagnostics.txt (append-only)

Chạy từ thư mục gốc dự án:
    python3 scripts/chi_partition_diagnostics.py
"""

import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────
CHI_PATH          = "data/chi_full_xy.csv"
OUTPUT_FILE       = "results/phase2_partition_diagnostics.txt"
TARGET_PARTITIONS = 16
SAMPLE_RATE       = 0.01
FALLBACK_THRESH   = 100
FALLBACK_WARN_PCT = 15.0
# ──────────────────────────────────────────────────────────────────────────────

SEP = "=" * 62

def section(title):
    print(f"\n[{title}]")

# ─── CHECK FILE ───────────────────────────────────────────────────────────────
print(SEP)
print("PHASE 2 - STEP 1: CHI DATA PARTITION DIAGNOSTICS (v2)")
print(f"Timestamp : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(SEP)

if not os.path.exists(CHI_PATH):
    print(f"❌  Không tìm thấy: {CHI_PATH}")
    sys.exit(1)

# ─── 1. LOAD ──────────────────────────────────────────────────────────────────
section("1  LOAD DATA")
df = pd.read_csv(CHI_PATH, header=None, names=["x", "y"])
total_rows = len(df)
null_x = int(df["x"].isna().sum())
null_y = int(df["y"].isna().sum())
print(f"   Total rows : {total_rows:,}")
print(f"   Null X     : {null_x}   |   Null Y : {null_y}")

# ─── 2. OUTLIER ANALYSIS ─────────────────────────────────────────────────────
section("2  OUTLIER ANALYSIS (quan trong!)")

for col, label in [("x", "Longitude X"), ("y", "Latitude  Y")]:
    s = df[col]
    p1, p99 = s.quantile(0.01), s.quantile(0.99)
    raw_min, raw_max = s.min(), s.max()
    outliers_low  = int((s < p1).sum())
    outliers_high = int((s > p99).sum())
    print(f"\n   {label}:")
    print(f"     raw range : [{raw_min:.6f}, {raw_max:.6f}]")
    print(f"     p1..p99   : [{p1:.6f}, {p99:.6f}]  (99% data trong day)")
    print(f"     outliers  : {outliers_low} duoi p1, {outliers_high} tren p99")
    pct = 100.0 * (outliers_low + outliers_high) / total_rows
    if pct > 0.5:
        print(f"     ⚠️  {pct:.2f}% outliers -- can FILTER truoc khi load vao Spark!")
    else:
        print(f"     ✅ {pct:.2f}% outliers -- chap nhan duoc.")

# ─── 3. FILTER VE VUNG HOP LE ────────────────────────────────────────────────
section("3  VALID REGION FILTER (Chicago thuc te)")

CHICAGO_X_MIN, CHICAGO_X_MAX = -87.9, -87.5
CHICAGO_Y_MIN, CHICAGO_Y_MAX =  41.6,  42.1

df_valid = df[
    (df["x"] >= CHICAGO_X_MIN) & (df["x"] <= CHICAGO_X_MAX) &
    (df["y"] >= CHICAGO_Y_MIN) & (df["y"] <= CHICAGO_Y_MAX)
].copy()

valid_rows   = len(df_valid)
invalid_rows = total_rows - valid_rows
invalid_pct  = 100.0 * invalid_rows / total_rows

print(f"   Filter box : X=[{CHICAGO_X_MIN}, {CHICAGO_X_MAX}], Y=[{CHICAGO_Y_MIN}, {CHICAGO_Y_MAX}]")
print(f"   Valid rows : {valid_rows:,}  ({100.0 - invalid_pct:.2f}%)")
print(f"   Dropped    : {invalid_rows:,}  ({invalid_pct:.2f}%)")

if invalid_pct > 5:
    print(f"   ⚠️  >5% bi drop -- nen cap nhat clean_chi.py de filter ky hon!")
else:
    print(f"   ✅ Ty le drop chap nhan duoc.")

x_min, x_max = float(df_valid["x"].min()), float(df_valid["x"].max())
y_min, y_max = float(df_valid["y"].min()), float(df_valid["y"].max())
x_range = x_max - x_min
y_range = y_max - y_min
print(f"\n   Valid bounding box:")
print(f"     X : [{x_min:.6f}, {x_max:.6f}]  range={x_range:.6f} deg")
print(f"     Y : [{y_min:.6f}, {y_max:.6f}]  range={y_range:.6f} deg")

# ─── 4. DISTRIBUTION STATS ───────────────────────────────────────────────────
section("4  DISTRIBUTION STATS (valid data only)")
x_skew = float(df_valid["x"].skew())
y_skew = float(df_valid["y"].skew())

for col, label in [("x", "Longitude X"), ("y", "Latitude  Y")]:
    s = df_valid[col]
    pct = s.quantile([0.01, 0.25, 0.50, 0.75, 0.99])
    skew = float(s.skew())
    print(f"\n   {label}:  mean={s.mean():.6f}  std={s.std():.6f}  skew={skew:.4f}")
    print(f"     p1={pct[0.01]:.6f}  p25={pct[0.25]:.6f}  "
          f"median={pct[0.50]:.6f}  p75={pct[0.75]:.6f}  p99={pct[0.99]:.6f}")

# ─── 5. PARTITION SIMULATION ─────────────────────────────────────────────────
section(f"5  PARTITION SIMULATION ({TARGET_PARTITIONS} partitions)")
print("   Phuong phap: sort by Y -> chia deu so diem (sat voi KD-tree leaf)")

df_sorted  = df_valid.sort_values("y").reset_index(drop=True)
chunk_size = len(df_sorted) // TARGET_PARTITIONS

counts = {}
for i in range(TARGET_PARTITIONS):
    start      = i * chunk_size
    end        = start + chunk_size if i < TARGET_PARTITIONS - 1 else len(df_sorted)
    counts[i]  = end - start

count_series = pd.Series(counts)
p_min  = int(count_series.min())
p_max  = int(count_series.max())
p_mean = float(count_series.mean())
p_std  = float(count_series.std())
imbal  = p_max / p_min if p_min > 0 else float("inf")

print(f"\n   Points per partition:")
print(f"     min    = {p_min:,}")
print(f"     max    = {p_max:,}")
print(f"     mean   = {p_mean:,.0f}")
print(f"     std    = {p_std:,.0f}")
print(f"     imbalance (max/min) = {imbal:.2f}x")

if imbal <= 2:
    print("   ✅ Imbalance rat tot.")
elif imbal <= 5:
    print("   ✅ Imbalance chap nhan duoc.")
else:
    print("   ⚠️  Imbalance > 5x -- kiem tra lai du lieu.")

# Histogram
h0     = int((count_series == 0).sum())
h1_100 = int(((count_series >= 1)    & (count_series <= 100)).sum())
h101   = int(((count_series >= 101)  & (count_series <= 1000)).sum())
h1001  = int(((count_series >= 1001) & (count_series <= 10000)).sum())
hbig   = int((count_series > 10000).sum())

print(f"\n   Partition histogram:")
print(f"     = 0 points         : {h0}")
print(f"     1-100   (fallback) : {h1_100}")
print(f"     101-1,000          : {h101}")
print(f"     1,001-10,000       : {h1001}")
print(f"     > 10,000           : {hbig}")

non_empty  = int((count_series > 0).sum())
fallback_n = h1_100
fallback_r = (100.0 * fallback_n / non_empty) if non_empty > 0 else 0.0

print(f"\n   Fallback ratio (n<=100 / non-empty) : "
      f"{fallback_n}/{non_empty} = {fallback_r:.1f}%")

if fallback_r > FALLBACK_WARN_PCT:
    print(f"   ⚠️  Fallback > {FALLBACK_WARN_PCT}% -> giam TARGET_PARTITIONS xuong 8.")
else:
    print(f"   ✅ Fallback ratio OK.")

# ─── 6. SAMPLE SIZE ──────────────────────────────────────────────────────────
section("6  SAMPLING ESTIMATE FOR KD-TREE")
sample_size  = int(valid_rows * SAMPLE_RATE)
pts_per_leaf = sample_size // TARGET_PARTITIONS

print(f"   sample_rate     = {SAMPLE_RATE} (1% -- dung paper)")
print(f"   sample size     = {sample_size:,} points")
print(f"   points per leaf = {pts_per_leaf:,}")

if pts_per_leaf >= 50:
    print("   ✅ Du diem/leaf de build KD-tree zone chat luong tot.")
else:
    print("   ⚠️  It diem/leaf -- tang sample_rate hoac giam partition count.")

# ─── 7. VERDICT ──────────────────────────────────────────────────────────────
section("7  VERDICT & NEXT STEPS")

all_ok = (imbal <= 5) and (fallback_r <= FALLBACK_WARN_PCT) and (invalid_pct <= 5)

if all_ok:
    print("   ✅ DU LIEU SAN SANG cho Phase 2 Spark.")
    print(f"\n   Config cho KDBTreePartitioner.java:")
    print(f"     sampleRate        = {SAMPLE_RATE}")
    print(f"     targetPartitions  = {TARGET_PARTITIONS}")
    print(f"     fallbackThreshold = {FALLBACK_THRESH}")
    print(f"\n   Spark config khuyen nghi:")
    print(f"     spark.default.parallelism    = {TARGET_PARTITIONS}")
    print(f"     spark.sql.shuffle.partitions = {TARGET_PARTITIONS}")
else:
    print("   ⚠️  Can xu ly them truoc khi sang Spark:")
    if invalid_pct > 5:
        print(f"     -> Cap nhat clean_chi.py de filter ky hon ({invalid_pct:.1f}% bi drop)")
    if imbal > 5:
        print(f"     -> imbalance={imbal:.1f}x cao, kiem tra lai du lieu")
    if fallback_r > FALLBACK_WARN_PCT:
        print(f"     -> Giam TARGET_PARTITIONS xuong 8 roi chay lai")

# ─── 8. WRITE OUTPUT ─────────────────────────────────────────────────────────
os.makedirs("results", exist_ok=True)
ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

with open(OUTPUT_FILE, "a") as f:
    f.write("\n" + SEP + "\n")
    f.write(f"RUN_TIMESTAMP={ts}\n")
    f.write(f"script_version=v2\n")
    f.write(f"dataset=CHI\n")
    f.write(f"total_rows={total_rows}\n")
    f.write(f"valid_rows={valid_rows}\n")
    f.write(f"invalid_rows={invalid_rows}\n")
    f.write(f"invalid_pct={invalid_pct:.2f}\n")
    f.write(f"valid_bbox_x=[{x_min:.6f},{x_max:.6f}]\n")
    f.write(f"valid_bbox_y=[{y_min:.6f},{y_max:.6f}]\n")
    f.write(f"x_skewness={x_skew:.4f}\n")
    f.write(f"y_skewness={y_skew:.4f}\n")
    f.write(f"sample_rate={SAMPLE_RATE}\n")
    f.write(f"sample_size={sample_size}\n")
    f.write(f"target_partitions={TARGET_PARTITIONS}\n")
    f.write(f"partition_min={p_min}\n")
    f.write(f"partition_max={p_max}\n")
    f.write(f"partition_mean={p_mean:.0f}\n")
    f.write(f"imbalance_ratio={imbal:.2f}\n")
    f.write(f"fallback_count={fallback_n}\n")
    f.write(f"fallback_ratio_pct={fallback_r:.1f}\n")
    f.write(f"histogram_0={h0}\n")
    f.write(f"histogram_1_100={h1_100}\n")
    f.write(f"histogram_101_1000={h101}\n")
    f.write(f"histogram_1001_10000={h1001}\n")
    f.write(f"histogram_gt10000={hbig}\n")
    f.write(f"verdict={'PASS' if all_ok else 'NEEDS_FIX'}\n")

print(f"\n✅ Ket qua ghi vao: {OUTPUT_FILE}")
print(SEP)