import os
import sys

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when


def save_bar_valid_invalid(output_dir):
    """
    Vẽ biểu đồ số dòng hợp lệ / không hợp lệ sau tiền xử lý.
    Số liệu này lấy từ log preprocessing NYC hiện tại của nhóm.
    """
    labels = ["Valid rows", "Invalid rows"]
    values = [46475157, 773688]

    plt.figure(figsize=(7, 5))
    plt.bar(labels, values)
    plt.ylabel("Number of records")
    plt.title("Valid and invalid records after NYC preprocessing")
    plt.tight_layout()

    output_path = os.path.join(output_dir, "nyc_valid_invalid_records.png")
    plt.savefig(output_path, dpi=300)
    plt.close()

    print(f"[OK] Saved: {output_path}")


def main():
    input_path = sys.argv[1] if len(sys.argv) > 1 else "data/nyc_full_xy.csv"
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "results/figures"

    os.makedirs(output_dir, exist_ok=True)

    spark = SparkSession.builder \
        .appName("NYCXYVisualization") \
        .getOrCreate()

    print("========== INPUT ==========")
    print(input_path)

    df = spark.read.csv(
        input_path,
        header=False,
        inferSchema=True
    ).toDF("x", "y")

    df = df.select(
        col("x").cast("double").alias("x"),
        col("y").cast("double").alias("y")
    )

    # Loại các dòng null hoặc NaN nếu còn sót lại
    df_clean = df.dropna(subset=["x", "y"]).filter(
        ~isnan(col("x")) & ~isnan(col("y"))
    )

    print("========== SCHEMA ==========")
    df_clean.printSchema()

    print("========== SAMPLE ==========")
    df_clean.show(5)

    print("========== STATISTICS ==========")
    df_clean.describe(["x", "y"]).show()

    print("========== NULL / NAN COUNT ==========")
    df.select([
        count(when(col(c).isNull() | isnan(col(c)), c)).alias(c)
        for c in ["x", "y"]
    ]).show()

    total_count = df_clean.count()
    print(f"Total clean rows: {total_count}")

    # Lấy mẫu để vẽ biểu đồ.
    # Với 46M dòng, fraction=0.002 lấy khoảng 90k điểm, đủ để vẽ nhanh.
    sample_fraction = 0.002
    max_sample_rows = 150000

    sample_df = df_clean.sample(
        withReplacement=False,
        fraction=sample_fraction,
        seed=42
    ).limit(max_sample_rows)

    sample_pd = sample_df.toPandas()

    print(f"Sample rows collected for plotting: {len(sample_pd)}")

    # 1. Histogram kinh độ x
    plt.figure(figsize=(9, 5))
    plt.hist(sample_pd["x"], bins=80)
    plt.xlabel("Longitude (x)")
    plt.ylabel("Frequency")
    plt.title("Distribution of pickup longitude")
    plt.tight_layout()

    output_path = os.path.join(output_dir, "nyc_longitude_distribution.png")
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"[OK] Saved: {output_path}")

    # 2. Histogram vĩ độ y
    plt.figure(figsize=(9, 5))
    plt.hist(sample_pd["y"], bins=80)
    plt.xlabel("Latitude (y)")
    plt.ylabel("Frequency")
    plt.title("Distribution of pickup latitude")
    plt.tight_layout()

    output_path = os.path.join(output_dir, "nyc_latitude_distribution.png")
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"[OK] Saved: {output_path}")

    # 3. Scatter plot phân bố không gian
    plt.figure(figsize=(8, 8))
    plt.scatter(sample_pd["x"], sample_pd["y"], s=1, alpha=0.3)
    plt.xlabel("Longitude (x)")
    plt.ylabel("Latitude (y)")
    plt.title("Spatial distribution of NYC taxi pickup points")
    plt.tight_layout()

    output_path = os.path.join(output_dir, "nyc_pickup_scatter_sample.png")
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"[OK] Saved: {output_path}")

    # 4. Hexbin plot mật độ không gian
    plt.figure(figsize=(8, 8))
    plt.hexbin(sample_pd["x"], sample_pd["y"], gridsize=90, mincnt=1)
    plt.xlabel("Longitude (x)")
    plt.ylabel("Latitude (y)")
    plt.title("Spatial density of NYC taxi pickup points")
    plt.colorbar(label="Point density")
    plt.tight_layout()

    output_path = os.path.join(output_dir, "nyc_pickup_spatial_density_hexbin.png")
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"[OK] Saved: {output_path}")

    # 5. Biểu đồ valid / invalid
    save_bar_valid_invalid(output_dir)

    spark.stop()


if __name__ == "__main__":
    main()
