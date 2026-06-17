import os
import math
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


INPUT_PATH = "data/nyc_full_xy.csv"
OUTPUT_DIR = "results/figures"

CHUNK_SIZE = 500_000
SAMPLE_FRAC = 0.003
MAX_SAMPLE_ROWS = 200_000

VALID_ROWS = 46_475_157
INVALID_ROWS = 773_688


def init_stats():
    return {
        "count": 0,
        "sum": 0.0,
        "sum_sq": 0.0,
        "min": float("inf"),
        "max": float("-inf"),
    }


def update_stats(stats, values):
    values = values.dropna()

    if values.empty:
        return stats

    stats["count"] += len(values)
    stats["sum"] += values.sum()
    stats["sum_sq"] += (values ** 2).sum()
    stats["min"] = min(stats["min"], values.min())
    stats["max"] = max(stats["max"], values.max())

    return stats


def finalize_stats(stats):
    count = stats["count"]

    if count == 0:
        return {
            "count": 0,
            "mean": None,
            "std": None,
            "min": None,
            "max": None,
        }

    mean = stats["sum"] / count

    if count > 1:
        variance = (stats["sum_sq"] - (stats["sum"] ** 2) / count) / (count - 1)
        std = math.sqrt(max(variance, 0))
    else:
        std = 0.0

    return {
        "count": count,
        "mean": mean,
        "std": std,
        "min": stats["min"],
        "max": stats["max"],
    }


def read_sample_and_stats():
    x_stats = init_stats()
    y_stats = init_stats()

    samples = []
    total_rows = 0

    reader = pd.read_csv(
        INPUT_PATH,
        header=None,
        names=["x", "y"],
        chunksize=CHUNK_SIZE,
    )

    for i, chunk in enumerate(reader, start=1):
        chunk["x"] = pd.to_numeric(chunk["x"], errors="coerce")
        chunk["y"] = pd.to_numeric(chunk["y"], errors="coerce")
        chunk = chunk.dropna(subset=["x", "y"])

        total_rows += len(chunk)

        update_stats(x_stats, chunk["x"])
        update_stats(y_stats, chunk["y"])

        current_sample_size = sum(len(s) for s in samples)

        if current_sample_size < MAX_SAMPLE_ROWS:
            sample_chunk = chunk.sample(frac=SAMPLE_FRAC, random_state=42)
            samples.append(sample_chunk)

        print(f"Processed chunk {i}, total rows so far: {total_rows}")

    sample_df = pd.concat(samples, ignore_index=True)

    if len(sample_df) > MAX_SAMPLE_ROWS:
        sample_df = sample_df.sample(n=MAX_SAMPLE_ROWS, random_state=42)

    return sample_df, finalize_stats(x_stats), finalize_stats(y_stats), total_rows


def save_stats(x_stat, y_stat, total_rows):
    output_path = os.path.join(OUTPUT_DIR, "nyc_xy_stats.txt")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("NYC XY DATA STATISTICS\n")
        f.write("======================\n\n")
        f.write(f"Total rows read: {total_rows}\n\n")

        f.write("x statistics:\n")
        for key, value in x_stat.items():
            f.write(f"{key}: {value}\n")

        f.write("\n")

        f.write("y statistics:\n")
        for key, value in y_stat.items():
            f.write(f"{key}: {value}\n")

    print(f"Saved stats: {output_path}")


def plot_longitude_distribution(sample_df):
    plt.figure(figsize=(9, 5))
    plt.hist(sample_df["x"], bins=80)
    plt.xlabel("Longitude (x)")
    plt.ylabel("Frequency")
    plt.title("Distribution of pickup longitude")
    plt.tight_layout()

    output_path = os.path.join(OUTPUT_DIR, "nyc_longitude_distribution.png")
    plt.savefig(output_path, dpi=300)
    plt.close()

    print(f"Saved: {output_path}")


def plot_latitude_distribution(sample_df):
    plt.figure(figsize=(9, 5))
    plt.hist(sample_df["y"], bins=80)
    plt.xlabel("Latitude (y)")
    plt.ylabel("Frequency")
    plt.title("Distribution of pickup latitude")
    plt.tight_layout()

    output_path = os.path.join(OUTPUT_DIR, "nyc_latitude_distribution.png")
    plt.savefig(output_path, dpi=300)
    plt.close()

    print(f"Saved: {output_path}")


def plot_spatial_scatter(sample_df):
    plt.figure(figsize=(8, 8))
    plt.scatter(sample_df["x"], sample_df["y"], s=1, alpha=0.3)
    plt.xlabel("Longitude (x)")
    plt.ylabel("Latitude (y)")
    plt.title("Spatial distribution of NYC taxi pickup points")
    plt.tight_layout()

    output_path = os.path.join(OUTPUT_DIR, "nyc_pickup_scatter_sample.png")
    plt.savefig(output_path, dpi=300)
    plt.close()

    print(f"Saved: {output_path}")


def plot_spatial_density(sample_df):
    plt.figure(figsize=(8, 8))
    plt.hexbin(sample_df["x"], sample_df["y"], gridsize=90, mincnt=1)
    plt.xlabel("Longitude (x)")
    plt.ylabel("Latitude (y)")
    plt.title("Spatial density of NYC taxi pickup points")
    plt.colorbar(label="Point density")
    plt.tight_layout()

    output_path = os.path.join(OUTPUT_DIR, "nyc_pickup_spatial_density_hexbin.png")
    plt.savefig(output_path, dpi=300)
    plt.close()

    print(f"Saved: {output_path}")


def plot_valid_invalid_records():
    labels = ["Valid rows", "Invalid rows"]
    values = [VALID_ROWS, INVALID_ROWS]

    plt.figure(figsize=(7, 5))
    plt.bar(labels, values)
    plt.ylabel("Number of records")
    plt.title("Valid and invalid records after preprocessing")
    plt.tight_layout()

    output_path = os.path.join(OUTPUT_DIR, "nyc_valid_invalid_records.png")
    plt.savefig(output_path, dpi=300)
    plt.close()

    print(f"Saved: {output_path}")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")

    print("Reading data and collecting sample...")
    sample_df, x_stat, y_stat, total_rows = read_sample_and_stats()

    print("Sample size:", len(sample_df))
    print("x stats:", x_stat)
    print("y stats:", y_stat)

    save_stats(x_stat, y_stat, total_rows)

    print("Drawing figures...")
    plot_longitude_distribution(sample_df)
    plot_latitude_distribution(sample_df)
    plot_spatial_scatter(sample_df)
    plot_spatial_density(sample_df)
    plot_valid_invalid_records()

    print("Done.")


if __name__ == "__main__":
    main()
