import os
import glob
import math
import pandas as pd


RAW_DIR = "data/NYC_Taxi"
OUTPUT_DIR = "results/raw_full"
MERGED_OUTPUT = os.path.join(OUTPUT_DIR, "nyc_raw_full_merged.csv")
STATS_OUTPUT = os.path.join(OUTPUT_DIR, "nyc_raw_full_stats.txt")

CHUNK_SIZE = 300_000


def init_col_stats():
    return {
        "count": 0,
        "numeric_count": 0,
        "sum": 0.0,
        "sum_sq": 0.0,
        "min": None,
        "max": None,
        "is_numeric": True,
        "dtype": "unknown",
    }


def update_numeric_stats(stat, series):
    numeric = pd.to_numeric(series, errors="coerce").dropna()

    if len(numeric) == 0:
        stat["is_numeric"] = False
        return stat

    stat["numeric_count"] += len(numeric)
    stat["sum"] += numeric.sum()
    stat["sum_sq"] += (numeric ** 2).sum()

    current_min = numeric.min()
    current_max = numeric.max()

    if stat["min"] is None or current_min < stat["min"]:
        stat["min"] = current_min

    if stat["max"] is None or current_max > stat["max"]:
        stat["max"] = current_max

    return stat


def finalize_stat(stat):
    n = stat["numeric_count"]

    if not stat["is_numeric"] or n == 0:
        return {
            "count": stat["count"],
            "mean": "NULL",
            "std": "NULL",
            "max": "NULL",
            "min": "NULL",
        }

    mean = stat["sum"] / n

    if n > 1:
        variance = (stat["sum_sq"] - (stat["sum"] ** 2) / n) / (n - 1)
        std = math.sqrt(max(variance, 0))
    else:
        std = 0.0

    return {
        "count": stat["count"],
        "mean": mean,
        "std": std,
        "max": stat["max"],
        "min": stat["min"],
    }


def detect_raw_files():
    files = sorted(glob.glob(os.path.join(RAW_DIR, "*.csv")))

    # Bỏ qua các file metadata của Windows nếu có
    files = [f for f in files if ":Zone.Identifier" not in f]

    if not files:
        raise FileNotFoundError(f"No CSV files found in {RAW_DIR}")

    return files


def merge_and_stats():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    raw_files = detect_raw_files()

    print("Found raw CSV files:")
    for f in raw_files:
        print(" -", f)

    all_columns = None
    stats = {}
    total_rows = 0
    wrote_header = False

    if os.path.exists(MERGED_OUTPUT):
        os.remove(MERGED_OUTPUT)

    for file_idx, path in enumerate(raw_files, start=1):
        print(f"\nProcessing file {file_idx}/{len(raw_files)}: {path}")

        reader = pd.read_csv(path, chunksize=CHUNK_SIZE, low_memory=False)

        for chunk_idx, chunk in enumerate(reader, start=1):
            chunk.columns = [str(c).strip() for c in chunk.columns]

            if all_columns is None:
                all_columns = list(chunk.columns)
                for col in all_columns:
                    stats[col] = init_col_stats()
            else:
                # Nếu các file có cùng schema thì nhánh này chỉ giữ nguyên.
                # Nếu có cột thiếu, thêm NA để thống nhất schema.
                for col in all_columns:
                    if col not in chunk.columns:
                        chunk[col] = pd.NA

                # Nếu file sau có cột mới, bổ sung vào schema chung.
                extra_cols = [c for c in chunk.columns if c not in all_columns]
                for col in extra_cols:
                    all_columns.append(col)
                    stats[col] = init_col_stats()

            chunk = chunk[all_columns]
            chunk_rows = len(chunk)
            total_rows += chunk_rows

            for col in all_columns:
                stats[col]["count"] += chunk[col].notna().sum()
                stats[col]["dtype"] = str(chunk[col].dtype)

                if stats[col]["is_numeric"]:
                    update_numeric_stats(stats[col], chunk[col])

            chunk.to_csv(
                MERGED_OUTPUT,
                mode="a",
                index=False,
                header=not wrote_header
            )
            wrote_header = True

            print(
                f"  chunk {chunk_idx}: rows={chunk_rows}, "
                f"total_rows={total_rows}"
            )

    return raw_files, all_columns, stats, total_rows


def save_stats(raw_files, columns, stats, total_rows):
    with open(STATS_OUTPUT, "w", encoding="utf-8") as f:
        f.write("NYC RAW FULL DATA STATISTICS\n")
        f.write("============================\n\n")

        f.write("Input files:\n")
        for path in raw_files:
            f.write(f"- {path}\n")

        f.write("\n")
        f.write(f"Total rows: {total_rows}\n")
        f.write(f"Total columns: {len(columns)}\n")
        f.write(f"Merged output: {MERGED_OUTPUT}\n\n")

        f.write("STT\tColumn\tDtype\tCount\tMean\tStd\tMax\tMin\n")

        for idx, col in enumerate(columns, start=1):
            result = finalize_stat(stats[col])

            f.write(
                f"{idx}\t"
                f"{col}\t"
                f"{stats[col]['dtype']}\t"
                f"{result['count']}\t"
                f"{result['mean']}\t"
                f"{result['std']}\t"
                f"{result['max']}\t"
                f"{result['min']}\n"
            )

    print(f"\nSaved stats to: {STATS_OUTPUT}")
    print(f"Saved merged file to: {MERGED_OUTPUT}")


def main():
    raw_files, columns, stats, total_rows = merge_and_stats()
    save_stats(raw_files, columns, stats, total_rows)
    print("\nDone.")


if __name__ == "__main__":
    main()
