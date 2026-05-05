from pathlib import Path
import pandas as pd


# =========================
# CONFIG
# =========================

# Sửa đường dẫn này thành thư mục chứa 4 file NYC raw của bạn.
# Ví dụ:
# RAW_DIR = Path("/mnt/d/Paper/nyc_yellow_taxi")
RAW_DIR = Path("/mnt/d/Paper/NYC_Taxi")
# Pattern tìm 4 file raw.
INPUT_PATTERN = "yellow_tripdata_*.csv"

# Output cho LiLIS: chỉ 2 cột x,y, không header.
OUT_PATH = Path("data/nyc_full_xy.csv")

# Log thống kê.
LOG_PATH = Path("logs/preprocess_nyc_taxi_xy.log")

# Đọc từng phần để không bị tràn RAM.
CHUNK_SIZE = 500_000

# Có lọc tọa độ ngoài vùng NYC không?
# Khuyên bật để loại các dòng lỗi rõ ràng.
ENABLE_NYC_BBOX_FILTER = True

# Bounding box rộng quanh NYC.
MIN_LON, MAX_LON = -75.0, -72.0
MIN_LAT, MAX_LAT = 40.0, 42.0


def main():
    input_files = sorted(RAW_DIR.glob(INPUT_PATTERN))

    if not input_files:
        raise FileNotFoundError(f"Không tìm thấy file nào trong {RAW_DIR} với pattern {INPUT_PATTERN}")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    if OUT_PATH.exists():
        OUT_PATH.unlink()

    total_rows = 0
    total_valid_rows = 0
    total_invalid_rows = 0

    with LOG_PATH.open("w", encoding="utf-8") as log:
        log.write("NYC taxi preprocessing to LiLIS x,y format\n")
        log.write(f"raw_dir={RAW_DIR}\n")
        log.write(f"output={OUT_PATH}\n")
        log.write(f"chunk_size={CHUNK_SIZE}\n")
        log.write(f"bbox_filter={ENABLE_NYC_BBOX_FILTER}\n")
        log.write("\n")

        print("Input files:")
        for f in input_files:
            print(f" - {f}")
            log.write(f"input_file={f}\n")

        write_mode = "w"

        for file_idx, csv_path in enumerate(input_files, start=1):
            print(f"\nProcessing file {file_idx}/{len(input_files)}: {csv_path}")
            log.write(f"\nProcessing {csv_path}\n")

            file_rows = 0
            file_valid_rows = 0
            file_invalid_rows = 0

            reader = pd.read_csv(
                csv_path,
                chunksize=CHUNK_SIZE,
                usecols=lambda c: c.strip() in {"pickup_longitude", "pickup_latitude"},
                low_memory=False,
            )

            for chunk_idx, chunk in enumerate(reader, start=1):
                # Chuẩn hóa tên cột phòng trường hợp có khoảng trắng.
                chunk.columns = [c.strip() for c in chunk.columns]

                # Đổi sang numeric; lỗi parse sẽ thành NaN.
                x = pd.to_numeric(chunk["pickup_longitude"], errors="coerce")
                y = pd.to_numeric(chunk["pickup_latitude"], errors="coerce")

                valid_mask = x.notna() & y.notna()

                # Loại 0,0 vì đây thường là tọa độ lỗi.
                valid_mask = valid_mask & ~((x == 0.0) & (y == 0.0))

                # Lọc bbox rộng quanh NYC để loại tọa độ lỗi rõ ràng.
                if ENABLE_NYC_BBOX_FILTER:
                    valid_mask = (
                        valid_mask
                        & (x >= MIN_LON) & (x <= MAX_LON)
                        & (y >= MIN_LAT) & (y <= MAX_LAT)
                    )

                out = pd.DataFrame({
                    "x": x[valid_mask],
                    "y": y[valid_mask],
                })

                rows = len(chunk)
                valid_rows = len(out)
                invalid_rows = rows - valid_rows

                total_rows += rows
                total_valid_rows += valid_rows
                total_invalid_rows += invalid_rows

                file_rows += rows
                file_valid_rows += valid_rows
                file_invalid_rows += invalid_rows

                # Không ghi header để PointRDDUtils đọc trực tiếp col[0], col[1].
                out.to_csv(
                    OUT_PATH,
                    mode=write_mode,
                    header=False,
                    index=False,
                )
                write_mode = "a"

                if chunk_idx % 10 == 0:
                    print(
                        f"  chunk={chunk_idx}, "
                        f"file_rows={file_rows:,}, "
                        f"file_valid={file_valid_rows:,}, "
                        f"file_invalid={file_invalid_rows:,}"
                    )

            print(
                f"Done {csv_path.name}: "
                f"rows={file_rows:,}, valid={file_valid_rows:,}, invalid={file_invalid_rows:,}"
            )
            log.write(
                f"file_summary={csv_path.name}, "
                f"rows={file_rows}, valid={file_valid_rows}, invalid={file_invalid_rows}\n"
            )

        print("\n==============================")
        print("NYC preprocessing completed")
        print(f"total_rows={total_rows:,}")
        print(f"total_valid_rows={total_valid_rows:,}")
        print(f"total_invalid_rows={total_invalid_rows:,}")
        print(f"output={OUT_PATH}")
        print(f"log={LOG_PATH}")

        log.write("\nSUMMARY\n")
        log.write(f"total_rows={total_rows}\n")
        log.write(f"total_valid_rows={total_valid_rows}\n")
        log.write(f"total_invalid_rows={total_invalid_rows}\n")
        log.write(f"output={OUT_PATH}\n")


if __name__ == "__main__":
    main()