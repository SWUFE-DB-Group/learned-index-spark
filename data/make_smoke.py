import pandas as pd
import os

# Đảm bảo thư mục data tồn tại
os.makedirs("data", exist_ok=True)

# Đường dẫn file train NYC của nhóm em trên WSL
file_path = "/mnt/d/CSDLPT_BIGDATA/learned-index-spark/train/train.csv"
columns_to_read = ['pickup_longitude', 'pickup_latitude']

print("Đang tạo Smoke Test CSV...")
df = pd.read_csv(file_path, usecols=columns_to_read).dropna()
df = df[(df['pickup_longitude'] != 0) & (df['pickup_latitude'] != 0)].head(10000)

# Lưu file KHÔNG header, KHÔNG index vào thư mục data
df.to_csv("data/smoke_points.csv", index=False, header=False)
print(f"Đã tạo data/smoke_points.csv thành công với {len(df)} dòng!")