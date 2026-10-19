# HỆ THỐNG PHÂN TÍCH DỮ LIỆU LỚN PHÂN TÁN & DỰ BÁO NHU CẦU XE CÔNG CỘNG (CITI BIKE NYC)

> **Đồ án môn học:** Xử lý & Phân tích Dữ liệu lớn (Big Data Processing & Analytics)  
> **Kiến trúc cốt lõi:** Hadoop HDFS 3.3.6 • Apache Spark 3.5.1 (PySpark Standalone) • Spark MLlib • Streamlit Dashboard  
> **Bộ dữ liệu thực nghiệm:** Citi Bike New York City Open Data (Quy mô 14.97+ triệu bản ghi)

---

## MỤC LỤC
1. [Tổng quan Dự án & Bối cảnh Thực tiễn](#1-tổng-quan-dự-án--bối-cảnh-thực-tiễn)
2. [Kiến trúc Kỹ thuật & Đặc tả 5 Trụ cột](#2-kiến-trúc-kỹ-thuật--đặc-tả-5-trụ-cột)
3. [Dữ liệu Thực nghiệm & Kết quả Mô hình MLlib](#3-dữ-liệu-thực-nghiệm--kết-quả-mô-hình-mllib)
4. [Cấu trúc Thư mục Dự án Chuẩn hóa](#4-cấu-trúc-thư-mục-dự-án-chuẩn-hóa)
5. [Yêu cầu Môi trường & Tiền điều kiện](#5-yêu-cầu-môi-trường--tiền-điều-kiện)
6. [Hướng dẫn Chạy Dự án Từ A đến Z](#6-hướng-dẫn-chạy-dự-án-từ-a-đến-z)
   - [Phương án A: Chạy tự động qua Bảng điều khiển Menu (.bat)](#phương-án-a-chạy-tự-động-qua-bảng-điều-khiển-menu-bat)
   - [Phương án B: Chạy thủ công chi tiết từng bước (Command-by-Command)](#phương-án-b-chạy-thủ-công-chi-tiết-từng-bước-command-by-command)
   - [Phương án C: Chạy các Thí nghiệm Big Data & MapReduce](#phương-án-c-chạy-các-thí-nghiệm-big-data--mapreduce)
7. [Hướng dẫn Thao tác trên Giao diện Web Dashboard](#7-hướng-dẫn-thao-tác-trên-giao-diện-web-dashboard)
8. [Các Cổng Web UI Quản trị Cụm & Minh chứng Báo cáo](#8-các-cổng-web-ui-quản-trị-cụm--minh-chứng-báo-cáo)
9. [Các Lệnh Dừng & Dọn dẹp Hệ thống](#9-các-lệnh-dừng--dọn-dẹp-hệ-thống)

---

## 1. TỔNG QUAN DỰ ÁN & BỐI CẢNH THỰC TIỄN

### 1.1. Bối cảnh Bài toán
**Citi Bike New York City** là hệ thống chia sẻ xe đạp công cộng lớn nhất Bắc Mỹ với hơn 2.000 trạm và hàng chục triệu chuyến đi mỗi năm. Tuy nhiên, hệ thống thường xuyên đối mặt với bài toán **mất cân bằng lưu lượng không gian - thời gian**:
- **Khung giờ cao điểm sáng (7h - 9h):** Người dùng đồng loạt lấy xe từ các khu dân cư (Brooklyn, Queens, Upper Manhattan) để di chuyển vào trung tâm tài chính và văn phòng (Midtown, Lower Manhattan). Hệ quả: Các trạm ngoại vi nhanh chóng **cạn kiệt xe (cháy xe)**, trong khi các trạm trung tâm **hết sạch bãi cắm khóa (không trả được xe)**.
- **Khung giờ cao điểm chiều (17h - 19h):** Dòng người di chuyển theo hướng ngược lại, gây quá tải đảo chiều.
- **Thời tiết:** Trời mưa hoặc nhiệt độ giảm sâu làm nhu cầu thuê xe sụt giảm đột ngột từ 30% đến 40%.

### 1.2. Mục tiêu Giải pháp
Xây dựng một **nền tảng xử lý dữ liệu lớn phân tán hoàn chỉnh** từ tầng thu thập, lưu trữ an toàn, làm sạch song song, kỹ thuật đặc trưng chuỗi thời gian, huấn luyện học máy phân tán đến tầng phục vụ hiển thị:
- **Dự báo trước chân trời $h = 1$ giờ** số lượng lượt xe sẽ được thuê tại từng trạm trọng điểm.
- **Tự động sinh Lệnh Điều phối Vận hành (Smart City Dispatch Directive)** trước 45 phút để đội xe tải chuyên dụng kịp bốc dỡ, tái phân bổ xe từ trạm thừa sang trạm thiếu.

---

## 2. KIẾN TRÚC KỸ THUẬT & ĐẶC TẢ 5 TRỤ CỘT

Hệ thống được thiết kế theo kiến trúc **Lakehouse phân tầng**:

```
[Raw Citi Bike CSV Data] (14.9M+ records)
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│ 1. HẠ TẦNG LƯU TRỮ PHÂN TÁN (Hadoop HDFS 3.3.6)             │
│    NameNode + 2 DataNodes, Replication = 2                  │
│    - /citibike/raw/year=YYYY/month=MM/*.csv                 │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. ĐỘNG CƠ TÍNH TOÁN SONG SONG (Apache Spark 3.5.1 PySpark) │
│    Spark Master + 2 Workers, Xử lý In-Memory trên RAM       │
│    - Job 02 Clean:     /citibike/cleaned (Parquet)          │
│    - Job 03 Hourly:    /citibike/hourly (Lưới Trạm x Giờ)   │
│    - Job 04 Features:  /citibike/features (Lags, Sin/Cos)   │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. THUẬT TOÁN HỌC MÁY PHÂN TÁN (Spark MLlib Core)          │
│    - Job 05: Baselines (Seasonal Naive, Historical Mean)    │
│    - Job 06: MLlib (Linear, Ridge, Lasso, DT, RF, GBT)      │
│    - Đánh giá đa chiều: RMSE, MAE, R², WMAPE, Fit Time      │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. CHUẨN HÓA QUY TRÌNH (Zero Data Leakage Integrity)        │
│    - Time-based Split theo trục thời gian thực              │
│    - station_mean chỉ tính từ tập Train                     │
│    - Tuyệt đối không xáo trộn (No Random Shuffle)           │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. GIAO DIỆN ĐIỀU PHỐI (Streamlit Smart Operations Cockpit) │
│    - Job 07 Serving:   data/serving/*.parquet               │
│    - Chân trời dự báo h = 1 Giờ                             │
│    - GIS Folium Map, Heatmap 24h, Leaderboard, Dispatch     │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. DỮ LIỆU THỰC NGHIỆM & KẾT QUẢ MÔ HÌNH MLLIB

### 3.1. Dữ liệu Thực nghiệm (Seasonal 4 Months - Năm 2024)
Để đảm bảo tính bao quát 4 mùa quanh năm mà không gây quá tải ổ đĩa cục bộ, đề tài lựa chọn 4 tháng đại diện:
- **Tháng 01/2024:** Mùa đông (Nhiệt độ thấp, lưu lượng cơ sở thấp).
- **Tháng 04/2024:** Mùa xuân (Thời tiết ấm dần, lưu lượng tăng trưởng).
- **Tháng 07/2024:** Mùa hè (Mùa cao điểm du lịch và hoạt động ngoài trời).
- **Tháng 10/2024:** Mùa thu (Thời tiết giao mùa, nhu cầu ổn định).
- **Tổng dung lượng:** ~2.79 GB CSV thô, nạp vào HDFS với `Replication = 2` tương đương **~5.5 GB**.
- **Tổng số dòng:** **14.978.098 bản ghi**.
- **Tỷ lệ dữ liệu sạch sau lọc:** **99.95%** (loại bỏ các chuyến đi ảo < 60 giây cùng trạm hoặc lỗi GPS).

### 3.2. Bảng Đấu trường Đánh giá Hiệu năng Mô hình (Leaderboard Benchmark)

| Hạng | Thuật toán Mô hình | Nhóm thuật toán | MAE (Lượt) | RMSE (Lượt) | Cải thiện RMSE | $R^2$ (Độ khớp) | WMAPE | Thời gian Fit |
| :---: | :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| 🥇 **#1** | **Random Forest** | **Tree Ensemble (Champion)** | **1.761** | **2.733** | **-27.9%** | **0.6495** | **44.71%** | 9.8s |
| 🥈 **#2** | **GBT Regressor** | Gradient Boosting | 1.780 | 2.758 | -27.2% | 0.6431 | 45.17% | 15.4s |
| 🥉 **#3** | **Decision Tree** | Decision Tree | 1.847 | 2.904 | -23.4% | 0.6045 | 46.88% | 1.6s |
| **#4** | **Lasso Regression** | Hồi quy L1 | 1.847 | 2.909 | -23.3% | 0.6029 | 46.89% | 0.9s |
| **#5** | **Ridge Regression** | Hồi quy L2 | 1.856 | 2.913 | -23.2% | 0.6018 | 47.11% | 1.4s |
| **#6** | **Linear Regression**| Tuyến tính OLS | 1.856 | 2.913 | -23.2% | 0.6018 | 47.12% | 5.8s |
| **MỐC**| **Historical Mean** | Baseline Thống kê | 2.109 | 3.350 | -11.6% | 0.4734 | 53.53% | 0.0s |
| **MỐC**| **Seasonal Naive (lag168)** | Baseline Naive | 2.391 | 3.791 | *Chuẩn gốc* | 0.3256 | 60.69% | 0.0s |

> **Nhận định học thuật:** Nhu cầu thuê xe đạp có tính chất phi tuyến cực kỳ rõ nét (hiệu ứng đột biến vào khung giờ cao điểm và ảnh hưởng của thời tiết). Nhóm cây phân nhánh (**Random Forest & GBT**) đạt hiệu quả vượt trội, giảm tới **28% sai số toàn phương (RMSE)** so với mô hình cơ sở Seasonal Naive.

---

## 4. CẤU TRÚC THƯ MỤC DỰ ÁN CHUẨN HÓA

```
Phan_Tich_Du_Bao_CitiBike/
│
├── .streamlit/
│   └── config.toml               # Cấu hình theme chuẩn Enterprise cho Streamlit (Primary Blue)
│
├── docker/
│   ├── docker-compose.yml        # Định nghĩa 5 container (Hadoop NameNode, 2 DataNodes, Spark Master, 2 Workers)
│   ├── config/                   # File cấu hình phân tán: core-site.xml, hdfs-site.xml, spark-defaults.conf
│   ├── start.bat                 # Script batch khởi động nhanh cụm container Docker
│   └── README.md                 # Hướng dẫn chi tiết vận hành cụm Docker & các lệnh HDFS
│
├── jobs/                         # Các Job tính toán PySpark phân tán chạy trên cụm Spark
│   ├── 01_explore.py             # Job 01: Khảo sát cấu trúc schema, volume, veracity, variety
│   ├── 02_clean.py               # Job 02: Tiền xử lý, lọc chuyến ảo, ghi Parquet /citibike/cleaned
│   ├── 03_hourly.py              # Job 03: Lưới (Trạm x Giờ) Top 400 trạm & Kỹ thuật Zero-Filling
│   ├── 04_features.py            # Job 04: Trích xuất Lags, Sin/Cos chu kỳ thời gian & Time-based Split
│   ├── 05_baseline.py            # Job 05: Đánh giá mô hình cơ sở Seasonal Naive & Historical Mean
│   ├── 06_train_eval.py          # Job 06: Huấn luyện 6 mô hình Spark MLlib & xuất metrics lên HDFS
│   ├── 07_export_serving.py      # Job 07: Xuất bảng tầng Serving định dạng Parquet phục vụ Dashboard
│   ├── 08_bigdata_experiments.py # Job 08: Thí nghiệm Big Data (So sánh thời gian đọc CSV vs Parquet)
│   └── mapreduce/                # Mã nguồn MapReduce Hadoop Streaming (mapper.py, reducer.py)
│
├── scripts/                      # Các kịch bản tiện ích điều phối
│   ├── download_data.py          # Script tải tự động dữ liệu Citi Bike chính thức từ Amazon S3
│   ├── load_to_hdfs.py           # Script nạp tự động dữ liệu CSV vào cụm phân tán HDFS (/citibike/raw)
│   └── run_pipeline.bat          # Menu điều khiển tự động toàn bộ quy trình từ 1 click
│
├── dashboard/
│   ├── app.py                    # Ứng dụng Streamlit Dashboard trực quan hóa & Cockpit điều phối
│   └── data/                     # Thư mục lưu các file Parquet tầng phục vụ cho Dashboard
│
├── data/
│   ├── csv/                      # Thư mục lưu file CSV thô tải về (phân thư mục theo năm-tháng)
│   ├── serving/                  # Thư mục lưu các file Parquet đã tối ưu do Job 07 xuất ra
│   └── zip/                      # Thư mục lưu tệp nén zip tải về từ nguồn S3
│
├── notebooks/                    # Jupyter Notebooks phục vụ EDA và phân tích thử nghiệm
├── report/                       # Tài liệu báo cáo, đề cương và hình ảnh minh chứng
├── requirements.txt              # Danh sách các thư viện Python máy chủ
├── README.md                     # Tài liệu tổng quan và hướng dẫn dự án chi tiết
├── HUONG_DAN_CHAY_DU_AN.md       # Sổ tay hướng dẫn thực thi quy trình từ A đến Z
└── PLAN_THUC_HIEN_DO_AN_CITIBIKE.md # Kế hoạch triển khai chi tiết 10 tuần & đối chiếu rubric
```

---

## 5. YÊU CẦU MÔI TRƯỜNG & TIỀN ĐIỀU KIỆN

1. **Hệ điều hành:** Windows 10/11 (hỗ trợ WSL2 và Docker Desktop).
2. **Docker Desktop:** Đã cài đặt và đang chạy. Khuyến nghị cấp tối thiểu **6 - 8 GB RAM** cho Docker (trong *Docker Desktop -> Settings -> Resources*).
3. **Python:** Phiên bản Python 3.10, 3.11 hoặc 3.12 trên máy chủ Windows.
4. **Cài đặt thư viện Python:**
   ```bash
   pip install -r requirements.txt
   ```

---

## 6. HƯỚNG DẪN CHẠY DỰ ÁN TỪ A ĐẾN Z

Người dùng có thể lựa chọn **Phương án A** (Chạy nhanh tự động qua Menu giao diện dòng lệnh) hoặc **Phương án B** (Chạy thủ công từng lệnh để hiểu sâu quy trình).

---

### PHƯƠNG ÁN A: Chạy tự động qua Bảng điều khiển Menu (.bat)

1. Mở cửa sổ **Terminal** hoặc **Command Prompt (CMD)** tại thư mục gốc dự án:
   ```cmd
   cd "e:\Big Data\Do_An_Big_Data\Phan_Tich_Du_Bao_CitiBike"
   ```
2. Khởi động cụm Docker:
   ```cmd
   cd docker
   docker compose up -d
   cd ..
   ```
   *(Hoặc click đúp file `docker\start.bat`)*

3. Chạy file Menu điều khiển:
   ```cmd
   scripts\run_pipeline.bat
   ```
4. Trên màn hình Menu xuất hiện các tùy chọn:
   - Nhập `1`: Tự động tải dữ liệu Citi Bike (mặc định Tháng 01/2024 hoặc 4 tháng theo mùa).
   - Nhập `2`: Tự động đẩy file CSV lên cụm HDFS tầng raw.
   - Nhập `A`: **CHẠY TỰ ĐỘNG TOÀN BỘ QUY TRÌNH** (Chạy liên hoàn từ Job 01 đến Job 07 và đồng bộ serving).
   - Nhập `11`: Mở trực tiếp Web Dashboard trên trình duyệt tại `http://localhost:8501`.

---

### PHƯƠNG ÁN B: Chạy thủ công chi tiết từng bước (Command-by-Command)

Dành cho việc trình chiếu trực tiếp trong buổi chấm đồ án hoặc chụp ảnh màn hình minh chứng cho từng chương của Báo cáo.

#### BƯỚC 1: Khởi động cụm phân tán Docker (Hadoop + Spark)
```bash
cd docker
docker compose up -d
cd ..
```
Kiểm tra đảm bảo các container đều ở trạng thái `healthy` hoặc `running`:
```bash
docker compose -f docker/docker-compose.yml ps
```
*(Bao gồm: `citibike-namenode`, `citibike-datanode1`, `citibike-datanode2`, `citibike-spark-master`, `citibike-spark-worker-1`, `citibike-spark-worker-2`)*.

---

#### BƯỚC 2: Tải dữ liệu thực nghiệm 4 mùa từ nguồn mở Citi Bike
Mở Terminal tại thư mục gốc của dự án:
```bash
# Tải 4 tháng đại diện 4 mùa (01, 04, 07, 10 năm 2024)
python scripts/download_data.py --seasonal

# Hoặc nếu máy cấu hình nhẹ, chỉ chạy thử nghiệm 1 tháng:
# python scripts/download_data.py --months 1
```
*Dữ liệu dạng nén `.zip` được tự động tải về, giải nén và lưu vào `data/csv/YYYY-MM/`.*

---

#### BƯỚC 3: Nạp dữ liệu CSV vào cụm lưu trữ phân tán HDFS
```bash
# Đẩy các file CSV theo từng phân vùng năm/tháng vào HDFS /citibike/raw
python scripts/load_to_hdfs.py --seasonal

# Hoặc nếu chạy 1 tháng:
# python scripts/load_to_hdfs.py --months 1
```
**Kiểm tra tính toàn vẹn trên HDFS:**
```bash
# Kiểm tra cây thư mục HDFS
docker exec -it citibike-namenode hdfs dfs -ls -R /citibike/raw

# Kiểm tra báo cáo cụm NameNode & 2 Live DataNodes
docker exec -it citibike-namenode hdfs dfsadmin -report
```

---

#### BƯỚC 4: Khảo sát dữ liệu thô & Tiền xử lý, Làm sạch dữ liệu (Job 01 & Job 02)
Chạy **Job 01** để khảo sát schema, đếm tổng khối lượng và tỷ lệ missing:
```bash
docker exec -it citibike-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/01_explore.py
```

Chạy **Job 02** để lọc dữ liệu rác, chuyến đi lỗi, và lưu dữ liệu sạch dạng Parquet phân vùng:
```bash
docker exec -it citibike-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/02_clean.py
```
*Kết quả ghi vào: `/citibike/cleaned` (định dạng Parquet, phân vùng theo `month`) trên HDFS.*

---

#### BƯỚC 5: Tạo lưới chuỗi thời gian (Trạm x Giờ) & Kỹ thuật Zero-Filling (Job 03)
Trong thực tế, khi trạm không có ai thuê xe vào một khung giờ (ví dụ 3h sáng), bản ghi đó sẽ biến mất trong bảng giao dịch. Job 03 áp dụng kỹ thuật **Zero-Filling** để bổ sung đầy đủ các khung giờ có số lượt thuê = 0 cho Top 400 trạm trọng điểm:
```bash
docker exec -it citibike-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/03_hourly.py
```
*Kết quả ghi vào: `/citibike/hourly` (định dạng Parquet) trên HDFS.*

---

#### BƯỚC 6: Trích xuất Đặc trưng Chuỗi thời gian & Chia tập không rò rỉ (Job 04)
Tạo các đặc trưng trễ (`lag_1`, `lag_24`, `lag_168`, `roll_mean_3`), các hàm lượng giác tuần hoàn (Sin/Cos của Giờ và Thứ), cờ ngày cuối tuần, `station_mean` chỉ tính từ tập Train (chống rò rỉ), và phân chia tập dữ liệu Train / Validation / Test:
```bash
docker exec -it citibike-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/04_features.py
```
*Kết quả ghi vào: `/citibike/features` (định dạng Parquet, phân vùng theo `split=train`, `split=val`, `split=test`).*

---

#### BƯỚC 7: Đánh giá Mô hình Cơ sở Baseline (Job 05)
Chạy 2 mô hình đối chứng chuẩn: **Seasonal Naive (`lag_168`)** và **Historical Mean (Trạm $\times$ Thứ $\times$ Giờ)**:
```bash
docker exec -it citibike-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/05_baseline.py
```

---

#### BƯỚC 8: Huấn luyện Đấu trường Mô hình Spark MLlib (Job 06)
Huấn luyện 6 mô hình Machine Learning phân tán trên cụm Spark: Linear Regression, Ridge, Lasso, Decision Tree, Random Forest, GBT:
```bash
docker exec -it citibike-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/06_train_eval.py
```
*Metrics đánh giá chi tiết được tính toán và lưu vào `/citibike/serving/metrics` trên HDFS.*

---

#### BƯỚC 9: Xuất dữ liệu tầng Serving cho Dashboard (Job 07)
Tổng hợp dữ liệu đã xử lý từ HDFS và trích xuất các tệp Parquet kích thước nhẹ phục vụ tức thời cho Dashboard hiển thị:
```bash
docker exec -it citibike-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/07_export_serving.py
```
*Dữ liệu serving được lưu đồng bộ về: `data/serving/` và sao chép sang `dashboard/data/`.*

---

#### BƯỚC 10: Khởi động Web Dashboard Trực quan hóa & Cockpit Điều phối
Chạy ứng dụng giao diện Streamlit tại máy chủ:
```bash
python -m streamlit run dashboard/app.py
```
Mở trình duyệt web truy cập: **[http://localhost:8501](http://localhost:8501)**.

---

### PHƯƠNG ÁN C: Chạy các Thí nghiệm Big Data & MapReduce

Dành riêng cho mục đánh giá hiệu năng và minh họa kiến trúc Big Data (chiếm 2.0 điểm trong Rubric):

#### 1. Thí nghiệm So sánh Hiệu năng CSV vs Parquet (Job 08)
Đo đạc và đối sánh tốc độ đọc dữ liệu phân tán giữa CSV truyền thống và định dạng cột nén Parquet (minh chứng cho cơ chế Column Pruning và nén dữ liệu):
```bash
docker exec -it citibike-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/08_bigdata_experiments.py
```

#### 2. Thí nghiệm Hadoop Streaming MapReduce đếm chuyến theo trạm
Chạy ví dụ minh họa MapReduce cổ điển bằng Hadoop Streaming trên HDFS NameNode để so sánh đối chứng với Spark:
```bash
docker exec -it citibike-namenode hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
  -files /scripts/../jobs/mapreduce/mapper.py,/scripts/../jobs/mapreduce/reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /citibike/raw/year=2024/month=01 \
  -output /citibike/mr_output_test
```
Xem kết quả đếm của MapReduce trên HDFS:
```bash
docker exec -it citibike-namenode hdfs dfs -cat /citibike/mr_output_test/part-* | head -n 20
```

---

## 7. HƯỚNG DẪN THAO TÁC TRÊN GIAO DIỆN WEB DASHBOARD

Ứng dụng Streamlit được thiết kế theo phong cách Enterprise Cockpit với các khu vực chức năng chính:

### 1. Banner Đầu trang (Hero Banner)
- **Tiêu đề chính:** *Nền tảng Phân tích Dữ liệu lớn Phân tán & Dự báo Nhu cầu Thuê xe*.
- **Nhãn Dataset:** *Citi Bike NYC Open Data (Quy mô 14.9M+ bản ghi)*.
- **5 Micro-Card Kiến trúc:** Tóm tắt vai trò 5 trụ cột kỹ thuật: Hạ tầng lưu trữ HDFS, Động cơ tính toán Spark, Thuật toán MLlib, Quy trình không rò rỉ và Chân trời dự báo $h = 1$ Giờ.

### 2. Bộ 4 Thẻ KPI Tổng quan
- **Tổng lượt thuê nạp:** 1,888,085 chuyến (Tỷ lệ sạch 99.95%).
- **Quy mô phân tích:** 400 Trạm trọng điểm (Chiếm 58% lưu lượng toàn NYC).
- **Loại phương tiện:** 64.3% Xe đạp điện (Electric Bike).
- **Mô hình tốt nhất:** Random Forest (RMSE 2.73, Giảm 28% sai số).

### 3. Hệ thống 5 Tab Điều hướng Trải dài 100% Độ rộng
Thanh tab được thiết kế theo dạng **Segmented Control** màu xám bạc tinh tế, không có icon màu mè, hiển thị 5 module chuyên sâu:

- **Tab 1: Xu hướng Chuỗi thời gian**
  - *Biểu đồ 24 giờ:* So sánh phân bố nhu cầu Ngày thường vs Cuối tuần, có đánh dấu vạch màu xanh vùng **Cao điểm sáng (8h)** và **Cao điểm chiều (17-18h)**.
  - *Biểu đồ Thứ trong tuần:* Đo đạc nhu cầu các ngày trong tuần (Thứ 4, Thứ 5 và Thứ 6 đạt đỉnh).
  - *Ma trận nhiệt Heatmap:* Thể hiện mật độ nhu cầu trên lưới Không gian - Thời gian (Thứ $\times$ Giờ).

- **Tab 2: Bản đồ Điều phối Trực quan (GIS Folium)**
  - Bản đồ vệ tinh hiển thị các trạm xe tại New York.
  - Màu sắc vòng tròn biểu thị trạng thái tải:
    - **Màu Đỏ:** Trạm quá tải / Nguy cơ cháy xe ($\ge 15$ lượt/giờ).
    - **Màu Cam:** Trạm bận rộn ($7 - 15$ lượt/giờ).
    - **Màu Xanh:** Trạm bình thường ($< 7$ lượt/giờ).
  - Nhấp vào từng trạm để xem popup chi tiết tên trạm, mã trạm và lưu lượng giờ trung bình.

- **Tab 3: Điểm nóng Trạm (Hotspots)**
  - Biểu đồ xếp hạng Top 20 trạm có lưu lượng thuê cao nhất toàn thành phố.
  - Bảng dữ liệu chi tiết kèm tọa độ GPS (Vĩ độ, Kinh độ) phục vụ phân tích cụm địa lý.

- **Tab 4: Đấu trường Mô hình (Benchmark Leaderboard)**
  - Bảng xếp hạng Leaderboard phong cách Enterprise so sánh 8 thuật toán học máy.
  - Có huy hiệu vàng **#1 CHAMPION** cho Random Forest, huy hiệu Bạc **#2**, Đồng **#3** và nhãn **MỐC** cho các mô hình cơ sở.
  - Cột *Cải thiện RMSE* minh chứng Random Forest giảm **-27.9%** sai số.

- **Tab 5: Trình giả lập Dự báo 1 Giờ Tới (Smart Operations Cockpit)**
  - Bố cục 2 cột chuyên nghiệp:
    - **Cột Trái (Thiết lập Ngữ cảnh):** Cho phép người vận hành chọn Trạm, Thứ, Khung giờ cần dự báo (thanh trượt màu xanh), Lượt thuê 1h trước, Nhiệt độ và Thời tiết mưa.
    - **Cột Phải (Kết quả & Lệnh điều phối):**
      - 3 Thẻ Metric cập nhật tức thời: Dự báo số lượt xe, Mức độ nhu cầu (Nguy cơ thiếu xe / Ổn định / Dư thừa xe), Thuật toán thực thi.
      - Biểu đồ thanh ngang so sánh nhanh: `1h trước (Lag 1)` vs `Dự báo giờ tới` vs `Ngưỡng báo động trạm (18 xe)`.
      - **Phiếu Lệnh Điều phối Xe Khẩn cấp (Dispatch Directive):** Tự động sinh mã lệnh điều phối, thời hạn hoàn thành trước giờ cao điểm và số lượng xe tải cần chuyển đến bù.

---

## 8. CÁC CỔNG WEB UI QUẢN TRỊ CỤM & MINH CHỨNG BÁO CÁO

Khi cụm Docker đang hoạt động, có thể truy cập các đường dẫn sau trên trình duyệt để kiểm tra trạng thái và chụp màn hình làm tài liệu minh chứng cho Báo cáo Đồ án:

| Thành phần Quản trị | Địa chỉ Web UI | Mục đích sử dụng / Minh chứng báo cáo |
| :--- | :--- | :--- |
| **HDFS NameNode UI** | [http://localhost:9870](http://localhost:9870) | Xem tổng dung lượng HDFS, kiểm tra 2 Live DataNodes, duyệt cây thư mục Data Lake `/citibike/raw` |
| **HDFS DataNode 1** | [http://localhost:9864](http://localhost:9864) | Kiểm tra trạng thái lưu trữ của node dữ liệu số 1 |
| **HDFS DataNode 2** | [http://localhost:9865](http://localhost:9865) | Kiểm tra trạng thái lưu trữ của node dữ liệu số 2 |
| **Spark Master UI** | [http://localhost:8080](http://localhost:8080) | Xem danh sách các Workers đang active, trạng thái các Application/Jobs PySpark đang chạy hoặc đã hoàn thành |
| **Spark Worker 1** | [http://localhost:8081](http://localhost:8081) | Kiểm tra tài nguyên CPU Cores và RAM phân bổ trên Worker 1 |
| **Spark Worker 2** | [http://localhost:8082](http://localhost:8082) | Kiểm tra tài nguyên CPU Cores và RAM phân bổ trên Worker 2 |
| **Spark App UI** | [http://localhost:4040](http://localhost:4040) | Theo dõi chi tiết DAG, stages, tasks và execution plan khi job đang chạy |
| **Streamlit Dashboard** | [http://localhost:8501](http://localhost:8501) | Giao diện tương tác người dùng, trực quan hóa chuỗi thời gian và giả lập lệnh điều phối xe |

---

## 9. CÁC LỆNH DỪNG & DỌN DẸP HỆ THỐNG

Khi hoàn thành phiên làm việc hoặc bảo vệ đồ án:
```bash
# 1. Dừng ứng dụng Streamlit: Bấm phím Ctrl + C trên cửa sổ Terminal đang chạy app.py

# 2. Dừng cụm Docker nhưng bảo lưu toàn bộ dữ liệu trên HDFS volumes:
cd docker
docker compose down
cd ..

# 3. (Tùy chọn) Xóa sạch hoàn toàn container và ổ đĩa nếu muốn cài đặt lại từ đầu:
cd docker
docker compose down -v
cd ..
```

---
*Tài liệu hướng dẫn được biên soạn hoàn chỉnh phục vụ Đồ án Chuyên ngành Dữ liệu lớn (Big Data Processing & Analytics).*
