@echo off
chcp 65001 >nul
title Citi Bike Big Data - Control Panel
set "PATH=%PATH%;C:\Program Files\Docker\Docker\resources\bin"
cd /d "%~dp0\.."

:MENU
cls
echo ╔═══════════════════════════════════════════════════════════════════════════╗
echo ║              🚲 CITI BIKE BIG DATA - BẢNG ĐIỀU KHIỂN HỆ THỐNG            ║
echo ║         Kiến trúc: Hadoop HDFS 3.3.6  x  Apache Spark MLlib 3.5.1         ║
echo ╚═══════════════════════════════════════════════════════════════════════════╝
echo.
echo  ┌─ [PHẦN I: THU THẬP & LƯU TRỮ PHÂN TÁN HDFS] ───────────────────────────┐
echo  │  [1] Tải dữ liệu Citi Bike (Tháng 01/2024 hoặc 12 tháng)                │
echo  │  [2] Nạp dữ liệu CSV vào cụm HDFS tầng raw (/citibike/raw)             │
echo  └────────────────────────────────────────────────────────────────────────┘
echo  ┌─ [PHẦN II: TIỀN XỬ LÝ & ĐẶC TRƯNG CHUỖI THỜI GIAN VỚI SPARK] ─────────┐
echo  │  [3] Job 01: Khảo sát dữ liệu thô (Schema, Volume, Veracity, Variety)  │
echo  │  [4] Job 02: Tiền xử lý & Làm sạch (Lọc chuyến ảo, xuất Bảng 3.1)       │
echo  │  [5] Job 03: Lưới (Trạm, Giờ) & Kỹ thuật Zero-filling điền số 0         │
echo  │  [6] Job 04: Trích xuất đặc trưng Lags, Sin/Cos & Chia tập không rò rỉ  │
echo  └────────────────────────────────────────────────────────────────────────┘
echo  ┌─ [PHẦN III: HỌC MÁY MLLIB & CÁC THÍ NGHIỆM BIG DATA] ──────────────────┐
echo  │  [7] Job 05: Đánh giá mô hình cơ sở (Seasonal Naive & Historical Mean)  │
echo  │  [8] Job 06: Huấn luyện 6 mô hình Spark MLlib (Linear, Tree, RF, GBT)  │
echo  │  [9] Job 08: Thí nghiệm Big Data (So sánh tốc độ CSV vs Parquet)       │
echo  └────────────────────────────────────────────────────────────────────────┘
echo  ┌─ [PHẦN IV: XUẤT PHỤC VỤ & DASHBOARD TRỰC QUAN HÓA] ────────────────────┐
echo  │  [10] Job 07: Xuất dữ liệu tầng Serving cho Dashboard                   │
echo  │  [11] Mở Dashboard Streamlit trên trình duyệt (http://localhost:8501)   │
echo  └────────────────────────────────────────────────────────────────────────┘
echo  ┌─ [TIỆN ÍCH] ───────────────────────────────────────────────────────────┐
echo  │  [A]  CHẠY TỰ ĐỘNG TOÀN BỘ QUY TRÌNH (Từ bước 3 đến bước 10)            │
echo  │  [0]  Thoát                                                            │
echo  └────────────────────────────────────────────────────────────────────────┘
echo.
set /p choice="👉 Nhập lựa chọn của bạn (0-11 hoặc A): "

if /i "%choice%"=="A" goto RUN_ALL
if "%choice%"=="1" goto DOWNLOAD
if "%choice%"=="2" goto LOAD_HDFS
if "%choice%"=="3" goto JOB_01
if "%choice%"=="4" goto JOB_02
if "%choice%"=="5" goto JOB_03
if "%choice%"=="6" goto JOB_04
if "%choice%"=="7" goto JOB_05
if "%choice%"=="8" goto JOB_06
if "%choice%"=="9" goto JOB_08
if "%choice%"=="10" goto JOB_07
if "%choice%"=="11" goto DASHBOARD
if "%choice%"=="0" exit /b 0
goto MENU

:DOWNLOAD
echo.
echo [1/2] Đang tải dữ liệu tháng 01/2024...
python scripts\download_data.py --months 1
pause
goto MENU

:LOAD_HDFS
echo.
echo [2/2] Đang đẩy dữ liệu lên HDFS...
python scripts\load_to_hdfs.py --months 1
pause
goto MENU

:JOB_01
echo.
echo -> Đang chạy Job 01: Khảo sát dữ liệu thô trên Spark...
docker exec -it citibike-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/01_explore.py
pause
goto MENU

:JOB_02
echo.
echo -> Đang chạy Job 02: Tiền xử lý & Làm sạch dữ liệu...
docker exec -it citibike-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/02_clean.py
pause
goto MENU

:JOB_03
echo.
echo -> Đang chạy Job 03: Lưới (Trạm, Giờ) & Kỹ thuật Zero-filling...
docker exec -it citibike-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/03_hourly.py
pause
goto MENU

:JOB_04
echo.
echo -> Đang chạy Job 04: Trích xuất đặc trưng & Chia tập không rò rỉ...
docker exec -it citibike-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/04_features.py
pause
goto MENU

:JOB_05
echo.
echo -> Đang chạy Job 05: Đánh giá mô hình cơ sở (Baselines)...
docker exec -it citibike-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/05_baseline.py
pause
goto MENU

:JOB_06
echo.
echo -> Đang chạy Job 06: Huấn luyện 6 mô hình Spark MLlib...
docker exec -it citibike-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/06_train_eval.py
pause
goto MENU

:JOB_08
echo.
echo -> Đang chạy Job 08: Thí nghiệm Big Data (CSV vs Parquet)...
docker exec -it citibike-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/08_bigdata_experiments.py
pause
goto MENU

:JOB_07
echo.
echo -> Đang chạy Job 07: Xuất dữ liệu Serving cho Dashboard...
docker exec -it citibike-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/07_export_serving.py
powershell -Command "Copy-Item -Path 'data\serving\*' -Destination 'dashboard\data' -Force -ErrorAction SilentlyContinue"
echo ✓ Đã đồng bộ bảng phục vụ sang dashboard\data!
pause
goto MENU

:RUN_ALL
echo.
echo ====================================================================
echo        BẮT ĐẦU CHẠY TỰ ĐỘNG TOÀN BỘ QUY TRÌNH (TỪ JOB 01 ĐẾN JOB 07)
echo ====================================================================
echo.
echo [1/7] Chạy Job 01 Khảo sát...
docker exec citibike-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/01_explore.py
echo.
echo [2/7] Chạy Job 02 Làm sạch...
docker exec citibike-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/02_clean.py
echo.
echo [3/7] Chạy Job 03 Lưới giờ & Điền 0...
docker exec citibike-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/03_hourly.py
echo.
echo [4/7] Chạy Job 04 Đặc trưng...
docker exec citibike-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/04_features.py
echo.
echo [5/7] Chạy Job 05 Baselines...
docker exec citibike-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/05_baseline.py
echo.
echo [6/7] Chạy Job 06 Train MLlib...
docker exec citibike-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/06_train_eval.py
echo.
echo [7/7] Chạy Job 07 Xuất Serving...
docker exec citibike-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/07_export_serving.py
powershell -Command "Copy-Item -Path 'data\serving\*' -Destination 'dashboard\data' -Force -ErrorAction SilentlyContinue"
echo.
echo ====================================================================
echo   ✓ TOÀN BỘ PIPELINE ĐÃ CHẠY HOÀN TẤT THÀNH CÔNG!
echo ====================================================================
pause
goto MENU

:DASHBOARD
echo.
echo -> Đang mở Dashboard trên trình duyệt (http://localhost:8501)...
start http://localhost:8501
python -m streamlit run dashboard\app.py
pause
goto MENU
