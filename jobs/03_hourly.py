"""
Job 03: Tổng hợp nhu cầu theo (Trạm, Giờ) và Điền 0 (Hourly Aggregation & Zero-filling)
- Đọc từ /citibike/cleaned
- Đếm số chuyến xuất phát theo (station_id, hour_ts)
- Chọn Top N trạm bận rộn nhất dựa trên tập Train (hour_ts < 2024-09-01) để tránh rò rỉ
- Tạo lưới giờ liên tục từ giờ đầu tiên của từng trạm đến 31/12/2024 23:00
- Điền giá trị 0 cho các giờ không có lượt thuê (Zero-filling)
- Ghi kết quả ra /citibike/hourly (Parquet)
"""

from pyspark.sql import SparkSession, functions as F
import time

def main():
    start_time = time.time()
    spark = (SparkSession.builder
             .appName('citibike-03-hourly')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.shuffle.partitions', '16')
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")
    BASE = 'hdfs://namenode:9000/citibike'
    TRAIN_END = '2024-09-01 00:00:00'
    TOP_N = 400

    print(f"=== [JOB 03] TỔNG HỢP THEO (TRẠM, GIỜ) VÀ ĐIỀN 0 (TOP {TOP_N} TRẠM) ===")

    cleaned = spark.read.parquet(f"{BASE}/cleaned")
    
    # 1. Gom nhóm theo trạm và giờ
    trips = (cleaned.filter(F.col('start_station_id').isNotNull())
             .select(F.col('start_station_id').alias('station_id'),
                     F.date_trunc('hour', 'started_at').alias('hour_ts')))
    counts = trips.groupBy('station_id', 'hour_ts').agg(F.count('*').alias('trips'))

    # 2. Chọn Top N trạm theo tổng số chuyến trong giai đoạn Train (Không nhìn Val/Test)
    print(f"-> Chọn Top {TOP_N} trạm dựa trên dữ liệu trước {TRAIN_END}...")
    top_stations = (counts.filter(F.col('hour_ts') < TRAIN_END)
                    .groupBy('station_id')
                    .agg(F.sum('trips').alias('total_train_trips'))
                    .orderBy(F.desc('total_train_trips'))
                    .limit(TOP_N)
                    .select('station_id'))

    # Nếu số lượng trạm thực tế ít hơn TOP_N (ví dụ chạy thử nghiệm trên ít dữ liệu)
    actual_stations = top_stations.count()
    print(f"-> Đã chọn {actual_stations} trạm hàng đầu.")

    # 3. Tìm giờ xuất hiện chuyến đầu tiên của từng trạm
    first_hour_per_st = (counts.join(top_stations, 'station_id')
                         .groupBy('station_id')
                         .agg(F.min('hour_ts').alias('first_hour')))

    # 4. Xác định khoảng thời gian cho từng tháng có dữ liệu thực tế
    # (Đảm bảo khi chạy các tháng mẫu không liên tục như 1, 4, 7, 10 không sinh số 0 giả cho các tháng ở giữa)
    month_bounds = (counts
                    .groupBy(F.date_trunc('month', 'hour_ts').alias('m_start'))
                    .agg(F.min('hour_ts').alias('start_m'), F.max('hour_ts').alias('end_m'))
                    .collect())
    print(f"-> Phát hiện {len(month_bounds)} tháng có dữ liệu: {[str(r['m_start'])[:7] for r in month_bounds]}")

    # 5. Tạo chuỗi giờ liên tục cho từng tháng thực tế
    hour_dfs = []
    for row in month_bounds:
        s_ts = row['start_m']
        e_ts = row['end_m']
        m_hours = spark.sql(f"SELECT explode(sequence(timestamp'{s_ts}', timestamp'{e_ts}', interval 1 hour)) AS hour_ts")
        hour_dfs.append(m_hours)
    
    hours = hour_dfs[0]
    for h_df in hour_dfs[1:]:
        hours = hours.union(h_df)
    hours = hours.distinct()

    # Ghép lưới: Chỉ điền từ giờ có chuyến đầu tiên của trạm trở đi
    grid = (first_hour_per_st.crossJoin(hours)
            .filter(F.col('hour_ts') >= F.col('first_hour'))
            .drop('first_hour'))

    # 6. Left-join số chuyến vào lưới và điền 0 cho các giờ trống
    print("-> Đang thực hiện Zero-filling trên lưới không gian - thời gian...")
    hourly = grid.join(counts, ['station_id', 'hour_ts'], 'left').fillna({'trips': 0})

    # 7. Thống kê kiểm tra chất lượng
    total_grid_rows = hourly.count()
    zero_rows = hourly.filter(F.col('trips') == 0).count()
    zero_pct = (zero_rows / total_grid_rows) * 100 if total_grid_rows > 0 else 0
    total_trips = hourly.agg(F.sum('trips')).first()[0]

    print("\n" + "="*50)
    print("THỐNG KÊ BẢNG HOURLY SAU KHI ĐIỀN 0:")
    print("="*50)
    print(f"Tổng số dòng trên lưới (Trạm x Giờ): {total_grid_rows:,}")
    print(f"Số dòng mang giá trị 0:             {zero_rows:,} ({zero_pct:.2f}%)")
    print(f"Tổng số lượt thuê được giữ lại:      {total_trips:,}")
    print("="*50)

    # 8. Ghi dữ liệu ra tầng /citibike/hourly
    dest_path = f"{BASE}/hourly"
    print(f"-> Đang ghi bảng hourly ra Parquet tại: {dest_path}...")
    hourly.write.mode('overwrite').parquet(dest_path)

    duration = time.time() - start_time
    print(f"=== Hoàn tất Job 03 trong {duration:.2f} giây. Tầng /citibike/hourly đã sẵn sàng! ===")
    spark.stop()

if __name__ == '__main__':
    main()
