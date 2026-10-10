"""
Job 04: Trích xuất đặc trưng và chia tập dữ liệu chống rò rỉ (Feature Engineering & Splitting)
- Đọc từ /citibike/hourly và /citibike/cleaned
- Tạo đặc trưng thời gian (hour, dow, month, is_weekend, is_peak)
- Biến đổi chu kỳ hình sin/cosin (hour_sin, hour_cos, dow_sin, dow_cos)
- Tính đặc trưng chuỗi thời gian (lag_1, lag_24, lag_168, roll_mean_3)
- Chia tập theo mốc thời gian: Train (01-08), Validation (09-10), Test (11-12)
- Trích xuất tọa độ không gian (lat, lng) và station_mean CHỈ tính từ tập Train
- Ghi ra /citibike/features (phân vùng theo split)
"""

from pyspark.sql import SparkSession, functions as F, Window
import math
import time

def main():
    start_time = time.time()
    spark = (SparkSession.builder
             .appName('citibike-04-features')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.shuffle.partitions', '16')
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")
    BASE = 'hdfs://namenode:9000/citibike'
    print(f"=== [JOB 04] TRÍCH XUẤT ĐẶC TRƯNG CHỐNG RÒ RỈ DỮ LIỆU ===")

    hourly = spark.read.parquet(f"{BASE}/hourly")
    cleaned = spark.read.parquet(f"{BASE}/cleaned")

    # 1. Thiết lập Window function theo trạm sắp xếp theo thời gian
    num_months = hourly.select(F.month('hour_ts')).distinct().count()
    if num_months >= 10:
        print(f"-> [Chế độ đủ 12 tháng liên tục ({num_months} tháng)]: Window toàn chuỗi & Split theo quý...")
        w = Window.partitionBy('station_id').orderBy('hour_ts')
    else:
        print(f"-> [Chế độ mẫu theo mùa ({num_months} tháng)]: Window theo từng tháng để lag không bị gián đoạn giữa các mùa...")
        w = Window.partitionBy('station_id', F.year('hour_ts'), F.month('hour_ts')).orderBy('hour_ts')

    print("-> Đang tính toán các đặc trưng thời gian, chu kỳ và lag...")
    feat = (hourly
      .withColumn('hour', F.hour('hour_ts'))
      .withColumn('dow', F.dayofweek('hour_ts')) # 1: CN, 2: T2, ..., 7: T7
      .withColumn('month', F.month('hour_ts'))
      .withColumn('is_weekend', F.col('dow').isin(1, 7).cast('int'))
      .withColumn('is_peak', ((F.col('is_weekend') == 0) &
                              F.col('hour').isin(7, 8, 9, 17, 18, 19)).cast('int'))
      .withColumn('hour_sin', F.sin(2 * math.pi * F.col('hour') / 24))
      .withColumn('hour_cos', F.cos(2 * math.pi * F.col('hour') / 24))
      .withColumn('dow_sin', F.sin(2 * math.pi * (F.col('dow') - 1) / 7))
      .withColumn('dow_cos', F.cos(2 * math.pi * (F.col('dow') - 1) / 7))
      .withColumn('lag_1', F.lag('trips', 1).over(w))
      .withColumn('lag_24', F.lag('trips', 24).over(w))
      .withColumn('lag_168', F.lag('trips', 168).over(w))
      .withColumn('roll_mean_3', F.avg('trips').over(w.rowsBetween(-3, -1)))
      .filter(F.col('lag_168').isNotNull())) # Bỏ 168 giờ đầu của mỗi trạm vì không đủ dữ liệu lag

    # 2. Phân chia tập dữ liệu thích ứng (Adaptive Time Split)
    if num_months >= 10:
        print("-> [Chia tập 12 tháng]: Train (01-08/2024), Val (09-10/2024), Test (11-12/2024)")
        feat = (feat
          .withColumn('split', F.when(F.col('hour_ts') < '2024-09-01', 'train')
                                .when(F.col('hour_ts') < '2024-11-01', 'val')
                                .otherwise('test')))
    else:
        print(f"-> [Chia tập theo mùa/ngày]: Mỗi mùa đều có Train (ngày 1-20), Val (ngày 21-25), Test (ngày 26-cuối)...")
        feat = (feat
          .withColumn('split', F.when(F.dayofmonth('hour_ts') <= 20, 'train')
                                .when(F.dayofmonth('hour_ts') <= 25, 'val')
                                .otherwise('test')))

    # 2. Tính station_mean CHỈ trên tập Train (Ngăn chặn Data Leakage từ Val/Test)
    print("-> Tính station_mean nghiêm ngặt từ tập Train...")
    st_mean = (feat.filter(F.col('split') == 'train')
               .groupBy('station_id')
               .agg(F.avg('trips').alias('station_mean')))

    # 3. Lấy thông tin tọa độ và tên trạm (trung vị tọa độ) từ bảng cleaned
    stations = (cleaned.filter(F.col('start_station_id').isNotNull())
                .groupBy(F.col('start_station_id').alias('station_id'))
                .agg(F.expr('percentile_approx(start_lat, 0.5)').alias('lat'),
                     F.expr('percentile_approx(start_lng, 0.5)').alias('lng'),
                     F.first('start_station_name', ignorenulls=True).alias('station_name')))

    # 4. Kết hợp tất cả đặc trưng
    feat_final = (feat.join(st_mean, 'station_id', 'left')
                      .fillna({'station_mean': 0.0})
                      .join(stations, 'station_id', 'left'))

    # 5. Thống kê phân chia tập dữ liệu
    print("\n" + "="*50)
    print("PHÂN CHIA TẬP DỮ LIỆU THEO THỜI GIAN (TIME-BASED SPLIT):")
    print("="*50)
    feat_final.groupBy('split').count().show()
    print("="*50)

    # 6. Ghi ra tầng features
    dest_path = f"{BASE}/features"
    print(f"-> Ghi tập đặc trưng ra Parquet tại: {dest_path}...")
    (feat_final.write
               .mode('overwrite')
               .partitionBy('split')
               .parquet(dest_path))

    duration = time.time() - start_time
    print(f"=== Hoàn tất Job 04 trong {duration:.2f} giây. Tầng /citibike/features đã sẵn sàng! ===")
    spark.stop()

if __name__ == '__main__':
    main()
