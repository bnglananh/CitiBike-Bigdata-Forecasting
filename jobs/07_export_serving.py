"""
Job 07: Xuất dữ liệu tầng Serving cho Dashboard Streamlit
- Tổng hợp các bảng dữ liệu thống kê từ Spark
- Xuất các bảng Parquet nhỏ gọn vào /citibike/serving và lưu cục bộ vào dashboard/data
- Giúp Dashboard chạy hoàn toàn độc lập, siêu mượt mà không cần duy trì Spark khi demo
"""

from pyspark.sql import SparkSession, functions as F
import os
import shutil

def main():
    spark = (SparkSession.builder
             .appName('citibike-07-export-serving')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.shuffle.partitions', '16')
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")
    BASE = 'hdfs://namenode:9000/citibike'
    LOCAL_DATA_DIR = '/data/serving' # bên trong container, tương ứng với data/serving trên host
    print(f"=== [JOB 07] TỔNG HỢP VÀ XUẤT DỮ LIỆU TẦNG SERVING CHO DASHBOARD ===")

    hourly = spark.read.parquet(f"{BASE}/hourly")
    cleaned = spark.read.parquet(f"{BASE}/cleaned")

    # 1. Bảng 1: Nhu cầu theo giờ trong ngày (Tách ngày thường và cuối tuần)
    print("-> Xuất bảng 1: by_hour...")
    by_hour = (hourly
               .withColumn('h', F.hour('hour_ts'))
               .withColumn('dow', F.dayofweek('hour_ts'))
               .withColumn('day_type', F.when(F.col('dow').isin(1, 7), 'Cuối tuần').otherwise('Ngày thường'))
               .groupBy('day_type', 'h')
               .agg(F.avg('trips').alias('avg_trips'))
               .orderBy('day_type', 'h')
               .toPandas())

    # 2. Bảng 2: Nhu cầu theo các thứ trong tuần
    print("-> Xuất bảng 2: by_dow...")
    dow_names = {1: 'Chủ nhật', 2: 'Thứ 2', 3: 'Thứ 3', 4: 'Thứ 4', 5: 'Thứ 5', 6: 'Thứ 6', 7: 'Thứ 7'}
    by_dow = (hourly
              .withColumn('dow', F.dayofweek('hour_ts'))
              .groupBy('dow')
              .agg(F.avg('trips').alias('avg_trips'))
              .orderBy('dow')
              .toPandas())
    by_dow['dow_name'] = by_dow['dow'].map(dow_names)

    # 3. Bảng 3: Nhu cầu theo tháng
    print("-> Xuất bảng 3: by_month...")
    by_month = (hourly
                .withColumn('m', F.month('hour_ts'))
                .groupBy('m')
                .agg(F.sum('trips').alias('total_trips'), F.avg('trips').alias('avg_trips'))
                .orderBy('m')
                .toPandas())

    # 4. Bảng 4: Heatmap giờ x thứ
    print("-> Xuất bảng 4: heat...")
    heat = (hourly
            .withColumn('h', F.hour('hour_ts'))
            .withColumn('dow', F.dayofweek('hour_ts'))
            .groupBy('dow', 'h')
            .agg(F.avg('trips').alias('avg_trips'))
            .toPandas())
    heat['dow_name'] = heat['dow'].map(dow_names)

    # 5. Thông tin trạm (Tên, lat, lng)
    print("-> Trích xuất thông tin trạm...")
    stations = (cleaned.filter(F.col('start_station_id').isNotNull())
                .groupBy(F.col('start_station_id').alias('station_id'))
                .agg(F.expr('percentile_approx(start_lat, 0.5)').alias('lat'),
                     F.expr('percentile_approx(start_lng, 0.5)').alias('lng'),
                     F.first('start_station_name', ignorenulls=True).alias('station_name')))

    # 6. Bảng 5: Top 20 trạm đông nhất
    print("-> Xuất bảng 5: by_station (Top 20)...")
    by_station = (hourly.groupBy('station_id')
                  .agg(F.sum('trips').alias('total_trips'))
                  .join(stations, 'station_id', 'left')
                  .orderBy(F.desc('total_trips'))
                  .limit(20)
                  .toPandas())

    # 7. Bảng 6: Bản đồ trạm theo từng giờ (Folium)
    print("-> Xuất bảng 6: station_hour_avg...")
    st_hour = (hourly
               .withColumn('h', F.hour('hour_ts'))
               .groupBy('station_id', 'h')
               .agg(F.avg('trips').alias('avg_trips'))
               .join(stations, 'station_id', 'inner')
               .filter(F.col('lat').isNotNull() & F.col('lng').isNotNull())
               .toPandas())

    # Lưu ra thư mục mount /data/serving
    target_dir = '/data/serving' if os.path.exists('/data') else 'data/serving'
    os.makedirs(target_dir, exist_ok=True)

    by_hour.to_parquet(f"{target_dir}/by_hour.parquet", index=False)
    by_dow.to_parquet(f"{target_dir}/by_dow.parquet", index=False)
    by_month.to_parquet(f"{target_dir}/by_month.parquet", index=False)
    heat.to_parquet(f"{target_dir}/heat.parquet", index=False)
    by_station.to_parquet(f"{target_dir}/by_station.parquet", index=False)
    st_hour.to_parquet(f"{target_dir}/station_hour_avg.parquet", index=False)

    # 8. Bảng 7: Xuất bảng đánh giá mô hình MLlib từ HDFS
    try:
        metrics_df = spark.read.parquet(f"{BASE}/serving/metrics").toPandas()
        metrics_df.to_parquet(f"{target_dir}/metrics.parquet", index=False)
        print("-> Đã xuất bảng metrics.parquet thành công!")
    except Exception as e:
        print(f"-> Ghi chú metrics: {e}")

    print(f"\n✓ Đã xuất thành công các bảng tổng hợp vào thư mục {target_dir}/!")
    print("=== Hoàn tất Job 07 ===")
    spark.stop()

if __name__ == '__main__':
    main()
