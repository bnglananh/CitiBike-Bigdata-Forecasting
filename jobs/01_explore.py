"""
Job 01: Khảo sát dữ liệu thô trên HDFS (Exploration)
- Đọc dữ liệu từ hdfs://namenode:9000/citibike/raw
- Kiểm tra tính nhất quán của schema giữa các tháng
- Thống kê Volume (tổng số dòng, số dòng theo tháng)
- Thống kê Veracity (tỷ lệ thiếu/null theo từng cột)
- Thống kê Variety (phân bố loại xe, loại người dùng)
"""

from pyspark.sql import SparkSession, functions as F, types as T
import json
import time

def main():
    start_time = time.time()
    spark = (SparkSession.builder
             .appName('citibike-01-explore')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.shuffle.partitions', '16')
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")
    BASE = 'hdfs://namenode:9000/citibike/raw'
    print(f"=== [JOB 01] KHẢO SÁT DỮ LIỆU THÔ CITI BIKE TẠI: {BASE} ===")

    # 1. Định nghĩa Schema tường minh (nhanh hơn inferSchema, tránh lệch kiểu dữ liệu)
    schema = T.StructType([
        T.StructField('ride_id', T.StringType(), True),
        T.StructField('rideable_type', T.StringType(), True),
        T.StructField('started_at', T.StringType(), True),
        T.StructField('ended_at', T.StringType(), True),
        T.StructField('start_station_name', T.StringType(), True),
        T.StructField('start_station_id', T.StringType(), True),
        T.StructField('end_station_name', T.StringType(), True),
        T.StructField('end_station_id', T.StringType(), True),
        T.StructField('start_lat', T.DoubleType(), True),
        T.StructField('start_lng', T.DoubleType(), True),
        T.StructField('end_lat', T.DoubleType(), True),
        T.StructField('end_lng', T.DoubleType(), True),
        T.StructField('member_casual', T.StringType(), True),
    ])

    # 2. Đọc dữ liệu thô từ HDFS (Spark tự nhận diện phân vùng year=..., month=...)
    print("-> Đang đọc dữ liệu từ HDFS...")
    raw = spark.read.option('header', True).schema(schema).csv(BASE)

    total_records = raw.count()
    print(f"\n[VOLUME] Tổng số bản ghi (chuyến đi) thô: {total_records:,}")

    # 3. Phân bố theo tháng
    print("\n[VOLUME THEO THÁNG]:")
    raw.groupBy('month').count().orderBy('month').show()

    # 4. Kiểm tra tỷ lệ giá trị thiếu (NULL/Missing) cho từng cột (Veracity)
    print("\n[VERACITY] Thống kê số lượng giá trị NULL theo từng cột:")
    null_exprs = [F.count(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), c)).alias(c) for c in raw.columns]
    null_df = raw.select(null_exprs)
    null_df.show()

    # 5. Phân bố loại xe và loại thành viên (Variety)
    print("\n[VARIETY] Phân bố loại xe (rideable_type):")
    raw.groupBy('rideable_type').count().orderBy(F.desc('count')).show()

    print("\n[VARIETY] Phân bố loại khách hàng (member_casual):")
    raw.groupBy('member_casual').count().orderBy(F.desc('count')).show()

    # 6. Đếm số lượng trạm xuất phát khác nhau
    num_stations = raw.filter(F.col('start_station_id').isNotNull()).select('start_station_id').distinct().count()
    print(f"\n[KHÔNG GIAN] Số lượng trạm xuất phát khác nhau (distinct start_station_id): {num_stations}")

    # 7. Khoảng thời gian thực tế của dữ liệu
    time_range = raw.select(
        F.min('started_at').alias('min_start'),
        F.max('started_at').alias('max_start')
    ).collect()[0]
    print(f"\n[THỜI GIAN] Chuyến sớm nhất: {time_range['min_start']} | Chuyến muộn nhất: {time_range['max_start']}")

    duration = time.time() - start_time
    print(f"\n=== Hoàn tất Job 01 sau {duration:.2f} giây ===")
    spark.stop()

if __name__ == '__main__':
    main()
