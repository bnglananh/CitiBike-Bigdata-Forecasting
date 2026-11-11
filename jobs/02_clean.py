"""
Job 02: Tiền xử lý, làm sạch và chuẩn hóa schema dữ liệu (Data Cleaning)
- Đọc từ /citibike/raw
- Áp dụng 7 quy tắc nghiệp vụ làm sạch
- Xuất số liệu thống kê dòng vi phạm phục vụ lập Bảng 3.1 trong Báo cáo
- Ghi dữ liệu sạch sang định dạng Parquet tại /citibike/cleaned (phân vùng theo month)
"""

from pyspark.sql import SparkSession, functions as F, types as T
import time

def flag(cond):
    return F.coalesce(cond, F.lit(False))

def main():
    start_time = time.time()
    spark = (SparkSession.builder
             .appName('citibike-02-clean')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.shuffle.partitions', '16')
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")
    BASE = 'hdfs://namenode:9000/citibike'
    LO, END = '2024-01-01 00:00:00', '2025-01-01 00:00:00'
    FLAGS = ['f_null_time', 'f_bad_dur', 'f_fake_trip', 'f_geo']

    print(f"=== [JOB 02] TIỀN XỬ LÝ VÀ LÀM SẠCH DỮ LIỆU CITI BIKE ===")

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

    raw = spark.read.option('header', True).schema(schema).csv(f"{BASE}/raw")
    raw_count = raw.count()
    print(f"-> Số dòng thô ban đầu: {raw_count:,}")

    # 1. Loại trùng ride_id và chuyển đổi thời gian
    d = (raw.dropDuplicates(['ride_id'])
         .withColumn('started_at', F.col('started_at').cast('timestamp'))
         .withColumn('ended_at', F.col('ended_at').cast('timestamp'))
         .withColumn('dur_s', F.col('ended_at').cast('long') - F.col('started_at').cast('long')))

    # 2. Đánh cờ các vi phạm quy tắc làm sạch
    d = (d.withColumn('f_null_time', F.col('started_at').isNull() | F.col('ended_at').isNull())
           .withColumn('f_bad_dur', flag((F.col('dur_s') <= 0) | (F.col('dur_s') > 86400)))
           .withColumn('f_fake_trip', flag((F.col('dur_s') < 60) &
                                           (F.col('start_station_id') == F.col('end_station_id'))))
           .withColumn('f_geo', flag(~F.col('start_lat').between(40.5, 40.95) |
                                     ~F.col('start_lng').between(-74.1, -73.7)))
           .withColumn('f_no_station', F.col('start_station_id').isNull()))

    # 3. Quét một lần thống kê số dòng vi phạm (phục vụ Bảng 3.1 trong Báo cáo)
    print("-> Đang tính toán thống kê làm sạch (Lazy Evaluation single-pass)...")
    stats = d.agg(
        F.count('*').alias('after_dedup'),
        *[F.sum(F.col(c).cast('int')).alias(c) for c in FLAGS + ['f_no_station']]
    ).first().asDict()

    print("\n" + "="*50)
    print("BẢNG 3.1: KẾT QUẢ ÁP DỤNG CÁC QUY TẮC LÀM SẠCH")
    print("="*50)
    print(f"Tổng số bản ghi ban đầu:            {raw_count:,}")
    print(f"Sau khi khử trùng (ride_id):         {stats['after_dedup']:,}")
    print(f"Số dòng thiếu timestamp:             {stats['f_null_time']:,}")
    print(f"Số dòng thời lượng bất thường (<=0s hoặc >24h): {stats['f_bad_dur']:,}")
    print(f"Số dòng chuyến ảo (<60s cùng trạm):   {stats['f_fake_trip']:,}")
    print(f"Số dòng tọa độ ngoài New York:       {stats['f_geo']:,}")
    print(f"Số dòng thiếu start_station_id:      {stats['f_no_station']:,}")
    print("="*50)

    # 4. Lọc dữ liệu hợp lệ và ghi ra tầng Cleaned
    cleaned = (d.filter(~(F.col('f_null_time') | F.col('f_bad_dur') | F.col('f_fake_trip') | F.col('f_geo') | F.col('f_no_station')))
                .filter((F.col('started_at') >= LO) & (F.col('started_at') < END))
                .drop(*FLAGS, 'f_no_station'))

    clean_count = cleaned.count()
    print(f"-> Số dòng hợp lệ còn lại đưa vào tầng cleaned: {clean_count:,} ({clean_count/raw_count*100:.2f}%)")

    dest_path = f"{BASE}/cleaned"
    print(f"-> Ghi dữ liệu sạch ra Parquet tại: {dest_path}...")
    (cleaned.write
            .mode('overwrite')
            .partitionBy('month')
            .parquet(dest_path))

    duration = time.time() - start_time
    print(f"=== Hoàn tất Job 02 trong {duration:.2f} giây. Tầng /citibike/cleaned đã sẵn sàng! ===")
    spark.stop()

if __name__ == '__main__':
    main()
