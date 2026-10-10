"""
Job 08: Thực hiện các Thí nghiệm Big Data (Chiếm 2.0 điểm Rubric)
- Thí nghiệm 1: So sánh hiệu năng CSV vs Parquet (Dung lượng lưu trữ và Tốc độ truy vấn cột)
- Thí nghiệm 2: Đánh giá khả năng mở rộng (Scale-up) theo số lượng Cores và Partitions
- Xuất số liệu đối chiếu cho Báo cáo Chương 3 (mục 3.8)
"""

from pyspark.sql import SparkSession, functions as F, types as T
import time
import statistics

def main():
    spark = (SparkSession.builder
             .appName('citibike-08-experiments')
             .config('spark.sql.session.timeZone', 'UTC')
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")
    BASE = 'hdfs://namenode:9000/citibike'
    print(f"=== [JOB 08] BẮT ĐẦU CÁC THÍ NGHIỆM BIG DATA (CHƯƠNG 3) ===")

    # ========================================================
    # THÍ NGHIỆM 1: SO SÁNH CSV VÀ PARQUET (LƯU TRỮ VÀ TỐC ĐỘ)
    # ========================================================
    print("\n" + "="*70)
    print("THÍ NGHIỆM 1: ĐỐI CHIẾU HIỆU NĂNG LƯU TRỮ & TRUY VẤN (CSV VS PARQUET)")
    print("="*70)

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

    print("-> Đọc dữ liệu raw CSV và ghi một bản Parquet nguyên gốc để so sánh cùng số dòng...")
    raw_csv = spark.read.option('header', True).schema(schema).csv(f"{BASE}/raw")
    raw_parquet_path = f"{BASE}/raw_parquet_benchmark"
    raw_csv.write.mode('overwrite').parquet(raw_parquet_path)

    df_csv = spark.read.option('header', True).schema(schema).csv(f"{BASE}/raw")
    df_pq = spark.read.parquet(raw_parquet_path)

    # Truy vấn phân tích: Gom nhóm theo start_station_id (Chỉ cần 1 cột -> Tận dụng Column Pruning của Parquet)
    def run_query(df):
        return df.groupBy('start_station_id').count().orderBy(F.desc('count')).limit(10).collect()

    # Chạy 3 lần lấy trung vị (tránh lệch do JVM warmup)
    times = {'CSV': [], 'Parquet': []}
    for name, df in [('CSV', df_csv), ('Parquet', df_pq)]:
        print(f"-> Đang đo thời gian truy vấn trên định dạng {name} (3 lượt)...")
        for i in range(3):
            t0 = time.time()
            run_query(df)
            dur = time.time() - t0
            times[name].append(dur)
            print(f"   Lượt {i+1}: {dur:.2f}s")

    med_csv = statistics.median(times['CSV'])
    med_pq = statistics.median(times['Parquet'])
    speedup = med_csv / med_pq if med_pq > 0 else 1.0

    print("\nKẾT QUẢ THÍ NGHIỆM 1:")
    print(f"- Thời gian truy vấn CSV (Median):     {med_csv:.2f} giây")
    print(f"- Thời gian truy vấn Parquet (Median): {med_pq:.2f} giây")
    print(f"- Tốc độ tăng tốc (Speedup):           {speedup:.2f}x lần nhanh hơn nhờ Column Pruning")
    print("="*70)

    print("\n=== Hoàn tất Thí nghiệm 1! ===")
    spark.stop()

if __name__ == '__main__':
    main()
