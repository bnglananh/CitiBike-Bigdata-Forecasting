"""
Job 05: Đánh giá mô hình cơ sở (Baseline Models)
- Đọc từ /citibike/features
- Triển khai 2 mô hình cơ sở:
  1. Seasonal Naive (dự báo bằng cùng giờ tuần trước: lag_168)
  2. Historical Mean (trung bình lịch sử theo station_id x dow x hour tính từ Train)
- Đánh giá bằng 4 độ đo: MAE, RMSE, R2, WMAPE
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.ml.evaluation import RegressionEvaluator
import time

def evaluate_metrics(df, pred_col='y_hat', label_col='label'):
    metrics = {}
    for m in ('mae', 'rmse', 'r2'):
        evaluator = RegressionEvaluator(labelCol=label_col, predictionCol=pred_col, metricName=m)
        metrics[m] = evaluator.evaluate(df)
    
    # Tính WMAPE = sum(|y - y_hat|) / sum(y)
    agg_res = df.agg(
        F.sum(F.abs(F.col(label_col) - F.col(pred_col))).alias('sum_ae'),
        F.sum(label_col).alias('sum_y')
    ).first()
    
    sum_y = agg_res['sum_y'] if agg_res['sum_y'] else 1.0
    metrics['wmape'] = (agg_res['sum_ae'] / sum_y) * 100 if sum_y > 0 else 0.0
    return metrics

def main():
    start_time = time.time()
    spark = (SparkSession.builder
             .appName('citibike-05-baseline')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.shuffle.partitions', '16')
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")
    BASE = 'hdfs://namenode:9000/citibike'
    print(f"=== [JOB 05] ĐÁNH GIÁ CÁC MÔ HÌNH CƠ SỞ (BASELINES) ===")

    feat = (spark.read.parquet(f"{BASE}/features")
            .withColumn('label', F.col('trips').cast('double')))

    train = feat.filter(F.col('split') == 'train').cache()
    val = feat.filter(F.col('split') == 'val').cache()
    test = feat.filter(F.col('split') == 'test').cache()

    train_n = train.count()
    val_n = val.count()
    test_n = test.count()
    print(f"-> Số bản ghi: Train={train_n:,} | Val={val_n:,} | Test={test_n:,}")

    target_eval = val if val_n > 0 else test
    eval_name = "Validation" if val_n > 0 else "Test"

    # 1. Baseline 1: Seasonal Naive (dự báo bằng lag_168)
    print(f"\n1. Đánh giá Seasonal Naive trên tập {eval_name}...")
    pred_naive = (target_eval
                  .withColumn('y_hat', F.greatest(F.col('lag_168').cast('double'), F.lit(0.0))))
    m_naive = evaluate_metrics(pred_naive)

    # 2. Baseline 2: Historical Mean (trung bình theo trạm, thứ, giờ từ Train)
    print(f"2. Đánh giá Historical Mean trên tập {eval_name}...")
    hist_table = (train.groupBy('station_id', 'dow', 'hour')
                  .agg(F.avg('label').alias('y_hat_hist')))
    
    pred_hist = (target_eval.join(hist_table, ['station_id', 'dow', 'hour'], 'left')
                 .fillna({'y_hat_hist': 0.0})
                 .withColumn('y_hat', F.greatest(F.col('y_hat_hist'), F.lit(0.0))))
    m_hist = evaluate_metrics(pred_hist)

    print("\n" + "="*65)
    print(f"KẾT QUẢ ĐÁNH GIÁ MÔ HÌNH CƠ SỞ (TẬP {eval_name.upper()}):")
    print("="*65)
    print(f"{'Mô hình':<22} | {'MAE':<8} | {'RMSE':<8} | {'R2':<8} | {'WMAPE (%)':<10}")
    print("-"*65)
    print(f"{'Seasonal Naive (lag168)':<22} | {m_naive['mae']:<8.3f} | {m_naive['rmse']:<8.3f} | {m_naive['r2']:<8.4f} | {m_naive['wmape']:<10.2f}")
    print(f"{'Historical Mean':<22} | {m_hist['mae']:<8.3f} | {m_hist['rmse']:<8.3f} | {m_hist['r2']:<8.4f} | {m_hist['wmape']:<10.2f}")
    print("="*65)

    duration = time.time() - start_time
    print(f"\n=== Hoàn tất Job 05 trong {duration:.2f} giây ===")
    spark.stop()

if __name__ == '__main__':
    main()
