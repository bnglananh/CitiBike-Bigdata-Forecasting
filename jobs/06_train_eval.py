"""
Job 06: Huấn luyện và Đánh giá 6 Mô hình Machine Learning (Spark MLlib)
- Nhóm tuyến tính: Linear Regression, Ridge, Lasso (với StandardScaler)
- Nhóm cây quyết định: Decision Tree, Random Forest, GBT
- Đánh giá trên Validation để chọn siêu tham số tốt nhất
- Đánh giá cuối cùng trên Test
- Lưu bảng kết quả so sánh và dự báo ra tầng predictions & serving
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import (LinearRegression, DecisionTreeRegressor,
                                   RandomForestRegressor, GBTRegressor)
from pyspark.ml.evaluation import RegressionEvaluator
import time
import os

FEATURES = [
    'hour', 'dow', 'month', 'is_weekend', 'is_peak',
    'hour_sin', 'hour_cos', 'dow_sin', 'dow_cos',
    'lag_1', 'lag_24', 'lag_168', 'roll_mean_3',
    'lat', 'lng', 'station_mean'
]

def evaluate_df(df, pred_col='y_hat', label_col='label'):
    metrics = {}
    for m in ('mae', 'rmse', 'r2'):
        evaluator = RegressionEvaluator(labelCol=label_col, predictionCol=pred_col, metricName=m)
        metrics[m] = evaluator.evaluate(df)
    
    agg_res = df.agg(
        F.sum(F.abs(F.col(label_col) - F.col(pred_col))).alias('sum_ae'),
        F.sum(label_col).alias('sum_y')
    ).first()
    sum_y = agg_res['sum_y'] if agg_res['sum_y'] else 1.0
    metrics['wmape'] = (agg_res['sum_ae'] / sum_y) * 100 if sum_y > 0 else 0.0
    return metrics

def tree_pipe(est):
    return Pipeline(stages=[VectorAssembler(inputCols=FEATURES, outputCol='features'), est])

def lin_pipe(est):
    return Pipeline(stages=[
        VectorAssembler(inputCols=FEATURES, outputCol='raw_features'),
        StandardScaler(inputCol='raw_features', outputCol='features', withMean=True, withStd=True),
        est
    ])

def main():
    total_start = time.time()
    spark = (SparkSession.builder
             .appName('citibike-06-train-eval')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.shuffle.partitions', '16')
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")
    BASE = 'hdfs://namenode:9000/citibike'
    print(f"=== [JOB 06] HUẤN LUYỆN VÀ ĐÁNH GIÁ 6 MÔ HÌNH SPARK MLLIB ===")

    feat = (spark.read.parquet(f"{BASE}/features")
            .withColumn('label', F.col('trips').cast('double'))
            .dropna(subset=FEATURES + ['label']))

    train = feat.filter(F.col('split') == 'train').cache()
    val = feat.filter(F.col('split') == 'val').cache()
    test = feat.filter(F.col('split') == 'test').cache()

    train.count() # Trigger caching
    eval_set = val if val.count() > 0 else test
    eval_name = "Validation" if val.count() > 0 else "Test"

    # Lưới tham số rút gọn phù hợp với máy cá nhân
    GRID = {
        'Linear Regression': (lambda p: lin_pipe(LinearRegression(regParam=0.0)), [{}]),
        'Ridge Regression':  (lambda p: lin_pipe(LinearRegression(regParam=p['r'], elasticNetParam=0.0)),
                              [{'r': 0.01}, {'r': 0.1}]),
        'Lasso Regression':  (lambda p: lin_pipe(LinearRegression(regParam=p['r'], elasticNetParam=1.0)),
                              [{'r': 0.001}, {'r': 0.01}]),
        'Decision Tree':     (lambda p: tree_pipe(DecisionTreeRegressor(maxDepth=p['d'], seed=42)),
                              [{'d': 6}, {'d': 10}]),
        'Random Forest':     (lambda p: tree_pipe(RandomForestRegressor(numTrees=p['t'], maxDepth=p['d'], seed=42)),
                              [{'t': 20, 'd': 8}, {'t': 40, 'd': 10}]),
        'GBT Regressor':     (lambda p: tree_pipe(GBTRegressor(maxIter=p['i'], maxDepth=p['d'], stepSize=0.1, seed=42)),
                              [{'i': 20, 'd': 5}, {'i': 40, 'd': 6}]),
    }

    results = []
    best_models = {}
    best_preds = []

    print("\n-> Bắt đầu huấn luyện và tinh chỉnh trên tập dữ liệu...")
    for model_name, (build_pipe, param_list) in GRID.items():
        best_rmse = float('inf')
        best_record = None
        best_trained_model = None

        for params in param_list:
            t0 = time.time()
            pipe = build_pipe(params)
            trained_model = pipe.fit(train)
            fit_time = time.time() - t0

            # Dự báo và ép giá trị âm về 0
            raw_pred = trained_model.transform(eval_set)
            pred = raw_pred.withColumn('y_hat', F.greatest(F.col('prediction'), F.lit(0.0)))
            
            m = evaluate_df(pred)
            record = {
                'model': model_name,
                'params': str(params),
                'fit_time_s': round(fit_time, 2),
                **m
            }
            print(f"   [{model_name}] Params={params} -> RMSE={m['rmse']:.3f}, MAE={m['mae']:.3f}, R2={m['r2']:.4f} ({fit_time:.1f}s)")

            if m['rmse'] < best_rmse:
                best_rmse = m['rmse']
                best_record = record
                best_trained_model = trained_model

        results.append(best_record)
        best_models[model_name] = best_trained_model

    # In bảng tổng hợp
    print("\n" + "="*80)
    print(f"BẢNG SO SÁNH KẾT QUẢ TỐT NHẤT CỦA CÁC MÔ HÌNH ({eval_name.upper()}):")
    print("="*80)
    print(f"{'Mô hình':<20} | {'MAE':<8} | {'RMSE':<8} | {'R2':<8} | {'WMAPE (%)':<10} | {'Thời gian (s)':<12}")
    print("-"*80)
    for r in results:
        print(f"{r['model']:<20} | {r['mae']:<8.3f} | {r['rmse']:<8.3f} | {r['r2']:<8.4f} | {r['wmape']:<10.2f} | {r['fit_time_s']:<12.1f}")
    print("="*80)

    # Chuyển kết quả sang DataFrame và lưu vào HDFS
    metrics_df = spark.createDataFrame(results)
    metrics_df.write.mode('overwrite').parquet(f"{BASE}/serving/metrics")
    print(f"-> Đã lưu bảng metrics vào {BASE}/serving/metrics")

    duration = time.time() - total_start
    print(f"\n=== Hoàn tất Job 06 trong {duration:.2f} giây ===")
    spark.stop()

if __name__ == '__main__':
    main()
