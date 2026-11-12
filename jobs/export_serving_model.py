"""
Script huấn luyện và xuất mô hình Random Forest thật 100% (Hướng A)
- Đọc dữ liệu từ 17 file CSV gốc của Top 400 trạm
- Tổng hợp theo (trạm, giờ) và điền 0 (Zero-filling) cho các giờ không có chuyến
- Trích xuất 16 đặc trưng chuẩn:
  hour, dow, month, is_weekend, is_peak,
  hour_sin, hour_cos, dow_sin, dow_cos,
  lag_1, lag_24, lag_168, roll_mean_3,
  lat, lng, station_mean
- Phân chia: Train (ngày 8-20), Validation (ngày 21-25), Test (ngày 26-hết)
- Huấn luyện scikit-learn RandomForestRegressor(n_estimators=40, max_depth=10, random_state=42)
- Đánh giá trên Validation và Test
- Xuất rf_serving_model.joblib và bảng mẫu test_sample.parquet cho Dashboard
"""

import os
import glob
import math
import time
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import joblib

def main():
    t0 = time.time()
    print("=== [HƯỚNG A] HUẤN LUYỆN MÔ HÌNH RANDOM FOREST PHỤC VỤ DASHBOARD TRÊN DỮ LIỆU THẬT ===")
    
    # 1. Đọc danh sách Top 400 trạm từ bảng serving có sẵn
    df_map = pd.read_parquet('data/serving/station_hour_avg.parquet')
    station_meta = df_map[['station_id', 'lat', 'lng', 'station_name']].drop_duplicates('station_id').set_index('station_id')
    top400 = set(station_meta.index.astype(str))
    print(f"-> Đã nạp thông tin tọa độ của {len(top400)} trạm.")

    # 2. Tổng hợp số chuyến từ 17 file CSV
    print("-> Đang quét 17 file CSV để tổng hợp số chuyến theo (trạm, giờ)...")
    csvs = glob.glob('data/csv/*/*.csv')
    counts = {}
    
    for f in csvs:
        df = pd.read_csv(f, usecols=['started_at', 'start_station_id'], dtype=str)
        df = df[df['start_station_id'].isin(top400)]
        df['hour_ts'] = df['started_at'].str.slice(0, 13)
        vc = df.groupby(['start_station_id', 'hour_ts']).size()
        for (st, hr), cnt in vc.items():
            counts[(st, hr)] = counts.get((st, hr), 0) + cnt
            
    print(f"-> Đã tổng hợp được {len(counts):,} cặp (trạm, giờ) có chuyến phát sinh.")

    # 3. Tạo lưới đầy đủ cho từng tháng thực tế (Zero-filling)
    print("-> Đang tạo lưới không gian - thời gian và điền 0 (Zero-filling)...")
    month_days = {
        '2024-01': 31,
        '2024-04': 30,
        '2024-07': 31,
        '2024-10': 31
    }
    
    grid_rows = []
    for ym, n_days in month_days.items():
        dt_range = pd.date_range(start=f"{ym}-01 00:00:00", end=f"{ym}-{n_days:02d} 23:00:00", freq='h')
        hours_str = dt_range.strftime('%Y-%m-%d %H').tolist()
        
        for st in top400:
            for hr in hours_str:
                trips = counts.get((st, hr), 0)
                grid_rows.append({
                    'station_id': st,
                    'hour_ts': hr,
                    'trips': trips
                })

    df_grid = pd.DataFrame(grid_rows)
    print(f"-> Tổng số dòng sau khi điền 0: {len(df_grid):,} dòng.")

    # 4. Trích xuất đặc trưng chuỗi thời gian & không gian
    print("-> Đang tính toán các đặc trưng chuỗi thời gian (Lags, Rolling, Sin/Cos)...")
    df_grid['dt'] = pd.to_datetime(df_grid['hour_ts'], format='%Y-%m-%d %H')
    df_grid['hour'] = df_grid['dt'].dt.hour
    df_grid['dow'] = df_grid['dt'].dt.dayofweek + 1 # 1: Thứ 2, ..., 7: CN (quy ước Spark)
    df_grid['month'] = df_grid['dt'].dt.month
    df_grid['day'] = df_grid['dt'].dt.day
    df_grid['is_weekend'] = df_grid['dow'].isin([1, 7]).astype(int) # CN hoặc T7
    df_grid['is_peak'] = ((df_grid['is_weekend'] == 0) & df_grid['hour'].isin([7, 8, 9, 17, 18, 19])).astype(int)
    
    df_grid['hour_sin'] = np.sin(2 * np.pi * df_grid['hour'] / 24.0)
    df_grid['hour_cos'] = np.cos(2 * np.pi * df_grid['hour'] / 24.0)
    df_grid['dow_sin'] = np.sin(2 * np.pi * (df_grid['dow'] - 1) / 7.0)
    df_grid['dow_cos'] = np.cos(2 * np.pi * (df_grid['dow'] - 1) / 7.0)

    # Tính Lags phân vùng theo (station_id, month) để không bị gián đoạn giữa các mùa
    df_grid = df_grid.sort_values(['station_id', 'month', 'dt']).reset_index(drop=True)
    grp = df_grid.groupby(['station_id', 'month'])
    
    df_grid['lag_1'] = grp['trips'].shift(1)
    df_grid['lag_24'] = grp['trips'].shift(24)
    df_grid['lag_168'] = grp['trips'].shift(168)
    df_grid['roll_mean_3'] = grp['trips'].transform(lambda s: s.shift(1).rolling(3).mean())

    # Lọc bỏ các dòng có lag_168 bị null (7 ngày đầu mỗi tháng)
    df_feat = df_grid.dropna(subset=['lag_168', 'roll_mean_3']).copy()
    print(f"-> Số dòng sau khi lọc bỏ lag NULL đầu tháng: {len(df_feat):,} dòng.")

    # Gán nhãn phân chia theo thời gian
    conditions = [
        (df_feat['day'] >= 8) & (df_feat['day'] <= 20),
        (df_feat['day'] >= 21) & (df_feat['day'] <= 25),
        (df_feat['day'] >= 26)
    ]
    choices = ['train', 'val', 'test']
    df_feat['split'] = np.select(conditions, choices, default='other')
    df_feat = df_feat[df_feat['split'] != 'other'].copy()

    # Tính station_mean CHỈ từ tập Train
    st_train_mean = df_feat[df_feat['split'] == 'train'].groupby('station_id')['trips'].mean().to_dict()
    df_feat['station_mean'] = df_feat['station_id'].map(st_train_mean).fillna(0.0)

    # Thêm tọa độ trạm
    df_feat['lat'] = df_feat['station_id'].map(station_meta['lat']).fillna(40.74)
    df_feat['lng'] = df_feat['station_id'].map(station_meta['lng']).fillna(-73.99)
    df_feat['station_name'] = df_feat['station_id'].map(station_meta['station_name']).fillna('Unknown Station')

    # Định nghĩa 16 đặc trưng chuẩn
    FEATURES = [
        'hour', 'dow', 'month', 'is_weekend', 'is_peak',
        'hour_sin', 'hour_cos', 'dow_sin', 'dow_cos',
        'lag_1', 'lag_24', 'lag_168', 'roll_mean_3',
        'lat', 'lng', 'station_mean'
    ]

    train_df = df_feat[df_feat['split'] == 'train']
    val_df = df_feat[df_feat['split'] == 'val']
    test_df = df_feat[df_feat['split'] == 'test']

    print(f"-> Số lượng mẫu: Train={len(train_df):,} | Val={len(val_df):,} | Test={len(test_df):,}")

    # 5. Huấn luyện Random Forest Regressor
    print("-> Bắt đầu huấn luyện RandomForestRegressor (40 cây, depth 10)...")
    t_train = time.time()
    rf = RandomForestRegressor(
        n_estimators=40,
        max_depth=10,
        random_state=42,
        n_jobs=-1
    )
    rf.fit(train_df[FEATURES], train_df['trips'])
    train_time = time.time() - t_train
    print(f"-> Huấn luyện xong trong {train_time:.2f} giây.")

    # 6. Đánh giá trên Validation và Test
    def eval_model(df, name):
        preds = np.clip(rf.predict(df[FEATURES]), 0, None)
        y = df['trips'].values
        mae = mean_absolute_error(y, preds)
        rmse = np.sqrt(mean_squared_error(y, preds))
        r2 = r2_score(y, preds)
        wmape = (np.sum(np.abs(y - preds)) / max(1.0, np.sum(y))) * 100
        print(f"   [{name.upper()}] MAE={mae:.3f} | RMSE={rmse:.3f} | R2={r2:.4f} | WMAPE={wmape:.2f}%")
        return {'mae': mae, 'rmse': rmse, 'r2': r2, 'wmape': wmape}

    print("\nKẾT QUẢ ĐÁNH GIÁ MÔ HÌNH RANDOM FOREST SERVING (DỮ LIỆU THẬT):")
    val_metrics = eval_model(val_df, 'Validation')
    test_metrics = eval_model(test_df, 'Test')

    # 7. Đóng gói mô hình phục vụ Dashboard
    os.makedirs('dashboard/data', exist_ok=True)
    os.makedirs('data/serving', exist_ok=True)

    artifact = {
        'model': rf,
        'features': FEATURES,
        'val_metrics': val_metrics,
        'test_metrics': test_metrics,
        'train_time_s': round(train_time, 2),
        'model_name': 'Random Forest Regressor (Sklearn Replica)'
    }

    joblib.dump(artifact, 'dashboard/data/rf_serving_model.joblib')
    joblib.dump(artifact, 'data/serving/rf_serving_model.joblib')
    print("✓ Đã lưu mô hình chuẩn vào: dashboard/data/rf_serving_model.joblib")

    # 8. Xuất một tập mẫu Test kèm thông tin thực tế phục vụ Tab 5 Dashboard
    sample_cols = ['station_id', 'station_name', 'hour_ts', 'trips'] + FEATURES
    sample_cols = list(dict.fromkeys(sample_cols)) # Loại bỏ cột trùng lặp
    test_sample = test_df[sample_cols].sample(n=min(5000, len(test_df)), random_state=42).reset_index(drop=True)
    test_sample.to_parquet('dashboard/data/test_sample.parquet')
    test_sample.to_parquet('data/serving/test_sample.parquet')
    print("✓ Đã lưu bảng test_sample.parquet phục vụ chọn kịch bản thực tế trên Tab 5.")

    print(f"\n=== HOÀN TẤT TRONG {time.time()-t0:.2f} GIÂY ===")

if __name__ == '__main__':
    main()
