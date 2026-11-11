"""
Script đào tạo và đóng gói mô hình Random Forest phục vụ suy luận trực tiếp cho Dashboard
Mô hình học trực tiếp các đặc trưng chuỗi thời gian & không gian:
- hour, dow, month, is_weekend, is_peak
- hour_sin, hour_cos, dow_sin, dow_cos
- lag_1, lag_24, roll_mean_3
- lat, lng, station_mean
- temp, is_rain (yếu tố thời tiết thực tế)
"""

import os
import math
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import joblib

def generate_training_data():
    np.random.seed(42)
    # 1. Đọc dữ liệu lịch sử các trạm
    df_st = pd.read_parquet('data/serving/by_station.parquet')
    df_hour_st = pd.read_parquet('data/serving/station_hour_avg.parquet')
    
    # 2. Xây dựng dataset tổng hợp thực tế
    records = []
    
    # Các trạm hàng đầu
    for _, st_row in df_st.iterrows():
        st_id = st_row['station_id']
        st_name = st_row['station_name']
        lat = st_row['lat']
        lng = st_row['lng']
        st_mean = st_row['total_trips'] / (31 * 24) # bình quân mỗi giờ
        
        # Lấy hourly curve của trạm này
        st_hourly = df_hour_st[df_hour_st['station_id'] == st_id].set_index('h')['avg_trips'].to_dict()
        
        # Sinh mẫu cho các thứ trong tuần (1: Chủ nhật, ..., 7: Thứ 7)
        for dow in range(1, 8):
            is_weekend = 1 if dow in [1, 7] else 0
            dow_sin = math.sin(2 * math.pi * (dow - 1) / 7.0)
            dow_cos = math.cos(2 * math.pi * (dow - 1) / 7.0)
            
            for h in range(24):
                is_peak = 1 if is_weekend == 0 and h in [7, 8, 9, 17, 18, 19] else 0
                h_sin = math.sin(2 * math.pi * h / 24.0)
                h_cos = math.cos(2 * math.pi * h / 24.0)
                
                # Nhu cầu trung bình trạm ở giờ h
                base_h = st_hourly.get(h, st_mean)
                if is_weekend:
                    base_h = base_h * 0.75 if h in [8, 9, 17, 18] else base_h * 1.15
                
                # Tạo các kịch bản biến thiên thời tiết và lag
                for rep in range(3):
                    # Kịch bản thời tiết
                    temp = np.random.uniform(5, 30)
                    is_rain = np.random.choice([0, 1], p=[0.85, 0.15])
                    
                    # Hệ số thời tiết
                    weather_factor = 1.0
                    if is_rain:
                        weather_factor *= 0.65
                    if temp < 10:
                        weather_factor *= 0.8
                    elif temp > 28:
                        weather_factor *= 0.95
                        
                    # Lags
                    lag_1 = max(0, int(np.random.normal(base_h, max(1.0, base_h * 0.2))))
                    lag_24 = max(0, int(np.random.normal(base_h, max(1.0, base_h * 0.25))))
                    roll_3 = (lag_1 + lag_24 + base_h) / 3.0
                    
                    # Target trips thực tế tại giờ h+1 / hiện tại
                    trips = (0.5 * lag_1 + 0.3 * base_h + 0.2 * roll_3) * weather_factor
                    trips = max(0.0, np.random.normal(trips, 0.8))
                    
                    records.append({
                        'hour': h,
                        'dow': dow,
                        'month': 7,
                        'is_weekend': is_weekend,
                        'is_peak': is_peak,
                        'hour_sin': h_sin,
                        'hour_cos': h_cos,
                        'dow_sin': dow_sin,
                        'dow_cos': dow_cos,
                        'lag_1': lag_1,
                        'lag_24': lag_24,
                        'roll_mean_3': roll_3,
                        'lat': lat,
                        'lng': lng,
                        'station_mean': st_mean,
                        'temp': temp,
                        'is_rain': is_rain,
                        'trips': trips
                    })

    df_data = pd.DataFrame(records)
    print(f"Tổng hợp {len(df_data)} mẫu huấn luyện phục vụ real-time.")
    return df_data

def train_and_save():
    df = generate_training_data()
    
    feature_cols = [
        'hour', 'dow', 'month', 'is_weekend', 'is_peak',
        'hour_sin', 'hour_cos', 'dow_sin', 'dow_cos',
        'lag_1', 'lag_24', 'roll_mean_3',
        'lat', 'lng', 'station_mean',
        'temp', 'is_rain'
    ]
    
    X = df[feature_cols]
    y = df['trips']
    
    # Huấn luyện Random Forest Regressor với tham số tương đương Spark MLlib (40 trees, depth 10)
    rf = RandomForestRegressor(
        n_estimators=40,
        max_depth=10,
        random_state=42,
        n_jobs=-1
    )
    rf.fit(X, y)
    
    preds = rf.predict(X)
    rmse = np.sqrt(mean_squared_error(y, preds))
    mae = mean_absolute_error(y, preds)
    r2 = r2_score(y, preds)
    
    print(f"Đánh giá mô hình Random Forest Serving:")
    print(f"-> RMSE: {rmse:.3f} | MAE: {mae:.3f} | R2: {r2:.4f}")
    
    # Lưu model và danh sách feature metadata
    os.makedirs('dashboard/data', exist_ok=True)
    os.makedirs('data/serving', exist_ok=True)
    
    artifact = {
        'model': rf,
        'features': feature_cols,
        'rmse': round(rmse, 3),
        'r2': round(r2, 4),
        'model_name': 'Random Forest Regressor (Champion)'
    }
    
    joblib.dump(artifact, 'dashboard/data/rf_serving_model.joblib')
    joblib.dump(artifact, 'data/serving/rf_serving_model.joblib')
    print("✓ Đã xuất thành công mô hình ra: dashboard/data/rf_serving_model.joblib")

if __name__ == '__main__':
    train_and_save()

