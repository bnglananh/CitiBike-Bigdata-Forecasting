# Hướng dẫn vận hành cụm Docker (Hadoop & Spark)

## 1. Yêu cầu trước khi chạy
- Khởi động **Docker Desktop** trên Windows.
- Cấp tối thiểu **8–12 GB RAM** cho Docker (trong Docker Desktop: *Settings -> Resources -> Advanced/WSL2*).

## 2. Khởi động cụm
Mở terminal tại thư mục `docker/`:
```bash
docker compose up -d
```

Kiểm tra trạng thái các container:
```bash
docker compose ps
```

## 3. Các cổng Web UI quản trị (Chụp màn hình làm minh chứng báo cáo)
- **HDFS NameNode UI:** [http://localhost:9870](http://localhost:9870)
  - Kiểm tra xem 2 DataNode đã kết nối thành công chưa tại tab *Datanodes*.
- **DataNode 1 UI:** [http://localhost:9864](http://localhost:9864)
- **DataNode 2 UI:** [http://localhost:9865](http://localhost:9865)
- **Spark Master UI:** [http://localhost:8080](http://localhost:8080)
  - Kiểm tra 2 Workers đã active (mỗi worker 2 Cores, 3 GB RAM).
- **Spark Worker 1 UI:** [http://localhost:8081](http://localhost:8081)
- **Spark Worker 2 UI:** [http://localhost:8082](http://localhost:8082)

## 4. Các lệnh kiểm tra môi trường theo Kế hoạch (Phần 3)

### a. Kiểm tra HDFS
```bash
# Xem báo cáo tình trạng cụm HDFS (phải thấy 2 Live DataNodes)
docker exec -it citibike-namenode hdfs dfsadmin -report

# Tạo thư mục gốc trên HDFS
docker exec -it citibike-namenode hdfs dfs -mkdir -p /citibike/raw

# Kiểm tra phân vùng và tình trạng khối dữ liệu (dùng cho Thí nghiệm 3)
docker exec -it citibike-namenode hdfs fsck / -files -blocks -locations
```

### b. Đưa dữ liệu thô từ máy lên HDFS
```bash
# Thư mục ../data trên máy được mount vào /data bên trong container
docker exec -it citibike-namenode hdfs dfs -put /data/raw/*.csv /citibike/raw/
```

### c. Chạy Job PySpark trên cụm
```bash
docker exec -it citibike-spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/01_explore.py
```

## 5. Dừng cụm
```bash
# Dừng cụm nhưng giữ nguyên dữ liệu trên volume
docker compose down

# Dừng cụm và xóa sạch volume dữ liệu (nếu muốn làm mới từ đầu)
docker compose down -v
```
