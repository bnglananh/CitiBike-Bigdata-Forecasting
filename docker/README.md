# Hướng dẫn vận hành cụm Docker (Hadoop & Spark)

## 1. Yêu cầu trước khi chạy
- Khởi động **Docker Desktop** trên Windows.
- Cấp tối thiểu **6–8 GB RAM** cho Docker (trong Docker Desktop: *Settings -> Resources -> Advanced/WSL2*).

## 2. Khởi động cụm
Mở terminal tại thư mục `docker/`:
```bash
docker compose up -d
```
*(Hoặc click đúp chạy file `docker/start.bat`)*

Kiểm tra trạng thái các container:
```bash
docker compose ps
```
Đảm bảo 5 service đều đang hoạt động:
- `citibike-namenode`
- `citibike-datanode1`
- `citibike-datanode2`
- `citibike-spark-master`
- `citibike-spark-worker-1`
- `citibike-spark-worker-2`

## 3. Các cổng Web UI quản trị (Chụp màn hình làm minh chứng báo cáo)
- **HDFS NameNode UI:** [http://localhost:9870](http://localhost:9870)
  - Kiểm tra xem 2 DataNodes đã kết nối thành công chưa tại tab *Datanodes*.
  - Duyệt cây thư mục Data Lake HDFS tại tab *Utilities -> Browse the file system*.
- **DataNode 1 UI:** [http://localhost:9864](http://localhost:9864)
- **DataNode 2 UI:** [http://localhost:9865](http://localhost:9865)
- **Spark Master UI:** [http://localhost:8080](http://localhost:8080)
  - Kiểm tra 2 Workers đã active (mỗi worker 2 Cores, 3 GB RAM).
  - Giám sát các Applications / Spark Jobs đã và đang thực thi.
- **Spark Worker 1 UI:** [http://localhost:8081](http://localhost:8081)
- **Spark Worker 2 UI:** [http://localhost:8082](http://localhost:8082)
- **Spark Application Web UI:** [http://localhost:4040](http://localhost:4040) (khi có job đang chạy)

## 4. Các lệnh kiểm tra & thao tác trên cụm

### a. Kiểm tra HDFS
```bash
# Xem báo cáo tình trạng cụm HDFS (phải thấy 2 Live DataNodes)
docker exec -it citibike-namenode hdfs dfsadmin -report

# Tạo thư mục gốc trên HDFS
docker exec -it citibike-namenode hdfs dfs -mkdir -p /citibike/raw

# Liệt kê cây thư mục dữ liệu trên HDFS
docker exec -it citibike-namenode hdfs dfs -ls -R /citibike

# Kiểm tra phân vùng và tình trạng khối dữ liệu (Thí nghiệm HDFS Block & Replication)
docker exec -it citibike-namenode hdfs fsck /citibike -files -blocks -locations
```

### b. Đưa dữ liệu CSV lên HDFS
Thư mục `data/` trên máy chủ được mount vào `/data` bên trong container.
Sử dụng script Python tự động:
```bash
# Từ thư mục gốc dự án:
python scripts/load_to_hdfs.py --seasonal
```
Hoặc dùng lệnh HDFS thủ công:
```bash
docker exec -it citibike-namenode hdfs dfs -mkdir -p /citibike/raw/year=2024/month=01
docker exec -it citibike-namenode bash -c "hdfs dfs -put -f /data/csv/2024-01/*.csv /citibike/raw/year=2024/month=01/"
```

### c. Chạy Job PySpark trên cụm
```bash
docker exec -it citibike-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/01_explore.py
```

### d. Chạy Thí nghiệm Hadoop Streaming MapReduce
```bash
docker exec -it citibike-namenode hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
  -files /scripts/../jobs/mapreduce/mapper.py,/scripts/../jobs/mapreduce/reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /citibike/raw/year=2024/month=01 \
  -output /citibike/mr_out_test
```

## 5. Dừng cụm
```bash
# Dừng cụm nhưng giữ nguyên dữ liệu trên volume:
docker compose down

# Dừng cụm và xóa sạch volume dữ liệu (nếu muốn làm mới từ đầu):
docker compose down -v
```
