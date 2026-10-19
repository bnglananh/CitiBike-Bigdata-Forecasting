# Kế hoạch triển khai đồ án: dự báo nhu cầu Citi Bike bằng Hadoop và Spark

Oct 9, 2026 · @Su

Kế hoạch này đi từ dựng môi trường đến demo trong 7 bước, mỗi bước gắn với một mục ở Chương 2 để Chương 3 viết ra khớp với lý thuyết đã trình bày.

## Phần 1. Các quyết định cần chốt trước khi làm

Chốt 11 quyết định dưới đây trong Tuần 1–3. Đề xuất đều bám Chương 1–2, và điền luôn những chỗ Chương 1 còn để ngỏ (khoảng thời gian, thời tiết, Poisson, Parquet).

| # | Quyết định | Đề xuất | Căn cứ trong báo cáo |
| --- | --- | --- | --- |
| 1 | Phạm vi thời gian (điền \[MM/YYYY\] ở 1.3.2a) | 12 tháng liên tục, 01/2024 đến 12/2024. Phát triển trên 1–2 tháng, chạy đủ 12 tháng ở bản cuối | Cần đủ bốn mùa (2.7.1). Các tệp zip 2024 cộng lại khoảng 8,7 GB nén (cộng từ kích thước tệp trên máy chủ dữ liệu). Nếu muốn dùng năm 2025, kiểm tra tệp có sẵn trước, mình chưa kiểm tra |
| 2 | Chia tập theo thời gian | Train: 01–08/2024. Validation: 09–10/2024. Test: 11–12/2024 | 2.6.2, 2.7.6, 2.10.6. Test rơi vào cuối thu và đầu đông, ghi vào hạn chế |
| 3 | Biến mục tiêu và chân trời dự báo | y = số chuyến bắt đầu tại trạm trong mỗi giờ. Dự báo trước 1 giờ | 1.2. Dự báo trước h giờ thì chỉ được dùng lag từ h trở lên. Với h = 1, cả lag\_1, lag\_24, lag\_168 đều hợp lệ (2.7.4, 2.7.6) |
| 4 | Mức không gian (điền 1.3.2a) | Cấp trạm. Mô hình hóa trên Top N trạm có nhiều chuyến nhất trong tập Train, N khoảng 300–500, chốt sau khi đo thời gian chạy | Bảng đầy đủ có trên 2.000 trạm × 8.784 giờ, khoảng 17 triệu dòng, phần lớn là số 0 (2.7.7). Đếm số trạm thực tế ở Bước 2 |
| 5 | Dữ liệu bổ sung | Ngày lễ: làm. Thời tiết: làm như một thí nghiệm bổ sung (có và không có thời tiết) | 1.2 ghi "nếu được triển khai". Dùng thời tiết thực tế thay cho dự báo thời tiết là một giả định, phải ghi trong báo cáo |
| 6 | Mô hình | Baseline (trung bình lịch sử, Seasonal Naive), Linear/Ridge/Lasso, Decision Tree, Random Forest, GBT. Poisson là tùy chọn | 2.8. Spark ML có GeneralizedLinearRegression với family poisson, link log. Nếu không kịp, giữ Poisson ở mức lý thuyết như 2.8.3 đã viết |
| 7 | Độ đo | Chính: MAE, RMSE, R². Bổ sung: WMAPE. Không dùng MAPE | 2.10 |
| 8 | Định dạng lưu trữ | Raw: CSV. Cleaned và features: Parquet. Có thí nghiệm CSV so với Parquet | 2.3.4 yêu cầu Chương 3 mô tả đúng thực tế. Làm thật thì được viết như đã triển khai |
| 9 | Chuẩn hóa đặc trưng | StandardScaler chỉ cho nhóm tuyến tính. Nhóm cây không chuẩn hóa | 2.9.2 |
| 10 | Ngoài phạm vi | Không làm streaming thời gian thực, không điều phối xe | 1.3.2d |
| 11 | Công cụ trực quan | Streamlit, Plotly, Folium | 2.11.4 |

**Cập nhật Chương 1–2 sau khi có số liệu thật:**

- 1.3.2a: điền khoảng thời gian và số trạm (Top N).
- 1.4.2: điền số bản ghi và dung lượng đã đếm trước khi đưa lên HDFS (Bước 2).
- 2.1.2 (Velocity): nói rõ dữ liệu được phát hành theo tháng, nên đề tài xử lý theo lô (khớp 2.1.4).
- 2.10.6: vì dùng chia cố định, nêu Time-Series Cross-Validation như hướng mở rộng.

## Phần 2. Kiến trúc và luồng dữ liệu

Dữ liệu chạy một chiều qua bảy tầng thư mục trên HDFS, mỗi tầng do một bước tạo ra. Cách tổ chức này thể hiện đúng ý ở 2.2.8: dữ liệu gốc để riêng, dữ liệu sau xử lý và kết quả trung gian để riêng.

| Tầng | Đường dẫn HDFS | Định dạng | Tạo bởi | Nội dung | Mục Chương 2 |
| --- | --- | --- | --- | --- | --- |
| raw | /citibike/raw/year=2024/month=01/ | CSV | hdfs dfs -put | Bản gốc, không sửa | 2.2.8, 2.3.4 |
| cleaned | /citibike/cleaned/ (chia theo month) | Parquet | Spark | Chuyến đi hợp lệ, schema thống nhất | 2.9, 2.5.3 |
| hourly | /citibike/hourly/ | Parquet | Spark | (station\_id, hour, trips), đã điền 0 | 2.5.6, 2.7.7 |
| features | /citibike/features/ | Parquet | Spark | Đặc trưng, nhãn y, cột split (train, val, test) | 2.6.3, 2.7 |
| models | /citibike/models/\<tên mô hình> | Spark ML | MLlib | Pipeline đã huấn luyện | 2.6.4 |
| predictions | /citibike/predictions/ | Parquet | Spark | y, y\_hat, tên mô hình, split | 2.10 |
| serving | /citibike/serving/ | Parquet nhỏ | Spark | Các bảng tổng hợp cho dashboard, kéo về máy | 2.11 |

Luồng đầy đủ:

1. Tải zip từ trang dữ liệu Citi Bike, giải nén trên máy.
2. Đưa CSV lên HDFS (tầng raw).
3. Spark đọc, làm sạch, ghi Parquet (cleaned).
4. Spark đếm chuyến theo trạm và giờ, điền giờ trống bằng 0 (hourly).
5. Spark xây đặc trưng và chia tập theo thời gian (features).
6. MLlib huấn luyện trên Train, chọn cấu hình trên Validation, đánh giá cuối trên Test.
7. Ghi dự báo và bảng tổng hợp ra tầng serving.
8. Streamlit đọc tầng serving để vẽ biểu đồ và bản đồ.

Dashboard không gọi Spark trực tiếp. Spark làm phần nặng, dashboard chỉ đọc bảng kết quả đã nhỏ gọn. Nhờ vậy demo không phụ thuộc vào cụm chạy trong lúc thuyết trình.

## Phần 3. Môi trường và công cụ

Dựng cụm HDFS và Spark bằng Docker Compose trên một máy (phương án A). Nếu Docker quá nặng, chạy Hadoop ở chế độ giả phân tán trên WSL2 (phương án B).

### Phần cứng tối thiểu

Các con số dưới đây là ước lượng của mình. Hãy đo lại sau khi giải nén tháng dữ liệu đầu tiên.

| Tài nguyên | Mức tối thiểu cho 12 tháng dữ liệu | Nếu máy yếu hơn |
| --- | --- | --- |
| RAM | 16 GB, cấp 10–12 GB cho Docker | Giảm xuống 6 tháng dữ liệu, hoặc giảm N trạm |
| Ổ đĩa trống | Khoảng 150 GB SSD (CSV giải nén, bản sao HDFS với replication 2, Parquet) | Chỉ giữ raw của các tháng đang dùng, xóa zip sau khi giải nén |
| CPU | 4 nhân trở lên | Giảm số worker xuống 1 |

### Bộ công cụ

| Thành phần | Đề xuất | Ghi chú |
| --- | --- | --- |
| Hệ điều hành | Windows 11 với WSL2 Ubuntu, hoặc Linux, macOS |  |
| Container | Docker Desktop và Docker Compose | Dùng image chính thức apache/hadoop và apache/spark. Tránh image cộng đồng lâu không cập nhật vì thường là Spark và Hadoop bản cũ |
| Hadoop | 3.3.x hoặc 3.4.x, chỉ cần HDFS | YARN không bắt buộc (2.4.3: Hadoop chủ yếu để lưu trữ) |
| Spark | 3.5.x, dùng PySpark | Chế độ standalone, 1 master và 2 worker. Khi phát triển dùng local\[\*\] |
| Java | JDK 11 nếu cài tay | JDK 11 chạy được cả Hadoop 3.3.x và Spark 3.5.x. Dùng image chính thức thì Java đã đóng gói sẵn |
| Python | 3.10 hoặc 3.11 | Chọn bản mà PySpark đã chọn hỗ trợ |
| Thư viện Python | pyspark, pandas, pyarrow, plotly, folium, streamlit, streamlit-folium, holidays, requests | Ghi vào requirements.txt |
| Notebook | JupyterLab | Dùng cho EDA, còn quy trình chính viết thành script để chạy lại được |
| Quản lý mã | Git, một kho riêng tư trên GitHub | Mỗi người một nhánh, gộp qua pull request |

Chốt phiên bản khi cài và ghi vào Bảng "Môi trường thực nghiệm" ở Chương 3 mục 3.2 của báo cáo. Trước khi cài, đối chiếu bảng tương thích Spark, Hadoop, Java, Python trong tài liệu chính thức. Mình chưa chạy thử cấu hình này trên máy của bạn.

### Cấu hình cụm (khớp lý thuyết Chương 2)

- HDFS: 1 NameNode và 2 DataNode, dfs.replication = 2, block size mặc định 128 MB. Hai DataNode cho phép minh họa Block và Replication (2.2.3) và chịu lỗi (2.2.5).
- Spark: 1 master và 2 worker, mỗi worker 2 core và 3–4 GB RAM. Driver 2–4 GB.
- Đường dẫn trong Spark: hdfs://namenode:9000/citibike/... (kiểm tra cổng trong cấu hình fs.defaultFS của cụm).
- spark.sql.session.timeZone = UTC. Thời gian trong dữ liệu là giờ địa phương New York, ta coi nó là giờ trên đồng hồ tường để lưới giờ luôn đủ 24 giờ mỗi ngày. Ghi giả định này vào hạn chế.
- spark.sql.shuffle.partitions = 16 đến 32 (mặc định 200 là quá nhiều cho cụm nhỏ).
- Giao diện web: NameNode (cổng 9870), Spark master (8080), ứng dụng Spark (4040). Chụp màn hình cho báo cáo.

### Kiểm tra môi trường (làm xong mới sang bước tiếp)

- [ ] hdfs dfs -mkdir -p /citibike/raw, rồi -put một tệp nhỏ và -ls thấy tệp
- [ ] hdfs dfsadmin -report báo 2 DataNode đang chạy
- [ ] PySpark đọc CSV từ hdfs:// và count() ra số
- [ ] Ghi Parquet lên HDFS rồi đọc lại đúng số dòng
- [ ] Mở được giao diện NameNode và Spark UI, đã chụp ảnh
- [ ] Streamlit chạy được trang thử và đọc được một tệp Parquet nhỏ

## Phần 4. Lộ trình 10 tuần

Lộ trình đẩy việc có dữ liệu lên sớm hơn file kế hoạch của khoa. File đặt việc tìm dữ liệu ở Tuần 8–9, nhưng baseline ở Tuần 6–7 cần dữ liệu từ trước. File cũng ghi mốc Giai đoạn 1 là "Tuần 5, 6" ở phần mô tả nhưng là "Tuần 6–7" ở bảng. Để an toàn cho cả hai cách hiểu, lộ trình dưới đây có baseline xong cuối Tuần 5. Nên hỏi giảng viên ngày nộp chính xác.

| Tuần | Việc chính | Sản phẩm xong | Phụ trách | Mục Chương 3 |
| --- | --- | --- | --- | --- |
| 1–2 | Chốt 11 quyết định ở Phần 1, xin giảng viên duyệt đề tài và dữ liệu. Dựng cụm, chạy đủ checklist kiểm tra môi trường ở Phần 3 | Đề tài được duyệt. Cụm chạy được. Bản kế hoạch nhóm | 3.1, 3.2 |  |
| 3 | Tải 12 tháng, giải nén, đưa lên HDFS, khảo sát schema, đếm bản ghi và dung lượng (Bước 1–2) | Dữ liệu trên HDFS. Số liệu điền 1.4.2. Chụp fsck và giao diện HDFS | A | 3.3 |
| 4 | Làm sạch và tổng hợp theo (trạm, giờ). Thử trên 1 tháng rồi chạy 12 tháng (Bước 3) | Bảng số dòng trước và sau từng quy tắc. Tầng cleaned và hourly | A | 3.5 |
| 5 | Baseline trên Validation và Test. Ví dụ minh họa MapReduce. Báo cáo Giai đoạn 1 | Bảng MAE, RMSE, R² của hai baseline. Phân tích lỗi baseline để nêu điểm cần cải thiện | A, B | 3.7, 3.8 |
| 6 | EDA và xây đặc trưng (Bước 4) | Biểu đồ EDA. Tầng features, kiểm tra không rò rỉ | B | 3.4, 3.6 |
| 7 | Huấn luyện mô hình tuyến tính và cây, chọn tham số trên Validation (Bước 5) | Bảng so sánh trên Validation | B | 3.7 |
| 8 | Tinh chỉnh RF và GBT, đánh giá trên Test, phân tích lỗi, thí nghiệm thời tiết, thí nghiệm hiệu năng và chịu lỗi (Bước 5–6) | Bảng kết quả cuối trên Test. Số đo thời gian CSV so với Parquet, theo số core | A, B | 3.8 |
| 9 | Dashboard (Bước 7), viết xong Chương 3–4, hoàn thiện slide, nộp bài | Mã nguồn, file Word, file PowerPoint, hướng dẫn chạy | A, B | 3.9, Chương 4 |
| 10 | Tập thuyết trình, quay sẵn video demo dự phòng, trả lời phản biện | Buổi thuyết trình đúng giờ | A, B |  |

Quy tắc làm việc: sau mỗi bước, viết ngay mục Chương 3 tương ứng và chụp màn hình, đừng để dồn đến Tuần 9. Lưu câu lệnh và tham số đã dùng vào README.

## Phần 5. Bước 1–2: tải dữ liệu, đưa lên HDFS, khảo sát

Mục tiêu: có 12 tháng CSV nằm trên HDFS, và có đủ số liệu thật để điền 1.4.2 và Chương 3 mục 3.3. Bước này dùng các kiến thức ở 2.2 (HDFS) và 2.3.1 (CSV).

### 5.1. Tải và giải nén

Tên tệp 2024 theo dạng YYYYMM-citibike-tripdata.zip trên máy chủ dữ liệu (đã kiểm tra). Kiểm tra lại tên tệp ở trang dữ liệu trước khi chạy.

```bash
mkdir -p data/zip data/csv
for m in $(seq -w 1 12); do
  curl -L -o data/zip/2024${m}-citibike-tripdata.zip https://s3.amazonaws.com/tripdata/2024${m}-citibike-tripdata.zip
  mkdir -p data/csv/2024-${m}
  unzip -o data/zip/2024${m}-citibike-tripdata.zip -d data/csv/2024-${m}
done
```

- Một tháng có thể gồm nhiều tệp CSV (chia phần) và thư mục thừa. Chạy `unzip -l` một tháng trước để xem. Mình chưa mở thử nội dung các tệp zip.
- Chỉ dùng tệp New York. Các tệp có tiền tố JC là Jersey City, nằm ngoài phạm vi 1.3.2a.

### 5.2. Đưa lên HDFS, chia thư mục theo năm và tháng

```bash
hdfs dfs -mkdir -p /citibike/raw
for m in $(seq -w 1 12); do
  hdfs dfs -mkdir -p /citibike/raw/year=2024/month=${m}
  hdfs dfs -put data/csv/2024-${m}/*.csv /citibike/raw/year=2024/month=${m}/
done
hdfs dfs -ls -R /citibike/raw
hdfs dfs -du -h /citibike/raw
```

Nếu dùng Docker, gắn thư mục data vào container có lệnh hdfs rồi chạy bằng docker exec. Tên thư mục year=2024/month=01 để Spark tự nhận ra hai cột phân vùng year và month khi đọc.

### 5.3. Lấy bằng chứng cho mục 2.2

```bash
hdfs fsck /citibike/raw/year=2024/month=06 -files -blocks -locations
hdfs dfsadmin -report
```

Chụp màn hình ba điều: số Block của một tệp (tệp lớn hơn 128 MB bị chia thành nhiều Block), replication bằng 2, và vị trí các bản sao trên hai DataNode. Đây là minh chứng cho 2.2.3 trong Chương 3.

### 5.4. Khảo sát bằng Spark

Kiểm tra tên cột của từng tháng trước khi đặt schema. Khi đọc CSV với schema khai báo sẵn, Spark ghép cột theo vị trí chứ không theo tên, nên một tháng đổi thứ tự cột sẽ làm lệch dữ liệu mà không báo lỗi.

```python
from pyspark.sql import SparkSession, functions as F, types as T

spark = (SparkSession.builder.appName('citibike-explore')
         .config('spark.sql.session.timeZone', 'UTC')
         .config('spark.sql.shuffle.partitions', '16')
         .getOrCreate())

BASE = 'hdfs://namenode:9000/citibike/raw'

# 1) Tên cột từng tháng phải giống nhau
for m in range(1, 13):
    cols = spark.read.option('header', True).csv(f'{BASE}/year=2024/month={m:02d}').columns
    print(m, cols)

# 2) Đọc với schema tường minh (nhanh hơn inferSchema, kiểu dữ liệu chắc chắn)
schema = T.StructType([
    T.StructField('ride_id', T.StringType()),
    T.StructField('rideable_type', T.StringType()),
    T.StructField('started_at', T.StringType()),
    T.StructField('ended_at', T.StringType()),
    T.StructField('start_station_name', T.StringType()),
    T.StructField('start_station_id', T.StringType()),
    T.StructField('end_station_name', T.StringType()),
    T.StructField('end_station_id', T.StringType()),
    T.StructField('start_lat', T.DoubleType()),
    T.StructField('start_lng', T.DoubleType()),
    T.StructField('end_lat', T.DoubleType()),
    T.StructField('end_lng', T.DoubleType()),
    T.StructField('member_casual', T.StringType()),
])
raw = spark.read.option('header', True).schema(schema).csv(BASE)

print(raw.count())
raw.groupBy('month').count().orderBy('month').show()   # month là cột phân vùng suy ra từ thư mục
raw.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in raw.columns]).show()
```

Nếu thứ tự cột ở bước 1 khác nhau giữa các tháng, đọc từng tháng riêng, đổi tên theo tên cột rồi mới gộp. Nếu schema thật khác với bảng trên (ví dụ thêm hoặc bớt cột), sửa schema theo tên cột thật.

### 5.5. Số liệu phải ghi lại

| Số liệu | Dùng ở |
| --- | --- |
| Số tệp, dung lượng zip, dung lượng CSV (hdfs dfs -du -h) | 1.4.2, 3.3 |
| Số bản ghi từng tháng và tổng | 1.4.2, 2.1.2 (Volume) |
| Số trạm xuất phát khác nhau | Quyết định N ở Phần 1 |
| Tỷ lệ thiếu start\_station\_id, end\_station\_id, tọa độ | 3.5 (Veracity ở 2.1.2) |
| Phân bố rideable\_type và member\_casual | 3.4 (Variety ở 2.1.2) |
| Giá trị nhỏ nhất và lớn nhất của started\_at | 3.5, kiểm tra chuyến lệch tháng |

## Phần 6. Bước 3: tiền xử lý và tổng hợp theo (trạm, giờ)

Mục tiêu: biến hàng chục triệu dòng chuyến đi thành bảng (trạm, giờ, số chuyến) sạch, không thiếu giờ trống, và ghi lại số dòng của từng quy tắc cho Bảng 3.1. Bước này dùng 2.5.3 và 2.5.4 (DataFrame, Spark SQL), 2.9.1, 2.9.4 (làm sạch, thời gian), 2.7.7 (nhiều giá trị 0).

### Quy tắc làm sạch

| # | Quy tắc | Lý do và lưu ý |
| --- | --- | --- |
| 1 | Loại trùng theo ride\_id | Tránh đếm một chuyến nhiều lần (2.9.1) |
| 2 | Loại dòng thiếu started\_at hoặc ended\_at | Không xác định được giờ |
| 3 | Loại chuyến có thời lượng nhỏ hơn hoặc bằng 0 giây, hoặc dài hơn 24 giờ | Lỗi ghi nhận hoặc xe thất lạc |
| 4 | Loại chuyến dưới 60 giây mà trạm đi trùng trạm đến | Thuê rồi trả ngay, không phản ánh nhu cầu đi lại. Đây là quyết định nghiệp vụ, phải ghi trong báo cáo |
| 5 | Loại tọa độ xuất phát nằm ngoài hộp bao quanh New York | Chỉnh hộp sau khi xem phân bố thật |
| 6 | Dòng thiếu start\_station\_id: không đưa vào bảng theo trạm | Không điền bù. Vẫn giữ trong thống kê tổng, ghi rõ tỷ lệ |
| 7 | Lọc chuyến ngoài khoảng 01/01/2024 đến 31/12/2024 | Tệp một tháng có thể chứa chuyến lệch tháng |
| 8 | Không dùng IQR để loại số chuyến mỗi giờ | Nhu cầu cao có thể là thật (2.9.1). Chỉ dùng IQR để khảo sát thời lượng chuyến |

Một dòng có thể vi phạm nhiều quy tắc, nên Bảng 3.1 ghi số dòng bị đánh dấu của từng quy tắc (có chồng lấn) và số dòng còn lại cuối cùng. Cách này chỉ cần một lần quét dữ liệu, hợp với Lazy Evaluation ở 2.5.5.

### Mã PySpark: làm sạch và ghi tầng cleaned

Mã mẫu viết theo API Spark 3.5, mình chưa chạy trên dữ liệu thật. Hãy thử trên một tháng trước.

```python
from pyspark.sql import functions as F

BASE = 'hdfs://namenode:9000/citibike'
LO, END = '2024-01-01 00:00:00', '2025-01-01 00:00:00'
FLAGS = ['f_null_time', 'f_bad_dur', 'f_fake_trip', 'f_geo']

def flag(cond):                      # null tính là không vi phạm
    return F.coalesce(cond, F.lit(False))

d = (raw.dropDuplicates(['ride_id'])
     .withColumn('started_at', F.col('started_at').cast('timestamp'))
     .withColumn('ended_at', F.col('ended_at').cast('timestamp'))
     .withColumn('dur_s', F.col('ended_at').cast('long') - F.col('started_at').cast('long')))

d = (d.withColumn('f_null_time', F.col('started_at').isNull() | F.col('ended_at').isNull())
       .withColumn('f_bad_dur', flag((F.col('dur_s') <= 0) | (F.col('dur_s') > 86400)))
       .withColumn('f_fake_trip', flag((F.col('dur_s') < 60) &
                                       (F.col('start_station_id') == F.col('end_station_id'))))
       .withColumn('f_geo', flag(~F.col('start_lat').between(40.5, 40.95) |
                                 ~F.col('start_lng').between(-74.1, -73.7)))
       .withColumn('f_no_station', F.col('start_station_id').isNull()))

# Một lần quét: số dòng bị đánh dấu theo từng quy tắc -> Bảng 3.1
stats = d.agg(F.count('*').alias('after_dedup'),
              *[F.sum(F.col(c).cast('int')).alias(c) for c in FLAGS + ['f_no_station']]).first().asDict()
print(stats)

cleaned = (d.filter(~(F.col('f_null_time') | F.col('f_bad_dur') | F.col('f_fake_trip') | F.col('f_geo')))
            .filter((F.col('started_at') >= LO) & (F.col('started_at') < END))
            .drop(*FLAGS, 'f_no_station'))

(cleaned.write.mode('overwrite').partitionBy('month').parquet(f'{BASE}/cleaned'))
```

Cột month lấy từ tên thư mục month=01 khi đọc, nên nó là tháng của tệp chứ không phải tháng khởi hành chính xác. Đừng dùng nó làm đặc trưng, hãy tính lại từ started\_at.

### Mã PySpark: tổng hợp theo (trạm, giờ) và điền giờ trống

```python
cleaned = spark.read.parquet(f'{BASE}/cleaned')
TRAIN_END, N = '2024-09-01 00:00:00', 400

trips = (cleaned.filter(F.col('start_station_id').isNotNull())
         .select(F.col('start_station_id').alias('station_id'),
                 F.date_trunc('hour', 'started_at').alias('hour_ts')))
counts = trips.groupBy('station_id', 'hour_ts').agg(F.count('*').alias('trips'))

# Top N trạm theo tổng chuyến trong giai đoạn Train (không nhìn Validation, Test)
top = (counts.filter(F.col('hour_ts') < TRAIN_END).groupBy('station_id')
       .agg(F.sum('trips').alias('n')).orderBy(F.desc('n')).limit(N).select('station_id'))

# Chỉ điền 0 từ giờ có chuyến đầu tiên của trạm trở đi
first = (counts.join(top, 'station_id').groupBy('station_id')
         .agg(F.min('hour_ts').alias('first_hour')))
hours = spark.sql("SELECT explode(sequence(timestamp'2024-01-01 00:00:00', "
                  "timestamp'2024-12-31 23:00:00', interval 1 hour)) AS hour_ts")
grid = first.crossJoin(hours).filter(F.col('hour_ts') >= F.col('first_hour')).drop('first_hour')

hourly = grid.join(counts, ['station_id', 'hour_ts'], 'left').fillna({'trips': 0})
hourly.write.mode('overwrite').parquet(f'{BASE}/hourly')
```

Vì sao phải điền 0: nhóm theo (trạm, giờ) chỉ trả về những giờ có chuyến, nên lag\_1, lag\_24, lag\_168 ở 2.7.4 sẽ lấy nhầm dòng nếu không có đủ mọi giờ. Điền 0 từ giờ đầu tiên của trạm trở đi để tránh gán số 0 cho thời gian trạm chưa mở.

### Kiểm tra trước khi sang bước sau

- [ ] Mỗi trạm có đúng số dòng bằng số giờ từ first\_hour đến 31/12 23:00, không đứt quãng
- [ ] Tổng trips của bảng hourly bằng số chuyến của N trạm đó trong tầng cleaned
- [ ] Không còn giá trị null ở station\_id, hour\_ts, trips
- [ ] Ghi lại phần trăm tổng số chuyến mà N trạm chiếm được, để nêu trong phạm vi
- [ ] Ghi lại tỷ lệ số 0 trong bảng hourly (dùng cho phần dữ liệu đếm, 2.7.7)
- [ ] Đã điền đủ Bảng 3.1 từ biến stats

## Phần 7. Bước 4: EDA và xây dựng đặc trưng

Mục tiêu: hiểu quy luật nhu cầu rồi biến nó thành đặc trưng mà không để lọt thông tin từ tương lai. Bước này dùng 2.7 (chuỗi thời gian, lag, Sin/Cos, rò rỉ), 2.9.3, 2.9.4 và 2.6.3 (features và label).

### EDA bằng Spark SQL

Chỉ dùng giai đoạn Train (và Validation) để rút kết luận chọn đặc trưng. Không nhìn Test.

```python
hourly = spark.read.parquet(f'{BASE}/hourly')
hourly.createOrReplaceTempView('hourly')

by_hour = spark.sql('''SELECT hour(hour_ts) AS h, AVG(trips) AS avg_trips
                       FROM hourly WHERE hour_ts < '2024-09-01' GROUP BY h ORDER BY h''').toPandas()

# dayofweek của Spark: 1 = Chủ nhật, ..., 7 = Thứ bảy
heat = spark.sql('''SELECT dayofweek(hour_ts) AS dow, hour(hour_ts) AS h, AVG(trips) AS avg_trips
                    FROM hourly WHERE hour_ts < '2024-09-01' GROUP BY dow, h''').toPandas()
```

Kết quả sau khi gom đã nhỏ, nên chuyển sang pandas và vẽ bằng Plotly. Các biểu đồ EDA cần có, cũng là nội dung của Chương 3 mục 3.4:

- Nhu cầu trung bình theo giờ trong ngày, tách ngày thường và cuối tuần.
- Nhu cầu theo thứ trong tuần và theo tháng.
- Heatmap giờ × thứ.
- Top trạm đông nhất, phân bố tổng chuyến theo trạm (đa số trạm thấp, ít trạm rất cao).
- Tỷ lệ số 0 theo nhóm trạm (nhóm đông, vừa, vắng).
- So sánh thành viên và khách vãng lai, xe thường và xe điện (tính từ tầng cleaned).
- Phân bố thời lượng chuyến.

### Dữ liệu bổ sung

- Ngày lễ: tạo danh sách ngày lễ liên bang và của bang New York năm 2024 bằng thư viện holidays hoặc nhập tay, lưu thành bảng nhỏ rồi join theo ngày.
- Thời tiết: tải theo giờ cho New York (nhiệt độ, lượng mưa, tốc độ gió) từ Open-Meteo hoặc NOAA, lưu CSV vào /citibike/external/weather/ và join theo hour\_ts. Đặt múi giờ của nguồn về America/New\_York để giờ khớp giờ địa phương trong dữ liệu chuyến đi. Kiểm tra điều khoản và tên tham số trên trang của nguồn, mình chưa gọi thử API.
- Thời tiết cùng giờ chỉ có thật khi dự báo xong. Khi dùng thời tiết thực tế, phải ghi giả định "coi như dự báo thời tiết chính xác" trong báo cáo (2.7.6).

### Bảng đặc trưng

| Nhóm | Đặc trưng | Cách tính | Chống rò rỉ |
| --- | --- | --- | --- |
| Thời gian | hour, dow, month, is\_weekend, is\_peak, is\_holiday | Từ hour\_ts. is\_peak = ngày thường, giờ 7–9 và 17–19 | Lịch biết trước, không rủi ro |
| Chu kỳ | hour\_sin, hour\_cos, dow\_sin, dow\_cos | sin và cos của 2π·x/P với P = 24 hoặc 7 | Thử có và không, chọn theo kết quả như 2.7.5 |
| Lịch sử | lag\_1, lag\_24, lag\_168, roll\_mean\_3 | Window theo trạm, roll\_mean\_3 là trung bình 3 giờ liền trước | Chỉ dùng giờ nhỏ hơn t |
| Không gian | lat, lng, station\_mean | lat và lng là trung vị tọa độ của trạm. station\_mean là trung bình trips mỗi giờ của trạm | station\_mean chỉ tính từ Train |
| Thời tiết (thí nghiệm) | temp, precip, wind, is\_rain | Join theo giờ | Ghi giả định ở trên |

### Mã PySpark: đặc trưng và chia tập

```python
import math
from pyspark.sql import Window

w = Window.partitionBy('station_id').orderBy('hour_ts')
feat = (hourly
  .withColumn('hour', F.hour('hour_ts'))
  .withColumn('dow', F.dayofweek('hour_ts'))
  .withColumn('month', F.month('hour_ts'))
  .withColumn('is_weekend', F.col('dow').isin(1, 7).cast('int'))
  .withColumn('is_peak', ((F.col('is_weekend') == 0) &
                          F.col('hour').isin(7, 8, 9, 17, 18, 19)).cast('int'))
  .withColumn('hour_sin', F.sin(2 * math.pi * F.col('hour') / 24))
  .withColumn('hour_cos', F.cos(2 * math.pi * F.col('hour') / 24))
  .withColumn('dow_sin', F.sin(2 * math.pi * (F.col('dow') - 1) / 7))
  .withColumn('dow_cos', F.cos(2 * math.pi * (F.col('dow') - 1) / 7))
  .withColumn('lag_1', F.lag('trips', 1).over(w))
  .withColumn('lag_24', F.lag('trips', 24).over(w))
  .withColumn('lag_168', F.lag('trips', 168).over(w))
  .withColumn('roll_mean_3', F.avg('trips').over(w.rowsBetween(-3, -1)))
  .filter(F.col('lag_168').isNotNull())          # bỏ 168 giờ đầu của mỗi trạm
  .withColumn('split', F.when(F.col('hour_ts') < '2024-09-01', 'train')
                        .when(F.col('hour_ts') < '2024-11-01', 'val')
                        .otherwise('test')))

st_mean = (feat.filter(F.col('split') == 'train').groupBy('station_id')
           .agg(F.avg('trips').alias('station_mean')))          # chỉ từ Train
stations = (cleaned.filter(F.col('start_station_id').isNotNull())
            .groupBy(F.col('start_station_id').alias('station_id'))
            .agg(F.expr('percentile_approx(start_lat, 0.5)').alias('lat'),
                 F.expr('percentile_approx(start_lng, 0.5)').alias('lng'),
                 F.first('start_station_name', ignorenulls=True).alias('name')))

feat = feat.join(st_mean, 'station_id').join(stations, 'station_id')
feat.write.mode('overwrite').partitionBy('split').parquet(f'{BASE}/features')
```

lag\_1, lag\_24 và lag\_168 chỉ đúng vì bảng hourly đã điền kín từng giờ ở Bước 3. Mỗi lần dòng t lấy lag, nó lấy đúng giờ t−1, t−24, t−168.

### Hai lưu ý khi dùng mã trạm

- Không đưa station\_id vào cây dưới dạng biến phân loại. Spark yêu cầu maxBins không nhỏ hơn số giá trị của biến phân loại, mà N khoảng 300–500 trạm vượt xa mặc định 32. Dùng lat, lng, station\_mean để cây phân biệt trạm.
- Với mô hình tuyến tính có thể thử StringIndexer và OneHotEncoder cho station\_id (2.9.3), nhưng cần fit trên Train và dùng lại cho Validation và Test.

### Kiểm tra chống rò rỉ

- [ ] Mọi đặc trưng của dòng t chỉ dùng dữ liệu đến giờ t−1, trừ đặc trưng lịch vốn biết trước
- [ ] station\_mean chỉ tính từ Train
- [ ] Chọn đặc trưng và kết luận EDA không dựa vào Test
- [ ] Thử tay một trạm: lag\_1 của dòng t bằng trips của dòng t−1, lag\_168 bằng trips cùng giờ tuần trước
- [ ] Nếu thêm tỷ lệ thành viên hoặc xe điện, chỉ dùng dạng lag. Tỷ lệ của chính giờ cần dự báo là rò rỉ

## Phần 8. Bước 5: baseline, mô hình MLlib và đánh giá

Mục tiêu: có bảng so sánh baseline và 6 mô hình (Linear, Ridge, Lasso, Decision Tree, Random Forest, GBT) trên cùng dữ liệu, chọn tham số trên Validation, chỉ chạy Test một lần cuối. Bước này là quy trình ở 2.6.2 đến 2.6.6, các mô hình ở 2.8, chuẩn hóa ở 2.9.2, độ đo ở 2.10.

### Baseline (làm ở Tuần 5)

Baseline không cần huấn luyện bằng MLlib (2.8.1):

- Seasonal Naive: dự báo bằng lag\_168.
- Trung bình lịch sử theo (trạm, thứ, giờ), tính từ Train rồi áp cho Validation và Test.

```python
from pyspark.ml.evaluation import RegressionEvaluator

feat = spark.read.parquet(f'{BASE}/features').withColumn('label', F.col('trips').cast('double'))
train, val, test = [feat.filter(F.col('split') == s) for s in ('train', 'val', 'test')]
train.cache(); train.count()

def metrics(df, pred='y_hat', label='label'):
    out = {m: RegressionEvaluator(labelCol=label, predictionCol=pred, metricName=m).evaluate(df)
           for m in ('mae', 'rmse', 'r2')}
    s = df.agg(F.sum(F.abs(F.col(label) - F.col(pred))).alias('ae'), F.sum(label).alias('y')).first()
    out['wmape'] = s['ae'] / s['y']
    return out

naive = val.withColumn('y_hat', F.col('lag_168').cast('double'))
hist = (train.groupBy('station_id', 'dow', 'hour').agg(F.avg('label').alias('y_hat')))
mean_hist = val.join(hist, ['station_id', 'dow', 'hour'], 'left').fillna({'y_hat': 0.0})
print(metrics(naive), metrics(mean_hist))
```

Kết quả baseline là mốc cho báo cáo Giai đoạn 1 và là mốc để kết luận "mô hình có hiệu quả" ở 2.8.1. Xem lỗi baseline theo giờ cao điểm, trạm đông và ngày mưa để nêu điểm cần cải thiện.

### Pipeline và lưới tham số

Chuẩn hóa chỉ dùng cho nhóm tuyến tính. Nhóm cây dùng thẳng vector đặc trưng (2.9.2). Chọn tham số bằng vòng lặp tay trên Validation. Không dùng CrossValidator hay TrainValidationSplit của Spark vì chúng chia ngẫu nhiên, sẽ làm dữ liệu tương lai lọt vào Train (2.7.6, 2.10.6).

```python
import time
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import (LinearRegression, DecisionTreeRegressor,
                                   RandomForestRegressor, GBTRegressor,
                                   GeneralizedLinearRegression)

FEATURES = ['hour', 'dow', 'month', 'is_weekend', 'is_peak', 'hour_sin', 'hour_cos',
            'dow_sin', 'dow_cos', 'lag_1', 'lag_24', 'lag_168', 'roll_mean_3',
            'lat', 'lng', 'station_mean']

def tree_pipe(est):
    return Pipeline(stages=[VectorAssembler(inputCols=FEATURES, outputCol='features'), est])

def lin_pipe(est):
    return Pipeline(stages=[VectorAssembler(inputCols=FEATURES, outputCol='raw'),
                            StandardScaler(inputCol='raw', outputCol='features',
                                           withMean=True, withStd=True), est])

GRID = {
  'linear': (lambda p: lin_pipe(LinearRegression(regParam=0.0)), [{}]),
  'ridge':  (lambda p: lin_pipe(LinearRegression(regParam=p['r'], elasticNetParam=0.0)),
             [{'r': r} for r in (0.01, 0.1, 1.0)]),
  'lasso':  (lambda p: lin_pipe(LinearRegression(regParam=p['r'], elasticNetParam=1.0)),
             [{'r': r} for r in (0.001, 0.01, 0.1)]),
  'dt':     (lambda p: tree_pipe(DecisionTreeRegressor(maxDepth=p['d'], minInstancesPerNode=p['m'])),
             [{'d': d, 'm': m} for d in (5, 8, 12) for m in (1, 50)]),
  'rf':     (lambda p: tree_pipe(RandomForestRegressor(numTrees=p['t'], maxDepth=p['d'],
                                                       featureSubsetStrategy='onethird', seed=42)),
             [{'t': 50, 'd': d} for d in (8, 12)]),
  'gbt':    (lambda p: tree_pipe(GBTRegressor(maxIter=p['i'], maxDepth=p['d'], stepSize=0.1, seed=42)),
             [{'i': i, 'd': d} for i in (50, 100) for d in (5, 8)]),
}

results, models = [], {}
for name, (make, grid) in GRID.items():
    for p in grid:
        t0 = time.time()
        m = make(p).fit(train)
        pred = m.transform(val).withColumn('y_hat', F.greatest(F.col('prediction'), F.lit(0.0)))
        results.append({'model': name, 'params': str(p), 'fit_s': round(time.time() - t0, 1),
                        **metrics(pred)})
        models[(name, str(p))] = m
```

Ghi chú kỹ thuật:

- greatest(prediction, 0) đưa dự báo âm về 0, vì số chuyến không âm (2.7.7). Ghi việc này trong báo cáo.
- Chọn cấu hình tốt nhất mỗi mô hình theo RMSE trên Validation (hàm mất mát bình phương mà các mô hình tối ưu). Vẫn báo cáo đủ MAE, RMSE, R² và WMAPE.
- Thời gian huấn luyện GBT và RF trên 12 tháng có thể lâu trên máy cá nhân. Chạy lưới trên 1–2 tháng trước, rồi chỉ chạy cấu hình đã chọn trên 12 tháng. Ghi cả fit\_s vào bảng kết quả.
- Poisson (tùy chọn): GeneralizedLinearRegression(family='poisson', link='log', regParam=0.01, maxIter=50) trong lin\_pipe. Nhãn phải không âm, nên không cần greatest.
- Không huấn luyện lại trên Train + Validation trước khi chạy Test, để đúng quy trình 2.6.5 và 2.6.6. Nếu muốn huấn luyện lại, phải nêu rõ trong báo cáo.

### Đánh giá trên Test (một lần)

1. Lấy cấu hình tốt nhất của mỗi mô hình từ bảng results.
2. Chạy các mô hình đã chọn và hai baseline trên Test, tính MAE, RMSE, R², WMAPE.
3. Ghi y, y\_hat, tên mô hình ra /citibike/predictions cho dashboard.
4. Lấy featureImportances của RF và GBT: dict(zip(FEATURES, model.stages\[-1\].featureImportances.toArray())).

### Thí nghiệm để có phát hiện (chạy với mô hình tốt nhất)

- Bỏ nhóm lag: nhu cầu phụ thuộc lịch sử đến mức nào (2.7.4).
- Có và không có Sin/Cos: chọn theo kết quả như 2.7.5 đã nêu.
- Có và không có thời tiết, có và không có ngày lễ.
- Sai số theo nhóm: giờ cao điểm và thấp điểm, ngày thường và cuối tuần, trạm đông, vừa, vắng (chia theo station\_mean), ngày mưa và ngày khô.
- Biểu đồ y so với y\_hat cho 3 trạm (đông, vừa, vắng) trong một tuần của Test.

Tiêu chí xong: có bảng kết quả đủ 8 dòng (2 baseline và 6 mô hình, thêm Poisson nếu làm), ít nhất một mô hình cải thiện RMSE so với baseline tốt nhất trên Test, và giải thích được vì sao. Nếu không mô hình nào thắng baseline, đó vẫn là kết quả đáng báo cáo, nhưng cần kiểm tra rò rỉ và lỗi đặc trưng trước.

## Phần 9. Bước 6: thí nghiệm Big Data

Bốn thí nghiệm nhỏ biến lý thuyết ở 2.2, 2.3, 2.4, 2.5 thành số liệu và ảnh chụp thật cho Chương 3. Đây cũng là phần giúp đề tài ghi điểm ở tiêu chí lưu trữ và xử lý dữ liệu (2,0 điểm trong rubric) mà không chỉ dừng ở dự báo. Làm thí nghiệm 4 ở Tuần 5 (MapReduce) và Tuần 8 (ba thí nghiệm còn lại).

| # | Thí nghiệm | Ghi lại | Mục Chương 2 |
| --- | --- | --- | --- |
| 1 | CSV so với Parquet | Dung lượng trên HDFS, thời gian cùng một truy vấn | 2.3 |
| 2 | Thay đổi số core và số partition | Thời gian chạy cùng một job, biểu đồ tăng tốc | 2.5.2, 2.5.5 |
| 3 | Chịu lỗi khi tắt một DataNode | Ảnh fsck trước, trong, sau khi tắt | 2.2.3, 2.2.5 |
| 4 | MapReduce đếm chuyến theo trạm | Kết quả trùng Spark, so sánh thời gian | 2.4.2 |

### Thí nghiệm 1: CSV so với Parquet

Phải so cùng số dòng. Tầng cleaned đã bị lọc nên không so được với raw. Hãy ghi thêm một bản Parquet của chính dữ liệu raw rồi so hai bản này.

```python
import time, statistics

raw.write.mode('overwrite').parquet(f'{BASE}/raw_parquet')      # cùng số dòng với CSV
csv_df = spark.read.option('header', True).schema(schema).csv(f'{BASE}/raw')
pq_df = spark.read.parquet(f'{BASE}/raw_parquet')

def q(d):   # chỉ cần 1 cột: Parquet đọc được riêng cột đó (column pruning)
    return d.groupBy('start_station_id').count().orderBy(F.desc('count')).limit(10).collect()

for name, d in (('csv', csv_df), ('parquet', pq_df)):
    ts = []
    for _ in range(3):
        t0 = time.time(); q(d); ts.append(time.time() - t0)
    print(name, round(statistics.median(ts), 1), 's')
```

```bash
hdfs dfs -du -h /citibike/raw
hdfs dfs -du -h /citibike/raw_parquet
```

Chạy 3 lần và lấy trung vị, vì lần đầu bị ảnh hưởng bởi bộ nhớ đệm của hệ điều hành. Ghi rõ đây là thí nghiệm trên một máy.

### Thí nghiệm 2: số core và số partition

Dùng cùng một script (ví dụ job tổng hợp theo giờ ở Bước 3) và đo thời gian.

```bash
spark-submit --master local[1] jobs/hourly_agg.py
spark-submit --master local[2] jobs/hourly_agg.py
spark-submit --master local[4] jobs/hourly_agg.py
```

Sau đó giữ 4 core và đổi spark.sql.shuffle.partitions lần lượt 4, 16, 64, 200. Trên cụm standalone thì đổi --total-executor-cores. Kết quả hiếm khi tăng tuyến tính, hãy giải thích bằng chi phí shuffle và giới hạn đĩa, đúng với mô tả "wide transformation" ở 2.5.

### Thí nghiệm 3: chịu lỗi HDFS

1. Chụp hdfs fsck của một tệp lớn khi cả hai DataNode chạy.
2. Tắt một DataNode (ví dụ docker stop datanode2).
3. Đọc lại tệp hoặc chạy Spark count trên raw: vẫn đọc được nhờ bản sao còn lại.
4. Chờ NameNode đánh dấu DataNode chết rồi chụp fsck, sẽ thấy Block thiếu bản sao (under-replicated). Theo cấu hình mặc định, thời gian chờ khoảng 10 phút rưỡi. Khi demo có thể giảm thời gian này trong cấu hình.
5. Bật lại DataNode và chụp fsck lần nữa khi hệ thống tự nhân bản trở lại.

Vì đề tài dùng replication 2 trên hai DataNode, thí nghiệm này chỉ làm được ở phương án A. Ở phương án B (replication 1) không làm được, hãy nói rõ điều này trong báo cáo.

### Thí nghiệm 4: MapReduce đếm chuyến theo trạm

Làm đúng ví dụ ở 2.4.2 bằng Hadoop Streaming trên một tháng. Kiểm tra thứ tự cột: cột thứ 6 (chỉ số 5) phải là start\_station\_id.

```python
# mapper.py
import sys, csv
for row in csv.reader(sys.stdin):
    if len(row) > 5 and row[0] != 'ride_id' and row[5]:
        print(f'{row[5]}\t1')
```

```python
# reducer.py
import sys
cur, n = None, 0
for line in sys.stdin:
    k, v = line.rstrip('\n').split('\t')
    if k != cur:
        if cur is not None:
            print(f'{cur}\t{n}')
        cur, n = k, 0
    n += int(v)
if cur is not None:
    print(f'{cur}\t{n}')
```

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar -files mapper.py,reducer.py -mapper 'python3 mapper.py' -reducer 'python3 reducer.py' -input /citibike/raw/year=2024/month=01 -output /citibike/mr_out_01
hdfs dfs -cat /citibike/mr_out_01/part-* | sort > mr.txt
```

- So kết quả với Spark chạy trên cùng dữ liệu thô: đếm theo start\_station\_id khác rỗng, không loại trùng, không lọc gì thêm. Hai kết quả phải trùng từng trạm.
- Máy chạy tác vụ cần có python3. Ở image Hadoop có thể không có sẵn, kiểm tra và cài nếu thiếu. Mình chưa chạy thử lệnh này.
- Đây là thí nghiệm minh họa. Phần xử lý chính của đề tài vẫn là Spark (2.4.3). Ghi kết quả so sánh thời gian như một minh chứng cho nhận xét "MapReduce ghi đĩa nhiều" chứ không kết luận quá mạnh từ một lần đo.

## Phần 10. Bước 7: dashboard

Mục tiêu: một ứng dụng Streamlit hiển thị đủ 8 nội dung trực quan hóa đã nêu ở cuối mục 2.11, chạy được trên máy thuyết trình mà không cần cụm Hadoop. Dashboard đọc các bảng nhỏ ở tầng serving, không gọi Spark.

### Bảng đối chiếu nội dung trực quan

| # | Nội dung ở 2.11 | Biểu đồ | Bảng dữ liệu (tầng serving) |
| --- | --- | --- | --- |
| 1 | Nhu cầu theo giờ trong ngày | Đường, tách ngày thường và cuối tuần | by\_hour |
| 2 | So sánh các ngày trong tuần | Cột | by\_dow |
| 3 | Nhu cầu theo tháng | Cột hoặc đường | by\_month |
| 4 | Heatmap giờ và ngày trong tuần | Heatmap Plotly | heat |
| 5 | So sánh trạm hoặc khu vực | Cột ngang, top 20 trạm | by\_station |
| 6 | Phân bố nhu cầu trên bản đồ | Folium, chấm tròn to nhỏ theo nhu cầu, thanh chọn giờ | station\_hour\_avg, stations |
| 7 | So sánh thực tế và dự báo | Hai đường, chọn trạm và khoảng ngày | predictions |
| 8 | So sánh các mô hình theo MAE, RMSE, R² | Bảng và cột nhóm | metrics |

### Xuất dữ liệu cho dashboard (cuối Bước 5)

```python
OUT = f'{BASE}/serving'
by_hour.to_parquet('by_hour.parquet')     # các bảng đã toPandas ở Bước 4
# Dự báo: chỉ giữ Test, mô hình tốt nhất và baseline tốt nhất để file nhỏ
(preds.filter(F.col('split') == 'test')
      .select('station_id', 'hour_ts', 'label', 'y_hat', 'model')
      .coalesce(1).write.mode('overwrite').parquet(f'{OUT}/predictions'))
```

```bash
hdfs dfs -get /citibike/serving ./dashboard/data
```

Các bảng by\_dow, by\_month, heat, by\_station, station\_hour\_avg (trung bình trips theo trạm và giờ, kèm lat, lng, name lấy từ bảng stations ở Phần 7) và metrics đều tạo bằng Spark SQL giống by\_hour rồi toPandas. Riêng by\_hour cần thêm cột day\_type (ngày thường, cuối tuần) nếu muốn tách hai đường. Biến preds là bảng dự báo gộp của các mô hình ở Phần 8, có thêm cột model và split.

Bảng dự báo đủ 8 mô hình sẽ rất lớn (hàng triệu dòng mỗi mô hình), nên chỉ đưa vào dashboard mô hình tốt nhất và một baseline. Số liệu so sánh đủ các mô hình nằm trong bảng metrics nhỏ.

### Khung ứng dụng

```python
# dashboard/app.py
import pandas as pd, plotly.express as px, folium, streamlit as st
from streamlit_folium import st_folium

@st.cache_data
def load(name):
    return pd.read_parquet(f'data/{name}.parquet')

page = st.sidebar.radio('Trang', ['Tổng quan', 'Bản đồ', 'Dự báo theo trạm', 'So sánh mô hình'])

if page == 'Tổng quan':
    st.plotly_chart(px.line(load('by_hour'), x='h', y='avg_trips'))
    heat = load('heat').pivot(index='dow', columns='h', values='avg_trips')
    st.plotly_chart(px.imshow(heat, aspect='auto'))

elif page == 'Bản đồ':
    h = st.slider('Giờ trong ngày', 0, 23, 8)
    d = load('station_hour_avg').query('h == @h')
    m = folium.Map(location=[40.73, -73.99], zoom_start=12)
    for r in d.itertuples():
        folium.CircleMarker([r.lat, r.lng], radius=2 + 3 * r.avg_trips ** 0.5,
                            tooltip=f'{r.name}: {r.avg_trips:.1f} chuyến/giờ').add_to(m)
    st_folium(m, height=550)
```

Chạy bằng streamlit run dashboard/app.py. Khung trên chỉ là xương sống, trang Dự báo theo trạm và So sánh mô hình viết tương tự với bảng predictions và metrics.

### Tiêu chí xong

- [ ] Cả 8 nội dung ở bảng trên đều có trên dashboard
- [ ] Mỗi biểu đồ có tiêu đề, đơn vị trục và một câu nêu điều rút ra
- [ ] Dashboard chạy với dữ liệu trong thư mục dashboard/data, không cần Docker
- [ ] README có đủ lệnh cài và chạy (đáp ứng yêu cầu hướng dẫn cài đặt ở file kế hoạch)
- [ ] Đã quay video demo 2–3 phút để dự phòng khi thuyết trình

## Phần 11. Cấu trúc dự án và phân công

Một kho Git chung, mỗi bước là một script chạy lại được. Notebook chỉ dùng để khám phá và vẽ hình cho báo cáo. Cách tổ chức này đáp ứng yêu cầu nộp mã nguồn và hướng dẫn chạy ở file kế hoạch.

```text
Phan_Tich_Du_Bao_CitiBike/
  README.md                     # Tổng quan dự án và hướng dẫn chạy hoàn chỉnh
  HUONG_DAN_CHAY_DU_AN.md       # Sổ tay chi tiết các bước thực thi
  PLAN_THUC_HIEN_DO_AN_CITIBIKE.md # Kế hoạch chi tiết 10 tuần bám sát rubric
  requirements.txt              # Danh sách thư viện Python cần thiết
  docker/                       # Docker Compose 5 container (Hadoop NameNode, 2 DataNodes, Spark Master, 2 Workers)
    docker-compose.yml
    config/                     # core-site.xml, hdfs-site.xml, spark-defaults.conf
    start.bat                   # Khởi động cụm Docker 1-click trên Windows
    README.md                   # Hướng dẫn chi tiết vận hành cụm
  scripts/
    download_data.py            # Tải dữ liệu S3 (hỗ trợ --seasonal, --months, --all)
    load_to_hdfs.py             # Đưa dữ liệu CSV lên HDFS theo hive partition
    run_pipeline.bat            # Menu điều khiển chạy tự động toàn bộ pipeline
  jobs/                         # Chạy bằng spark-submit phân tán
    01_explore.py               # Khảo sát schema, volume, veracity, variety
    02_clean.py                 # Tiền xử lý, lọc chuyến ảo, xuất Bảng 3.1
    03_hourly.py                # Lưới giờ Top 400 trạm & Zero-filling
    04_features.py              # Đặc trưng chuỗi thời gian & chia tập chống rò rỉ
    05_baseline.py              # Đánh giá Seasonal Naive & Historical Mean
    06_train_eval.py            # Huấn luyện 6 mô hình MLlib & xuất metrics
    07_export_serving.py        # Xuất các bảng tầng serving cho dashboard
    08_bigdata_experiments.py   # Thí nghiệm CSV vs Parquet & Column Pruning
    mapreduce/                  # Mã nguồn Hadoop Streaming MapReduce
      mapper.py
      reducer.py
  notebooks/                    # Phân tích EDA và trực quan hóa
  dashboard/
    app.py                      # Streamlit Enterprise Cockpit & Trình giả lập điều phối
    data/                       # Các bảng Parquet tổng hợp từ tầng serving
  data/
    csv/                        # Dữ liệu CSV giải nén theo tháng
    serving/                    # Dữ liệu Parquet tối ưu phục vụ dashboard
    zip/                        # File nén zip tải về từ Citi Bike
  report/                       # Tài liệu báo cáo, đề cương và minh chứng
```

### Phân công

| Thành viên | Phụ trách chính | Viết ở Chương 3 |
| --- | --- | --- |
| A | Cụm Docker, HDFS, tải và đưa dữ liệu lên (Bước 1–2), làm sạch và tổng hợp (Bước 3), thí nghiệm Big Data (Bước 6), xuất tầng serving | 3.1, 3.2, 3.3, 3.5, phần hiệu năng của 3.8 |
| B | EDA và đặc trưng (Bước 4), baseline, mô hình, đánh giá (Bước 5), dashboard (Bước 7) | 3.4, 3.6, 3.7, 3.8, 3.9 |
| Cả hai | Chương 1 và 4, slide, demo, rà soát chéo mã và báo cáo |  |

Trao đổi chéo: mỗi người đọc và chạy lại script của người kia ít nhất một lần, để cả hai trả lời được câu hỏi phản biện về toàn bộ quy trình. Lịch sử commit và bảng phân công là bằng chứng cho tiêu chí làm việc nhóm.

### Dồn công sức theo rubric

| Tiêu chí | Điểm | Phần trong kế hoạch này |
| --- | --- | --- |
| Tính ứng dụng của kỹ thuật | 1,0 | Mục 1.1 và Chương 4 nêu giá trị cho điều phối xe |
| Lưu trữ và xử lý dữ liệu | 2,0 | Bước 1–3 và thí nghiệm Big Data (Bước 6) |
| Mô hình và công nghệ xử lý | 2,5 | Bước 4–5: baseline, 6 mô hình, chống rò rỉ, phân tích lỗi |
| Trực quan hóa và ứng dụng | 2,5 | Bước 7: dashboard có bản đồ và dự báo theo trạm |
| Trình bày báo cáo | 1,0 | Viết Chương 3 sau mỗi bước, có ảnh chụp và bảng số liệu |
| Phân công và phối hợp | 1,0 | Git, bảng phân công, rà soát chéo |

## Phần 12. Đối chiếu Chương 2 với việc thực hiện

Mỗi mục lý thuyết phải có một việc làm thật và một bằng chứng trong Chương 3. Bảng dưới là phép kiểm tra "khớp nội dung": nếu một dòng không có bằng chứng, hoặc sửa Chương 2 cho đúng thực tế, hoặc làm thêm việc.

| Mục Chương 2 | Việc phải làm | Bằng chứng cho Chương 3 | Bước |
| --- | --- | --- | --- |
| 2.1 Big Data, 5V, batch | Ghi số liệu Volume (số dòng, GB), Variety (thời gian, tọa độ, phân loại), Veracity (tỷ lệ thiếu và lỗi). Nêu rõ dữ liệu phát hành theo tháng nên xử lý theo lô | Bảng số liệu 5V, câu nêu chọn batch ở 3.1 | 2, 3 |
| 2.2 HDFS | Replication 2 trên 2 DataNode, thư mục raw đến serving, dùng đủ các lệnh put, ls, cat, get, rm ở 2.2.7 | Ảnh fsck, dfsadmin, giao diện NameNode, bảng thư mục | 2, TN3 |
| 2.3 CSV và Parquet | Raw giữ CSV, cleaned và features lưu Parquet, đo dung lượng và thời gian | Bảng so sánh CSV với Parquet | 3, TN1 |
| 2.4 Hadoop, MapReduce | Chạy ví dụ đếm chuyến theo trạm, nói rõ xử lý chính là Spark | Kết quả MapReduce trùng Spark, thời gian | TN4 |
| 2.5 Spark | DataFrame và Spark SQL cho làm sạch và tổng hợp. Mở Spark UI xem DAG và dùng explain() để minh họa Lazy Evaluation | Ảnh Spark UI, kết quả explain() | 1–3, TN2 |
| 2.6 MLlib | Pipeline, cột features và label, chia Train, Validation, Test theo thời gian | Sơ đồ pipeline, bảng khoảng ngày của ba tập | 4, 5 |
| 2.7 Chuỗi thời gian | lag\_1, lag\_24, lag\_168, trung bình trượt, Sin/Cos, kiểm soát rò rỉ, tỷ lệ số 0 | Bảng đặc trưng, danh sách kiểm tra rò rỉ, thống kê số 0 | 3, 4 |
| 2.8 Mô hình | Baseline, Linear, Ridge, Lasso, DT, RF, GBT, Poisson (tùy chọn). Ghi RMSE trên Train bên cạnh Validation của cấu hình tốt nhất để minh họa overfitting (2.8.7) | Bảng tham số và kết quả, bảng Train so với Validation | 5 |
| 2.9 Tiền xử lý, đặc trưng | Bảng 3.1 cho 2.9.1. Chuẩn hóa chỉ cho nhóm tuyến tính (2.9.2). Với nhóm tuyến tính, mã hóa one-hot cho hour và dow bằng OneHotEncoder để mục 2.9.3 có ứng dụng thật. Dùng mã trạm trực tiếp, không dùng lưới địa lý (2.9.4) | Bảng 3.1, mô tả pipeline | 3, 5 |
| 2.10 Độ đo | MAE, RMSE, R² làm độ đo chính, WMAPE bổ sung, không dùng MAPE | Bảng kết quả trên Validation và Test | 5 |
| 2.11 Trực quan hóa | Đủ 8 nội dung ở danh sách cuối 2.11 | Ảnh chụp từng trang dashboard | 7 |

### Chỗ cần sửa cho thống nhất

- Poisson (2.8.3): nếu không làm, giữ nguyên ở mức lý thuyết như đoạn "Ứng dụng trong đề tài" đã viết. Nếu làm, thêm vào bảng kết quả.
- Time-Series Cross-Validation (2.10.6): kế hoạch dùng chia cố định. Giữ nó là hướng mở rộng, không viết như đã làm.
- Sin/Cos (2.7.5): đoạn văn nói việc dùng sẽ "quyết định dựa trên kết quả". Thí nghiệm có và không có Sin/Cos ở Bước 5 chính là câu trả lời.
- Thời tiết và ngày lễ (1.2): làm thì sửa "nếu được triển khai" thành đã triển khai, và ghi giả định ở Bước 4.
- Parquet (2.3.4): vì kế hoạch dùng Parquet thật, Chương 3 được viết như đã triển khai.
- Hai câu dẫn "một quy trình có thể được mô tả như sau" ở 2.5.5 và "Quy trình tổng quát có thể được mô tả như sau" ở 2.6.2: trong bản mình đọc được, hai câu này không có nội dung đi kèm. Có thể đó là hình mình không đọc được, hãy kiểm tra lại trong file Word.

## Phần 13. Rủi ro và phương án dự phòng

| Rủi ro | Dấu hiệu | Phương án |
| --- | --- | --- |
| Máy thiếu RAM hoặc ổ đĩa | Spark báo OutOfMemory, Docker treo, hết dung lượng | Giảm số tháng hoặc N trạm, chạy local\[\*\], bỏ bớt một worker, hạ shuffle partitions |
| Thứ tự cột lệch giữa các tháng | Dữ liệu bị đọc sai cột mà không báo lỗi | In tên cột từng tháng trước khi đặt schema (Phần 5) |
| Nhiều chuyến thiếu start\_station\_id | Tỷ lệ null cao ở khảo sát | Không điền bù. Nêu tỷ lệ và ảnh hưởng trong hạn chế: nhu cầu theo trạm bị ước lượng thấp |
| Trạm đóng tạm thời | Chuỗi số 0 dài bất thường | Dữ liệu không cho biết trạm đóng hay vắng. Ghi vào hạn chế, không tự loại bỏ |
| Mô hình không thắng baseline | Seasonal Naive (lag\_168) rất mạnh với dữ liệu theo giờ | Kiểm tra rò rỉ và lỗi đặc trưng, thêm đặc trưng. Nếu vẫn thua, báo cáo trung thực và giải thích |
| Kết quả đẹp bất thường (R² sát 1) | R² cao hấp dẫn, sai số gần 0 | Gần như chắc chắn là rò rỉ. Chạy lại danh sách kiểm tra ở Phần 7, thử bỏ từng đặc trưng lịch sử |
| Lệch mùa giữa Train và Test | Sai số Test cao hơn hẳn Validation | Ghi vào hạn chế. Nếu còn thời gian và máy cho phép, mở rộng sang 24 tháng |
| RF hoặc GBT chạy quá lâu | Một lần fit hơn vài chục phút | Thu nhỏ lưới, giảm N, tinh chỉnh trên 1–2 tháng rồi chỉ chạy cấu hình đã chọn |
| DataNode không lên, HDFS ở safe mode | hdfs dfsadmin -report thiếu DataNode | Kiểm tra log, hdfs dfsadmin -safemode get. Nếu không sửa được, chuyển sang phương án B và bỏ thí nghiệm 3 |
| API thời tiết lỗi hoặc đổi điều khoản | Không tải được | Tải sớm và lưu CSV. Nếu không được, bỏ thí nghiệm thời tiết và sửa 1.2 |
| Giảng viên cho rằng phạm vi dữ liệu chưa đủ "lớn" | Góp ý khi duyệt | Mở rộng số năm là cách rẻ nhất, vì quy trình đã viết theo tháng và năm, chỉ cần đổi hằng số thời gian |
| Sự cố khi thuyết trình | Cụm không chạy | Dashboard đọc file cục bộ, có video demo dự phòng, có ảnh chụp Spark UI và HDFS |
| Giống bài người khác (điểm 0) | Đề tài Citi Bike là đề phổ biến trên mạng | Tự viết bằng lời nhóm, dùng số liệu và ảnh chụp từ máy mình, ghi rõ nguồn tham khảo |

## Phần 14. Checklist các mốc nộp bài

### Giai đoạn 1 (Mục tiêu hoàn thành Giai đoạn 1)

- [x] Đề tài và bộ dữ liệu đã được duyệt (Citi Bike NYC 2024, quy mô 14.97+ triệu bản ghi)
- [x] Dữ liệu đã trên HDFS, có số liệu điền 1.4.2 (~2.79 GB CSV thô, nạp HDFS replication=2 là ~5.5 GB)
- [x] Bảng 3.1 số dòng trước và sau từng quy tắc làm sạch (tính tự động single-pass trong Job 02)
- [x] Kết quả hai baseline (MAE, RMSE, R², WMAPE) và phân tích điểm cần cải thiện (Job 05)
- [x] Sơ đồ kiến trúc và luồng dữ liệu (Lakehouse phân tầng 5 bước chuẩn hóa)
- [x] Tài liệu tóm tắt lý thuyết kèm ví dụ minh họa (Chương 2 và ví dụ MapReduce jobs/mapreduce/)

### Giai đoạn 2 (Hoàn thiện sản phẩm & Nộp bài)

- [x] Mã nguồn đầy đủ các bước (Job 01 đến Job 08, MapReduce, download, load HDFS), có README.md, HUONG_DAN_CHAY_DU_AN.md và requirements.txt
- [ ] File Word đúng cấu trúc: bìa, lịch làm việc hàng tuần, công việc mỗi thành viên, mục lục, giới thiệu, cơ sở lý thuyết, xây dựng ứng dụng và trực quan hóa, kết luận, tài liệu tham khảo, phụ lục
- [ ] File PowerPoint bảo vệ đồ án
- [x] Dữ liệu thực nghiệm và liên kết dữ liệu Amazon S3 chính thức
- [x] Bảng phân công và đối chiếu rubric chi tiết
- [x] Đã rà soát lỗi chính tả và khớp nối số liệu 100% giữa mã nguồn, HDFS, mô hình và dashboard

### Tuần 10 (Thuyết trình & Demo)

- [x] Demo hoàn chỉnh sẵn sàng chạy trên máy thuyết trình (Dashboard Streamlit Enterprise Cockpit đọc tầng serving cục bộ, độc lập không phụ thuộc cụm trong lúc live demo)
- [ ] Mỗi người nắm vững và trả lời được các câu hỏi phản biện dưới đây

Câu hỏi phản biện dự kiến:

1. Vì sao dùng Spark thay vì MapReduce hay pandas? (2.1.3, 2.4.3)
2. Vì sao chia dữ liệu theo thời gian chứ không ngẫu nhiên? Rò rỉ dữ liệu là gì? (2.7.6)
3. Vì sao phải điền giờ trống bằng 0? (2.7.4, 2.7.7)
4. Vì sao không dùng MAPE? (2.10.5)
5. Vì sao chỉ lấy Top N trạm, và việc đó ảnh hưởng kết quả thế nào?
6. Mô hình nào tốt nhất, tốt hơn baseline bao nhiêu phần trăm, vì sao?
7. Nếu một DataNode hỏng thì điều gì xảy ra? (2.2.5)
8. Làm sao dùng kết quả này để điều phối xe? Vì sao đề tài không làm luôn? (1.3.2d)
