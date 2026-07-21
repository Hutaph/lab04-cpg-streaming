# Spark Structured Streaming Jobs

Thư mục này chứa các công việc ingestion bằng Apache Spark Structured Streaming.

## Mục tiêu Task 5

Job `metadata_to_mongodb.py` đọc event `FILE_METADATA_UPSERT` từ Kafka topic
`source.metadata`, giải mã JSON theo schema metadata của dự án, rồi upsert một
document cho mỗi `file_id` vào MongoDB collection `file_statistics`. Spark
checkpoint lưu offset Kafka để query có thể tiếp tục sau khi khởi động lại.

Writer dùng `replace` với `upsertDocument=true` và `idFieldList=file_id`; vì vậy
replay cùng metadata sẽ cập nhật document hiện có thay vì tạo bản ghi mới.

## Khởi chạy job ghi metadata sang MongoDB

Chạy local bằng lệnh `spark-submit`:

```bash
MONGODB_URI='mongodb://root:CHANGE_ME_MONGO_PASSWORD@localhost:27017/?authSource=admin' \
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
             spark_jobs/metadata_to_mongodb.py
```

Các tham số kết nối có thể truyền qua biến môi trường hoặc CLI. Nếu đã copy
`.env.example` thành `.env`, dùng script PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_metadata_to_mongodb.ps1
```

Để xử lý các event hiện đang có trong Kafka rồi dừng job:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_metadata_to_mongodb.ps1 -AvailableNow
```

Script tự nạp `KAFKA_BOOTSTRAP_SERVERS`, `MONGODB_URI`,
`MONGODB_DATABASE`, `MONGODB_COLLECTION` và `SPARK_CHECKPOINT_PATH` từ `.env`.

Hoặc truyền trực tiếp các biến môi trường:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
MONGODB_URI='mongodb://root:CHANGE_ME_MONGO_PASSWORD@localhost:27017/?authSource=admin' \
SPARK_CHECKPOINT_PATH=workspace/checkpoints/spark \
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
             spark_jobs/metadata_to_mongodb.py
```

Chạy một lần để kiểm tra dữ liệu hiện có trong Kafka:

```bash
MONGODB_URI='mongodb://root:CHANGE_ME_MONGO_PASSWORD@localhost:27017/?authSource=admin' \
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
             spark_jobs/metadata_to_mongodb.py --available-now
```

Không xóa thư mục checkpoint khi restart job. Nếu xóa checkpoint, Spark sẽ
không còn offset đã commit và có thể đọc lại các event cũ.
