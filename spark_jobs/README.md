# Spark Structured Streaming Jobs

Thư mục này chứa các công việc ingestion bằng Apache Spark Structured Streaming.

## Khởi chạy job ghi metadata sang MongoDB

Chạy local bằng lệnh `spark-submit`:

```bash
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
             spark_jobs/metadata_to_mongodb.py
```
