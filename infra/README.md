# Cấu hình Hạ tầng & Môi trường Docker

Thư mục này chi tiết các hướng dẫn cài đặt dockerized cho các thành phần hạ tầng phát triển local (Kafka, Neo4j, MongoDB, Spark).

## Chuẩn bị môi trường

Tạo file môi trường local và điền các password nếu file chưa tồn tại:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

Tối thiểu cần điền `MONGO_ROOT_PASSWORD` và `NEO4J_PASSWORD`. URI
`MONGODB_URI` phải dùng cùng password MongoDB và thêm `authSource=admin`.
File `.env` đã được thêm vào `.gitignore`; không commit file này.

## Khởi chạy hạ tầng

### 1. Khởi chạy Kafka Broker (KRaft Mode)

Kafka chạy ở chế độ KRaft (không cần Zookeeper). Đây là cấu hình chuẩn của Task 3. Toàn bộ các Spark jobs và downstream consumers trong dự án đều chạy nhất quán trên nền tảng Zookeeper-less Kafka KRaft (Kafka 7.4.0).

Chạy lệnh sau từ thư mục root của dự án:

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d kafka
```

Kiểm tra trạng thái:

```bash
docker compose --env-file .env -f infra/docker-compose.yml ps
```

### 2. Khởi tạo các topic bắt buộc

Sau khi Kafka Broker ở trạng thái healthy, chạy script để khởi tạo các topic:

```bash
./scripts/create_topics.sh
```

Hoặc tạo riêng topic `source.metadata` (dùng cho Task 5):

```bash
docker exec cpg-kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic source.metadata --partitions 1 --replication-factor 1
```

Script `infra/kafka/create-topics.sh` tạo toàn bộ topics nếu máy host đã có Kafka CLI.
Trong container Kafka, dùng bootstrap server `kafka:29092`; từ máy host, dùng `localhost:9092`.

Các topic được cấu hình bao gồm:
- **Required Task 3 topics**:
  - `cpg.nodes`: Chứa các sự kiện trích xuất nodes (3 partitions).
  - `cpg.edges`: Chứa các sự kiện trích xuất edges (3 partitions).
  - `source.metadata`: Chứa metadata thông tin và thống kê của file (1 partition). Được consume bởi Spark Structured Streaming (Task 5).
  - `parser.errors`: Topic chứa các sự kiện lỗi nghiệp vụ (PARSER_ERROR) sinh ra khi parser phân tích thất bại (1 partition).
- **Planned Kafka Connect DLQ topic**:
  - `connector.errors`: Dead Letter Queue chứa các sự kiện lỗi từ Kafka Connect (được cấu hình và kiểm chứng ở Task 4) (1 partition).


### 3. Khởi chạy Neo4j & Kafka Connect (cho Task 4)

Khởi chạy cơ sở dữ liệu đồ thị Neo4j và Kafka Connect container (yêu cầu nạp cả hai file compose):

```bash
docker compose --env-file .env -f infra/docker-compose.yml -f infra/docker-compose.neo4j.yml up -d neo4j kafka-connect
```

Sau khi dịch vụ khởi chạy, thực hiện các bước sau để thiết lập schema và nạp connector:

1. **Khởi tạo Neo4j Schema (Constraints & Indexes)**:
   ```bash
   PYTHONPATH=src uv run python scripts/create_neo4j_schema.py
   ```
   Script này sẽ thiết lập ràng buộc duy nhất (`UNIQUE`) cho ID của Node, ID và thế hệ (`generation_id`) của Node/Edge Tombstones để đảm bảo an toàn ghi trùng lặp và chặn stale resurrection.

2. **Deploy Kafka Connect Sinks**:
   ```bash
   export $(grep -v '^#' .env | xargs)
   PYTHONPATH=src uv run python scripts/deploy_connectors.py
   ```
   Lệnh này tự động kiểm tra và cấu hình hai Sink Connectors: `neo4j-nodes-sink` và `neo4j-edges-sink`. Cấu hình bao gồm cơ chế bắt lỗi DLQ (`connector.errors`) và cơ chế batching.

3. **Giám sát Dead Letter Queue (DLQ)**:
   Để kiểm tra các bản ghi bị đẩy vào DLQ do lỗi không hợp lệ (như endpoint mismatch):
   ```bash
   PYTHONPATH=src uv run python scripts/inspect_kafka_events.py --start-offsets ...
   ```

### 4. Khởi chạy MongoDB (cho Task 5)

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d mongodb
```

## Dừng hạ tầng

Dừng tất cả các container đang chạy:

```bash
docker compose --env-file .env -f infra/docker-compose.yml -f infra/docker-compose.neo4j.yml down
```
