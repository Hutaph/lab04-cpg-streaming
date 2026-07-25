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
- **Kafka Connect DLQ topic**:
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

---

## Quy trình phục hồi sự cố (Recovery Runbook - Task 4)

### 1. Sau khi reset volume Neo4j

Khi volume của Neo4j bị xóa hoặc reset (ví dụ: chạy `docker compose down -v` hoặc xóa các volume Docker):
- **Cài đặt lại ràng buộc cơ sở dữ liệu**: Database sẽ trống hoàn toàn và các ràng buộc dữ liệu bị mất. Bạn **phải** chạy lại script khởi tạo schema để tạo lại các constraints:
  ```bash
  PYTHONPATH=src uv run python scripts/create_neo4j_schema.py
  ```
  Nếu không có các uniqueness constraints, Neo4j sẽ không chặn được dữ liệu trùng lặp khi chạy lại, làm mất tính idempotent.
- **Triển khai lại Connector**: Các thông tin credentials đăng ký trong Kafka Connect Connectors có thể không tự động cập nhật nếu có thay đổi trong `.env`. Chạy lại script triển khai để đồng bộ lại:
  ```bash
  PYTHONPATH=src uv run python scripts/deploy_connectors.py
  ```
- **Replay dữ liệu cũ**: Việc reset Neo4j database không tự động làm các offset của consumer quay lại từ đầu. Để nạp lại toàn bộ dữ liệu đã có trên Kafka vào Neo4j, bạn phải reset consumer offsets của các group `connect-neo4j-nodes-sink` và `connect-neo4j-edges-sink` thủ công:
  ```bash
  docker exec cpg-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group connect-neo4j-nodes-sink --reset-offsets --to-earliest --execute --topic cpg.nodes
  docker exec cpg-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group connect-neo4j-edges-sink --reset-offsets --to-earliest --execute --topic cpg.edges
  ```
- **Xử lý DLQ**: Các bản ghi đã đi vào DLQ (`connector.errors`) không tự động quay lại source topic. Chúng yêu cầu phân tích thủ công hoặc sử dụng một consumer phụ để xử lý lại. Không giải quyết các lỗi runtime bằng cách đơn giản là xóa sạch Docker volumes.

### 2. Xử lý lệch cấu hình Credentials (Credential Mismatch)

Nếu connector hoặc các task chuyển sang trạng thái `FAILED` do thay đổi mật khẩu Neo4j:
- So sánh mật khẩu trong `.env` với cấu hình hiện tại của connector (che giấu mật khẩu thật trong log đầu ra).
- Cập nhật trường credentials bằng cách chạy `deploy_connectors.py`.
- Xác minh trạng thái của connector và tasks qua REST API (`GET /connectors/neo4j-nodes-sink/status` và `GET /connectors/neo4j-edges-sink/status`) để đảm bảo các task đã trở lại `RUNNING` sau khi rebalance.
