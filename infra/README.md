# Hạ tầng Docker cho Lab 04

Thư mục `infra/` chứa Docker Compose và cấu hình hạ tầng local cho Kafka, Kafka Connect, Neo4j, MongoDB và Spark-related runtime.

## Service topology

| Service | Host | Container network | Vai trò |
|---|---|---|---|
| Kafka | `localhost:9092` | `kafka:29092` | Kafka KRaft broker cho Parser events |
| Kafka Connect | `localhost:8083` | `kafka-connect:8083` | Neo4j sink connectors |
| Neo4j Browser | `localhost:7474` | `cpg-neo4j:7474` | Giao diện Neo4j |
| Neo4j Bolt | `localhost:7687` | `cpg-neo4j:7687` | Driver/connector endpoint |
| MongoDB | `localhost:27017` | `mongodb:27017` | Metadata documents |
| Mongo Express | `localhost:8081` | `mongo-express:8081` | Giao diện kiểm tra MongoDB |

Không commit `.env`. File này chứa biến môi trường runtime và được tạo từ `.env.example`.

## Chuẩn bị môi trường

```bash
cp -n .env.example .env
```

Cập nhật các biến mật khẩu trong `.env` trước khi khởi động dịch vụ. Tài liệu chỉ nhắc tên biến môi trường, không ghi giá trị credential thật.

## Khởi động hạ tầng

Khởi động toàn bộ stack chính:

```bash
docker compose \
  --env-file "$PWD/.env" \
  -f "$PWD/infra/docker-compose.yml" \
  -f "$PWD/infra/docker-compose.neo4j.yml" \
  up -d --build
```

Kiểm tra trạng thái:

```bash
docker compose \
  --env-file "$PWD/.env" \
  -f "$PWD/infra/docker-compose.yml" \
  -f "$PWD/infra/docker-compose.neo4j.yml" \
  ps
```

## Kafka KRaft

Kafka chạy ở chế độ KRaft, không dùng ZooKeeper. Từ máy host dùng `localhost:9092`; từ container network dùng `kafka:29092`.

Khởi động riêng Kafka:

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d kafka
```

## Topic provisioning

Tạo các topic bắt buộc:

```bash
./scripts/create_topics.sh
```

Topic layout:

| Topic | Partitions | Vai trò |
|---|---:|---|
| `cpg.nodes` | 3 | Node upsert/delete events |
| `cpg.edges` | 3 | Edge upsert/delete events |
| `source.metadata` | 1 | Metadata events cho Spark/MongoDB |
| `parser.errors` | 1 | Parser Service business errors |
| `connector.errors` | 1 | Kafka Connect DLQ |

`parser.errors` do Parser Service publish. `connector.errors` do Kafka Connect dùng làm dead-letter topic cho records downstream lỗi.

## Kafka Connect và Neo4j

Khởi động Neo4j và Kafka Connect:

```bash
docker compose \
  --env-file .env \
  -f infra/docker-compose.yml \
  -f infra/docker-compose.neo4j.yml \
  up -d neo4j kafka-connect
```

Tạo constraints Neo4j:

```bash
PYTHONPATH=src uv run python scripts/create_neo4j_schema.py
```

Deploy connectors:

```bash
set -a
. ./.env
set +a
PYTHONPATH=src uv run python scripts/deploy_connectors.py
```

Connectors chính:

| Connector | Topic | Batch size | Vai trò |
|---|---|---:|---|
| `neo4j-nodes-sink` | `cpg.nodes` | 100 | Node upsert/delete |
| `neo4j-edges-sink` | `cpg.edges` | 5 | Edge upsert/delete |

Neo4j graph path đi trực tiếp Kafka -> Kafka Connect -> Neo4j. Spark không ghi graph vào Neo4j.

## MongoDB và Spark metadata path

Khởi động MongoDB và Mongo Express:

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d mongodb mongo-express
```

Task 5 dùng Spark Structured Streaming consume `source.metadata`, checkpoint offsets và upsert MongoDB theo `file_id`. Mongo Express chỉ là giao diện kiểm tra local; assertions chính vẫn nằm trong notebook/test.

Trước khi chạy evidence live, dùng `uv run python scripts/preflight_mongodb.py`. Preflight kiểm tra container running, host/container authentication, database/collection access, unique indexes và khả năng Spark resolve host MongoDB. Nếu preflight fail, không xóa volume để "sửa" trạng thái.

Stack canonical cho nhánh metadata/replay là Spark 3.3.0, Scala 2.12, `spark-sql-kafka-0-10_2.12:3.3.0` và `mongo-spark-connector_2.12:10.1.1`.

## Readiness checks

Kafka topics:

```bash
docker exec cpg-kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Kafka Connect:

```bash
curl -sS http://localhost:8083/connectors
curl -sS http://localhost:8083/connectors/neo4j-nodes-sink/status
curl -sS http://localhost:8083/connectors/neo4j-edges-sink/status
```

Neo4j schema:

```bash
PYTHONPATH=src uv run python scripts/create_neo4j_schema.py
```

MongoDB container:

```bash
docker compose --env-file .env -f infra/docker-compose.yml ps mongodb
```

## Troubleshooting cơ bản

- Nếu connector `FAILED`, kiểm tra status qua Kafka Connect REST API, cập nhật `.env` nếu credentials đổi, rồi chạy lại `scripts/deploy_connectors.py`.
- Nếu Neo4j volume bị reset, chạy lại `scripts/create_neo4j_schema.py` trước khi replay graph events.
- Nếu cần nạp lại Kafka records vào Neo4j sau khi reset database, reset consumer offsets của các group `connect-neo4j-nodes-sink` và `connect-neo4j-edges-sink` theo từng topic.
- Records trong `connector.errors` không tự quay lại source topic; cần đọc DLQ, phân tích nguyên nhân và xử lý lại có chủ ý.
- Nếu MongoDB auth fail do credential cũ trong volume, giữ nguyên volume và kiểm tra lại `.env`, `authSource=admin` và URL-encoding password trước khi chạy preflight lại.

## Dừng hạ tầng

```bash
docker compose \
  --env-file .env \
  -f infra/docker-compose.yml \
  -f infra/docker-compose.neo4j.yml \
  down
```
