# Lab 04: Incremental Code Property Graph Streaming Pipeline

Đồ án thực hành môn Nhập môn Dữ liệu lớn: Xây dựng pipeline streaming tăng dần (incremental) để trích xuất Code Property Graph (CPG) từ repository Python, gửi sự kiện qua Kafka và nạp dữ liệu song song vào Neo4j (Graph Database) và MongoDB (Document Database).

---

## 1. Tổng quan Dự án
- **Repository nguồn phân tích**: `huggingface/transformers-pr-agent` (shallow clone tại runtime vào thư mục `workspace/source/`).
- **Mục tiêu**: Phân tích cú pháp sinh AST, CFG, DFG, Call graph từ mã nguồn Python để phục vụ phân tích tĩnh, xử lý streaming thời gian thực.
- **Tiến độ Hiện tại**:
  - **Task 1 & Task 2**: Hoàn thành và kiểm chứng (Shallow clone, Discovery, CPG Parser Service với stable deterministic ID, dry-run JSONL output).
  - **Task 3 (Kafka Ingestion)**: Hoàn thành và kiểm chứng (Khởi chạy Kafka KRaft, khởi tạo topics tự động, live-mode stream CPG events lên Kafka với key là `file_id` và xác minh cấu trúc/phân vùng).
  - **Task 4 (Neo4j Ingestion)**: Hoàn thành và kiểm chứng (Neo4j Kafka Sink Connector với Cypher FOREACH rẽ nhánh, cơ chế Node/Edge Tombstone chống stale resurrection, mixed-batch DLQ isolation, idempotent replay).
  - **Task 5 (Spark/MongoDB Ingestion)**: Hoàn thành và kiểm chứng (Spark Structured Streaming ghi metadata vào MongoDB bằng Docker).
  - **Task 6 (Idempotent Replay Verification)**: Hoàn thành và kiểm chứng (Xác minh cơ chế chạy lại tăng dần và kháng trùng lặp đầu cuối).

---

## 2. Hướng dẫn Khởi chạy nhanh (Quick Start)

### Bước 1: Cài đặt và đồng bộ môi trường
```bash
uv sync --all-extras
```

### Bước 2: Chuẩn bị hạ tầng
Điền `MONGO_ROOT_PASSWORD`, `MONGODB_URI` và `NEO4J_PASSWORD` trong `.env` (copy từ `.env.example`).
Sau đó khởi động toàn bộ dịch vụ (Zookeeper-less Kafka KRaft, Neo4j, Kafka Connect, MongoDB):

**Linux / WSL:**
```bash
cp -n .env.example .env

docker compose \
  --env-file "$PWD/.env" \
  -f "$PWD/infra/docker-compose.yml" \
  -f "$PWD/infra/docker-compose.neo4j.yml" \
  up -d --build
```

**Windows (PowerShell):**
```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }

docker compose `
  --env-file .env `
  -f infra/docker-compose.yml `
  -f infra/docker-compose.neo4j.yml `
  up -d --build
```

Chi tiết cấu hình và lệnh kiểm tra nằm trong [infra/README.md](infra/README.md).

### Bước 3: Clone repository nguồn mục tiêu
```bash
uv run lab04 clone-source
```

### Bước 4: Khảo sát danh sách file nguồn Python
```bash
uv run lab04 discover --scope final --manifest artifacts/manifests/source-files.jsonl
```

### Bước 5: Khởi tạo Kafka topics
```bash
./scripts/create_topics.sh
```

### Bước 6: Chạy Parser dry-run thử nghiệm trên một tệp tin
```bash
uv run lab04 parse-file --file tests/fixtures/reassignment.py --dry-run --clean-output --out-dir workspace/tmp/parser-output
```

### Bước 7: Chạy Parser ở live mode để publish events sang Kafka
```bash
uv run lab04 parse-repository --scope smoke --limit 5 --no-dry-run
```

### Bước 8: Kiểm tra và xác minh dữ liệu trong Kafka
```bash
uv run python scripts/inspect_kafka_events.py
```

### Bước 9: Chạy Spark metadata ingestion (Task 5)
**Linux / WSL:**
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
MONGODB_URI='mongodb://root:${MONGO_ROOT_PASSWORD}@localhost:27017/?authSource=admin' \
SPARK_CHECKPOINT_PATH=workspace/checkpoints/spark \
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
             spark_jobs/metadata_to_mongodb.py --available-now
```

**Windows (PowerShell):**
```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_metadata_to_mongodb.ps1 -AvailableNow
```

### Bước 10: Chạy bộ kiểm thử (Unit & Integration Tests)
```bash
# Chạy unit tests
PYTHONPATH=src uv run pytest tests/unit -q

# Chạy integration tests (yêu cầu hạ tầng Docker đang chạy)
PYTHONPATH=src uv run pytest tests/integration -v
```

---

## 3. Bản đồ Tài liệu dự án
- **[AGENTS.md](AGENTS.md)**: Hướng dẫn cấu trúc, ngôn ngữ và các quy tắc bắt buộc cho AI Coding Agents.
- **[docs/system_architecture.md](docs/system_architecture.md)**: Tài liệu đặc tả kỹ thuật chi tiết nhất (Kiến trúc hệ thống, Stable ID, cấu trúc thư mục, quy tắc dependency, và 7 quyết định thiết kế).
- **[docs/implementation_plan.md](docs/implementation_plan.md)**: Kế hoạch triển khai chi tiết 15 phases, ma trận truy vết yêu cầu, chiến lược kiểm thử và hướng dẫn nộp bài.
- **[infra/README.md](infra/README.md)**: Hướng dẫn quản lý hạ tầng Docker (Kafka KRaft, Neo4j, MongoDB, Kafka Connect) và triển khai connector.
- **[lab04-book/](lab04-book/)**: Báo cáo Jupyter Book chính thức (gồm Landing Page, chương Sơ đồ kiến trúc, và các chương chạy thực nghiệm từ Task 1 đến Task 6 kèm reflections).
