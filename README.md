# Lab 04: Incremental Code Property Graph Streaming Pipeline

Đồ án thực hành môn Nhập môn Dữ liệu lớn: Xây dựng pipeline streaming tăng dần (incremental) để trích xuất Code Property Graph (CPG) từ repository Python, gửi sự kiện qua Kafka và nạp dữ liệu song song vào Neo4j (Graph Database) và MongoDB (Document Database).

---

## 1. Tổng quan Dự án
- **Repository nguồn phân tích**: `huggingface/transformers-pr-agent` (shallow clone tại runtime vào thư mục `workspace/source/`).
- **Mục tiêu**: Phân tích cú pháp sinh AST, CFG, DFG, Call graph từ mã nguồn Python để phục vụ phân tích tĩnh, xử lý streaming thời gian thực.
- **Tiến độ Hiện tại**:
  - **Task 1 & Task 2**: Đã hoàn thành và được kiểm chứng local (Shallow clone, Discovery, CPG Parser Service với stable deterministic ID, dry-run JSONL output).
  - **Task 3, 4 & 6**: Đã scaffold sẵn cấu trúc config, sẽ triển khai trong các phase tiếp theo.
  - **Task 5**: Đã triển khai và xác minh pipeline Spark Structured Streaming ghi metadata vào MongoDB bằng Docker.

---

## 2. Hướng dẫn Khởi chạy nhanh (Quick Start)

### Bước 1: Cài đặt và đồng bộ môi trường
```bash
uv sync --all-extras
```

### Bước 2: Chuẩn bị hạ tầng cho Task 5
```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

Điền `MONGO_ROOT_PASSWORD`, `MONGODB_URI` và `NEO4J_PASSWORD` trong `.env`,
sau đó khởi động các service cần cho Task 5:

```powershell
docker compose --env-file .env -f infra/docker-compose.yml up -d zookeeper kafka mongodb
docker exec cpg-kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic source.metadata --partitions 1 --replication-factor 1
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

### Bước 5: Chạy Parser dry-run thử nghiệm trên một tệp tin
```bash
uv run lab04 parse-file --file tests/fixtures/reassignment.py --dry-run --clean-output --out-dir workspace/tmp/parser-output
```

### Bước 6: Chạy Spark metadata ingestion
```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_metadata_to_mongodb.ps1 -AvailableNow
```

### Bước 7: Chạy bộ kiểm thử (Unit Tests)
```bash
PYTHONPATH=src uv run pytest tests/unit -q
```

---

## 3. Bản đồ Tài liệu dự án
- **[AGENTS.md](AGENTS.md)**: Hướng dẫn cấu trúc, ngôn ngữ và các quy tắc bắt buộc cho AI Coding Agents.
- **[docs/system_architecture.md](docs/system_architecture.md)**: Tài liệu đặc tả kỹ thuật chi tiết nhất (Kiến trúc hệ thống, Stable ID, cấu trúc thư mục, quy tắc dependency, và 7 quyết định thiết kế).
- **[docs/implementation_plan.md](docs/implementation_plan.md)**: Kế hoạch triển khai chi tiết 15 phases, ma trận truy vết yêu cầu, chiến lược kiểm thử và hướng dẫn nộp bài.
- **[lab04-book/](lab04-book/)**: Mã nguồn của báo cáo Jupyter Book chính thức (chứa kết quả chạy thực nghiệm Task 1 & Task 2).
