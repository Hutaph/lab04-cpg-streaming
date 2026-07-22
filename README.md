# Lab 04: Incremental Code Property Graph Streaming Pipeline

Đồ án thực hành môn Nhập môn Dữ liệu lớn: Xây dựng pipeline streaming tăng dần (incremental) để trích xuất Code Property Graph (CPG) từ repository Python, gửi sự kiện qua Kafka và nạp dữ liệu song song vào Neo4j (Graph Database) và MongoDB (Document Database).

---

## 1. Tổng quan Dự án
- **Repository nguồn phân tích**: `huggingface/transformers-pr-agent` (shallow clone tại runtime vào thư mục `workspace/source/`).
- **Mục tiêu**: Phân tích cú pháp sinh AST, CFG, DFG, Call graph từ mã nguồn Python để phục vụ phân tích tĩnh, xử lý streaming thời gian thực.
- **Tiến độ Hiện tại**:
  - **Task 1 & Task 2**: Đã hoàn thành và kiểm chứng (Shallow clone, Discovery, CPG Parser Service với stable deterministic ID, dry-run JSONL output).
  - **Task 3 (Kafka Ingestion)**: Đã hoàn thành và kiểm chứng (Khởi chạy Kafka KRaft, khởi tạo topics tự động, live-mode stream CPG events lên Kafka với key là `file_id` và xác minh schema/partitioning).
  - **Task 4, 5 & 6**: Đã scaffold sẵn cấu trúc config, sẽ triển khai trong các phase tiếp theo.

---

## 2. Hướng dẫn Khởi chạy nhanh (Quick Start)

### Bước 1: Cài đặt và đồng bộ môi trường
```bash
uv sync --all-extras
```

### Bước 2: Clone repository nguồn mục tiêu
```bash
uv run lab04 clone-source
```

### Bước 3: Khảo sát danh sách file nguồn Python
```bash
uv run lab04 discover --scope final --manifest artifacts/manifests/source-files.jsonl
```

### Bước 4: Chạy Parser dry-run thử nghiệm trên một tệp tin
```bash
uv run lab04 parse-file --file tests/fixtures/reassignment.py --dry-run --clean-output --out-dir workspace/tmp/parser-output
```

### Bước 5: Khởi chạy Apache Kafka (KRaft mode) và khởi tạo topics
```bash
docker compose -f infra/docker-compose.yml up -d kafka
./scripts/create_topics.sh
```

### Bước 6: Chạy Parser ở live mode để publish events sang Kafka
```bash
uv run lab04 parse-repository --scope smoke --limit 5 --no-dry-run
```

### Bước 7: Kiểm tra và xác minh dữ liệu trong Kafka
```bash
uv run python scripts/inspect_kafka_events.py
```

### Bước 8: Chạy bộ kiểm thử (Unit & Integration Tests)
```bash
# Chạy unit tests
PYTHONPATH=src uv run pytest tests/unit -q

# Chạy integration tests
PYTHONPATH=src uv run pytest tests/integration -v
```

---

## 3. Bản đồ Tài liệu dự án
- **[AGENTS.md](AGENTS.md)**: Hướng dẫn cấu trúc, ngôn ngữ và các quy tắc bắt buộc cho AI Coding Agents.
- **[docs/system_architecture.md](docs/system_architecture.md)**: Tài liệu đặc tả kỹ thuật chi tiết nhất (Kiến trúc hệ thống, Stable ID, cấu trúc thư mục, quy tắc dependency, và 7 quyết định thiết kế).
- **[docs/implementation_plan.md](docs/implementation_plan.md)**: Kế hoạch triển khai chi tiết 15 phases, ma trận truy vết yêu cầu, chiến lược kiểm thử và hướng dẫn nộp bài.
- **[lab04-book/](lab04-book/)**: Mã nguồn của báo cáo Jupyter Book chính thức (chứa kết quả chạy thực nghiệm Task 1, Task 2 & Task 3).