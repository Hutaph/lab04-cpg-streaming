# AGENTS.md

## 1. Project Context
Đây là đồ án môn Nhập môn Dữ liệu lớn (Lab 04 — Incremental Code Property Graph Streaming).
- **Người thực hiện**: Solo bởi 1 sinh viên đóng vai trò Data Engineer chính.
- **Mục tiêu**: Xây dựng pipeline streaming tăng dần để trích xuất CPG từ repository mục tiêu `huggingface/transformers-pr-agent`, publish events ra Apache Kafka và ingest song song vào Neo4j (via Kafka Connect) và MongoDB (via Spark Streaming).

## 2. Current Implementation Status
- **Task 1 (Clone & Discovery)**: **Verified**. Discovery CLI hoạt động chính xác lọc được 2779 file Python.
- **Task 2 (CPG Parser Service)**: **Verified locally**. Duyệt AST trích xuất AST, CFG, DFG, Call graph và sinh stable deterministic ID.
- **Task 3, 4, 5 (Kafka/Neo4j/Spark/MongoDB)**: **Not started / Scaffolded**.
- **Task 6 (Idempotent Replay)**: **Partially implemented (SQLite local)**.

## 3. Architecture Boundaries
Để giữ kiến trúc Layered (Hexagonal Architecture) luôn sạch sẽ trong thư mục `src/`:
- **Domain layer** (`src/domain/`): Tuyệt đối độc lập. Không import từ bất kỳ layer nào khác.
- **Parsing layer** (`src/parsing/`): Chỉ phụ thuộc vào `domain`. Không import Kafka client, State Store hay CLI.
- **Application layer** (`src/application/`): Chỉ phụ thuộc vào `domain`. Giao tiếp qua ports interface định nghĩa trong `ports.py`, không khởi tạo adapters cụ thể.
- **Infrastructure layer** (`src/infrastructure/`): Triển khai các Port interface. Chứa các concrete adapters (Kafka, Sqlite, Settings).
- **CLI layer** (`src/cli/`): Composition root thực hiện nạp cấu hình và inject các adapter cụ thể.
- **Spark Job** (`spark_jobs/`): Tổ chức độc lập hoàn toàn, không liên kết với ứng dụng parser core.

## 4. Source of Truth
Bảng dưới đây xác định vị trí tài liệu chính thức chứa thông tin chuẩn xác nhất:

| Chủ đề (Concern) | Đường dẫn chính thức (Canonical Location) |
|---|---|
| Tổng quan & Chạy nhanh | [README.md](README.md) |
| Agent instructions | [AGENTS.md](AGENTS.md) |
| Thiết kế kiến trúc & Cấu trúc thư mục | [docs/system_architecture.md](docs/system_architecture.md) |
| Kế hoạch, tiến độ & Chiến lược kiểm thử | [docs/implementation_plan.md](docs/implementation_plan.md) |
| Event schemas | [schemas/](schemas/) |
| Cấu hình runtime | [config/](config/) |
| Báo cáo Jupyter Book | [lab04-book/](lab04-book/) |

## 5. Code Language Policy
Mọi định danh và mã nguồn trong các thư mục phát triển của dự án (`src/`, `tests/`, `spark_jobs/`, `infra/`, `scripts/`) bao gồm:
- Tên file, tên class, hàm, biến, enums.
- Docstrings và comments giải thích code.
- Exception messages và log messages.
- CLI help texts và schema descriptions.
Bắt buộc sử dụng **tiếng Anh 100%**. Không viết tiếng Việt hoặc không dịch các từ chuyên môn chuẩn (AST, CFG, DFG, Call target, Kafka, Spark, Neo4j, MongoDB, checkpoint, upsert).

## 6. Documentation Language Policy
Mọi tệp tin README và các tài liệu Markdown khác do dự án sở hữu (bao gồm `README.md` ở root, `AGENTS.md`, các file trong `docs/`, `lab04-book/*.md`, các file `README.md` nội bộ thư mục) bắt buộc phải viết bằng **tiếng Việt**. Có thể giữ các thuật ngữ chuyên ngành tiếng Anh nếu phù hợp.

## 7. Testing Requirements
- Chạy unit tests qua:
  ```bash
  PYTHONPATH=src uv run pytest tests/unit -q
  ```
- Ruff linter & formatter:
  ```bash
  uv run ruff check src tests scripts spark_jobs
  uv run ruff format --check src tests scripts spark_jobs
  ```
- Strict Mypy checking:
  ```bash
  MYPYPATH=src uv run mypy --explicit-package-bases src
  ```
- Chạy compile check:
  ```bash
  uv run python -m compileall -q src scripts spark_jobs
  ```

## 8. Forbidden Changes
- Không thay đổi thuật toán sinh Stable ID mà không có sự đồng thuận từ thiết kế kiến trúc.
- Không thay đổi Event Schema trái với định nghĩa trong thư mục `schemas/`.
- Không thay đổi công cụ Parser Core (sử dụng module `ast` chuẩn của Python).
- Không sử dụng random UUID làm định danh cho entities (ID bắt buộc deterministic).
- Không dùng Spark để ghi graph vào Neo4j (chỉ nhận trực tiếp qua Kafka Connect Sink).
- Không xóa file Spark Checkpoint để vượt qua kiểm thử.

## 9. How to Update Docs
- Khi cập nhật tài liệu kỹ thuật, luôn viết trực tiếp vào file canonical tương ứng trong bảng mục 4.
- Không tạo thêm file mới nếu thông tin đã thuộc phạm vi của file canonical.
- Khi di chuyển hoặc cập nhật file tài liệu, bắt buộc phải scan và cập nhật các relative links tương ứng.
