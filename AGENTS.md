# AGENTS.md

## 1. Project Context
Đây là đồ án thực hành môn Nhập môn Dữ liệu lớn (Lab 04 — Incremental Code Property Graph Streaming). 
- **Người thực hiện**: Đồ án được thiết kế ban đầu cho nhóm 4 người nhưng thực hiện cá nhân (solo) bởi một sinh viên, đóng vai trò là Data Engineer chính.
- **Mục tiêu**: Xây dựng một pipeline streaming tăng dần (incremental) để trích xuất Code Property Graph (CPG) từ repository mục tiêu `huggingface/transformers-pr-agent` (clone shallow vào `workspace/source/transformers-pr-agent`), xuất các event ra Apache Kafka và ghi song song vào Neo4j (đọc trực tiếp từ Kafka Connect) và MongoDB (thông qua Spark Structured Streaming).

## 2. Canonical Architecture
Hệ thống sử dụng các công nghệ cố định:
- **Parser Core**: Python standard `ast` module.
- **Message Broker**: Apache Kafka (KRaft mode).
- **Graph Database Ingestion**: Neo4j Kafka Connector Sink (Kafka Connect).
- **Metadata Document Ingestion**: Apache Spark Structured Streaming + MongoDB Spark Connector.
- **Databases**: Neo4j (Graph) và MongoDB (Document).
- **Infrastructure**: Docker Compose.
- **Submission Output**: Jupyter Book published to GitHub Pages.

Ứng dụng Parser được xây dựng theo kiến trúc phân lớp (**Hexagonal Architecture / Ports & Adapters**) tách biệt các logic nghiệp vụ và phân tích cú pháp khỏi chi tiết hạ tầng truyền nhận thông điệp hay cơ sở dữ liệu.

## 3. Source of Truth
Bảng dưới đây xác định tệp tin chứa thông tin chuẩn xác nhất cho từng khía cạnh của dự án. Mọi tác nhân (Agent hoặc Developer) bắt buộc phải đọc và cập nhật các file này khi cần thay đổi, tránh nhân bản thông tin ở nhiều nơi:

| Concern | Canonical source |
|---|---|
| **Project overview** | [README.md](README.md) |
| **Agent instructions** | [AGENTS.md](AGENTS.md) |
| **Documentation navigation** | [docs/README.md](docs/README.md) |
| **System architecture** | [docs/architecture/system_architecture.md](docs/architecture/system_architecture.md) |
| **Project layout** | [docs/architecture/project_structure.md](docs/architecture/project_structure.md) |
| **Architecture decisions** | [docs/architecture/adr/](docs/architecture/adr/) |
| **Implementation phases** | [docs/planning/implementation_plan.md](docs/planning/implementation_plan.md) |
| **Requirement and task status** | [docs/planning/traceability_matrix.md](docs/planning/traceability_matrix.md) |
| **Testing approach** | [docs/quality/testing_strategy.md](docs/quality/testing_strategy.md) |
| **Submission requirements** | [docs/quality/submission_checklist.md](docs/quality/submission_checklist.md) |
| **Event schemas** | [schemas/](schemas/) |
| **Runtime configuration** | [config/](config/) |
| **Official report** | [lab04-book/](lab04-book/) |
| **Historical migration/audit** | [docs/archive/](docs/archive/) |

## 4. Dependency Rules
Để giữ cho kiến trúc hệ thống luôn sạch sẽ, các quy tắc phụ thuộc sau đây phải được tuân thủ nghiêm ngặt trong thư mục `src/`:
- **Domain layer** (`src/domain/`): Tuyệt đối không được import hay phụ thuộc vào bất kỳ layer nào khác (`application`, `parsing`, `infrastructure`, `cli`).
- **Parsing layer** (`src/parsing/`): Chỉ được phép phụ thuộc vào `domain` layer. Không import trực tiếp Kafka client, State Store hay CLI.
- **Application layer** (`src/application/`): Chỉ phụ thuộc vào `domain` layer. Nó giao tiếp với thế giới bên ngoài thông qua các interfaces (Ports) khai báo trong `ports.py`, không khởi tạo trực tiếp các adapter cụ thể của hạ tầng.
- **Infrastructure layer** (`src/infrastructure/`): Triển khai (implement) các interface Port từ `application` layer. Lớp này chứa các concrete adapter kết nối Kafka Broker, SQLite State Store, hay local writer.
- **CLI layer** (`src/cli/`): CLI là composition root thực hiện nạp cấu hình và khởi tạo/inject các adapter cụ thể vào service.
- **Spark Job**: Được tổ chức riêng biệt trong thư mục `spark_jobs/`, hoàn toàn tách biệt khỏi ứng dụng parser vì được submit vào Spark Cluster riêng.

## 5. Event and Topic Contracts
Mọi event streaming đẩy qua Kafka bắt buộc tuân thủ hợp đồng dữ liệu:
- **Tên Topic**:
  - Node events: `cpg.nodes`
  - Edge events: `cpg.edges`
  - Source Metadata: `source.metadata`
  - Parser Errors: `parser.errors`
  - Dead Letter Queue: `connector.errors`
- **Kafka Message Key**: Bắt buộc là `file_id`.
- **Schema Version**: `"1.0"` (kiểu string).
- **Time Field**: `event_time` (ISO 8601 UTC string format).
- **Event Types**: `NODE_UPSERT`, `NODE_DELETE`, `EDGE_UPSERT`, `EDGE_DELETE`, `FILE_METADATA_UPSERT`, `PARSER_ERROR`.

## 6. Development Workflow
1. **Giải thích trước khi code**: Giải thích ngắn gọn các khái niệm lý thuyết (Kafka Connect, Spark streaming, Cypher...) bằng ngôn ngữ đơn giản trước khi thực hiện viết code.
2. **Ngôn ngữ phản hồi**: Luôn trả lời người dùng bằng **Tiếng Việt**.
3. **Quản lý dependencies**: Sử dụng `uv` làm trình quản lý gói. Chạy `uv sync --all-extras` để đồng bộ hóa môi trường ảo.

## 7. Testing Requirements
Mọi thay đổi nghiệp vụ hoặc adapter phải đi kèm kiểm thử và đảm bảo chất lượng tĩnh:
- **Unit & Integration tests**: Chạy qua lệnh:
  ```bash
  PYTHONPATH=src uv run pytest tests/unit -q
  ```
- **Linter & Formatter**: Sử dụng Ruff để duy trì chất lượng code:
  ```bash
  uv run ruff check src tests scripts spark_jobs
  uv run ruff format --check src tests scripts spark_jobs
  ```
- **Type Checking**: Sử dụng strict Mypy để kiểm tra kiểu dữ liệu:
  ```bash
  MYPYPATH=src uv run mypy --explicit-package-bases src
  ```
- **Compilation**: Chạy kiểm tra biên dịch toàn bộ tệp tin:
  ```bash
  uv run python -m compileall -q src scripts spark_jobs
  ```

## 8. Documentation Update Rules
- **Nguyên tắc Source of Truth**: Không tạo thêm tệp tài liệu mới nếu nội dung đã thuộc phạm vi một tệp canonical hiện hành trong bảng mục 3. Cập nhật trực tiếp tệp canonical tương ứng.
- **Cập nhật link**: Khi di chuyển hoặc đổi tên các file tài liệu, bắt buộc phải scan và cập nhật các relative links tương ứng ở tất cả các tệp Markdown và Jupyter Book để tránh hỏng đường dẫn (broken links).

## 9. Forbidden Changes
- **Không tự ý thay đổi thuật toán sinh Stable ID** (cơ chế sinh ID dựa trên thuộc tính thực thể mã nguồn) nếu không có ADR chính thức được thông qua.
- **Không thay đổi Event Schema** (cấu trúc event envelope) trái với định nghĩa trong thư mục `schemas/`.
- **Không thay đổi công cụ Parser Core** (module `ast` chuẩn của Python) sang các thư viện ngoài.
- **Không sử dụng ngẫu nhiên UUID** làm định danh cho các nút hoặc cạnh trong graph; ID phải mang tính deterministic.
- **Không sử dụng Spark cho việc nạp graph** vào Neo4j (Neo4j chỉ nhận trực tiếp từ Kafka Connect).
- **Không xóa file Spark Checkpoint** để ép các test vượt qua (vi phạm tính toàn vẹn trạng thái streaming).

## 10. Current Project Status
- **Task 1 (Clone & Discovery)**: **Verified**. Discovery CLI hoạt động chính xác với 2779 eligible files.
- **Task 2 (Parser Service)**: **Verified locally**. Trích xuất thành công AST, CFG, DFG, Call graph dưới dạng CPG; chạy unit tests và dry-run validation không trùng lặp thành công.
- **Task 3 (Kafka Topics)**: **Not started / Scaffolded**. Cấu hình topic và JSON schemas đã sẵn sàng, chưa chạy live broker.
- **Task 4 (Neo4j Ingestion)**: **Not started / Scaffolded**. Cấu hình connectors Cypher MERGE đã sẵn sàng, chưa chạy hạ tầng Neo4j.
- **Task 5 (Spark MongoDB)**: **Not started / Scaffolded**.
- **Task 6 (Idempotent Replay)**:
  - SQLite local parser replay: **Implemented and verified**.
  - Full end-to-end replay (Neo4j/MongoDB/Spark): **Not started / Pending subsequent tasks**.
