# Cấu trúc Thư mục và Quy tắc Dependency của Dự án

Tài liệu này chi tiết hóa cấu trúc thư mục của dự án và các quy tắc kiến trúc bắt buộc về dependency giữa các thành phần.

## 1. Cây thư mục đầy đủ của Project

```
.
├── .env.example                # File cấu hình mẫu biến môi trường
├── .gitignore                  # Khai báo các file loại trừ khỏi git
├── Makefile                    # Các lệnh build, lint, test nhanh
├── pyproject.toml              # Định nghĩa dependencies và python tooling
├── README.md                   # Hướng dẫn chạy và tổng quan dự án
│
├── config/                     # Cấu hình tĩnh ứng dụng
│   ├── application.yaml        # Config môi trường, db connection
│   ├── file_filters.yaml       # Quy tắc lọc file (smoke, final scope)
│   └── topics.yaml             # Topic name, partition mapping của Kafka
│
├── schemas/                    # Thư mục lưu trữ JSON Schemas chuẩn của event
│   ├── node-event.schema.json
│   ├── edge-event.schema.json
│   ├── metadata-event.schema.json
│   └── error-event.schema.json
│
├── src/                        # Mã nguồn ứng dụng Python (nằm trực tiếp tại root)
│   ├── __init__.py
│   │
│   ├── domain/                 # Lớp Nghiệp vụ chính (Core Domain)
│   │   ├── __init__.py
│   │   ├── enums.py            # Node/Edge type enums
│   │   ├── events.py           # Domain Event dataclasses
│   │   ├── models.py           # FileState, CpgGraph models
│   │   └── errors.py           # Custom Domain exceptions
│   │
│   ├── application/            # Lớp Ứng dụng điều phối (Application Services)
│   │   ├── __init__.py
│   │   ├── ports.py            # Interfaces (outbound ports)
│   │   └── services/           # Triển khai các usecases
│   │       ├── __init__.py
│   │       ├── discover_repository.py
│   │       ├── process_file.py
│   │       ├── process_repository.py
│   │       └── replay_file.py
│   │
│   ├── parsing/                # Lớp Parser phân tích cú pháp
│   │   ├── __init__.py
│   │   ├── ast_builder.py      # Xây dựng AST nodes/edges
│   │   ├── cfg_builder.py      # Xây dựng CFG edges
│   │   ├── dfg_builder.py      # Xây dựng DFG edges
│   │   ├── call_builder.py     # Xây dựng call relationships
│   │   ├── cpg_parser.py       # Orchestrator phối hợp các builders
│   │   ├── identifiers.py      # Sinh deterministic stable ID
│   │   ├── metadata.py         # Trích xuất metadata stats từ AST
│   │   └── diff.py             # Diff đồ thị để tìm stale elements
│   │
│   ├── infrastructure/         # Lớp Hạ tầng và Adapters (Adapters)
│   │   ├── __init__.py
│   │   │
│   │   ├── config/             # Tải settings từ env và yaml
│   │   │   ├── __init__.py
│   │   │   └── settings.py
│   │   │
│   │   ├── filesystem/         # Tương tác git repository, viết manifest
│   │   │   ├── __init__.py
│   │   │   ├── git_source_repository.py
│   │   │   └── manifest_writer.py
│   │   │
│   │   ├── messaging/          # Adapters cho Kafka, JSONL, Schema Validator
│   │   │   ├── __init__.py
│   │   │   ├── kafka_producer.py
│   │   │   ├── jsonl_event_writer.py
│   │   │   └── event_validator.py
│   │   │
│   │   ├── state/              # SQLite State store cho incremental parser
│   │   │   ├── __init__.py
│   │   │   └── sqlite_state_store.py
│   │   │
│   │   └── observability/      # Logging, metrics tracker
│   │       ├── __init__.py
│   │       ├── logging.py
│   │       └── metrics.py
│   │
│   └── cli/                    # CLI entrypoint commands
│       ├── __init__.py
│       └── main.py
│
├── spark_jobs/                 # Các jobs Apache Spark độc lập
│   ├── README.md
│   └── metadata_to_mongodb.py  # Spark streaming ingest metadata → MongoDB
│
├── infra/                      # Triển khai và hạ tầng ảo hóa Docker
│   ├── README.md
│   ├── docker-compose.yml      # Docker compose cho Kafka/Neo4j/Mongo/Connect
│   ├── version-matrix.env      # Lưu phiên bản tương thích của hệ thống
│   │
│   ├── kafka/
│   │   ├── README.md
│   │   └── create-topics.sh    # Script khởi tạo topic tự động
│   │
│   ├── kafka-connect/
│   │   ├── README.md
│   │   ├── connectors/         # Connector configurations
│   │   │   ├── neo4j-nodes-sink.json
│   │   │   └── neo4j-edges-sink.json
│   │   └── plugins/            # Lưu trữ jar plugin của Neo4j connector
│   │
│   ├── neo4j/
│   │   ├── README.md
│   │   └── init/
│   │       └── constraints.cypher # Khởi tạo unique constraint & indexes
│   │
│   └── mongodb/
│       ├── README.md
│       └── init/
│           └── indexes.js      # Khởi tạo unique indexes cho metadata db
│
├── scripts/                    # Thư mục scripts tiện ích và prototype code
│   ├── explore_repo.py
│   ├── parser-service/         # Giữ nguyên toàn bộ code cũ của nhóm
│   │   └── ...
│   │
│   ├── clone_source_repo.sh
│   ├── create_topics.sh
│   ├── register_connectors.sh
│   ├── run_discovery.py
│   ├── run_parser.py
│   ├── verify_neo4j.cypher
│   └── verify_mongodb.js
│
├── tests/                      # Kiểm thử hệ thống
│   ├── README.md
│   ├── fixtures/               # Các file python test đầu vào cho parser
│   │   └── ...
│   ├── unit/                   # Unit test cho logic core
│   │   └── ...
│   ├── integration/            # Test tích hợp DB/Broker
│   └── e2e/                    # Test toàn trình hệ thống
│
├── artifacts/                  # Chứa tài liệu đầu ra của pipeline (Gitignored)
│   ├── manifests/
│   ├── samples/
│   ├── query-results/
│   └── screenshots/
│
└── workspace/                  # Thư mục runtime lưu trữ tạm thời (Gitignored)
    ├── source/                 # Nơi clone shallow repotransformers-pr-agent
    ├── state/                  # Lưu trữ file sqlite state DB
    ├── checkpoints/            # Spark streaming checkpoint
    ├── logs/                   # Log file chạy dịch vụ
    └── tmp/                    # Thư mục tạm thời
```

## 2. Vai trò của từng Top-Level Directory
- **`config/`**: Lưu trữ toàn bộ các file cấu hình YAML tĩnh của dự án.
- **`schemas/`**: Đóng vai trò là hợp đồng dữ liệu (data contracts) chuẩn hóa các sự kiện streaming đẩy qua Kafka.
- **`src/`**: Thư mục chứa mã nguồn chính của ứng dụng parser, được tổ chức theo kiến trúc phân lớp (Hexagonal Architecture / Ports & Adapters).
- **`spark_jobs/`**: Chứa code xử lý dữ liệu streaming của Spark, tách biệt hoàn toàn khỏi ứng dụng parser vì chạy trong môi trường Spark Cluster/Submit riêng.
- **`infra/`**: Chứa Docker Compose cấu hình hạ tầng cùng các tài nguyên setup cơ sở dữ liệu.
- **`scripts/`**: Chứa các script tự động hóa chạy các phase và giữ lại mã nguồn prototype cũ để đối chiếu.
- **`tests/`**: Tổ chức kiểm thử đa tầng (Unit, Integration, E2E).
- **`workspace/` & `artifacts/`**: Chứa dữ liệu sinh ra trong quá trình chạy thực tế.

## 3. Quy tắc Dependency giữa các Layer (Kiến trúc phân lớp)
Để đảm bảo khả năng bảo trì và độc lập kiểm thử, hệ thống bắt buộc tuân thủ các quy tắc sau:
- **`domain` không phụ thuộc vào bất kỳ layer nào khác**: Nó chỉ chứa các quy tắc nghiệp vụ cốt lõi, enums và data models. Không import từ `application`, `parsing`, hay `infrastructure`.
- **`application` chỉ phụ thuộc vào `domain`**: Nó sử dụng các interface (ports) định nghĩa trong `ports.py` để giao tiếp với hạ tầng bên ngoài mà không quan tâm chi tiết triển khai cụ thể của hạ tầng.
- **`parsing` chỉ phụ thuộc vào `domain`**: Nó đảm nhiệm phân tích cú pháp mã nguồn thành các mô hình dữ liệu đồ thị CPG của domain.
- **`infrastructure` phụ thuộc vào `domain`, `application` (để triển khai các ports) và `parsing`**: Lớp này chứa các adapter cụ thể như kết nối Kafka Broker thật (`KafkaEventProducer`), kết nối cơ sở dữ liệu SQLite (`SqliteStateStore`), ghi file (`JsonlEventWriter`).
- **`cli` phụ thuộc vào `application`**: CLI là tác nhân kích hoạt use case thông qua các service trong `application`.

## 4. Quy tắc Commit và Gitignore
- **File ĐƯỢC PHÉP commit**:
  - Toàn bộ mã nguồn Python dưới `src/` và `spark_jobs/`.
  - Các cấu hình dưới `config/` và `infra/`.
  - Các schema định nghĩa dưới `schemas/`.
  - Các tài liệu markdown trong `docs/` và Jupyter Book `lab04-book/` (ngoại trừ file PDF lớn hoặc file tạm).
  - Các `.gitkeep` để giữ cấu trúc thư mục rỗng.
- **File BỊ gitignore (Không được commit)**:
  - Môi trường ảo `.venv/`, cache Python (`__pycache__`), cache test (`.pytest_cache`, `.mypy_cache`).
  - Bản clone của repository mục tiêu dưới `workspace/source/`.
  - Dữ liệu cơ sở dữ liệu SQLite tại `workspace/state/`.
  - Checkpoint Spark tại `workspace/checkpoints/`.
  - Thư mục dữ liệu volume của Neo4j và MongoDB (ngăn rò rỉ dữ liệu lớn hoặc xung đột file khóa DB).
  - Tệp `.env` chứa mật khẩu/credential thật.

## 5. Trạng thái của Mã nguồn cũ (Prototype)
- **Vị trí hiện tại**: Toàn bộ mã nguồn cũ do nhóm phát triển trước đó được giữ nguyên vẹn tại `scripts/parser-service/`.
- **Lý do giữ lại**: Đảm bảo không làm ảnh hưởng đến mã nguồn hoạt động của nhóm trong phase thiết lập cấu trúc nền tảng và cung cấp tài liệu đối chiếu (Traceability) rõ ràng cho giai đoạn refactor tiếp theo.
- **Kế hoạch chuyển đổi**: Ở phase sau, logic từ các file prototype này sẽ được refactor một cách cẩn thận và chuyển dịch sang cấu trúc phân lớp tương ứng dưới `src/` (chi tiết tại `docs/refactor_mapping.md`).
