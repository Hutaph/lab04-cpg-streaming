# Lab 04: Spark Streaming

## Code Property Graph Streaming Pipeline

Báo cáo này trình bày quá trình xây dựng pipeline streaming để trích xuất Code Property Graph (CPG) từ một repository Python. Mỗi file nguồn được xử lý độc lập, sinh ra event có cấu trúc, sau đó làm đầu vào cho Kafka, Neo4j và MongoDB ở các task tiếp theo.

---

### 1. Phạm vi thực nghiệm
- **Repository nguồn**: `huggingface/transformers-pr-agent` tại commit `458c957fa1e8851825cd799f5d030876f0644194`.
- **Thư mục phân tích chính**: `transformers-pr-agent/src`, gồm `2779` file Python hợp lệ.

---

### 2. Danh sách các Chương báo cáo (Chapters)
- **[Architecture Diagram](architecture_diagram.ipynb)**: Tổng hợp sơ đồ kiến trúc pipeline, topic layout, replay flow và ranh giới các layer trong hệ thống.
- **[Task 1: Clone và khám phá repository](task1_clone_explore.ipynb)**: Thực hiện shallow clone, xác định git commit hash và khảo sát cấu trúc thư mục, thống kê danh sách file Python nguồn.
- **[Task 2: Incremental CPG Parser Service](task2_parser_service.ipynb)**: Triển khai CPG Parser phân tích AST, CFG, DFG, Call graph, sinh stable ID ổn định và chạy dry-run JSONL.
- **[Task 3: Kafka Topic Design](task3_kafka_topics.ipynb)**: Cấu hình broker, khởi tạo topic, publish events lên Kafka với key là `file_id` và xác minh schema, partition consistency, parser error flow.
- **[Task 5: Source Metadata Ingestion into MongoDB](task5_spark_mongodb.ipynb)**: Chạy Spark Structured Streaming đọc metadata từ Kafka, ghi MongoDB, kiểm tra checkpoint resume và upsert khi replay.
- **[Task 6: Idempotent Replay Verification](task6_idempotent_replay.ipynb)**: Kiểm chứng replay tăng dần qua stable IDs, graph diff, Neo4j idempotent writes, MongoDB upsert và Spark checkpoint.

---

### 3. Trạng thái Dự án hiện tại
- **Task 1 (Clone & Discovery)**: Hoàn thành (Verified).
- **Task 2 (Parser Service)**: Hoàn thành (Verified).
- **Task 3 (Kafka Integration)**: Hoàn thành (Verified).
- **Task 4 (Neo4j Graph Ingestion)**: Hoàn thành (Verified).
- **Task 5 (Spark/MongoDB)**: Đã triển khai và xác minh end-to-end bằng Docker.
- **Task 6 (Idempotent Replay)**: Hoàn thành (Verified).

Để tái hiện kiểm thử Task 5, chạy các lệnh trong [infra/README.md](../infra/README.md)
để khởi động Kafka, Zookeeper, MongoDB và tạo topic `source.metadata`, sau đó
chạy [Spark ingestion job](../spark_jobs/README.md) với tùy chọn `-AvailableNow`.
Không đưa `.env` hoặc password thật vào notebook và báo cáo công khai.

---

### 4. Developer Documentation
- Tài liệu kỹ thuật chuyên sâu dành cho lập trình viên phát triển hệ thống được duy trì độc lập trong thư mục `docs/` của repository:
  - **[Thiết kế Kiến trúc & Quyết định Thiết kế](../docs/system_architecture.md)**
  - **[Kế hoạch Triển khai & Kiểm thử](../docs/implementation_plan.md)**
