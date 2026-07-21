# Lab 04: Spark Streaming

## Code Property Graph Streaming Pipeline

Báo cáo này trình bày quá trình xây dựng pipeline streaming để trích xuất Code Property Graph (CPG) từ một repository Python. Mỗi file nguồn được xử lý độc lập, sinh ra event có cấu trúc, sau đó làm đầu vào cho Kafka, Neo4j và MongoDB ở các task tiếp theo.

---

### 1. Phạm vi thực nghiệm
- **Repository nguồn**: `huggingface/transformers-pr-agent` tại commit `458c957fa1e8851825cd799f5d030876f0644194`.
- **Thư mục phân tích chính**: `transformers-pr-agent/src`, gồm `2779` file Python hợp lệ.

---

### 2. Danh sách các Chương báo cáo (Chapters)
- **[Task 1: Clone và khám phá repository](task1_clone_explore.ipynb)**: Thực hiện shallow clone, xác định git commit hash và khảo sát cấu trúc thư mục, thống kê danh sách file Python nguồn.
- **[Task 2: Parser Service CPG](task2_parser_service.ipynb)**: Triển khai CPG Parser phân tích cú pháp AST, CFG, DFG, Call graph, sinh stable ID ổn định và chạy thử nghiệm dry-run xuất file JSONL.

---

### 3. Trạng thái Dự án hiện tại
- **Task 1 (Clone & Discovery)**: Hoàn thành (Verified).
- **Task 2 (Parser Service)**: Hoàn thành (Verified locally).
- **Task 3, 4, 5 & 6 (Kafka/Neo4j/Spark/MongoDB/Replay)**: Chưa thực hiện (Scaffolded).

---

### 4. Developer Documentation
- Tài liệu kỹ thuật chuyên sâu dành cho lập trình viên phát triển hệ thống được duy trì độc lập trong thư mục `docs/` của repository:
  - **[Thiết kế Kiến trúc & Quyết định Thiết kế](../docs/system_architecture.md)**
  - **[Kế hoạch Triển khai & Kiểm thử](../docs/implementation_plan.md)**
