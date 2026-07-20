# Lab 04: Spark Streaming

## Code Property Graph Streaming Pipeline

Báo cáo này trình bày quá trình xây dựng pipeline streaming để trích xuất Code Property Graph (CPG) từ một repository Python. Mỗi file nguồn được xử lý độc lập, sinh ra event có cấu trúc, sau đó làm đầu vào cho Kafka, Neo4j và MongoDB ở các task tiếp theo.

:::{important} Phạm vi thực nghiệm
Repository mục tiêu là `huggingface/transformers-pr-agent` tại commit `458c957fa1e8851825cd799f5d030876f0644194`. Nhóm chọn `transformers-pr-agent/src` làm đầu vào chính cho Parser Service, gồm `2779` file Python.
:::

## Nội dung đã hoàn thành

| Trang | Kết quả chính |
|---|---|
| [Task 1: Clone và khám phá repository](task1_clone_explore.ipynb) | Xác định repo, commit, cấu trúc thư mục và phạm vi parse chính |
| [Task 2: Parser Service CPG](task2_parser_service.ipynb) | Chạy parser dry-run, sinh node/edge/metadata/error event và kiểm tra JSONL output |

## Lộ trình và trạng thái các Task

| Task | Nội dung | Trạng thái |
|---|---|---|
| Task 3 | Thiết kế topic và schema Kafka | Scaffolded / Not started |
| Task 4 | Ingest graph vào Neo4j bằng Kafka Connect Sink | Scaffolded / Not started |
| Task 5 | Ghi metadata vào MongoDB bằng Spark Structured Streaming | Scaffolded / Not started |
| Task 6 | Xác minh replay idempotent | Partially implemented (SQLite local) |

---

## Developer Documentation
- Project developer documentation is maintained under the [docs/](../docs/README.md) directory.
- The public Jupyter Book focuses on lab execution evidence and reflections.
