# Lab 04: Incremental Code Property Graph Streaming Pipeline

Báo cáo này trình bày pipeline streaming tăng dần để trích xuất Code Property Graph (CPG) từ repository Python, phát sự kiện qua Kafka, rồi nạp vào Neo4j và MongoDB theo hai nhánh xử lý độc lập.

## Tổng quan
- Repository nguồn: `huggingface/transformers-pr-agent`
- Commit nguồn: `458c957fa1e8851825cd799f5d030876f0644194`
- Discovery scope hiện tại: 4.496 file Python raw, 2.963 file eligible sau exclusions
- Mục tiêu kỹ thuật: parsing tăng dần, stable identifiers, Kafka routing rõ ràng, Neo4j idempotent ingestion, Spark metadata ingestion

## Các chương
- [Task 1. Clone repository và khám phá file](task1_clone_explore.ipynb): ghi nhận nguồn, khám phá raw Python files và tạo manifest eligible.
- [Task 2. Xây dựng dịch vụ parser CPG tăng dần](task2_parser_service.ipynb): trích xuất AST, CFG, DFG, call edges và phát event theo contract ổn định.
- [Task 3. Phân phối sự kiện và cấu trúc Kafka Topics](task3_kafka_topics.ipynb): kiểm chứng topic layout, schema, key và hành vi publish/skip/error.
- [Task 4. Nạp đồ thị vào Neo4j bằng Kafka Sink Connector](task4_neo4j_sink.ipynb): kiểm chứng Kafka Connect sink, lag, DLQ, idempotent replay và integrity graph.
- [Task 5. Nạp metadata nguồn vào MongoDB bằng Spark](task5_spark_mongodb.ipynb): ghi metadata streaming vào MongoDB.
- [Task 6. Xác minh replay idempotent](task6_idempotent_replay.ipynb): kiểm chứng replay và trạng thái tăng dần.
- [Sơ đồ kiến trúc](architecture_diagram.ipynb): tổng hợp kiến trúc và luồng dữ liệu.

## Kết quả hiện có
- Task 1-4 đã có evidence thực thi và kiểm chứng cục bộ/Docker.
- Task 5-6 được giữ nguyên để phục vụ phần còn lại của đồ án.
