# Cấu hình Hạ tầng & Môi trường Docker

Thư mục này chi tiết các hướng dẫn cài đặt dockerized cho các thành phần hạ tầng phát triển local (Kafka, Neo4j, MongoDB, Spark).

## Khởi chạy hạ tầng

### 1. Khởi chạy riêng Kafka Broker (KRaft Mode)
Chạy lệnh sau từ thư mục root của dự án:

```bash
docker compose -f infra/docker-compose.yml up -d kafka
```

### 2. Khởi tạo các topic bắt buộc
Sau khi Kafka Broker ở trạng thái healthy, chạy script để khởi tạo các topic:

```bash
./scripts/create_topics.sh
```

Các topic được tạo bao gồm:
- `cpg.nodes`: Chứa các sự kiện trích xuất nodes (3 partitions).
- `cpg.edges`: Chứa các sự kiện trích xuất edges (3 partitions).
- `source.metadata`: Chứa metadata thông tin và thống kê của file (1 partition).
- `parser.errors`: Dead Letter Queue cho các lỗi cú pháp khi parse (1 partition).
- `connector.errors`: Dead Letter Queue chứa các sự kiện lỗi từ Kafka Connect (1 partition).

