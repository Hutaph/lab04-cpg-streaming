# Cấu hình Hạ tầng & Môi trường Docker

Thư mục này chi tiết các hướng dẫn cài đặt dockerized cho các thành phần hạ tầng phát triển local (Kafka, Neo4j, MongoDB, Spark).

## Khởi chạy hạ tầng

Tạo file môi trường local và điền các password:

```powershell
Copy-Item .env.example .env
```

Tối thiểu cần điền `MONGO_ROOT_PASSWORD` và `NEO4J_PASSWORD`. URI
`MONGODB_URI` phải dùng cùng password MongoDB và thêm `authSource=admin`.

Khởi động các service cần cho Task 5:

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d zookeeper kafka mongodb
```

Kiểm tra trạng thái:

```bash
docker compose --env-file .env -f infra/docker-compose.yml ps
```

Tạo topic Task 5 sau khi broker sẵn sàng:

```powershell
docker exec cpg-kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic source.metadata --partitions 1 --replication-factor 1
```

Script `infra/kafka/create-topics.sh` tạo toàn bộ topics nếu máy host đã có Kafka CLI.
Trong container Kafka, dùng bootstrap server `kafka:29092`; từ máy host, dùng
`localhost:9092`.
