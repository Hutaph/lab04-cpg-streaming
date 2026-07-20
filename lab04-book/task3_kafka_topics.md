# Task 3: Thiết kế Kafka Topic và Event Schema

## 1. Mục tiêu
Thiết kế cấu trúc và định dạng truyền tải dữ liệu của các sự kiện Code Property Graph (CPG) đi qua hệ thống Apache Kafka Broker.

## 2. Thiết kế
Chúng ta thiết kế 5 topics chính bao gồm:
- `cpg.nodes` (Nodes)
- `cpg.edges` (Edges)
- `source.metadata` (File Metadata)
- `parser.errors` (Syntax Errors)
- `connector.errors` (Connect failures)

Các message đều sử dụng định dạng JSON, bao bọc bởi một Envelope chung để truy vết nguồn gốc (provenance).

## 3. Lý do lựa chọn
- Chia nhỏ topic giúp tối ưu hóa consumer, tránh việc Neo4j Connect Sink hoặc Spark phải lọc bỏ các tin nhắn không mong muốn.
- JSON Schema giúp chuẩn hóa hợp đồng dữ liệu giữa parser và các database tiêu thụ.

## 4. Cách thực hiện
- Khai báo schema tại thư mục `schemas/`.
- Validate cấu trúc tự động bằng Python `jsonschema`.

## 5. Code hoặc command đã chạy
```bash
# TODO: Điền command chạy validate schema thực tế ở Phase 6
```

## 6. Output thực tế
```json
// TODO: Điền tin nhắn event thực tế trích xuất từ Kafka console consumer ở Phase 7
```

## 7. Verification
- Validate schema thành công đối với tất cả các event được ghi ra file JSONL cục bộ.

## 8. Screenshot
*(TODO: Đính kèm hình ảnh giao diện Kafka UI hoặc offset topics ở Phase 7)*

## 9. Vấn đề gặp phải
- Việc quản lý nhiều schema riêng lẻ dễ dẫn đến không đồng bộ nếu thay đổi cấu trúc envelope chung.

## 10. Cách khắc phục
- Kế thừa một base schema chung hoặc viết script tự động gen schema để tránh sai sót.

## 11. Reflection
- Việc thiết kế topic rõ ràng từ đầu giúp việc triển khai các tầng ingest phía sau cực kỳ thuận lợi và độc lập.
