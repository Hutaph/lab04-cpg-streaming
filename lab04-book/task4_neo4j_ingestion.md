# Task 4: Ingest đồ thị vào Neo4j bằng Kafka Connect Sink

## 1. Mục tiêu
Thiết lập tự động đồng bộ hóa dữ liệu node và edge từ Kafka vào cơ sở dữ liệu đồ thị Neo4j mà không thông qua lớp Spark trung gian.

## 2. Thiết kế
Sử dụng Neo4j Kafka Connect Sink để lắng nghe trên hai topic `cpg.nodes` và `cpg.edges`. Cấu hình lệnh Cypher `MERGE` để lưu trữ dữ liệu có tính idempotent.

## 3. Lý do lựa chọn
- Giảm thiểu trễ (latency) ghi nhận đồ thị.
- Tránh việc duy trì một cluster Spark cồng kềnh cho các thao tác ghi nhận đồ thị đơn giản.

## 4. Cách thực hiện
- Đăng ký cấu hình connector bằng JSON gửi qua Kafka Connect REST API.
- Tạo sẵn constraints và indexes trên Neo4j trước khi ingest.

## 5. Code hoặc command đã chạy
```bash
# TODO: Điền script đăng ký connector ở Phase 8
```

## 6. Output thực tế
```json
// TODO: Điền status trả về của Connector ở Phase 8
```

## 7. Verification
- Truy vấn đếm số lượng node và relationship bằng Cypher.

## 8. Screenshot
*(TODO: Đính kèm hình ảnh đồ thị CPG trực quan trên Neo4j Browser ở Phase 8)*

## 9. Vấn đề gặp phải
- Quan hệ cạnh ghi nhận trước khi node tương ứng được tạo dẫn đến lỗi toàn vẹn.

## 10. Cách khắc phục
- Sử dụng Cypher `MERGE` tự tạo node rỗng nếu không tìm thấy node nguồn/đích.

## 11. Reflection
- Việc tích hợp trực tiếp Kafka và Neo4j giúp hệ thống tinh gọn, hoạt động rất ổn định ở quy mô streaming.
