# Cấu trúc Kiểm thử cho Hệ thống CPG Ingestion

Thư mục này chứa các test suite để xác minh tính chính xác của parser, thuật toán stable ID, tính tương thích của topic event và tích hợp data streaming.

## Cấu trúc thư mục

- **`fixtures/`**: Các file Python tĩnh chứa các cấu trúc điều khiển (vòng lặp, cuộc gọi hàm, lỗi cú pháp) được sử dụng làm đầu vào cho parser.
- **`unit/`**: Kiểm thử các builders, bộ sinh stable ID, serialization event, validate schema và diff đồ thị.
- **`integration/`**: Kiểm thử Kafka producers và các transaction SQLite state local.
- **`e2e/`**: Kiểm thử toàn trình từ quét git repository đến các assertions trong Neo4j và MongoDB.
