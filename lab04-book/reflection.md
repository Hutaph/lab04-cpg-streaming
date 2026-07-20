# Đánh giá và Phản hồi tổng hợp (Reflection)

Bản tự đánh giá tổng hợp về quá trình thực hiện Lab 04 — Incremental Code Property Graph Streaming.

## 1. Điều đã hoạt động tốt
- Thiết kế phân lớp rõ ràng (domain, application, parsing, infrastructure, cli) giúp code có tính modular cao, dễ dàng kiểm thử độc lập mà không cần khởi chạy Kafka/Databases.
- Sự phối hợp nhịp nhàng giữa Kafka Connect Sink đẩy thẳng vào Neo4j và Spark Streaming ghi MongoDB mang lại sự linh hoạt và tối ưu hiệu suất ghi nhận.
- Cơ chế stable hash-based IDs giải quyết triệt để vấn đề dữ liệu bị nhân bản khi chạy re-run nhiều lần.

## 2. Các thách thức và lỗi đã gặp
- **Xử lý stale elements**: Khi cấu trúc file thay đổi, việc truy quét tìm các cạnh và nút cũ để xóa (diffing) tốn nhiều công sức để tính toán chính xác ID.
- **Thứ tự ghi nhận đồ thị**: Lỗi cạnh ghi trước nút do tính chất bất đồng bộ của streaming, cần cấu hình Cypher MERGE khôn ngoan tự động khởi tạo node nếu chưa tồn tại.

## 3. Bài học kinh nghiệm
- Kiểm thử đa tầng và viết các file fixtures đại diện cho các case cú pháp là cực kỳ quan trọng, giúp phát hiện sớm các bug thuật toán duyệt cây AST mà không cần deploy lên Spark/Kafka cluster thật.
- Thiết lập tài liệu kiến trúc hệ thống và các ADRs rõ ràng từ đầu đóng vai trò định hướng giúp quá trình lập trình diễn ra nhanh chóng, ít gặp xung đột thiết kế.
