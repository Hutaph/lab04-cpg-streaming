# Task 5: Ghi Metadata vào MongoDB bằng Spark Structured Streaming

## 1. Mục tiêu
Xây dựng một pipeline xử lý streaming tính toán và ghi nhận metadata thống kê của các file mã nguồn vào MongoDB.

## 2. Thiết kế
Sử dụng Apache Spark Structured Streaming consume topic `source.metadata`, áp dụng schema JSON và ghi đè tài liệu trong MongoDB dựa trên khóa chính `file_id` (upsert mode).

## 3. Lý do lựa chọn
- Spark Structured Streaming mạnh mẽ trong việc xử lý luồng sự kiện phân tán và đảm bảo tính nhất quán (exactly-once) nhờ checkpoint.
- MongoDB phù hợp lưu trữ tài liệu metadata không định hình chặt chẽ của file.

## 4. Cách thực hiện
- Viết job Spark SQL streaming reader kết nối Kafka.
- Ghi streaming sử dụng `.writeStream` với định dạng `mongodb`.

## 5. Code hoặc command đã chạy
```bash
# TODO: Điền lệnh spark-submit ở Phase 9/10
```

## 6. Output thực tế
```json
// TODO: Điền logs chạy thực tế của Spark streaming ở Phase 9/10
```

## 7. Verification
- Truy vấn collection MongoDB xác thực số lượng document trùng khớp số lượng file.

## 8. Screenshot
*(TODO: Đính kèm hình ảnh MongoDB Compass hiển thị danh sách metadata ở Phase 10)*

## 9. Vấn đề gặp phải
- Checkpoint của Spark bị lỗi không tương thích khi cấu hình schema metadata thay đổi.

## 10. Cách khắc phục
- Xóa thư mục checkpoint cũ trước khi restart job mới nếu có thay đổi schema.

## 11. Reflection
- Cơ chế checkpoint của Spark cực kỳ hữu ích giúp khôi phục pipeline tức thì từ điểm lỗi mà không làm duplicate bản ghi ở MongoDB nhờ cơ chế Upsert.
