# Architectural Decision Record: 0007-spark-mongodb-upsert

## Title
Sử dụng Spark Structured Streaming kết hợp MongoDB Spark Connector ghi đè theo file_id.

## Status
Accepted

## Context
Metadata của các file mã nguồn được phát hành liên tục vào topic `source.metadata`. Dữ liệu này cần được ingest vào MongoDB để phục vụ thống kê. Khi có sự kiện replay, cùng một file có thể phát ra nhiều bản tin metadata khác nhau tại các thời điểm khác nhau. Nếu ghi tuần tự nối đuôi (append), MongoDB sẽ chứa nhiều bản ghi của cùng một file, dẫn đến việc tính toán thống kê bị sai lệch (ví dụ: đếm thừa số dòng, số hàm).

## Decision
1. Xây dựng job Spark Structured Streaming sử dụng checkpoint location để duy trì offset Kafka.
2. Cấu hình MongoDB Spark Connector ghi dữ liệu ở chế độ `replace` (hoặc update) dựa trên khóa chính `file_id` (hoặc `file_path`).

## Alternatives Considered
- *Ghi trực tiếp từ Python Parser*: Bỏ qua lớp Spark, ghi thẳng từ parser vào MongoDB. Tuy nhiên, kiến trúc này vi phạm yêu cầu thiết kế pipeline streaming dữ liệu lớn của môn học (Spark Structured Streaming là bắt buộc).
- *Ghi append trong Spark*: Dẫn đến việc dữ liệu bị phình to và trùng lặp bản ghi, bắt buộc phải viết thêm các câu lệnh group-by phức tạp ở MongoDB khi truy vấn.

## Consequences
- Đảm bảo tính idempotent ở tầng MongoDB: dù re-run file bao nhiêu lần, MongoDB vẫn chỉ chứa duy nhất một document mới nhất cho file đó.
- Cơ chế checkpoint của Spark giúp hệ thống tự phục hồi từ offset lỗi gần nhất mà không làm mất mát tin nhắn khi hệ thống gặp sự cố mất điện hoặc crash.

## Risks
- MongoDB Spark Connector yêu cầu cấu hình chính xác khóa thay thế. Nếu cấu hình sai, Spark sẽ báo lỗi ghi hoặc tự động quay về chế độ append thông thường. Nhóm cần kiểm tra kỹ tham số cấu hình connector ở Phase 10.
