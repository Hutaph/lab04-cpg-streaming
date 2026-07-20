# Architectural Decision Record: 0003-stable-identifiers

## Title
Sử dụng thuật toán băm SHA-256 để sinh định danh duy nhất ổn định (Deterministic Stable Identifiers).

## Status
Accepted

## Context
CPG đồ thị chứa hàng ngàn node và edge được phân phối qua Kafka và nạp vào Neo4j. Khi có sự kiện re-run (quét lại một file hoặc quét lại toàn bộ repository), nếu sử dụng định danh ngẫu nhiên (như UUID v4), Neo4j sẽ tạo ra các node trùng lặp, phá vỡ cấu trúc của đồ thị. Hệ thống đòi hỏi các định danh phải có tính chất: cùng một phần tử mã nguồn (ở cùng vị trí, nội dung) luôn luôn cho ra một định danh duy nhất qua các lần chạy khác nhau.

## Decision
Thiết kế và triển khai hàm sinh định danh dựa trên thuật toán SHA-256, kết hợp các thuộc tính ngữ cảnh của phần tử:
- **Node ID**: `sha256(file_path + "|" + content_hash + "|" + ast_path + "|" + node_type)`
- **Edge ID**: `sha256(edge_type + "|" + source_id + "|" + target_id + "|" + field_name + "|" + index)`

## Alternatives Considered
- *UUID v4*: Sinh định danh ngẫu nhiên, không hỗ trợ idempotency ở mức ghi đè.
- *Số thứ tự tự tăng (Auto-increment)*: Đòi hỏi một central database để cấp phát ID, không phù hợp cho mô hình phân tán streaming.

## Consequences
- Đảm bảo tính idempotency tuyệt đối tại Neo4j. Khi re-run, lệnh Cypher MERGE sẽ đối chiếu khóa `node_id` và thực hiện update thuộc tính thay vì chèn mới.
- Không cần duy trì kết nối mạng để xin cấp phát ID, tăng tốc độ xử lý của parser.

## Risks
- Khi file bị thay đổi nội dung (làm đổi `content_hash`), ID của các node bên trong sẽ thay đổi theo, dẫn đến việc các node cũ trở thành node mồ côi (stale) trong Neo4j. Điều này bắt buộc hệ thống phải có cơ chế tính toán diff để dọn dẹp.
