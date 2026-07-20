# Architectural Decision Record: 0006-parser-state-store

## Title
Sử dụng SQLite làm cơ sở dữ liệu lưu trữ trạng thái phân tích cục bộ (Local State Store).

## Status
Accepted

## Context
Để thực hiện phân tích tăng dần, Parser Service cần biết trạng thái cuối cùng của các file trong repository. Các thông tin cần lưu trữ bao gồm: đường dẫn file, hash nội dung tại lần parse gần nhất, commit hash của repo tại thời điểm đó và danh sách các ID của node/edge đã được tạo ra từ file đó (phục vụ việc dọn dẹp các node mồ côi khi re-run).

## Decision
Sử dụng một cơ sở dữ liệu SQLite cục bộ (`workspace/state/parser_state.db`) để lưu trữ thông tin trạng thái này.

## Alternatives Considered
- *In-Memory State*: Mất sạch dữ liệu trạng thái khi tắt CLI, không hỗ trợ chạy tiếp sức (resume) hoặc re-run sau khi tắt máy.
- *File JSON/YAML phẳng*: Dễ bị lỗi tranh chấp file, tốc độ truy vấn tìm kiếm ID cũ để diff rất chậm khi số lượng file và node tăng lên hàng ngàn.

## Consequences
- SQLite đi kèm sẵn trong thư viện chuẩn của Python, không yêu cầu cài đặt thêm service database phụ trợ nào khác.
- Cho phép thực hiện các câu lệnh SQL phức tạp để lấy danh sách ID cũ của một file, giúp việc diff đồ thị diễn ra cực nhanh và chính xác.
- Đảm bảo tính nhất quán dữ liệu nhờ cơ chế ACID giao dịch của SQLite.

## Risks
- SQLite ghi vào file đơn lẻ trên disk, nên nếu có nhiều tiến trình parser chạy đồng thời ghi vào cùng một file state db thì có thể xảy ra lỗi `database is locked`. Hệ thống sẽ giải quyết bằng cách cấu hình khóa ghi tuần tự hoặc chỉ chạy duy nhất một instance CLI parser tại một thời điểm.
