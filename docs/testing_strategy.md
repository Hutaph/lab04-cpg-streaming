# Chiến lược Kiểm thử Đa tầng cho Hệ thống CPG Ingestion

Tài liệu này xác định phương pháp tiếp cận kiểm thử của dự án, đảm bảo độ tin cậy của toàn bộ pipeline từ parsing mã nguồn cục bộ cho đến lưu trữ cơ sở dữ liệu.

---

## 1. Kim tự tháp Kiểm thử (Test Pyramid)

Chúng ta tổ chức kiểm thử theo 3 lớp chính:
- **Unit Tests (Lớp cơ sở - Chiếm ~70%)**: Kiểm thử các hàm thuật toán độc lập, các builder trích xuất đồ thị và sinh ID. Chạy nhanh, không phụ thuộc tài nguyên bên ngoài.
- **Integration Tests (Lớp trung gian - Chiếm ~20%)**: Kiểm thử tương tác giữa adapter và các dịch vụ chạy thật (như SQLite DB, Kafka Topic client).
- **End-to-End (E2E) Tests (Lớp đỉnh - Chiếm ~10%)**: Kiểm thử toàn bộ luồng tích hợp, từ khi scan repo mẫu cho đến khi kiểm tra số lượng bản ghi trong Neo4j và MongoDB.

---

## 2. Chiến lược sử dụng Fixtures
Dự án không chạy trực tiếp trên toàn bộ repository của huggingface cho mục đích kiểm thử thường ngày để tránh tốn thời gian. Thay vào đó, chúng ta xây dựng 5 file fixtures mã nguồn thu nhỏ đại diện cho các kịch bản cú pháp khác nhau tại thư mục `tests/fixtures/`:
- `simple_sequence.py`: Kiểm thử DFG & CFG cơ bản (gồm các dòng gán giá trị).
- `if_else.py`: Kiểm thử phân nhánh điều kiện CFG.
- `loop.py`: Kiểm thử tính chu kỳ, cạnh lặp CFG/DFG.
- `function_calls.py`: Kiểm thử Call Graph kết nối.
- `broken_syntax.py`: Kiểm thử khả năng bắt lỗi Syntax và phát event parser_error.

---

## 3. Các kịch bản kiểm thử cụ thể

### 3.1. Deterministic ID Tests
- **Mục tiêu**: Đảm bảo thuật toán sinh ID tạo ra các ID ổn định và duy nhất.
- **Cách thức**: Chạy parse cùng một file fixture nhiều lần và assert rằng danh sách node ID và edge ID trả về khớp nhau 100% giữa các lần chạy.

### 3.2. AST, CFG, DFG & Call Resolution Tests
- **Mục tiêu**: Xác thực thuật toán xây dựng đồ thị CPG.
- **Cách thức**: Parse các file fixture tương ứng và đếm chính xác số lượng cạnh có nhãn `AST_CHILD`, `CFG_NEXT`, `DFG_REACHES` và `CALLS` khớp với số lượng tính toán thủ công.

### 3.3. Event Schema Tests
- **Mục tiêu**: Bảo đảm event được serialize đúng định dạng cam kết.
- **Cách thức**: Sử dụng thư viện `jsonschema` để validate mọi event do parser sinh ra với file schema tương ứng trong `schemas/`.

### 3.4. Parser-State Tests
- **Mục tiêu**: Kiểm thử tính năng incremental scan.
- **Cách thức**: Parse lần 1 -> Lưu SQLite -> Sửa file -> Parse lần 2 -> Assert SQLite cập nhật hash mới của file đó và giữ nguyên hash của các file khác.

### 3.5. Kafka & Database Integration Tests
- **Mục tiêu**: Bảo đảm kết nối hạ tầng ổn định.
- **Cách thức**: Khởi tạo Mock Kafka, publish tin nhắn và đọc ngược lại từ topic để kiểm tra tính toàn vẹn.

---

## 4. Kiểm thử Idempotency trên Neo4j & MongoDB

- **Neo4j Idempotency**:
  - Chạy quét lần 1 -> Đếm số node/edge trong Neo4j.
  - Chạy quét lần 2 (không đổi file) -> Đếm lại số node/edge. Số lượng phải GIỮ NGUYÊN.
  - Sửa đổi 1 file (thêm 1 hàm) -> Đếm lại số node/edge. Số lượng node tăng đúng bằng số node của hàm mới, không sinh thêm node trùng cho các thành phần cũ.
- **MongoDB Upsert**:
  - Khi re-run metadata event của một file, assert rằng MongoDB thực hiện cập nhật trường ghi cũ của document đó thay vì append thêm một document mới (giữ nguyên tổng số lượng documents trong collection bằng số lượng file Python).
- **Spark Checkpoint Restart**:
  - Tắt job Spark streaming -> Gửi metadata event mới vào Kafka -> Khởi động lại job Spark.
  - Assert rằng Spark tự động khôi phục offset từ checkpoint directory và xử lý các tin nhắn còn tồn đọng trong queue mà không bỏ sót bản ghi nào.

---

## 5. Quy tắc Kiểm thử Quan trọng

> [!WARNING]
> **Tránh assert database ngay lập tức sau khi publish event**
> Vì pipeline hoạt động bất đồng bộ (streaming), dữ liệu từ Kafka cần một khoảng thời gian ngắn (lag) để được ingest vào Neo4j (qua Connector) và MongoDB (qua Spark). Assert DB quá sớm sẽ dẫn đến kết quả kiểm thử không chính xác (flaky tests).
>
> **Biện pháp khắc phục**: Triển khai cơ chế chờ đợi thông minh (Retry-based assertion) để kiểm tra trạng thái database định kỳ (ví dụ mỗi 500ms, tối đa 10s) cho đến khi Kafka lag của topic về 0 hoặc database đạt trạng thái mong muốn.

> [!IMPORTANT]
> **Không tạo các bài test giả**
> Nghiêm cấm sử dụng câu lệnh `assert True` hoặc `pass` trong các hàm test mà không có bất kỳ lệnh gọi API hoặc phép so sánh thực tế nào. Mọi test placeholder hiện tại phải được đánh dấu bằng `@pytest.mark.skip` kèm lý do cụ thể.
