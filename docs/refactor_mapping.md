# Kế hoạch Refactor và Mapping Mã nguồn Prototype

Tài liệu này phân tích cấu trúc mã nguồn prototype hiện có tại `scripts/` và thiết lập kế hoạch chuyển đổi, phân tách trách nhiệm sang cấu trúc phân lớp mới dưới `src/`.

---

## 1. Bản đồ di chuyển (Migration Mapping)

### 1.1. `scripts/explore_repo.py`
- **Trách nhiệm hiện tại**: Liệt kê toàn bộ file `.py` thuộc repository mục tiêu, thực hiện lọc các thư mục loại trừ và in ra HEAD commit hash.
- **Vấn đề Coupling**: Logic quét file và logic gọi git shell command nằm chung trong một file script tuyến tính.
- **Module đích**:
  - `src/infrastructure/filesystem/git_source_repository.py` (Adapter thực tế chạy lệnh git).
  - `src/application/services/discover_repository.py` (Service điều phối lọc danh sách file).
- **Cách Refactor**: Chuyển các khối lệnh thô thành phương thức trong class `GitSourceRepository`. Service `DiscoverRepositoryService` sẽ nhận vào repository adapter thông qua Dependency Injection.
- **Test cần viết trước**: Test quét thư mục giả lập chứa một số file `.py` và thư mục rác để verify bộ lọc hoạt động chính xác.
- **Rủi ro**: Thay đổi cách lọc dẫn đến bỏ sót hoặc thừa file so với danh sách ban đầu của nhóm.

### 1.2. `scripts/parser-service/parser.py`
- **Trách nhiệm hiện tại**: Nhận tham số CLI đầu vào, thực hiện vòng lặp qua các file để gọi parser và ghi event ra writer.
- **Vấn đề Coupling**: Đóng vai trò vừa là CLI parser vừa là bộ điều phối ứng dụng, phụ thuộc trực tiếp vào các file triển khai cục bộ của parser-service.
- **Module đích**:
  - `src/cli/main.py` (CLI commands parsing sử dụng Typer).
  - `src/application/services/process_file.py` (Service xử lý file đơn lẻ).
  - `src/application/services/process_repository.py` (Service điều phối lặp qua toàn bộ repo).
- **Cách Refactor**: CLI `main.py` sẽ chỉ nhận các flag, khởi tạo cấu hình settings và gọi `ProcessRepositoryService`. Service này sẽ lặp qua các file Python và gọi `ProcessFileService` xử lý.
- **Test cần viết trước**: Test tích hợp CLI mock gọi run lệnh scan.
- **Rủi ro**: Sai số lượng giới hạn `--limit` hoặc sai lệch đường dẫn đầu ra của tham số CLI cũ.

### 1.3. `scripts/parser-service/cpg_parser.py`
- **Trách nhiệm hiện tại**: Parse một file nguồn thành cây AST, CFG, DFG và Call graph bằng cách duyệt cây AST chuẩn của Python.
- **Vấn đề Coupling**: File quá lớn (hơn 300 dòng), chứa tất cả các builder cho AST, CFG, DFG, Call graph và logic định dạng event. Điều này vi phạm nguyên tắc Đơn trách nhiệm (Single Responsibility Principle).
- **Module đích**:
  - `src/parsing/cpg_parser.py` (Bộ điều phối chính).
  - `src/parsing/ast_builder.py` (Tách riêng phần duyệt AST nodes và AST edges).
  - `src/parsing/cfg_builder.py` (Tách riêng phần duyệt CFG next statements).
  - `src/parsing/dfg_builder.py` (Tách riêng phần phân tích data flow).
  - `src/parsing/call_builder.py` (Tách riêng phần phân tích cuộc gọi hàm).
- **Cách Refactor**: Tạo các class builder tương ứng. Khi parse một file, `CpgParser` sẽ tuần tự gọi các builder này và hợp nhất kết quả vào model `CpgGraph`.
- **Test cần viết trước**: Unit test độc lập cho từng Builder sử dụng các fixtures mã nguồn chuẩn trong `tests/fixtures/`.
- **Rủi ro**: Rất cao. Việc tách nhỏ logic dễ dẫn đến mất thông tin liên kết hoặc sai lệch thứ tự sinh ID của các cạnh.

### 1.4. `scripts/parser-service/stable_id.py`
- **Trách nhiệm hiện tại**: Chứa hàm sinh sha256 stable ID cho nodes và edges.
- **Vấn đề Coupling**: Không có, đây là helper thuần túy.
- **Module đích**: `src/parsing/identifiers.py`.
- **Cách Refactor**: Đưa các hàm sinh hash thành các static method hoặc class method trong `IdentifierGenerator` để thống nhất quản lý.
- **Test cần viết trước**: Unit test khẳng định tính deterministic của hàm sinh ID.
- **Rủi ro**: Rất thấp.

### 1.5. `scripts/parser-service/event_writer.py`
- **Trách nhiệm hiện tại**: Đóng vai trò vừa ghi file JSONL cục bộ vừa kết nối publish lên Kafka Broker.
- **Vấn đề Coupling**: Import trực tiếp thư viện `kafka-python`, dẫn đến việc kiểm thử offline bị phụ thuộc vào môi trường.
- **Module đích**:
  - `src/infrastructure/messaging/jsonl_event_writer.py` (Ghi file cục bộ).
  - `src/infrastructure/messaging/kafka_producer.py` (Ghi vào Kafka).
- **Cách Refactor**: Triển khai interface `EventWriterPort` định nghĩa trong `src/application/ports.py` cho cả hai class. Service sẽ gọi qua Port thay vì import trực tiếp adapter.
- **Test cần viết trước**: Test MockEventWriter để verify số lượng event được phát hành từ service.
- **Rủi ro**: Lỗi trễ kết nối (network connection timeout) khi chuyển từ ghi file sang đẩy Kafka Broker thật.

### 1.6. `scripts/parser-service/topics.py`
- **Trách nhiệm hiện tại**: Khai báo mapping các tên topic tĩnh.
- **Vấn đề Coupling**: Fix cứng cấu trúc trong code.
- **Module đích**:
  - `config/topics.yaml` (Cấu hình bên ngoài).
  - `src/infrastructure/config/settings.py` (Settings nạp động).
- **Cách Refactor**: Di chuyển cấu hình ra file yaml, nạp thông qua class Settings sử dụng pydantic-settings.
- **Test cần viết trước**: Test load cấu hình verify tên topic khớp với mong đợi.
- **Rủi ro**: Sai tên topic dẫn đến connector hoặc Spark job consume sai nguồn dữ liệu.

### 1.7. `scripts/parser-service/schemas/*.json`
- **Trách nhiệm hiện tại**: Schema JSON định nghĩa các trường của event.
- **Vấn đề Coupling**: Đang nằm sâu trong folder scripts.
- **Module đích**: `schemas/*.schema.json` ở root repository.
- **Cách Refactor**: Copy nguyên trạng các schema, chỉnh sửa đường dẫn ID và bổ sung các trường envelope đầy đủ như thiết kế hệ thống mới yêu cầu.
- **Test cần viết trước**: Kiểm thử hợp lệ JSON Schema.
- **Rủi ro**: Không tương thích hoàn toàn với các event được tạo ra từ code cũ của nhóm.
