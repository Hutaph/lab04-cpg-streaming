# Ma trận Truy vết Yêu cầu (Traceability Matrix)

Tài liệu này đối chiếu tất cả các yêu cầu từ đề bài Lab 04 với thiết kế chi tiết, module tương ứng, bằng chứng kiểm tra và trạng thái hiện tại.

| Yêu cầu đề bài | Thiết kế đáp ứng | Module/File | Bằng chứng cần có | Trạng thái |
| :--- | :--- | :--- | :--- | :--- |
| **Shallow clone** | Clone repository mẫu bằng git parameter `--depth 1` | `scripts/clone_source_repo.sh` | Output lệnh git clone và thư mục clone | **Scaffolded** |
| **Đếm file Python** | Tìm kiếm và đếm số lượng file `.py` không nằm trong danh sách exclude | [discover_repository.py](file:///home/phat/AI_Project/lab04-cpg-streaming/src/application/services/discover_repository.py) | Số lượng file Python in ra log hoặc màn hình CLI | **Scaffolded** |
| **Incremental parser** | So sánh hash nội dung để chỉ parse những file bị thay đổi | [sqlite_state_store.py](file:///home/phat/AI_Project/lab04-cpg-streaming/src/infrastructure/state/sqlite_state_store.py) | Log chạy parser lần 2 chỉ ra số lượng file xử lý là 0 | **Scaffolded** |
| **AST** | Trích xuất cây cú pháp trừu tượng AST của Python | [ast_builder.py](file:///home/phat/AI_Project/lab04-cpg-streaming/src/parsing/ast_builder.py) | Cạnh AST_CHILD kết nối các nút phân cấp trong Neo4j | **Scaffolded** |
| **CFG** | Trích xuất luồng điều khiển nhảy giữa các câu lệnh | [cfg_builder.py](file:///home/phat/AI_Project/lab04-cpg-streaming/src/parsing/cfg_builder.py) | Cạnh CFG_NEXT kết nối các câu lệnh kề nhau trong Neo4j | **Scaffolded** |
| **DFG** | Phân tích quan hệ lan truyền dữ liệu biến | [dfg_builder.py](file:///home/phat/AI_Project/lab04-cpg-streaming/src/parsing/dfg_builder.py) | Cạnh DFG_REACHES giữa định nghĩa biến và điểm sử dụng | **Scaffolded** |
| **Call edges** | Trích xuất cuộc gọi hàm kết nối tới CallTarget | [call_builder.py](file:///home/phat/AI_Project/lab04-cpg-streaming/src/parsing/call_builder.py) | Nút CallTarget và cạnh CALLS nối từ nút gọi hàm | **Scaffolded** |
| **Bounded memory** | Parse tuần tự từng file, giải phóng bộ nhớ ngay sau đó | [process_file.py](file:///home/phat/AI_Project/lab04-cpg-streaming/src/application/services/process_file.py) | Log giám sát dung lượng RAM tiêu thụ cố định khi chạy | **Scaffolded** |
| **Stable IDs** | Hàm hash sha256 sinh ID ổn định từ thuộc tính cố định | [identifiers.py](file:///home/phat/AI_Project/lab04-cpg-streaming/src/parsing/identifiers.py) | Unit test chứng minh ID không đổi qua các lần chạy | **Scaffolded** |
| **Bốn Kafka topics** | Thiết kế topic riêng cho nodes, edges, metadata và errors | `config/topics.yaml` | Output lệnh liệt kê topics của Kafka Broker | **Scaffolded** |
| **Schema version** | Trường `schema_version` trong envelope để đánh dấu phiên bản | `schemas/*.json` | Bản ghi JSON chứa trường schema_version dạng int | **Scaffolded** |
| **Event time** | Trường `event_time` đánh dấu thời điểm xảy ra sự kiện | `schemas/*.json` | Bản ghi JSON chứa trường event_time dạng ISO 8601 | **Scaffolded** |
| **Neo4j direct sink** | Đẩy node/edge từ Kafka vào Neo4j không qua Spark | `infra/kafka-connect/connectors/*.json` | Cấu hình connector hiển thị trên Kafka Connect REST API | **Scaffolded** |
| **Neo4j idempotency** | Sử dụng Cypher MERGE để ghi đè thay vì tạo mới | `infra/kafka-connect/connectors/*.json` | Số lượng bản ghi Neo4j không tăng khi chạy replay | **Scaffolded** |
| **Spark Streaming** | Job Spark consume metadata từ Kafka theo cơ chế streaming | `spark_jobs/metadata_to_mongodb.py` | Log Spark hiển thị luồng dữ liệu liên tục | **Scaffolded** |
| **MongoDB Connector** | Ghi dữ liệu từ Spark Structured Streaming sang MongoDB | `spark_jobs/metadata_to_mongodb.py` | Document lưu trữ trong MongoDB collection | **Scaffolded** |
| **Spark checkpoint** | Cấu hình persistent directory để lưu offset Kafka | `config/application.yaml` | Thư mục checkpoint chứa các file offset Spark | **Scaffolded** |
| **Modified-file replay**| Thay đổi nội dung file, parser re-run và cập nhật | [replay_file.py](file:///home/phat/AI_Project/lab04-cpg-streaming/src/application/services/replay_file.py) | Log chạy replay hiển thị số lượng event cập nhật | **Scaffolded** |
| **No duplication** | Replay không làm trùng lặp phần tử trên các databases | [replay_file.py](file:///home/phat/AI_Project/lab04-cpg-streaming/src/application/services/replay_file.py) | Kiểm tra số lượng bản ghi DB bằng verify script | **Scaffolded** |
| **Architecture diagram**| Vẽ sơ đồ kiến trúc hệ thống chi tiết | `docs/system_architecture.md` | Mermaid diagram tích hợp trong tài liệu | **Scaffolded** |
| **Jupyter Book** | Biên dịch toàn bộ tài liệu báo cáo dạng sách | `lab04-book/myst.yml` | Thư mục `lab04-book/_build/html` được tạo | **Existing** |
| **GitHub Pages** | Host Jupyter Book công khai | `.github/workflows/deploy.yml` | URL public hoạt động bình thường | **Existing** |
| **Executed cells** | Chạy notebook lưu lại kết quả hiển thị | `lab04-book/*.ipynb` | Kết quả hiển thị in ra dưới mỗi cell | **Existing** |
| **Screenshots** | Đính kèm hình ảnh database UI vào báo cáo | `lab04-book/` | Hình ảnh hiển thị trên trang báo cáo HTML | **TODO** |
| **Reflection** | Viết đánh giá phản hồi ở cuối mỗi chapter | `lab04-book/reflection.md` | Mục Reflection hiển thị ở cuối Jupyter Book | **Scaffolded** |
| **Meaningful commits** | Commit phản ánh tiến độ chi tiết của nhóm | Git history | Lịch sử commit chứa mã [Task N] tăng dần | **Scaffolded** |
