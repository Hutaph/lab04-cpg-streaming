# Kế hoạch Triển khai Chi tiết Hệ thống CPG Ingestion

Tài liệu này vạch ra lộ trình triển khai gồm 15 phases từ thiết kế nền tảng cho đến báo cáo hoàn chỉnh.

---

## Phase 0 — Architecture and scaffold
- **Mục tiêu**: Xây dựng cấu trúc thư mục chuẩn hóa, các file cấu hình nền tảng, các tệp JSON Schema ban đầu và tài liệu kiến trúc.
- **Input**: Đề bài Lab 04 và code prototype hiện có.
- **Output**: Cấu trúc project scaffold hoàn chỉnh, các file code Python placeholder compile được.
- **File dự kiến thay đổi**:
  - `src/**` (tạo mới toàn bộ placeholder)
  - `config/**` (application.yaml, file_filters.yaml, topics.yaml)
  - `schemas/**` (4 file JSON schemas)
  - `infra/**` (docker-compose, neo4j, mongodb configs)
  - `tests/**` (unit/integration/e2e structures)
  - `docs/**` (tất cả tài liệu kiến trúc và ADRs)
- **Test bắt buộc**: Chạy `compileall` kiểm tra cú pháp Python và kiểm tra cú pháp JSON/YAML.
- **Điều kiện hoàn thành**: Cú pháp các file hợp lệ, không có thư mục `src/lab04_cpg`, code prototype được bảo toàn.
- **Rủi ro**: Lỗi cú pháp trong các file JSON Schema hoặc YAML.
- **KHÔNG ĐƯỢC LÀM**: Không refactor code cũ, không cấu hình hay chạy Docker thực tế.

---

## Phase 1 — Repository cloning and discovery
- **Mục tiêu**: Hiện thực hóa việc shallow clone repository mục tiêu và liệt kê các file Python hợp lệ cần phân tích.
- **Input**: Cấu hình repo URL từ `config/application.yaml`.
- **Output**: Thư mục `workspace/source/transformers-pr-agent` được clone thành công và danh sách file Python in ra CLI.
- **File dự kiến thay đổi**:
  - `src/infrastructure/filesystem/git_source_repository.py`
  - `src/application/services/discover_repository.py`
  - `scripts/clone_source_repo.sh`
  - `scripts/run_discovery.py`
- **Test bắt buộc**: Chạy script clone và đếm số lượng file Python in ra so sánh với con số mẫu (`2779` file).
- **Điều kiện hoàn thành**: Clone thành công và đếm chính xác số file, bỏ qua đúng các thư mục exclude.
- **Rủi ro**: Lỗi mạng khi clone repo lớn.
- **KHÔNG ĐƯỢC LÀM**: Không bắt đầu viết logic parser.

---

## Phase 2 — AST parser and deterministic IDs
- **Mục tiêu**: Triển khai AST Builder phân tích cú pháp tạo CPG Node, quan quan hệ `AST_CHILD` và sinh deterministic ID.
- **Input**: File Python từ repository mục tiêu.
- **Output**: Danh sách các node và quan hệ phân cấp AST kèm theo ID dạng sha256 ổn định.
- **File dự kiến thay đổi**:
  - `src/parsing/ast_builder.py`
  - `src/parsing/identifiers.py`
  - `src/parsing/metadata.py`
  - `src/parsing/cpg_parser.py`
- **Test bắt buộc**: Unit test `test_identifiers.py` và `test_ast_builder.py` sử dụng fixtures.
- **Điều kiện hoàn thành**: AST Node và Edge được sinh ra với ID trùng khớp khi parse lại cùng một file.
- **Rủi ro**: Xung đột hash ID hoặc sai lệch đường dẫn AST.
- **KHÔNG ĐƯỢC LÀM**: Không triển khai CFG/DFG.

---

## Phase 3 — CFG builder
- **Mục tiêu**: Triển khai logic tạo cạnh luồng điều khiển `CFG_NEXT` cấp câu lệnh.
- **Input**: AST tree và bảng tra cứu ID của các nodes.
- **Output**: Danh sách các cạnh `CFG_NEXT` kết nối các câu lệnh kề nhau.
- **File dự kiến thay đổi**:
  - `src/parsing/cfg_builder.py`
- **Test bắt buộc**: Unit test `test_cfg_builder.py` chạy trên fixture `if_else.py` và `loop.py`.
- **Điều kiện hoàn thành**: Các khối điều kiện (If, For, While) được thiết lập cạnh nhảy chính xác.
- **Rủi ro**: Vòng lặp vô hạn khi duyệt cấu trúc lồng nhau phức tạp.
- **KHÔNG ĐƯỢC LÀM**: Không viết DFG hay Call graph.

---

## Phase 4 — DFG builder
- **Mục tiêu**: Xây dựng cạnh truyền dữ liệu `DFG_REACHES` giữa điểm định nghĩa biến và điểm sử dụng biến.
- **Input**: AST tree và thông tin tuyến tính hóa từ CFG.
- **Output**: Cạnh DFG chỉ rõ biến nào truyền tới vị trí nào.
- **File dự kiến thay đổi**:
  - `src/parsing/dfg_builder.py`
- **Test bắt buộc**: Unit test `test_dfg_builder.py` trên fixture `simple_sequence.py`.
- **Điều kiện hoàn thành**: Trích xuất đúng quan hệ định nghĩa biến (Store) và tải biến (Load).
- **Rủi ro**: Không phân tích được tầm vực biến (variable scoping) dẫn đến nối sai định nghĩa.
- **KHÔNG ĐƯỢC LÀM**: Không tích hợp gửi Kafka.

---

## Phase 5 — Call graph builder
- **Mục tiêu**: Tạo nút `CallTarget` ổn định và cạnh `CALLS` nối từ AST Call site tới CallTarget.
- **Input**: Các nút AST dạng `ast.Call`.
- **Output**: Mạng lưới liên kết cuộc gọi hàm.
- **File dự kiến thay đổi**:
  - `src/parsing/call_builder.py`
- **Test bắt buộc**: Unit test `test_call_builder.py` trên fixture `function_calls.py`.
- **Điều kiện hoàn thành**: Tạo được node `CallTarget` với tên hàm tường minh và kết nối thành công.
- **Rủi ro**: Không giải quyết được các cuộc gọi hàm động (dynamic calls/methods).
- **KHÔNG ĐƯỢC LÀM**: Không thay đổi schema Kafka.

---

## Phase 6 — Local JSONL events and schema validation
- **Mục tiêu**: Hỗ trợ ghi kết quả parse cục bộ ra file JSON Lines khi chạy ở chế độ dry-run và validate schema.
- **Input**: CpgGraph kết quả parse.
- **Output**: Các file `nodes.jsonl`, `edges.jsonl`, `metadata.jsonl`, `errors.jsonl` hợp lệ về cấu trúc JSON Schema.
- **File dự kiến thay đổi**:
  - `src/infrastructure/messaging/jsonl_event_writer.py`
  - `src/infrastructure/messaging/event_validator.py`
  - `tests/unit/test_event_schema.py`
- **Test bắt buộc**: Thực hiện validate schema tự động bằng thư viện `jsonschema` trên các file JSONL sinh ra.
- **Điều kiện hoàn thành**: File JSONL xuất ra thành công và vượt qua kiểm tra định dạng schema.
- **Rủi ro**: Lỗi định dạng kiểu dữ liệu trong file schema (như date-time string).
- **KHÔNG ĐƯỢC LÀM**: Không kết nối tới Kafka Broker.

---

## Phase 7 — Kafka producer and topic creation
- **Mục tiêu**: Thiết lập Kafka Broker ở local và viết Adapter đẩy event trực tiếp từ Parser Service vào Kafka.
- **Input**: Cấu hình Kafka và các event đồ thị.
- **Output**: Sự kiện được gửi thành công vào các topic Kafka tương ứng.
- **File dự kiến thay đổi**:
  - `src/infrastructure/messaging/kafka_producer.py`
  - `infra/kafka/create-topics.sh`
  - `scripts/create_topics.sh`
- **Test bắt buộc**: Kiểm tra sự tồn tại của topic và consume message bằng console-consumer.
- **Điều kiện hoàn thành**: Tạo đúng 5 topic và publish tin nhắn thành công với event key tương ứng.
- **Rủi ro**: Khởi động Kafka container bị lỗi thiếu tài nguyên hoặc port bị chiếm dụng.
- **KHÔNG ĐƯỢC LÀM**: Không cấu hình Neo4j hay MongoDB.

---

## Phase 8 — Neo4j Kafka Sink
- **Mục tiêu**: Cấu hình và khởi chạy Neo4j Kafka Connect Sink để tự động ghi node/edge vào Neo4j Graph.
- **Input**: Đồ thị Neo4j trống và luồng event từ Kafka topic `cpg.nodes` và `cpg.edges`.
- **Output**: Đồ thị Neo4j được cập nhật tự động dựa trên Cypher MERGE query.
- **File dự kiến thay đổi**:
  - `infra/kafka-connect/connectors/neo4j-nodes-sink.json`
  - `infra/kafka-connect/connectors/neo4j-edges-sink.json`
  - `scripts/register_connectors.sh`
- **Test bắt buộc**: Chạy lệnh query Cypher verify số lượng node/edge trong database sau khi publish event.
- **Điều kiện hoàn thành**: Connector đăng ký thành công, tự động map thuộc tính event vào property của Node/Relationship.
- **Rủi ro**: Lỗi syntax Cypher trong config connector khiến connector bị FAIL.
- **KHÔNG ĐƯỢC LÀM**: Không ghi trực tiếp từ code Python vào Neo4j.

---

## Phase 9 — Spark Structured Streaming
- **Mục tiêu**: Viết ứng dụng Spark Structured Streaming đọc metadata event từ Kafka.
- **Input**: Kafka broker và metadata events.
- **Output**: Dataframe streaming trong Spark nhận đủ dữ liệu.
- **File dự kiến thay đổi**:
  - `spark_jobs/metadata_to_mongodb.py`
- **Test bắt buộc**: Print stream console của Spark kiểm tra schema dữ liệu nhận được.
- **Điều kiện hoàn thành**: Job Spark chạy không lỗi, nhận diện đúng schema Kafka metadata.
- **Rủi ro**: Sai lệch phiên bản thư viện Spark SQL Kafka connector.
- **KHÔNG ĐƯỢC LÀM**: Không kết nối ghi vào MongoDB.

---

## Phase 10 — MongoDB upsert
- **Mục tiêu**: Kết nối ghi dữ liệu từ Spark Structured Streaming sang MongoDB sử dụng MongoDB Spark Connector với tính năng Upsert.
- **Input**: Spark stream dataframe.
- **Output**: Các document metadata được lưu trữ chính xác trong MongoDB collection.
- **File dự kiến thay đổi**:
  - `spark_jobs/metadata_to_mongodb.py` (cập nhật đầu ra ghi)
- **Test bắt buộc**: Query kiểm tra số lượng bản ghi trong collection `file_statistics`.
- **Điều kiện hoàn thành**: Ghi đè (upsert) tài liệu thành công dựa trên `file_id` mà không tạo trùng lặp.
- **Rủi ro**: Lỗi ghi hoặc mất kết nối giữa Spark và MongoDB.
- **KHÔNG ĐƯỢC LÀM**: Không xóa checkpoint Spark giữa các lần test trừ khi được yêu cầu.

---

## Phase 11 — Incremental state and delete events
- **Mục tiêu**: Hiện thực hóa SQLite state store để lưu trữ hash file và tính toán diff phát hành sự kiện DELETE khi file bị sửa đổi.
- **Input**: Trạng thái file cũ trong SQLite và CpgGraph mới.
- **Output**: Bảng SQLite cập nhật chính xác và sinh ra các sự kiện DELETE node/edge mồ côi.
- **File dự kiến thay đổi**:
  - `src/infrastructure/state/sqlite_state_store.py`
  - `src/parsing/diff.py`
  - `src/application/services/process_repository.py`
- **Test bắt buộc**: Chạy unit test kiểm tra hàm compute diff trả về đúng danh sách ID bị xóa.
- **Điều kiện hoàn thành**: Lưu vết chính xác lịch sử quét, sinh đúng delete events.
- **Rủi ro**: SQLite bị lock do truy cập đồng thời hoặc database file bị hỏng.
- **KHÔNG ĐƯỢC LÀM**: Không reset thủ công file state.db trong khi kiểm thử.

---

## Phase 12 — Full repository run
- **Mục tiêu**: Thực hiện quét toàn bộ repository mục tiêu lần đầu tiên (clean run) và ghi nhận số liệu.
- **Input**: Toàn bộ `2779` file Python của `transformers-pr-agent`.
- **Output**: Đồ thị hoàn chỉnh được ghi nhận trên Neo4j, metadata lưu trữ tại MongoDB và tệp manifest log.
- **File dự kiến thay đổi**:
  - `src/infrastructure/filesystem/manifest_writer.py`
  - `scripts/run_parser.py`
- **Test bắt buộc**: Kiểm tra logs hệ thống, kiểm tra tính toàn vẹn của Neo4j và MongoDB.
- **Điều kiện hoàn thành**: Hoàn thành việc quét toàn bộ file trong thời gian hợp lý, không crash, lưu manifest.
- **Rủi ro**: Tràn bộ nhớ do sinh lượng event quá lớn cùng lúc.
- **KHÔNG ĐƯỢC LÀM**: Không chạy lại mà không lưu logs.

---

## Phase 13 — Idempotent replay verification
- **Mục tiêu**: Thực hiện sửa đổi thử nghiệm một file Python, re-run riêng file đó và xác minh tính idempotent.
- **Input**: Một file Python được chỉnh sửa nội dung (ví dụ: thêm hàm mới hoặc xóa hàm cũ).
- **Output**: Neo4j cập nhật đúng nút mới và xóa nút cũ, MongoDB ghi nhận metadata cập nhật, Spark không xử lý lại các file không đổi.
- **File dự kiến thay đổi**:
  - `src/application/services/replay_file.py`
  - `scripts/verify_neo4j.cypher`
  - `scripts/verify_mongodb.js`
- **Test bắt buộc**: So sánh count trước/sau re-run tại các database.
- **Điều kiện hoàn thành**: Các node/edge cũ bị xóa thành công khỏi Neo4j, node/edge mới được tạo, không bị trùng lặp dữ liệu cũ.
- **Rủi ro**: Lỗi đồng bộ khiến đồ thị bị đứt gãy hoặc mồ côi.
- **KHÔNG ĐƯỢC LÀM**: Không thay đổi thủ công dữ liệu trong database để làm test pass.

---

## Phase 14 — Jupyter Book and GitHub Pages
- **Mục tiêu**: Cập nhật toàn bộ tài liệu báo cáo của các task vào Jupyter Book và xuất bản lên GitHub Pages.
- **Input**: Kết quả chạy thực tế, logs, screenshots từ các phase trước.
- **Output**: Jupyter Book được compile thành công và host công khai trên GitHub Pages.
- **File dự kiến thay đổi**:
  - `lab04-book/**` (hoàn thiện tất cả các trang md và ipynb)
- **Test bắt buộc**: Chạy `jupyter-book build lab04-book` kiểm tra lỗi compile.
- **Điều kiện hoàn thành**: Website hiển thị trực quan sơ đồ kiến trúc, kết quả chạy và reflection đầy đủ.
- **Rủi ro**: Lỗi build html do lỗi định dạng markdown hoặc thiếu thư viện bổ trợ.
- **KHÔNG ĐƯỢC LÀM**: Không nộp file nén ZIP thay cho URL GitHub Pages.
