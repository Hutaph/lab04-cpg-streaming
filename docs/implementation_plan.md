# Kế hoạch Triển khai, Kiểm thử và Ma trận Truy vết Yêu cầu

Tài liệu này vạch ra lộ trình triển khai gồm 15 phases, chiến lược đảm bảo chất lượng phần mềm, và ma trận đối chiếu yêu cầu thực tế của dự án.

---

## 1. Lộ trình Triển khai chi tiết (15 Phases)

### Phase 0 — Architecture and scaffold
- **Mục tiêu**: Xây dựng cấu trúc thư mục chuẩn hóa, cấu hình YAML tĩnh, JSON Schemas ban đầu và tài liệu kiến trúc kỹ thuật.
- **Output**: Cấu trúc project scaffold hoàn chỉnh, các file code Python placeholder compile được.
- **Trạng thái**: **Hoàn thành (Verified)**.

### Phase 1 — Repository cloning and discovery
- **Mục tiêu**: Shallow clone repository mục tiêu và liệt kê các file Python hợp lệ cần phân tích, loại trừ các mẫu exclude.
- **Output**: Thư mục `workspace/source/transformers-pr-agent` được clone thành công và danh sách file Python in ra CLI.
- **Trạng thái**: **Hoàn thành (Verified)**.

### Phase 2 — AST parser and deterministic IDs
- **Mục tiêu**: Triển khai AST Builder phân tích cú pháp tạo CPG Node, quan hệ `AST_CHILD` và sinh deterministic ID.
- **Output**: Danh sách các node và quan hệ phân cấp AST kèm theo ID dạng sha256 ổn định.
- **Trạng thái**: **Hoàn thành (Verified)**.

### Phase 3 — CFG builder
- **Mục tiêu**: Triển khai logic tạo cạnh luồng điều khiển `CFG_NEXT` cấp câu lệnh.
- **Output**: Danh sách các cạnh `CFG_NEXT` kết nối các câu lệnh kề nhau.
- **Trạng thái**: **Hoàn thành (Verified)**.

### Phase 4 — DFG builder
- **Mục tiêu**: Xây dựng cạnh truyền dữ liệu `DFG_REACHES` giữa điểm định nghĩa biến và điểm sử dụng biến.
- **Output**: Cạnh DFG chỉ rõ biến nào truyền tới vị trí nào.
- **Trạng thái**: **Hoàn thành (Verified)**.

### Phase 5 — Call graph builder
- **Mục tiêu**: Tạo nút `CallTarget` ổn định và cạnh `CALLS` nối từ AST Call site tới CallTarget.
- **Output**: Mạng lưới liên kết cuộc gọi hàm.
- **Trạng thái**: **Hoàn thành (Verified)**.

### Phase 6 — Local JSONL events and schema validation
- **Mục tiêu**: Hỗ trợ ghi kết quả parse cục bộ ra file JSON Lines khi chạy ở chế độ dry-run và validate schema.
- **Output**: Các file `nodes.jsonl`, `edges.jsonl`, `metadata.jsonl`, `errors.jsonl` hợp lệ về cấu trúc JSON Schema.
- **Trạng thái**: **Hoàn thành (Verified)**.

### Phase 7 — Kafka producer and topic creation
- **Mục tiêu**: Thiết lập Kafka Broker ở local và viết Adapter đẩy event trực tiếp từ Parser Service vào Kafka.
- **Output**: Sự kiện được gửi thành công vào các topic Kafka tương ứng.
- **Trạng thái**: **Hoàn thành (Verified)**.
- **Yêu cầu & Xác minh (Requirements & Verification)**:
  - Các lỗi phân tích cú pháp (Parser errors) được publish chủ động sang topic `parser.errors`.
  - Schema và routing của topic `parser.errors` được xác minh thông qua script kiểm tra và notebook.
  - Không thực hiện xác minh Kafka Connect DLQ trong Task 3 (chỉ kiểm thử luồng lỗi parser nghiệp vụ).

### Phase 8 — Neo4j Kafka Sink
- **Mục tiêu**: Cấu hình và khởi chạy Neo4j Kafka Connect Sink để tự động ghi node/edge vào Neo4j Graph.
- **Output**: Đồ thị Neo4j được cập nhật tự động dựa trên Cypher MERGE query.
- **Trạng thái**: **Hoàn thành (Docker E2E verified)**.
- **Yêu cầu Thiết kế (Design Requirements)**:
  - **Kháng xáo trộn thứ tự Node-Edge**: Edge ingestion phải chấp nhận và xử lý được trường hợp các node đầu/cuối của cạnh chưa tồn tại trong Neo4j (ví dụ: tạo placeholder node và bổ sung thuộc tính sau).
  - **Xóa Idempotent**: Các câu lệnh DELETE cho node và edge phải chạy idempotent (không báo lỗi khi đối tượng cần xóa chưa tồn tại hoặc đã bị xóa trước đó).
  - **Tránh xung đột do Replay**: Sự kiện trùng lặp do retry hoặc replay từ Kafka Connect phải không gây bất nhất hay trùng lặp phần tử đồ thị trong Neo4j.
  - **Cấu hình Kafka Connect DLQ**: Cấu hình Kafka Connect DLQ định tuyến sang topic `connector.errors` cho các lỗi ghi Neo4j Sink.
  - **Dung sai lỗi Connector**: Kích hoạt cơ chế connector error tolerance phù hợp với đặc tả yêu cầu của Đồ án.
  - **Bảo toàn bản ghi gốc**: Đảm bảo DLQ bảo toàn bản ghi gốc bị lỗi và đính kèm ngữ cảnh lỗi/headers hữu ích nếu được hỗ trợ.
  - **Kiểm chứng DLQ thực tế**: Minh họa bằng chứng thực tế về một lỗi ghi connector rơi vào topic `connector.errors` thay vì gửi tin nhắn test giả tạo trực tiếp lên DLQ.
  - **Kiểm thử Ingestion**: Bộ test kiểm nghiệm Task 4 bắt buộc phải bao gồm kịch bản kiểm tra hành vi xử lý cạnh đến trước node (edge-before-node handling).

### Phase 9 — Spark Structured Streaming
- **Mục tiêu**: Viết ứng dụng Spark Structured Streaming đọc metadata event từ Kafka.
- **Output**: Dataframe streaming trong Spark nhận đủ dữ liệu.
- **Trạng thái**: **Đã triển khai (Docker E2E verified)**.

### Phase 10 — MongoDB Spark Ingestion
- **Mục tiêu**: Cấu hình Spark Structured Streaming ghi trực tiếp dữ liệu metadata vào MongoDB.
- **Output**: Tài liệu metadata thống kê được cập nhật liên tục vào MongoDB.
- **Trạng thái**: **Đã triển khai (Docker E2E verified)**.

### Phase 11 — SQLite State Store and incremental parses
- **Mục tiêu**: Tích hợp SQLite State Store ghi nhận lịch sử parse để hỗ trợ quét so khớp tăng dần.
- **Output**: SQLite db ghi lưu hash của các file, CLI bỏ qua các file không đổi nội dung.
- **Trạng thái**: **Hoàn thành (Verified locally)**.

### Phase 12 — File replay and graph diffs
- **Mục tiêu**: Hiện thực hóa việc diff đồ thị CPG cũ/mới của một file khi file đó bị chỉnh sửa nội dung, phát hành Delete events tương ứng.
- **Output**: Logic CpgDiffer tính toán chính xác số node/edge bị thay đổi, gửi Delete events tương ứng để dọn dẹp Neo4j.
- **Trạng thái**: **Hoàn thành (Verified locally)**.

### Phase 13 — Overall replay validation
- **Mục tiêu**: Tích hợp toàn trình kiểm tra replay trên cả Neo4j và MongoDB để chứng minh tính idempotent.
- **Output**: Không phát sinh bản ghi trùng lặp trên database sau nhiều lần chạy lại.
- **Trạng thái**: **Hoàn thành (Docker E2E verified)**.

### Phase 14 — Official Jupyter Book and reflections
- **Mục tiêu**: Biên dịch toàn bộ tài liệu báo cáo thực hành và publish công khai qua GitHub Pages.
- **Output**: Jupyter Book chứa đầy đủ bằng chứng thực thi các Task và Reflection.
- **Trạng thái**: **Hoàn thành (Book build verified)**.

---

## 2. Chiến lược Kiểm thử (Testing Strategy)

Mọi thay đổi nghiệp vụ hoặc adapter phải đi kèm kiểm thử và đảm bảo chất lượng tĩnh:

### 2.1. Đảm bảo chất lượng mã nguồn tĩnh (Static Analysis)
- **Kiểm tra biên dịch**: Chạy biên dịch toàn bộ tệp tin Python trong dự án:
  ```bash
  uv run python -m compileall -q src scripts spark_jobs
  ```
- **Linter & Formatter**: Sử dụng Ruff để duy trì chất lượng code:
  ```bash
  uv run ruff check src tests scripts spark_jobs
  uv run ruff format --check src tests scripts spark_jobs
  ```
- **Type Checking**: Sử dụng strict Mypy để kiểm tra kiểu dữ liệu:
  ```bash
  MYPYPATH=src uv run mypy --explicit-package-bases src
  ```

### 2.2. Unit Tests
- Được tổ chức trong thư mục `tests/unit/`, chạy độc lập không phụ thuộc môi trường mạng hay database bên ngoài.
- **Lệnh chạy**:
  ```bash
  PYTHONPATH=src uv run pytest tests/unit -q
  ```
- **Mục tiêu**: Kiểm thử logic của AST/CFG/DFG/Call builders, thuật toán sinh Stable ID, logic diff đồ thị `CpgDiffer`, và SQLite state store adapter.

---

## 3. Ma trận Truy vết Yêu cầu (Traceability Matrix)

| Yêu cầu đề bài | Thiết kế đáp ứng | Module/File | Bằng chứng cần có | Trạng thái |
| :--- | :--- | :--- | :--- | :--- |
| **Shallow clone** | Clone repository mẫu bằng git parameter `--depth 1` | `scripts/clone_source_repo.sh` | Output lệnh git clone và thư mục clone | **Verified** |
| **Đếm file Python** | Tìm kiếm và đếm số lượng file `.py` không nằm trong danh sách exclude | [discover_repository.py](../src/application/services/discover_repository.py) | Số lượng file Python in ra log hoặc màn hình CLI | **Verified** |
| **Incremental parser** | So sánh hash nội dung để chỉ parse những file bị thay đổi | [sqlite_state_store.py](../src/infrastructure/state/sqlite_state_store.py) | Log chạy parser lần 2 chỉ ra số lượng file xử lý là 0 | **Verified locally** |
| **AST** | Trích xuất cây cú pháp trừu tượng AST của Python | [ast_builder.py](../src/parsing/ast_builder.py) | Cạnh AST_CHILD kết nối các nút phân cấp trong Neo4j | **Verified locally** |
| **CFG** | Trích xuất luồng điều khiển nhảy giữa các câu lệnh | [cfg_builder.py](../src/parsing/cfg_builder.py) | Cạnh CFG_NEXT kết nối các câu lệnh kề nhau trong Neo4j | **Verified locally** |
| **DFG** | Phân tích quan hệ lan truyền dữ liệu biến | [dfg_builder.py](../src/parsing/dfg_builder.py) | Cạnh DFG_REACHES giữa định nghĩa biến và điểm sử dụng | **Verified locally** |
| **Call edges** | Trích xuất cuộc gọi hàm kết nối tới CallTarget | [call_builder.py](../src/parsing/call_builder.py) | Nút CallTarget và cạnh CALLS nối từ nút gọi hàm | **Verified locally** |
| **Bounded memory** | Parse tuần tự từng file, giải phóng bộ nhớ ngay sau đó | [process_file.py](../src/application/services/process_file.py) | Log giám sát dung lượng RAM tiêu thụ cố định khi chạy | **Verified locally** |
| **Stable IDs** | Hàm hash sha256 sinh ID ổn định từ thuộc tính cố định | [identifiers.py](../src/parsing/identifiers.py) | Unit test chứng minh ID không đổi qua các lần chạy | **Verified locally** |
| **Bốn Kafka topics** | Thiết kế topic riêng cho nodes, edges, metadata và errors | `config/topics.yaml` | Output lệnh liệt kê topics của Kafka Broker | **Verified** |
| **Schema version** | Trường `schema_version` trong envelope để đánh dấu phiên bản | `schemas/*.json` | Bản ghi JSON chứa trường schema_version dạng string "1.0" | **Verified** |
| **Event time** | Trường `event_time` đánh dấu thời điểm xảy ra sự kiện | `schemas/*.json` | Bản ghi JSON chứa trường event_time dạng ISO 8601 | **Verified** |
| **Neo4j direct sink** | Đẩy node/edge từ Kafka vào Neo4j không qua Spark | `infra/kafka-connect/connectors/*.json` | Cấu hình connector hiển thị trên Kafka Connect REST API | **Docker E2E verified** |
| **Neo4j idempotency** | Sử dụng Cypher MERGE để ghi đè thay vì tạo mới | `infra/kafka-connect/connectors/*.json` | Số lượng bản ghi Neo4j không tăng khi chạy replay | **Docker E2E verified** |
| **Spark Streaming** | Job Spark consume metadata từ Kafka theo cơ chế streaming | `spark_jobs/metadata_to_mongodb.py`, `lab04-book/task5_spark_mongodb.ipynb` | Dataframe streaming lọc `FILE_METADATA_UPSERT` từ topic `source.metadata` | **Docker E2E verified** |
| **MongoDB Connector** | Ghi dữ liệu từ Spark Structured Streaming sang MongoDB | `spark_jobs/metadata_to_mongodb.py`, `lab04-book/task5_spark_mongodb.ipynb` | Writer MongoDB dùng `replace`/`upsertDocument` theo `file_id` | **Docker E2E verified** |
| **Spark checkpoint** | Cấu hình persistent directory để lưu offset Kafka | `spark_jobs/metadata_to_mongodb.py`, `lab04-book/task5_spark_mongodb.ipynb` | `checkpointLocation` trỏ tới `workspace/checkpoints/spark` | **Docker E2E verified** |
| **Modified-file replay**| Thay đổi nội dung file, parser re-run và cập nhật | [replay_file.py](../src/application/services/replay_file.py) | Log chạy replay hiển thị số lượng event cập nhật | **Docker E2E verified** |
| **No duplication** | Replay không làm trùng lặp phần tử trên các databases | [replay_file.py](../src/application/services/replay_file.py) | Kiểm tra số lượng bản ghi DB bằng verify script | **Docker E2E verified** |
| **Architecture diagram**| Vẽ sơ đồ kiến trúc hệ thống chi tiết | [system_architecture.md](system_architecture.md) | Mermaid diagram tích hợp trong tài liệu | **Book build verified** |
| **Jupyter Book** | Biên dịch toàn bộ tài liệu báo cáo dạng sách | `lab04-book/myst.yml` | Thư mục `lab04-book/_build/html` được tạo | **Book build verified** |
| **GitHub Pages** | Host Jupyter Book công khai | `.github/workflows/deploy.yml` | URL public hoạt động bình thường | **Book build verified** |
| **Executed cells** | Chạy notebook lưu lại kết quả hiển thị | `lab04-book/*.ipynb` | Kết quả hiển thị in ra dưới mỗi cell | **Book build verified** |
| **Screenshots** | Đính kèm hình ảnh hoặc embedded figure của kết quả database vào báo cáo | `lab04-book/task5_spark_mongodb.ipynb` | Figure runtime hiển thị luồng Kafka → Spark → MongoDB, document count và checkpoint artifacts | **Book build verified** |
| **Reflection** | Viết đánh giá phản hồi ở cuối mỗi chapter | `lab04-book/*.ipynb` | Mục Reflection hiển thị ở cuối mỗi notebook | **Book build verified** |
| **Meaningful commits** | Commit phản ánh tiến độ chi tiết của nhóm | Git history | Lịch sử commit chứa mã [Task N] tăng dần | **Verified** |

---

## 4. Hướng dẫn Nộp bài (Moodle Submission Rules)
- Bài thực hành được nộp chính thức dưới dạng **root URL của published Jupyter Book** (GitHub Pages).
- Moodle **chỉ nhận đúng 1 text entry** chứa URL này. Không chấp nhận nộp file nén ZIP, tệp tài liệu PDF hoặc Word.

---

## 5. Danh sách Backlog và Trạng thái Tồn đọng (Backlog and Pending Status)

### 5.1. Backlog của Task 2
Trạng thái tồn đọng kỹ thuật của Task 2 phục vụ các đợt tối ưu hóa trong tương lai:
- **Chuẩn hóa runtime verification directories — Still pending.**
  Implementation hiện vẫn sử dụng `workspace/tmp/notebook/` cho SQLite state và `workspace/tmp/notebook-parser/` cho JSONL output. Hai path này cần được gom về một cấu trúc semantic như `workspace/tmp/parser-verification/` trong một phiên refactor riêng.
- **Dọn dẹp runtime verification artifacts — Still pending.**
  Các SQLite và JSONL files sinh ra trong quá trình verification cần có cơ chế cleanup mặc định sau khi notebook hoặc verification flow kết thúc.
- **Bổ sung `line` và `column` cho `SyntaxError` — Still pending.**
  `PARSER_ERROR` hiện chưa lưu đầy đủ structured source position mặc dù exception message có thể chứa thông tin dòng và cột.
- **Re-audit Task 2 cached outputs — Not re-audited.**
  Task 2 notebook không được execute hoặc re-audit trong phiên này. Cần kiểm tra cached outputs sau khi runtime paths và error fields được refactor.
- **Ẩn runtime directories khỏi project explorer — Still pending.**
  Các thư mục verification tạm cần được loại khỏi source-oriented workspace view hoặc được cleanup để tránh bị hiểu nhầm là project artifacts.

### 5.2. Trạng thái Task 3
Task 3 đã hoàn thành các bước rà soát biên an toàn và kiểm chứng. Kafka broker, topic provisioning, event validation, full-batch pre-serialization, message keys, per-topic partition consistency, run-scoped inspection và publish-before-state-commit đã được kiểm chứng trong môi trường local single-broker.
Về giới hạn kiến trúc: Kafka và SQLite không tham gia cùng một distributed transaction. Crash sau Kafka acknowledgement nhưng trước SQLite commit có thể khiến cùng một batch được publish lại. Stable deterministic IDs tạo cơ sở để Task 4 triển khai Neo4j idempotent writes; duplicate handling đầu cuối được kiểm chứng và hoàn thiện ở các Task tiếp theo (Task 4 & 5).
Task 3 notebook đã được chạy thành công hai lần liên tiếp để xác nhận tính độc lập và dọn dẹp tài nguyên. Các cached outputs hiển thị đầy đủ kết quả của các Phase.

#### Accepted Limitations of Task 3

Các giới hạn dưới đây được biết đến và chấp nhận trong phạm vi Task 3. Chúng không phải là lỗi cần sửa trong Task 3.

1. **Single broker topology**: Pipeline chạy trên một KRaft broker với replication factor 1.
2. **No High Availability**: Không có broker redundancy hoặc failover.
3. **Local `acks=all` semantics**: Trong topology hiện tại, `acks=all` chỉ chờ ISR duy nhất (broker đơn), không tạo broker redundancy.
4. **No Kafka–SQLite distributed transaction**: Kafka và SQLite không cùng tham gia một atomic distributed transaction.
5. **Partial delivery possibility**: Sau khi Kafka enqueue bắt đầu, một phần batch có thể đã được broker nhận trước khi failure được phát hiện.
6. **Crash-window duplicate replay**: Crash sau Kafka acknowledgement nhưng trước SQLite commit có thể khiến cùng batch được publish lại ở lần chạy tiếp theo.
7. **Producer idempotence scope**: `enable.idempotence=True` giúp giảm duplicate do retry trong cùng một producer session. Nó không loại bỏ duplicate giữa các process runs, không đồng bộ Kafka với SQLite, và không xử lý side effects trên Neo4j hoặc MongoDB.
8. **No cross-topic ordering**: Kafka không bảo đảm ordering giữa `cpg.nodes`, `cpg.edges`, `source.metadata` và `parser.errors`.
9. **Per-partition ordering only**: Ordering chỉ được bảo toàn trong từng topic partition.
10. **Partition remapping**: Thay đổi partition count có thể ánh xạ cùng `file_id` sang partition khác.
11. **Limited topic drift detection**: Topic provisioning hiện kiểm tra partition count và replication factor; không xác nhận toàn bộ Kafka topic configuration.
12. **Synchronous processing assumption**: Processing flow hiện tại giả định single-process execution tuần tự.
13. **Downstream idempotency implemented and verified**: Neo4j idempotent ingestion đã được kiểm chứng đầy đủ qua các integration tests và notebook evidence sử dụng Cypher `MERGE` kết hợp ràng buộc unique ID.
14. **Stale-event protection implemented**: Đã triển khai cơ chế Node và Edge Tombstones để lưu vết thế hệ đã bị xóa, ngăn chặn việc hồi sinh (resurrection) từ stale events cùng thế hệ.
15. **Kafka Connect DLQ verified**: Đã cấu hình và kiểm chứng live topic `connector.errors` nhận các record lỗi mismatch endpoint, connector và tasks vẫn giữ trạng thái `RUNNING`.
16. **Hard-kill cleanup limitation**: Python `try/finally` không chạy nếu process bị hard kill (`SIGKILL`); safe cleanup chỉ giảm nguy cơ để lại artifacts trong trường hợp bình thường.

### 5.3. Optional Future Considerations
- **Đánh giá Kafka Transactions**: Kafka transactions chỉ cần được đánh giá nếu hệ thống phát sinh một luồng Kafka-to-Kafka yêu cầu transactional semantics. Cơ chế này không tạo exactly-once end-to-end cho các side effects trên SQLite, Neo4j hoặc MongoDB.
- **Tính nhất quán chéo hệ thống**: Nếu hệ thống sau này cần consistency mạnh hơn giữa nhiều storage systems, thiết kế phải cân nhắc các cơ chế như transactional outbox, staging, versioning, tombstones hoặc một consistency protocol tương đương.

### 5.4. Yêu cầu thiết kế cho Task 4 (Đã hoàn thành)
**Status**: Completed and verified through integration tests and local runtime smoke validation.

Neo4j Ingestion trong Task 4 đã được thiết kế và kiểm chứng đầy đủ với các kịch bản thực tế:
- **Ingestion Idempotency**: Neo4j ingestion is replay-safe for the verified scenarios through stable IDs, uniqueness constraints, MERGE, generation-aware tombstones, and idempotent delete handling. Chúng tôi không cam kết transaction phân tán exactly-once chéo hệ thống.
- **Ordering & Placeholders**: Kafka ordering is per partition within one topic. Không có thứ tự chéo topic. Edge events có thể đến trước node events, do đó placeholder handling là bắt buộc và đã được xử lý.
- **Mixed-Batch Rollback Limitation**: Các record trong một Neo4j connector batch chia sẻ chung một giao dịch. Một record lỗi (ví dụ: endpoint mismatch) sẽ rollback toàn bộ giao dịch của batch đó. Bản ghi lỗi đi vào DLQ, còn các bản ghi hợp lệ trong batch bị rollback đó yêu cầu phải replay/retry để nạp lại.
- **Cross-Generation Limitation**: Tombstone ngăn chặn stale replay cho cùng một thế hệ sự kiện (same generation). Tuy nhiên, thiết kế không xây dựng một thứ tự monotonic toàn cục giữa các thế hệ độc lập; thứ tự đến chéo thế hệ vẫn có thể ảnh hưởng đến kết quả cuối cùng.
- **Order-tolerant ingestion (Đã giải quyết):**
  Cypher tự động tạo placeholder nodes khi `EDGE_UPSERT` đến trước một hoặc cả hai endpoint nodes mà không bị lỗi.
- **Idempotent upsert và delete (Đã giải quyết):**
  Cypher sử dụng `MERGE` kết hợp uniqueness constraints đảm bảo replay không tạo trùng lặp hay duplicate nodes/edges/tombstones.
- **Stale-event protection (Đã giải quyết):**
  Đã triển khai hệ thống Tombstones lưu trữ `generation_id` dạng `file_id:content_hash:parser_version:schema_version`. `EDGE_UPSERT` và `NODE_UPSERT` bị chặn nếu tombstone cùng thế hệ tồn tại.
- **Delete race handling (Đã giải quyết):**
  `EDGE_DELETE` luôn tạo `CPGEdgeTombstone` dựa trên event fields bất kể relationship có tồn tại hay không, chặn hoàn toàn stale `EDGE_UPSERT` đến sau.
- **Kafka Connect DLQ (Đã giải quyết):**
  Cấu hình `connector.errors` làm DLQ và kiểm chứng live bằng lỗi endpoint mismatch. Các bản ghi hợp lệ trong batch bị ảnh hưởng bởi rollback giao dịch sẽ được ghi thành công sau khi gửi độc lập (replay/retry), còn record lỗi được đẩy vào DLQ thành công.
