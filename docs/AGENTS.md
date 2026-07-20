# CODEX.md

Hướng dẫn ngữ cảnh cho Codex khi làm việc trong repo này.

## Bối cảnh dự án

Đây là đồ án môn Nhập môn Dữ liệu lớn (Lab 04 — Spark Streaming), đề bài gốc thiết kế cho team 4 người nhưng **thực hiện solo bởi 1 sinh viên**, đóng vai trò "Thành viên A — Data Engineer chính". Xem chi tiết đầy đủ lộ trình và khái niệm ở file `plan.md` cùng thư mục — **luôn đọc file đó trước khi bắt đầu bất kỳ task nào**.

Mục tiêu: xây một pipeline streaming tăng dần (incremental) trích xuất Code Property Graph (CPG) từ 1 repository Python, đẩy qua Kafka, rồi ingest song song vào Neo4j (qua Kafka Connect Sink) và MongoDB (qua Spark Structured Streaming).

**Repo mục tiêu đã chốt:** `huggingface/transformers-pr-agent` (https://github.com/huggingface/transformers-pr-agent) — clone (shallow) vào `transformers-pr-agent/`, dùng làm input thật cho Parser Service. Xem `plan.md` mục 0.1 cho chiến lược triển khai đã chốt.

## Phạm vi trách nhiệm trong repo này

Ưu tiên các phần sau (tương ứng Task 2, 3, 4, 5 trong `plan.md`):
1. Parser Service (Python, dùng `ast` module) — sinh AST/CFG/DFG/Call events
2. Kafka topic schema design (4 topics: node, edge, metadata, error)
3. Neo4j ingestion qua Kafka Connect Sink connector
4. Spark Structured Streaming job → MongoDB

Task 1 (clone repo), Task 6 (verification), Architecture diagram, Jupyter Book vẫn cần hoàn thành nhưng ít ưu tiên về độ sâu kỹ thuật.

## Nguyên tắc làm việc quan trọng — LUÔN TUÂN THỦ

1. **Giải thích trước khi code.** Người dùng đang học các khái niệm này lần đầu. Trước khi viết bất kỳ đoạn code nào liên quan tới khái niệm mới (Kafka, Spark Structured Streaming, Neo4j Cypher, ast module...), tóm tắt ngắn gọn khái niệm đó bằng ngôn ngữ đơn giản trước, sau đó mới code.
2. **Không tự ý đổi công cụ đã chốt.** Dùng **`ast` module chuẩn của Python**, không đề xuất chuyển sang Joern hoặc tree-sitter trừ khi người dùng chủ động hỏi lại.
3. **Không tự ý đổi schema Kafka đã chốt** trong quá trình code — nếu thấy cần sửa field, hỏi lại người dùng trước, giải thích lý do.
4. **Ưu tiên chạy được ở quy mô nhỏ** hơn là tối ưu hiệu năng — đây là đồ án học thuật, không phải hệ thống production. Không thêm phức tạp không cần thiết (ví dụ: không cần Kafka multi-broker cluster, 1 broker là đủ).
5. **Idempotency là yêu cầu bắt buộc xuyên suốt**, không phải optional — mọi node/edge phải có stable ID (hash-based), mọi write vào Neo4j dùng `MERGE`, mọi Spark writeStream phải có `checkpointLocation`.
6. **Comment code song ngữ hoặc tiếng Việt là được** — người dùng ưu tiên hiểu rõ hơn là code "chuẩn enterprise".
7. **Luôn trả lời tôi bằng Tiếng Việt**: Dù người dùng có prompt bằng Tiếng anh hay Tiếng việt 
8. **Quy tắc bổ sung cho AI Agent khi Refactor & Triển khai**:
   - Không xóa code prototype nếu chưa có migration plan.
   - Không refactor nhiều layer trong một commit.
   - Viết test characterization trước khi di chuyển logic cũ.
   - Không thay đổi event schema âm thầm.
   - Không thay đổi stable ID algorithm mà không có ADR.
   - Không xóa Spark checkpoint để làm test pass.
   - Không dùng random UUID cho persistent identity.
   - Không dùng Spark cho nhánh Neo4j (Neo4j nạp trực tiếp qua Kafka Connect Sink).
   - Không ghi metadata trực tiếp từ parser vào MongoDB.
   - Không ghi node/edge trực tiếp từ parser vào Neo4j.
   - Không sử dụng smoke results làm final results.
   - Mọi thay đổi kiến trúc bắt buộc phải cập nhật tài liệu docs và ADR.

## Ngăn xếp công nghệ cố định (không đề xuất thay thế)

| Thành phần | Công nghệ |
|---|---|
| Parsing | Python `ast` module |
| Message broker | Apache Kafka (KRaft mode, không cần Zookeeper nếu version hỗ trợ) |
| Kafka → Neo4j | Neo4j Kafka Connector Sink (qua Kafka Connect) |
| Kafka → MongoDB | Apache Spark Structured Streaming + MongoDB Spark Connector |
| Graph DB | Neo4j |
| Document DB | MongoDB |
| Hạ tầng | Docker Compose |
| Submission | Jupyter Book (published qua GitHub Pages) |

## Cấu trúc thư mục dự kiến

```
.
├── README.md                  # cách chạy Jupyter Book
├── docs/                      # đề bài, kế hoạch, ghi chú triển khai
├── lab04-book/                # source Jupyter Book
├── scripts/
│   ├── explore_repo.py        # script khảo sát repo mục tiêu
│   └── parser-service/
│       ├── parser.py          # entrypoint Parser Service
│       ├── cpg_parser.py      # logic sinh AST/CFG/DFG/Call event
│       ├── event_writer.py    # ghi JSONL hoặc publish Kafka
│       ├── stable_id.py       # hàm hash sinh ID ổn định
│       ├── topics.py          # topic mapping
│       └── schemas/           # JSON schema mẫu cho 4 loại event
├── outputs/                   # output demo/local, không commit
└── transformers-pr-agent/      # repo mục tiêu để phân tích
```

Điều chỉnh cấu trúc này nếu người dùng đã có layout khác — không ép buộc đổi lại nếu không cần thiết.

## Quy ước đặt tên & thiết kế

- **Topic Kafka:** `code.events.nodes`, `code.events.edges`, `code.events.metadata`, `code.events.errors` (đổi tên nếu người dùng đã quyết định khác trong `plan.md`/schema files — luôn kiểm tra `scripts/scripts/scripts/parser-service/schemas/` trước khi giả định tên topic).
- **Stable ID:** `sha256(file_path + node_type + line_start + line_end + node_signature)`, cắt ngắn 16-24 ký tự hex nếu cần độ dài gọn.
- **Schema versioning field:** luôn tên `schema_version`, giá trị dạng string `"v1"`, `"v2"`...
- **Timestamp field:** luôn tên `event_timestamp`, ISO 8601 UTC.
- **Commit message:** tiếng Việt hoặc Anh đều được, nhưng phải mô tả rõ theo dạng `[Task N] Mô tả ngắn` để phản ánh tiến độ theo từng task khi chấm bài (đề bài yêu cầu commit thể hiện tiến độ tăng dần, không phải 1 commit "final").

## Cách chạy môi trường (cập nhật khi docker-compose.yml đã có)

```bash
# Dựng toàn bộ hạ tầng
docker compose up -d

# Xem log 1 service cụ thể
docker compose logs -f kafka

# Dọn sạch để test lại Task 6 từ đầu (xóa toàn bộ volume/dữ liệu)
docker compose down -v

# Kiểm tra message trong 1 topic (đổi tên topic tương ứng)
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic code.events.nodes --from-beginning
```

## Trạng thái tiến độ hiện tại

> Cập nhật mục này thủ công sau mỗi phiên làm việc để Codex (và bạn) nắm được đang ở đâu.
>
> **Chiến lược (2026-07-06):** đã học xong khái niệm → chuyển sang thực hành. Dựng `docker-compose.yml` **đầy đủ**, chạy thật toàn bộ pipeline trên repo `huggingface/transformers-pr-agent` ở **local** trước; chạy thông mới bàn giao Task 6 / diagram / Jupyter Book cho thành viên khác. Chi tiết ở `plan.md` mục 0.1.

- [ ] Task 3 — Kafka schema design
- [ ] Task 1 — Clone repo mẫu + liệt kê file
- [ ] Docker Compose hạ tầng tối thiểu (Kafka)
- [ ] Task 2 — Parser Service (logic AST/CFG/DFG/Call)
- [ ] Task 2 — Nối Parser Service vào Kafka thật
- [ ] Docker Compose mở rộng (Neo4j, Kafka Connect, MongoDB, Spark)
- [ ] Task 4 — Neo4j Sink Connector
- [ ] Task 5 — Spark Structured Streaming job
- [ ] Task 6 — Verification idempotent replay
- [ ] Architecture diagram
- [ ] Jupyter Book + publish GitHub Pages

## Khi không chắc chắn

Nếu một yêu cầu code có thể ảnh hưởng tới thiết kế đã chốt (schema, tên topic, cấu trúc ID), **hỏi lại người dùng trước khi sửa**, đừng tự quyết định âm thầm — người dùng cần hiểu rõ lý do đằng sau mỗi thay đổi để có thể giải thích trong Jupyter Book (yêu cầu bắt buộc của đề bài).
