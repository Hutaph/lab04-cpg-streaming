# Kế hoạch triển khai và kiểm thử Lab 04

Tài liệu này mô tả trạng thái triển khai hiện tại của các task trong Lab 04 và các quality gates dùng để xác minh hệ thống. Đây là tài liệu kỹ thuật nội bộ, còn narrative kết quả nằm trong Jupyter Book.

## Trạng thái hiện tại

| Task | Trạng thái | Evidence chính |
|---|---|---|
| Task 1 | Verified | Repository discovery, manifest và exact path-set assertions |
| Task 2 | Verified locally | Parser smoke run, schema validation, stable IDs, incremental skip |
| Task 3 | Verified with Kafka | Topic layout, Kafka key, schema validation, fresh publish, skip, parser error routing |
| Task 4 | Verified with Kafka Connect/Neo4j | Connector state, lag 0, graph integrity, DLQ delta, replay-safe checks |
| Task 5 | Hardening complete; runtime pending MongoDB metadata and Neo4j regression gates | Spark Structured Streaming metadata ingestion và checkpoint evidence |
| Task 6 | Hardening complete; runtime pending MongoDB metadata and Neo4j regression gates | Modified-file replay và duplicate checks |

## Task 1 — Repository discovery

Mục tiêu:

- clone hoặc tái sử dụng `huggingface/transformers-pr-agent`;
- ghi nhận commit `458c957fa1e8851825cd799f5d030876f0644194`;
- enumerate Python files từ repository root;
- áp dụng `config/file_filters.yaml`;
- ghi manifest ở `artifacts/manifests/source-files.jsonl`.

Evidence mong đợi:

- Raw Python discovery records: 4.496.
- Eligible parser inputs: 2.963.
- Manifest paths deterministic, sorted, POSIX relative và không duplicate.
- Eligible paths trong manifest khớp exact set với discovery service.

## Task 2 — Parser Service

Mục tiêu:

- đọc eligible manifest;
- xử lý từng file độc lập;
- parse bằng Python `ast`;
- sinh AST, CFG, DFG và CALLS;
- tạo stable IDs;
- validate event schema;
- hỗ trợ JSONL dry-run và Kafka publish mode;
- dùng SQLite state để skip file không đổi.

Evidence mong đợi:

- Smoke sample tạo node, edge và metadata events hợp lệ.
- Stable IDs và content hashes deterministic qua rerun.
- File không đổi được skip.
- Syntax error được route thành parser error event.

## Task 3 — Kafka

Mục tiêu:

- provision Kafka topics;
- publish Parser Service events theo contract;
- dùng `file_id` làm Kafka key;
- kiểm chứng routing, partition consistency và schema;
- phân biệt parser business errors với Kafka Connect DLQ.

Topic contract:

| Topic | Vai trò |
|---|---|
| `cpg.nodes` | Node upsert/delete events |
| `cpg.edges` | Edge upsert/delete events |
| `source.metadata` | File metadata events |
| `parser.errors` | Parser Service business errors |
| `connector.errors` | Kafka Connect dead-letter records |

Evidence mong đợi:

- Topics tồn tại với partition count đúng.
- Fresh publish của smoke file tạo graph và metadata events.
- Unchanged rerun không tạo batch mới.
- Syntax error tạo `parser.errors` và không tạo graph events.

## Task 4 — Neo4j

Mục tiêu:

- deploy `neo4j-nodes-sink` và `neo4j-edges-sink`;
- ghi graph trực tiếp từ Kafka Connect vào Neo4j;
- dùng constraints, `MERGE`, placeholders và tombstones;
- route connector failures vào `connector.errors`;
- kiểm chứng replay không tạo duplicate trong scenario đã chạy.

Evidence mong đợi:

- Connectors và tasks ở trạng thái `RUNNING`.
- Consumer lag trở về 0.
- Node/edge counts khớp file smoke.
- Duplicate nodes/edges = 0.
- Required null properties = 0.
- Unresolved placeholders = 0.
- Valid-run DLQ delta = 0.

## Task 5 — Spark/MongoDB

Mục tiêu:

- consume `source.metadata` bằng Spark Structured Streaming;
- dùng checkpoint để quản lý Kafka offsets;
- ghi MongoDB bằng replace/upsert theo `file_id`;
- giữ metadata path độc lập với Neo4j graph path.
- chạy MongoDB preflight trước khi tạo evidence live;

Evidence mong đợi:

- Spark job đọc đúng topic metadata.
- MongoDB có document metadata tương ứng.
- Checkpoint được tạo và dùng lại khi rerun.

## Task 6 — Replay verification

Mục tiêu:

- sửa một file nguồn trong môi trường kiểm chứng;
- chạy replay để sinh DELETE/UPSERT/metadata events;
- xác minh Neo4j và MongoDB không tạo duplicate;
- kiểm tra state store và downstream outputs sau replay.
- runtime evidence phải được query từ các service thật, không dùng hard-code.

Evidence mong đợi:

- Content hash thay đổi được phát hiện.
- Graph diff sinh events phù hợp.
- Re-run cùng nội dung dẫn đến skip hoặc no-op đúng thiết kế.
- Duplicate checks trên storage trả về 0 trong kịch bản đã kiểm chứng.

## Quality gates

Static checks:

```bash
git diff --check
uv lock --check
uv run python -m compileall -q src scripts spark_jobs
uv run ruff check src tests scripts spark_jobs
uv run ruff format --check src tests scripts spark_jobs
MYPYPATH=src uv run mypy --explicit-package-bases src
```

Unit tests:

```bash
PYTHONPATH=src uv run pytest tests/unit -q
```

Integration tests cần runtime Docker tương ứng:

```bash
PYTHONPATH=src uv run pytest tests/integration -v
```

Notebook và book checks:

```bash
python -m json.tool lab04-book/task1_clone_explore.ipynb >/dev/null
python -m json.tool lab04-book/task2_parser_service.ipynb >/dev/null
python -m json.tool lab04-book/task3_kafka_topics.ipynb >/dev/null
python -m json.tool lab04-book/task4_neo4j_sink.ipynb >/dev/null
python -m json.tool lab04-book/architecture_diagram.ipynb >/dev/null
npx mystmd build --html --force
```

## Ranh giới báo cáo

Các notebook trình bày runtime evidence tương ứng từng task. Không suy rộng smoke run thành full Kafka publish nếu chưa có evidence full run. Hệ thống không tuyên bố transaction phân tán toàn trình; replay safety dựa trên stable IDs, state, upsert, checkpoint, constraints và tombstones trong các kịch bản đã kiểm chứng.
