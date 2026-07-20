# Documentation Guide

Chào mừng bạn đến với tài liệu kỹ thuật của dự án Lab 04 — Incremental Code Property Graph Streaming. Thư mục này lưu trữ toàn bộ các ghi chú thiết kế, kế hoạch và tài liệu kiểm thử của hệ thống.

---

## 1. Bắt đầu từ đâu?

| Tôi muốn... | Hãy đọc |
|---|---|
| Hiểu nhanh dự án & cách chạy | [../README.md](../README.md) |
| Hiểu kiến trúc hệ thống | [architecture/system_architecture.md](architecture/system_architecture.md) |
| Hiểu cấu trúc mã nguồn & quy tắc import | [architecture/project_structure.md](architecture/project_structure.md) |
| Biết kế hoạch phát triển các bước tiếp theo | [planning/implementation_plan.md](planning/implementation_plan.md) |
| Theo dõi tiến độ & trạng thái các Task | [planning/traceability_matrix.md](planning/traceability_matrix.md) |
| Đọc chiến lược kiểm thử & chạy test suite | [quality/testing_strategy.md](quality/testing_strategy.md) |
| Kiểm tra các yêu cầu bắt buộc trước khi nộp | [quality/submission_checklist.md](quality/submission_checklist.md) |
| Xem các quyết định thiết kế cốt lõi (ADR) | [architecture/adr/](architecture/adr/) |
| Tìm hiểu lịch sử Clean Rewrite Task 1 & 2 | [archive/clean_rewrite_migration.md](archive/clean_rewrite_migration.md) |
| Đọc báo cáo thực hành chính thức (Jupyter Book) | [../lab04-book/](../lab04-book/) |

---

## 2. Thứ tự đọc khuyến nghị (Reading Order)

Để nhanh chóng nắm bắt dự án, một thành viên mới nên đọc tài liệu theo thứ tự sau:
1. **[../README.md](../README.md)**: Nắm tổng quan nhanh, các lệnh khởi chạy cơ bản.
2. **[docs/README.md](README.md)** (Trang này): Bản đồ điều hướng tài liệu.
3. **[architecture/system_architecture.md](architecture/system_architecture.md)**: Hiểu kiến trúc kỹ thuật end-to-end, luồng sự kiện và thiết kế database.
4. **[planning/implementation_plan.md](planning/implementation_plan.md)**: Xem kế hoạch phân chia giai đoạn và các phần việc tiếp theo.
5. **[planning/traceability_matrix.md](planning/traceability_matrix.md)**: Đối chiếu tiến độ hoàn thành so với yêu cầu đề bài.
6. **[../lab04-book/](../lab04-book/)**: Đọc báo cáo chi tiết và xem các minh họa/kết quả thực thi thực tế.

---

## 3. Vai trò của từng tài liệu (Documentation Ownership)

Tài liệu trong dự án được phân chia trách nhiệm rõ ràng để tránh trùng lặp:
- **README ở root**: Đóng vai trò giới thiệu dự án cực kỳ ngắn gọn, hướng dẫn khởi chạy nhanh và điều hướng.
- **Architecture Documents** (`docs/architecture/`): Mô tả hệ thống được thiết kế thế nào, các module giao tiếp ra sao, và lưu trữ các Quyết định Kiến trúc (ADRs).
- **Planning Documents** (`docs/planning/`): Theo dõi những gì cần làm tiếp theo, xác định tiêu chí hoàn thành (DoD) và tiến độ các Task.
- **Quality Documents** (`docs/quality/`): Hướng dẫn chi tiết chiến lược kiểm thử (Unit, Integration, E2E) và danh sách tự kiểm tra định dạng nộp bài.
- **Archive** (`docs/archive/`): Chứa các báo cáo kiểm toán cũ và tóm tắt lịch sử rewrite code. Tuyệt đối không dùng làm tài liệu hướng dẫn triển khai hiện hành.
- **Jupyter Book** (`lab04-book/`): Báo cáo chính thức gửi cho Giảng viên. Tập trung trình bày kết quả thực thi thực tế, mã nguồn demo, ảnh chụp màn hình cơ sở dữ liệu và phần tự nhận xét (Reflections).

---

## 4. Quy tắc Nguồn sự thật (Source of Truth Rules)

- **Không sao chép nội dung dài** giữa các file. Một thông tin chỉ được định nghĩa tại một tệp canonical duy nhất. Các tệp khác nếu cần tham chiếu sẽ dùng relative links.
- **Event Schemas**: Nguồn sự thật duy nhất về cấu trúc sự kiện nằm ở thư mục [schemas/](../schemas/). Không mô tả chi tiết trường dữ liệu trong tài liệu Markdown.
- **Topic Layout**: Cấu hình topic thật nằm ở [config/topics.yaml](../config/topics.yaml).
- **Task Status**: Tiến độ Task chỉ cập nhật duy nhất ở [planning/traceability_matrix.md](planning/traceability_matrix.md) và tóm tắt ngắn ở Jupyter Book index.

---

## 5. Trạng thái dự án hiện hành (Current Project Status)

- **Task 1 (Clone & Khám phá)**: **Verified**. Discovery CLI chạy tốt, lọc được 2779 files.
- **Task 2 (Parser Service)**: **Verified locally**. Phân tích cú pháp AST/CFG/DFG/Call và sinh ID ổn định thành công.
- **Task 3 (Thiết kế Topic Kafka)**: **Not started / Scaffolded**.
- **Task 4 (Neo4j Ingestion)**: **Not started / Scaffolded**.
- **Task 5 (Spark Ingestion MongoDB)**: **Not started / Scaffolded**.
- **Task 6 (Idempotent Replay)**:
  - Local parser SQLite replay: **Implemented and verified**.
  - Full end-to-end replay: **Not started / Pending integration**.
