# Danh sách tự kiểm tra trước khi nộp bài (Submission Checklist)

Bảng dưới đây liệt kê tất cả các tiêu chí chấm điểm và yêu cầu bắt buộc của Lab 04 để nhóm tự rà soát trước khi gửi link nộp bài trên Moodle.

---

## 1. Yêu cầu định dạng nộp bài
- [ ] Link nộp bài trên Moodle là duy nhất một URL trỏ đến Jupyter Book đã xuất bản trên GitHub Pages.
- [ ] KHÔNG nộp tệp nén ZIP, tệp tài liệu Word hay PDF export.
- [ ] Repository chứa source Jupyter Book là repository public trên GitHub.
- [ ] Source code đầy đủ của dự án nằm trong cùng repository đó để giảng viên đối chiếu.

---

## 2. Nhật ký Git và Commit
- [ ] Lịch sử commit thể hiện tiến độ tăng dần trong suốt thời gian làm lab (Commit messages có ý nghĩa, ví dụ: `[Task 1] ...`, `[Task 2] ...`).
- [ ] Không sử dụng một commit duy nhất dạng "final commit" cho toàn bộ đồ án.

---

## 3. Nội dung báo cáo Jupyter Book
- [ ] **Chapter 1 (Task 1: Clone & Khám phá)**:
  - [ ] Giải thích cách tiếp cận clone và lý do.
  - [ ] Liệt kê cây thư mục và đếm chính xác số lượng file Python trong repo transformers-pr-agent.
  - [ ] Chạy thực tế và hiển thị kết quả (Executed cells).
  - [ ] Có phần Reflection ở cuối chapter.
- [ ] **Chapter 2 (Task 2: Parser Service)**:
  - [ ] Giải thích logic trích xuất AST, CFG, DFG, Call graph bằng Python `ast`.
  - [ ] Hiển thị mẫu JSONL output của 4 loại event.
  - [ ] Chứng minh parser chạy trong giới hạn bộ nhớ ổn định.
  - [ ] Có phần Reflection ở cuối chapter.
- [ ] **Chapter 3 (Task 3: Kafka Topics)**:
  - [ ] Giải thích thiết kế 4 topics chính và 1 topic phụ.
  - [ ] Hiển thị các tệp cấu hình JSON Schema của các event.
  - [ ] Có phần Reflection ở cuối chapter.
- [ ] **Chapter 4 (Task 4: Neo4j Ingestion)**:
  - [ ] Giải thích thiết kế sink connector và các Cypher query MERGE tương ứng.
  - [ ] Đính kèm screenshot giao diện Neo4j Browser hiển thị các nút và quan hệ đồ thị CPG.
  - [ ] Có phần Reflection ở cuối chapter.
- [ ] **Chapter 5 (Task 5: Spark MongoDB)**:
  - [ ] Giải thích cách cấu hình Spark Structured Streaming và MongoDB connector.
  - [ ] Chứng minh cơ chế checkpoint hoạt động (ghi nhận offset).
  - [ ] Đính kèm screenshot giao diện MongoDB Compass hoặc CLI hiển thị các tài liệu metadata.
  - [ ] Có phần Reflection ở cuối chapter.
- [ ] **Chapter 6 (Task 6: Idempotent Replay)**:
  - [ ] Giải thích quy trình replay khi sửa đổi file.
  - [ ] Đưa ra bằng chứng truy vấn database đếm số node/edge không bị nhân bản sau khi chạy lại.
  - [ ] Có phần Reflection ở cuối chapter.
- [ ] **Sơ đồ kiến trúc**: Tích hợp sơ đồ luồng tổng thể rõ ràng (đã vẽ bằng Mermaid).
- [ ] **Hướng dẫn cài đặt**: Tài liệu hướng dẫn cài đặt môi trường chạy local chi tiết từng bước.

---

## 4. Tài liệu kỹ thuật đi kèm trong repo
- [ ] Tài liệu kiến trúc hệ thống (`docs/system_architecture.md`).
- [ ] Tài liệu cấu trúc thư mục (`docs/project_structure.md`).
- [ ] Kế hoạch triển khai chi tiết (`docs/implementation_plan.md`).
- [ ] Ma trận truy vết (`docs/traceability_matrix.md`).
- [ ] Tài liệu ánh xạ refactor (`docs/refactor_mapping.md`).
- [ ] Tài liệu chiến lược kiểm thử (`docs/testing_strategy.md`).
- [ ] Đầy đủ 7 Architectural Decision Records (ADRs) trong thư mục `docs/adr/`.
