# Architectural Decision Record: 0002-python-ast-parser

## Title
Sử dụng module chuẩn `ast` của Python cho bộ phân tích cú pháp CPG ban đầu.

## Status
Accepted

## Context
Để xây dựng Code Property Graph, chúng ta cần phân tích cú pháp mã nguồn Python để trích xuất AST, CFG, DFG và Call graph. Các công cụ hiện có bao gồm Joern (chuyên dụng cho CPG nhưng cài đặt phức tạp và viết bằng Scala/Java), tree-sitter (đa ngôn ngữ, tốc độ cao nhưng đòi hỏi compile bindings C/C++) và module chuẩn `ast` của Python.

## Decision
Sử dụng module `ast` tích hợp sẵn trong thư viện chuẩn của Python để xây dựng parser service.

## Alternatives Considered
- *Joern*: Cung cấp giải pháp trọn gói cho CPG nhưng khó nhúng trực tiếp vào một pipeline streaming Python tùy biến gọn nhẹ ở local, đòi hỏi tài nguyên hệ thống lớn.
- *Tree-sitter*: Tốt cho phân tích cú pháp đa ngôn ngữ nhưng việc viết các luật duyệt ngữ nghĩa bằng Python phức tạp hơn so với sử dụng trực tiếp module `ast` của chính Python.

## Consequences
- Loại bỏ hoàn toàn sự phụ thuộc vào các công cụ bên ngoài, giúp ứng dụng parser khởi động nhanh và chạy được trên mọi môi trường có cài đặt Python.
- Tận dụng trực tiếp các class node của Python `ast` (FunctionDef, ClassDef, Call...) giúp code phân tích trực quan và dễ hiểu đối với sinh viên.

## Risks
- Module `ast` chỉ hỗ trợ Python, nếu đề bài mở rộng ra các ngôn ngữ khác (như Java, Go) trong tương lai thì bắt buộc phải viết lại bộ parser mới.
