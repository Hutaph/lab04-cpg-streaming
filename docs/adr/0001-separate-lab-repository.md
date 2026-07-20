# Architectural Decision Record: 0001-separate-lab-repository

## Title
Sử dụng Repository độc lập cho Đồ án và nhận Source Repository làm đầu vào Runtime.

## Status
Accepted

## Context
Lab 04 yêu cầu trích xuất CPG từ repository mục tiêu `huggingface/transformers-pr-agent`. Có hai cách tiếp cận để tổ chức mã nguồn:
1. Phát triển trực tiếp các script parser bên trong repository transformers-pr-agent.
2. Xây dựng một repository riêng biệt cho Đồ án (lab04-cpg-streaming), clone transformers-pr-agent như là một thư mục workspace đầu vào và ignore nó khỏi Git của đồ án.

## Decision
Lựa chọn phương án 2. Repository đồ án sẽ hoàn toàn độc lập, chứa cấu trúc scaffold, hạ tầng Docker Compose, các Spark job và tài liệu Jupyter Book. Repository transformers-pr-agent sẽ được clone tự động vào thư mục `workspace/source/` tại runtime và được cấu hình trong gitignore.

## Alternatives Considered
- *Phương án 1 (Gộp chung)*: Khiến mã nguồn đồ án bị trộn lẫn với mã nguồn của HuggingFace, khó quản lý lịch sử commit của sinh viên và tăng kích thước commit không cần thiết.

## Consequences
- Giữ lịch sử Git của đồ án sạch sẽ, chỉ tập trung vào mã nguồn pipeline và tài liệu báo cáo.
- Dễ dàng publish Jupyter Book lên GitHub Pages trực tiếp từ repo đồ án.
- Cho phép chạy pipeline trên các repository Python khác chỉ bằng cách thay đổi URL cấu hình trong file `.env` hoặc `application.yaml`.

## Risks
- Sinh viên có thể vô tình commit code của transformers-pr-agent nếu cấu hình gitignore bị sai lệch.
