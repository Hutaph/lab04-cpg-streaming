# Architectural Decision Record: 0005-neo4j-edge-model

## Title
Sử dụng một loại Relationship Type duy nhất `CPG_EDGE` trong Neo4j và lưu trữ loại quan hệ trong thuộc tính.

## Status
Accepted

## Context
Code Property Graph tích hợp nhiều loại cạnh khác nhau: AST child link, CFG next transition, DFG reaches definition và CALLS target link. Trong cơ sở dữ liệu đồ thị Neo4j, các cạnh (relationships) có thể được lưu trữ theo hai cách:
1. Mỗi loại quan hệ là một nhãn quan hệ riêng biệt: `(a)-[:AST_CHILD]->(b)`, `(a)-[:CFG_NEXT]->(b)`...
2. Sử dụng chung một nhãn quan hệ `CPG_EDGE` và lưu loại quan hệ cụ thể trong thuộc tính của cạnh: `(a)-[:CPG_EDGE {edge_type: "AST_CHILD"}]->(b)`.

## Decision
Lựa chọn phương án 2: Sử dụng một loại relationship type duy nhất `CPG_EDGE` và phân biệt bằng property `edge_type`.

## Alternatives Considered
- *Phương án 1 (Nhãn riêng biệt)*: Tốt cho việc viết các câu truy vấn Cypher ngắn gọn, nhưng gây khó khăn lớn cho cấu hình Neo4j Kafka Connect Sink. Connector sẽ phải đăng ký nhiều luồng xử lý hoặc viết các Cypher script phức tạp để sinh động nhãn quan hệ dựa trên trường dữ liệu động của Kafka event (Neo4j Connect Sink hạn chế trong việc tạo động tên nhãn quan hệ trực tiếp từ value JSON mà không dùng APOC extension).

## Consequences
- Cấu hình của Neo4j Kafka Connect Sink cực kỳ đơn giản và đồng nhất:
  ```cypher
  MATCH (source:CodeNode {node_id: event.source_id}), (target:CodeNode {node_id: event.target_id})
  MERGE (source)-[r:CPG_EDGE {edge_id: event.edge_id}]->(target)
  SET r.edge_type = event.edge_type, r.properties = event.properties
  ```
- Dễ dàng tạo index chung trên thuộc tính `edge_id` của quan hệ để phục vụ việc xóa hoặc cập nhật cạnh nhanh chóng.

## Risks
- Truy vấn Cypher tìm đường đi theo một loại cạnh cụ thể (ví dụ chỉ đi theo cạnh CFG) sẽ chậm hơn một chút vì Neo4j phải quét và lọc thuộc tính của cạnh thay vì chỉ đi theo nhãn loại cạnh. Tuy nhiên với quy mô dữ liệu đồ thị của một repository đơn lẻ, sự chênh lệch này là không đáng kể.
