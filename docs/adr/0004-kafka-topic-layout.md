# Architectural Decision Record: 0004-kafka-topic-layout

## Title
Phân chia luồng sự kiện đồ thị và lỗi thành các Topic Kafka độc lập.

## Status
Accepted

## Context
Parser Service phát ra nhiều loại thông tin khác nhau bao gồm cấu trúc đồ thị (node, edge), thông tin thống kê (metadata) và các lỗi cú pháp. Nếu dồn chung toàn bộ vào một topic duy nhất:
1. Các consumer (Neo4j Sink, Spark Streaming) sẽ phải lọc bỏ các tin nhắn không thuộc phạm vi quan tâm của mình, gây lãng phí băng thông và CPU.
2. Khó khăn trong việc thiết lập cấu hình phân vùng (partition keys) tối ưu cho từng loại dữ liệu.

## Decision
Thiết kế hệ thống 5 topics Kafka rạch ròi:
- `cpg.nodes`: Chỉ chứa các sự kiện định nghĩa node đồ thị. Consumer là Neo4j Sink.
- `cpg.edges`: Chỉ chứa các quan hệ cạnh đồ thị. Consumer là Neo4j Sink.
- `source.metadata`: Chứa metadata thống kê của file. Consumer là Spark Job.
- `parser.errors`: Chứa thông tin file lỗi cú pháp.
- `connector.errors`: Dead letter queue cho lỗi ghi nhận của Neo4j connector.

## Alternatives Considered
- *Single Topic*: Dùng chung một topic `cpg.events` và phân loại bằng trường `event_type`. Gây coupling lớn giữa các hệ thống tiêu thụ.

## Consequences
- Tối ưu hóa hiệu năng ghi nhận. Neo4j Connect Sink chỉ subcribe đúng 2 topic đồ thị, Spark subcribe đúng topic metadata.
- Dễ dàng quản lý và giám sát lỗi thông qua các topic errors chuyên dụng.

## Risks
- Tăng chi phí quản lý và duy trì metadata của Kafka Cluster khi số lượng topic tăng lên. Tuy nhiên với quy mô Lab 04 thì rủi ro này không đáng kể.
