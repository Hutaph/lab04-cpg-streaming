##### ĐẠI HỌC QUỐC GIA THÀNH PHỐ HỒ CHÍ MINH

##### TRƯỜNG ĐẠI HỌC KHOA HỌC TỰ NHIÊN

# NHẬP MÔN DỮ LIỆU LỚN

```
Giảng viên
TS. Lê Ngọc Thành | lnthanh@fit.hcmus.edu.vn
TS. Nguyễn Ngọc Thảo | nnthao@fit.hcmus.edu.vn
```

```
Hướng dẫn thực hành
```

ThS. Huỳnh Lâm Hải Đăng | hlhdang@fit.hcmus.edu.vn

```
Trần Huy Bân | huyban.han@gmail.com
```

## Lab 04: Spark Streaming

### 1. Đề bài

Bài thực hành này tập trung vào việc xây dựng Code Property Graph (CPG) theo cách tăng dần, kết hợp với một pipeline ingest dữ liệu streaming theo thời gian thực. Mỗi nhóm sinh viên phải chọn một repository Python công khai từ một hoặc nhiều tổ chức GitHub được chỉ định, xây dựng CPG tăng dần từ các file mã nguồn Python của repository đó, và xây dựng một pipeline streaming để lưu trữ graph thu được cùng metadata của mã nguồn vào hai hệ cơ sở dữ liệu riêng biệt.

Việc chọn repository được quản lý trên Moodle của môn học thông qua hoạt động Choice. Mỗi repository có số lượng nhóm tối đa cố định được phép chọn. Khi quota đã đầy, repository đó sẽ không còn khả dụng cho các nhóm khác. Các nhóm phải hoàn tất việc chọn repository trước hạn được ghi trên Moodle. Không cho phép hai nhóm làm cùng một repository; việc chọn trễ có thể khiến số lượng repository còn lại bị hạn chế.

Sau khi việc chọn repository được xác nhận, mỗi nhóm clone repository và bắt đầu triển khai pipeline theo mô tả trong các phần bên dưới.

**Lưu ý:**
Bài thực hành này chỉ phục vụ mục đích giáo dục và minh họa kỹ thuật. Tài liệu này không khuyến khích, xác nhận, hay đề xuất việc thực hiện phân tích bảo mật, nhận diện lỗ hổng, hoặc thu thập tình báo về mối đe dọa an ninh mạng.

#### 1.1. Mục tiêu

- Hiểu cấu trúc của một Code Property Graph. Các thành phần liên quan gồm node của abstract syntax tree (AST), edge của control flow graph (CFG), edge của data flow graph (DFG), và call edge. Khi kết hợp lại, các thành phần này mô tả cấu trúc cú pháp và ngữ nghĩa của mã nguồn để phục vụ phân tích chương trình tĩnh.
- Triển khai một parser Python tăng dần, xử lý từng file mã nguồn một và phát ra các event có cấu trúc vào một cụm topic Apache Kafka.
- Thiết kế và triển khai bố cục topic Kafka mà pipeline sử dụng để truyền các event node của graph, event edge của graph, và event metadata của mã nguồn.
- Ingest topology của graph vào Neo4j bằng Neo4j Kafka Connector Sink, không dùng lớp Spark trung gian.
- Ingest metadata của mã nguồn vào MongoDB bằng Apache Spark Structured Streaming với MongoDB Spark Connector.
- Chứng minh pipeline replay có tính idempotent bằng cách xử lý lại ít nhất một file Python đã chỉnh sửa và xác minh rằng Neo4j cùng MongoDB phản ánh trạng thái đã cập nhật mà không tạo dữ liệu trùng lặp.

#### 1.2. Clone Repository và Khám Phá File

Dùng git để shallow-clone repository được phân công nhằm giảm kích thước tải xuống. Clone URL sẽ theo mẫu chuẩn `https://github.com/<assigned-org>/<assigned-repo>.git`. Sau khi clone, liệt kê toàn bộ file nguồn `.py` trong cây thư mục của repository. Việc loại trừ file test, file setup, và file tự sinh là tùy chọn nhưng được khuyến nghị. Ghi lại tổng số file Python tìm được trong báo cáo thực hành.


#### 1.3. Parser Service CPG Tăng Dần

Xây dựng một service Python tên là Parser Service, xử lý từng file nguồn Python một thay vì xử lý toàn bộ repository trong một batch duy nhất. Service này sử dụng một thư viện CPG và AST parsing phía Python do nhóm chọn, trong các lựa chọn Joern, tree-sitter, hoặc module chuẩn `ast`, để trích xuất các AST node, CFG edge, DFG edge, và call edge tạo nên CPG. Sau đó, từng loại dữ liệu được phát ra dưới dạng event message có cấu trúc vào một topic Apache Kafka. Service nên hoạt động trong giới hạn bộ nhớ hợp lý và phải gán định danh ổn định cho mọi phần tử được phát ra để việc xử lý lại cùng một nội dung không tạo dữ liệu trùng lặp ở các hệ thống phía sau.

#### 1.4. Thiết Kế Topic Kafka

Thiết kế bố cục topic Kafka mà pipeline dùng để truyền bốn nhóm event do Parser Service phát ra. Với Apache Kafka đóng vai trò message broker và bề mặt quản lý topic, bố cục này phải bao gồm các topic riêng cho node event, edge event, source metadata event, và parser error event. Mỗi message phải có field phiên bản schema để hỗ trợ tương thích trong tương lai và timestamp biểu thị thời điểm event.

#### 1.5. Ingest Topology Graph vào Neo4j

Kết nối Neo4j Kafka Connector Sink với các topic chứa node event và edge event để topology của graph được ghi trực tiếp từ Kafka vào Neo4j, không qua lớp Spark trung gian. Logic ingest phải có tính idempotent để việc xử lý lại cùng một node hoặc edge không tạo bản ghi trùng lặp.

#### 1.6. Ingest Metadata Mã Nguồn vào MongoDB

Xây dựng một job Apache Spark Structured Streaming để consume metadata event từ Kafka và ghi chúng vào một collection MongoDB thông qua MongoDB Spark Connector. Job phải sử dụng checkpoint location để có thể tiếp tục từ offset đã xử lý cuối cùng khi khởi động lại.

#### 1.7. Xác Minh Replay Idempotent

Chỉnh sửa một file nguồn Python trong repository đã clone và xử lý lại riêng file đó thông qua Parser Service. Xác minh rằng số lượng node và edge trong Neo4j phản ánh bản cập nhật mà không tạo node trùng lặp, collection MongoDB chứa document metadata đã cập nhật cho file đó, và checkpoint của Apache Spark Structured Streaming bỏ qua đúng các offset đã xử lý đối với mọi file không thay đổi.

### 2. Hướng Dẫn Nộp Bài

Mỗi nhóm phải nộp một URL duy nhất trỏ đến Jupyter Book đã publish trên GitHub Pages. Jupyter Book phải được phục vụ từ một repository GitHub công khai do nhóm sở hữu và phải được tổ chức như một bài tường thuật có cấu trúc, trình bày lần lượt từng task.

Mỗi chapter hoặc section của Jupyter Book tương ứng với một task trong bài lab. Nội dung của từng chapter phải bao gồm phần giải thích bằng văn bản về cách tiếp cận mà nhóm đã chọn và lý do đằng sau lựa chọn đó. Nội dung cũng phải bao gồm các notebook cell đã chạy, thể hiện output trung gian thực tế như số lượng node đã parse, mẫu message Kafka, và kết quả query database. Chapter cũng cần có screenshot hoặc hình nhúng về giao diện database. Mỗi chapter kết thúc bằng một phần reflection ngắn về điều đã hoạt động tốt, điều đã thất bại, và cách nhóm xử lý các vấn đề gặp phải.

Repository GitHub dùng để host Jupyter Book cũng phải chứa toàn bộ mã nguồn mà nhóm đã viết, được tổ chức trong cấu trúc thư mục hợp lý, và được commit với commit message có ý nghĩa, phản ánh tiến độ tăng dần của nhóm trong suốt thời gian làm lab.

Bài nộp trên Moodle vì vậy chỉ là đúng một text entry: URL gốc của site Jupyter Book đã publish. Không chấp nhận file zip, file PDF export, hoặc tài liệu Word.

#### Tiêu Chí Chấm Điểm

```
Tiêu chí chấm điểm được tóm tắt trong bảng dưới đây.

Yêu cầu                                           Điểm
Task 1. Clone Repository và Khám Phá File        1
Task 2. Parser Service CPG Tăng Dần              1
Task 3. Thiết Kế Topic Kafka                     1
Task 4. Ingest Topology Graph vào Neo4j          2
Task 5. Ingest Metadata Mã Nguồn vào MongoDB     2
Task 6. Xác Minh Replay Idempotent               1
Sơ Đồ Kiến Trúc                                  1
TỔNG                                             10
```

```
Cũng cần lưu ý rằng:
```

- Đảm bảo code được viết tài liệu rõ ràng với comment dễ hiểu.
- Bao gồm đầy đủ các file, log, và screenshot cần thiết để xác minh việc thực thi thành công.
- Mỗi task có thể được thực hiện trong các môi trường phức tạp và bằng các ngôn ngữ lập trình khác nhau; trong trường hợp đó, hãy cung cấp hướng dẫn chạy cho từng task.

```
Chúc các bạn lập trình vui vẻ và may mắn!
```

```
Giảng viên hướng dẫn./.
```
