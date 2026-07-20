# Task 6: Xác minh Replay Idempotent

## 1. Mục tiêu
Chứng minh pipeline hoạt động hoàn toàn chính xác và idempotent khi xử lý lại (replay) một file mã nguồn bị chỉnh sửa nội dung.

## 2. Thiết kế
Khi re-parse một file:
- Sinh ID mới cho các thành phần sửa đổi.
- Tính toán diff để tìm ra các node/edge cũ bị xóa, phát hành delete events.
- Ghi đè metadata thống kê tại MongoDB.

## 3. Lý do lựa chọn
- Đảm bảo cơ sở dữ liệu đồ thị không bị phình to bởi các node rác (stale nodes) qua nhiều phiên bản chỉnh sửa code.

## 4. Cách thực hiện
- Chỉnh sửa thủ công một file Python fixture.
- Kích hoạt lệnh CLI replay riêng file đó.
- Đối chiếu số lượng node/edge trong Neo4j và văn bản thống kê trong MongoDB trước/sau chạy.

## 5. Code hoặc command đã chạy
```bash
# TODO: Điền lệnh CLI chạy replay ở Phase 13
```

## 6. Output thực tế
```json
// TODO: Điền log in ra của phiên chạy replay ở Phase 13
```

## 7. Verification
- So sánh số lượng node trong Neo4j không đổi hoặc tăng/giảm chính xác tương ứng số lượng thay đổi thực tế trong file sửa đổi.

## 8. Screenshot
*(TODO: Đính kèm hình ảnh so sánh đồ thị Neo4j trước/sau re-run ở Phase 13)*

## 9. Vấn đề gặp phải
- Việc dọn dẹp các cạnh cũ đòi hỏi độ chính xác tuyệt đối, nếu không sẽ làm đứt gãy luồng điều khiển của file.

## 10. Cách khắc phục
- Viết unit test bao phủ toàn bộ logic CpgDiffer để đảm bảo không xóa nhầm node đang hoạt động.

## 11. Reflection
- Idempotency và incremental là hai thành phần khó nhất nhưng cũng giá trị nhất của hệ thống, giúp pipeline tự tin xử lý hàng triệu file mà không lo sợ rác cơ sở dữ liệu.
