# PB-005 — Bằng chứng và cách chạy lại

Ngày kiểm chứng: 11/11/2026. Phạm vi: thiết kế kiến trúc và proof local bằng
Python/JSON/SQLite. Không phải benchmark hoặc kiểm thử cluster phân tán.

## Thành phần bàn giao

- [Kiến trúc và ranh giới](../../docs/architecture.md).
- [Cấu hình namespace và bảng](../../config/architecture.json).
- [Raw fixture tổng hợp và manifest](../../tests/fixtures/pb005/manifest.json).
- [Hai nhánh, checkpoint và reference loader](../../scripts/verify_pb005.py).
- [Kiểm thử thành công và các trường hợp bị từ chối](../../tests/test_pb005.py).
- [Kết quả chạy đã lưu](result.json).

## Chạy lại

Từ root repo, dùng Python 3 với thư viện chuẩn; không cần tải package hoặc dataset:

```powershell
python scripts/verify_pb005.py --workspace data/pb005-review-001
python -m unittest discover -s tests -p "test_*.py" -v
```

`--workspace` phải là đường dẫn chưa tồn tại. Đổi hậu tố cho lần kiểm chứng mới;
script từ chối ghi đè bằng chứng cũ. Trong môi trường giới hạn quyền, Python cần
quyền tạo/dọn thư mục tạm của hệ điều hành cho unittest.

Lệnh tạo bằng chứng đính kèm:

```powershell
python scripts/verify_pb005.py --workspace data/pb005-evidence-002
```

Snapshot `pb005-fixture-v1` gồm hai file giờ tổng hợp ở UTC, không phải dump đã
tải từ Wikimedia. Checksum trong manifest cố định bộ input cho cả hai nhánh.
Trong proof, thư mục workspace đóng vai trò namespace của một snapshot/run;
`outputs/history`, `outputs/replay`, `checkpoints/history`, `checkpoints/replay`
tương ứng bốn prefix S3 trong config. File `serving.sqlite` chứa hai bảng riêng.

## Kết quả quan sát

Python 3.12.11 trên Windows (`win32`). Sáu kiểm thử đạt:

| Kiểm tra | Kết quả |
|---|---|
| Hai nhánh cùng snapshot và giá trị output | 6 events mỗi nhánh, tổng views = 45 |
| Replay dừng sau 2 events, tiếp tục bằng checkpoint | Hoàn tất 6 events, không mất/trùng |
| Replay không sửa checkpoint history | SHA-256 checkpoint history không đổi |
| Loader chạy hai lần trên mỗi output | Mỗi bảng vẫn 6 dòng, tổng views = 45 |
| Đổi run, giữ nguyên raw snapshot | Event IDs không đổi |
| Raw trước/sau chạy | Khớp checksum manifest |
| Cấu hình dùng chung bảng hoặc output/checkpoint | Bị từ chối |
| Raw sai checksum, loader nhận sai nhánh, checkpoint sai run | Bị từ chối |

Proof dừng ở ranh giới event rồi gọi lại để mô phỏng resume; không kill process,
không kiểm chứng checkpoint Spark, Kafka delivery semantics hoặc ClickHouse.
Output được định danh theo event để có thể ghi lại cùng event khi retry. Proof
không đánh giá hỏng file do mất điện giữa lúc ghi hoặc nhiều writer đồng thời.

## Checklist nghiệm thu và review

- [x] Một raw snapshot và manifest dùng chung cho hai nhánh.
- [x] Output/checkpoint tách biệt và có ví dụ chạy lại.
- [x] Loader và bảng history/replay trong reference implementation local.
- [x] Tài liệu nêu luồng production, ownership, namespace và giới hạn kiểm chứng.
- [x] Có fixture, lệnh, kết quả và kiểm thử.
- [ ] PB-007 được DATA chốt và STREAM/PLATFORM ký nhận; cập nhật draft trong proof.
- [ ] Một thành viên khác review PR và ghi nhận kết quả trên issue.

Người review kiểm tra: batch/replay không tải hai dataset riêng; namespace không
đè nhau; event time không bị thay bằng thời gian replay; query không cộng hai
bản sao history/replay; chính sách nạp lại được xác định; contract khớp PB-007.

Chưa có PR/commit mới được tạo hoặc review của người khác trong lần bàn giao này.
Điền link PR/commit, tên reviewer và kết quả ký nhận PB-007 khi thực sự có bằng
chứng. Trạng thái phù hợp hiện tại: **chờ review và phụ thuộc PB-007, chưa Done**.
