# Product Backlog — Wikipedia Attention Observatory

> Học phần: Big Data Storage and Processing  
> Project 5: Wikipedia Attention Observatory  
> Nhóm: 3 thành viên — Thời gian: 12 tuần

## 1. Mục tiêu sản phẩm

Xây dựng một hệ thống Big Data phân tán có khả năng:
- Tự động thu thập Wikimedia hourly pageview.
- Lưu giữ raw data bất biến.
- Làm sạch, chuẩn hóa và lưu dữ liệu tối ưu cho phân tích.
- Xử lý batch bằng Apache Spark.
- Replay dữ liệu lịch sử như một stream.
- Xử lý streaming bằng Spark Structured Streaming.
- Tính toán attention/burst metrics.
- Phục vụ truy vấn qua OLAP serving layer.
- Hiển thị kết quả trên dashboard.
- Đánh giá scalability, hiệu năng và fault tolerance.
- Triển khai trên Kubernetes hoặc cloud environment.

## 2. Vai trò trong nhóm

| Thành viên | Vai trò chính | Trách nhiệm |
|---|---|---|
| Member A | Infrastructure / Platform | Kubernetes, storage, Kafka, Spark deployment, monitoring, fault tolerance |
| Member B | Data / Processing | Downloader, parsing, cleaning, schema, Spark batch, Spark streaming |
| Member C | Analytics / Serving | Attention metric, SQL, ClickHouse, dashboard, interpretation |

Vai trò là ownership chính, không chia cứng. Mỗi PR quan trọng nên có ít nhất 1 người khác review.

## 3. Priority và Story Point

| Priority | Ý nghĩa |
|---|---|
| P0 | Bắt buộc để hệ thống chạy end-to-end |
| P1 | Bắt buộc để đáp ứng đầy đủ yêu cầu môn |
| P2 | Nên có nếu còn thời gian |
| P3 | Stretch goal |

| Point | Ước lượng |
|---:|---|
| 1 | Rất nhỏ |
| 2 | Nhỏ |
| 3 | Khoảng 1 ngày |
| 5 | 1–2 ngày |
| 8 | Lớn, nên cân nhắc tách |

# 4. Product Backlog

## EPIC 0 — Project Setup & Planning

| ID | Issue | Pri. | Pt. | Owner | Tuần | Phụ thuộc |
|---|---|---:|---:|---|---|---|
| PB-001 | Khởi tạo repository và cấu trúc thư mục | P0 | 2 | A | W1 | - |
| PB-002 | Hoàn thiện README và architecture v1 | P0 | 2 | C | W1 | PB-001 |
| PB-003 | Thiết lập GitHub Project, labels, milestones | P0 | 2 | A | W1 | PB-001 |
| PB-004 | Chốt scope dữ liệu: enwiki, viwiki, thời gian | P0 | 2 | B+C | W1 | - |
| PB-005 | Lập experiment plan và tiêu chí đo | P1 | 3 | A+B+C | W2 | PB-004 |

## EPIC 1 — Dataset Exploration & Ingestion

| ID | Issue | Pri. | Pt. | Owner | Tuần | Phụ thuộc |
|---|---|---:|---:|---|---|---|
| PB-101 | Tải thử 1 file Wikimedia pageview và kiểm tra format | P0 | 2 | B | W1 | PB-004 |
| PB-102 | Viết script download dữ liệu theo ngày/giờ | P0 | 5 | B | W1–W2 | PB-101 |
| PB-103 | Viết parser cho Wikimedia records | P0 | 5 | B | W2 | PB-101 |
| PB-104 | Xác định rule lọc enwiki và viwiki | P0 | 2 | B | W2 | PB-103 |
| PB-105 | Ghi metadata/log cho ingestion | P1 | 3 | B | W4 | PB-102 |
| PB-106 | Lưu raw data theo cấu trúc reproducible | P0 | 3 | A+B | W4 | PB-102 |
| PB-107 | Validate input và xử lý malformed records | P1 | 3 | B | W4–W5 | PB-103 |

Acceptance Criteria:
- Downloader chạy bằng command line.
- Chọn được date/hour hoặc time range.
- Raw file được giữ nguyên.
- Có log file tải thành công/thất bại.
- Parser đọc đúng record.
- Có rule rõ ràng cho enwiki/viwiki.
- Có thống kê dung lượng và số record.

## EPIC 2 — Distributed Environment & Storage

| ID | Issue | Pri. | Pt. | Owner | Tuần | Phụ thuộc |
|---|---|---:|---:|---|---|---|
| PB-201 | Chuẩn bị Kubernetes hoặc cloud cluster | P0 | 8 | A | W3 | PB-003 |
| PB-202 | Cấu hình distributed/object storage | P0 | 5 | A | W3 | PB-201 |
| PB-203 | Triển khai Spark cluster nhiều worker | P0 | 8 | A | W3 | PB-201 |
| PB-204 | Kiểm tra Spark job chạy trên nhiều worker | P0 | 3 | A+B | W3 | PB-203 |
| PB-205 | Thiết kế raw / curated / aggregate layers | P0 | 3 | A+B | W2–W3 | PB-004 |
| PB-206 | Xây dựng curated storage bằng Parquet | P0 | 5 | B | W5 | PB-107, PB-202 |
| PB-207 | Thử Apache Iceberg sau khi Parquet ổn định | P2 | 5 | A+B | W5–W6 | PB-206 |

## EPIC 3 — Data Cleaning & Curated Layer

| ID | Issue | Pri. | Pt. | Owner | Tuần | Phụ thuộc |
|---|---|---:|---:|---|---|---|
| PB-301 | Thiết kế curated schema | P0 | 3 | B+C | W5 | PB-103 |
| PB-302 | Chuẩn hóa timestamp, project, title, views, content size | P0 | 5 | B | W5 | PB-301 |
| PB-303 | Xử lý malformed/invalid records | P0 | 3 | B | W5 | PB-107 |
| PB-304 | Tạo partition layout A | P0 | 3 | B | W5 | PB-206 |
| PB-305 | Tạo partition layout B để benchmark | P1 | 3 | B | W5 | PB-304 |
| PB-306 | Viết data quality report cơ bản | P1 | 3 | B+C | W5 | PB-302, PB-303 |

## EPIC 4 — Batch Analytics

| ID | Issue | Pri. | Pt. | Owner | Tuần | Phụ thuộc |
|---|---|---:|---:|---|---|---|
| PB-401 | Tính top pages theo giờ | P0 | 3 | B | W6 | PB-302 |
| PB-402 | Tính top pages theo ngày | P0 | 3 | B | W6 | PB-302 |
| PB-403 | Tính hourly/daily/weekly traffic patterns | P0 | 5 | B | W6 | PB-302 |
| PB-404 | Phân tích long-tail distribution | P1 | 3 | C | W6 | PB-302 |
| PB-405 | So sánh selected language editions | P1 | 3 | C | W6 | PB-302 |
| PB-406 | Tạo aggregate tables phục vụ dashboard | P0 | 5 | B+C | W6 | PB-401, PB-402, PB-403 |

## EPIC 5 — Attention Analytics

| ID | Issue | Pri. | Pt. | Owner | Tuần | Phụ thuộc |
|---|---|---:|---:|---|---|---|
| PB-501 | Xây historical baseline cho pageviews | P0 | 5 | C | W7 | PB-403 |
| PB-502 | Xây attention/burst score | P0 | 5 | C | W7 | PB-501 |
| PB-503 | Phát hiện rapid-growth pages | P0 | 3 | C | W7 | PB-502 |
| PB-504 | Tính burst duration | P0 | 3 | C | W7 | PB-502 |
| PB-505 | Phân loại short-lived / sustained / recurring | P1 | 5 | C | W7 | PB-504 |
| PB-506 | Chọn ít nhất 1 historical attention burst để minh họa | P1 | 3 | C | W7 | PB-502 |
| PB-507 | Viết attention-metric specification | P1 | 3 | C | W7 | PB-505 |

## EPIC 6 — Streaming Pipeline

| ID | Issue | Pri. | Pt. | Owner | Tuần | Phụ thuộc |
|---|---|---:|---:|---|---|---|
| PB-601 | Triển khai Kafka trên cluster/environment được chọn | P0 | 8 | A | W8 | PB-201 |
| PB-602 | Viết historical replay producer | P0 | 5 | B | W8 | PB-102 |
| PB-603 | Tạo Kafka topic/schema | P0 | 3 | A+B | W8 | PB-601 |
| PB-604 | Xây Spark Structured Streaming consumer | P0 | 8 | B | W8 | PB-603 |
| PB-605 | Thiết lập checkpointing | P0 | 3 | A+B | W8 | PB-604 |
| PB-606 | Tính streaming window aggregates | P0 | 5 | B | W8 | PB-604 |
| PB-607 | Kiểm tra batch-stream consistency trên sample | P1 | 3 | B+C | W8 | PB-606 |

## EPIC 7 — Serving Layer & Dashboard

| ID | Issue | Pri. | Pt. | Owner | Tuần | Phụ thuộc |
|---|---|---:|---:|---|---|---|
| PB-701 | Triển khai ClickHouse hoặc serving layer tương đương | P0 | 5 | A+C | W9 | PB-201 |
| PB-702 | Thiết kế analytical tables | P0 | 3 | C | W9 | PB-406 |
| PB-703 | Nạp batch aggregates vào serving layer | P0 | 3 | B+C | W9 | PB-702 |
| PB-704 | Nạp streaming aggregates vào serving layer | P0 | 3 | B+C | W9 | PB-606, PB-702 |
| PB-705 | Xây dashboard Overview | P0 | 3 | C | W9 | PB-703 |
| PB-706 | Xây dashboard Trending/Attention | P0 | 3 | C | W9 | PB-502, PB-703 |
| PB-707 | Xây dashboard Historical Trends | P1 | 3 | C | W9 | PB-403 |
| PB-708 | Xây dashboard Language Comparison | P2 | 3 | C | W9 | PB-405 |
| PB-709 | Tối ưu các query chính của dashboard | P1 | 5 | C | W9–W10 | PB-705, PB-706 |

## EPIC 8 — Performance & Scalability Experiments

| ID | Issue | Pri. | Pt. | Owner | Tuần | Phụ thuộc |
|---|---|---:|---:|---|---|---|
| PB-801 | Benchmark raw query vs pre-aggregated query | P0 | 5 | B+C | W10 | PB-406, PB-709 |
| PB-802 | Benchmark partition layout A vs B | P0 | 5 | B | W10 | PB-304, PB-305 |
| PB-803 | Benchmark batch với 1 worker | P0 | 3 | A+B | W10 | PB-204, PB-403 |
| PB-804 | Benchmark batch với 2 workers | P0 | 3 | A+B | W10 | PB-803 |
| PB-805 | Benchmark batch với 4 workers | P0 | 3 | A+B | W10 | PB-804 |
| PB-806 | Tính speedup và phân tích scaling behavior | P1 | 3 | A+C | W10 | PB-803, PB-804, PB-805 |
| PB-807 | Replay stream ở nhiều tốc độ | P0 | 5 | A+B | W10 | PB-602, PB-606 |
| PB-808 | Đo throughput, lag, processing rate, latency | P0 | 5 | A+B | W10 | PB-807 |
| PB-809 | Xác định điểm hệ thống bắt đầu falling behind | P1 | 3 | A+C | W10 | PB-808 |

## EPIC 9 — Fault Tolerance & Recovery

| ID | Issue | Pri. | Pt. | Owner | Tuần | Phụ thuộc |
|---|---|---:|---:|---|---|---|
| PB-901 | Chuẩn bị fault-injection scenario | P0 | 3 | A | W11 | PB-604 |
| PB-902 | Terminate một worker/executor khi job đang chạy | P0 | 3 | A | W11 | PB-901 |
| PB-903 | Đo recovery time | P0 | 3 | A | W11 | PB-902 |
| PB-904 | Kiểm tra missing windows | P0 | 3 | B | W11 | PB-902 |
| PB-905 | Kiểm tra duplicate records | P0 | 3 | B | W11 | PB-902 |
| PB-906 | Phân tích checkpoint/recovery behavior | P1 | 5 | A+B | W11 | PB-903, PB-904, PB-905 |
| PB-907 | Viết fault-tolerance experiment report | P1 | 3 | A+C | W11 | PB-906 |

## EPIC 10 — Documentation, Demo & Final Report

| ID | Issue | Pri. | Pt. | Owner | Tuần | Phụ thuộc |
|---|---|---:|---:|---|---|---|
| PB-1001 | Hoàn thiện architecture diagram | P0 | 3 | A | W12 | Core epics |
| PB-1002 | Hoàn thiện setup/reproducibility guide | P0 | 5 | A+B | W12 | Core epics |
| PB-1003 | Tổng hợp experiment logs và bảng kết quả | P0 | 5 | C | W12 | EPIC 8, 9 |
| PB-1004 | Viết final technical report | P0 | 8 | A+B+C | W12 | PB-1003 |
| PB-1005 | Chuẩn bị demo end-to-end | P0 | 5 | A+B+C | W12 | Core epics |
| PB-1006 | Chuẩn bị slide 15–20 phút | P0 | 5 | C + team | W12 | PB-1004 |
| PB-1007 | Rehearsal và test demo từ đầu | P0 | 3 | A+B+C | W12 | PB-1005, PB-1006 |

# 5. Stretch Backlog

| ID | Issue | Pri. | Pt. | Owner |
|---|---|---:|---:|---|
| PB-S01 | Tích hợp Wikimedia Recent Changes | P3 | 8 | B |
| PB-S02 | Multilingual attention correlation | P3 | 5 | C |
| PB-S03 | Watermark / late-data experiment | P3 | 5 | B |
| PB-S04 | ClickHouse vs Druid comparison | P3 | 8 | A+C |
| PB-S05 | Prometheus/Grafana monitoring | P3 | 5 | A |
| PB-S06 | Automated burst alerts | P3 | 5 | C |
| PB-S07 | Iceberg partition evolution / compaction | P3 | 8 | A+B |

# 6. Milestones

| Milestone | Nội dung | Deadline |
|---|---|---|
| M1 — Scope & Design | Dataset, scope, architecture, experiment plan | Cuối W2 |
| M2 — Data Platform | Cluster, storage, ingestion, curated data | Cuối W5 |
| M3 — Batch & Attention | Batch analytics + attention metric | Cuối W7 |
| M4 — Streaming | Kafka replay + Structured Streaming | Cuối W8 |
| M5 — End-to-End MVP | Serving + dashboard | Cuối W9 |
| M6 — Experiments | Scalability + streaming benchmark | Cuối W10 |
| M7 — Reliability | Fault-tolerance experiment | Cuối W11 |
| M8 — Final Delivery | Report + demo + presentation | Cuối W12 |

# 7. GitHub Labels

```text
type:feature
type:bug
type:experiment
type:docs
type:infra
type:data
type:analytics

priority:P0
priority:P1
priority:P2
priority:P3

epic:setup
epic:ingestion
epic:storage
epic:batch
epic:attention
epic:streaming
epic:serving
epic:experiment
epic:fault-tolerance
epic:docs

status:blocked
good-first-task
```

# 8. GitHub Project Board

Status:

```text
Backlog
   ↓
Ready
   ↓
In Progress
   ↓
In Review
   ↓
Done
```

Custom fields:

```text
Priority: P0 / P1 / P2 / P3
Epic: Setup / Ingestion / Storage / Batch / Attention / Streaming / Serving / Experiment / Fault Tolerance / Docs
Estimate: 1 / 2 / 3 / 5 / 8
Week: W1 ... W12
Owner: Member A / Member B / Member C
```

# 9. Workflow cho mỗi task

```text
Tạo Issue
   ↓
Assign người phụ trách
   ↓
Status = Ready
   ↓
Tạo branch
   ↓
Code + test
   ↓
Push
   ↓
Pull Request
   ↓
Thành viên khác review
   ↓
Merge vào main
   ↓
Issue = Done
```

Không nên commit trực tiếp vào `main`.

# 10. Quy ước Branch

```text
feature/PB-xxx-ten-feature
fix/PB-xxx-ten-bug
experiment/PB-xxx-ten-experiment
docs/PB-xxx-ten-doc
```

Ví dụ:

```text
feature/PB-102-wikimedia-downloader
feature/PB-401-hourly-top-pages
experiment/PB-803-one-worker-benchmark
docs/PB-1002-setup-guide
```

# 11. Quy ước Commit

```text
feat(PB-102): add Wikimedia hourly downloader
fix(PB-103): handle malformed records
data(PB-301): add curated schema
infra(PB-203): configure Spark workers
experiment(PB-803): add one-worker benchmark
docs(PB-1002): update setup guide
test(PB-103): add parser tests
```

# 12. Pull Request Template

```markdown
## Mục tiêu
Issue nào được giải quyết?

## Thay đổi
Đã thêm/sửa gì?

## Cách test
Chạy command nào?

## Kết quả
Output / log / screenshot nếu cần.

## Checklist
- [ ] Code chạy được
- [ ] Không commit dataset lớn
- [ ] Không chứa secret/password
- [ ] Có cập nhật docs nếu cần
- [ ] Đã self-review
```

# 13. Quy tắc làm việc nhóm

1. Mỗi thành viên chỉ nên có tối đa 1–2 task `In Progress`.
2. Task 8 points trở lên nên cân nhắc tách nhỏ.
3. Không merge PR của chính mình nếu có thể tránh.
4. Task bị block phải ghi rõ lý do.
5. Review Project Board ít nhất 1 lần/tuần.
6. Không push dataset lớn lên GitHub.
7. Không commit password, token hoặc credentials.
8. Experiment phải lưu config, command, data size, worker count, runtime, metrics và kết quả.
9. `main` luôn phải ở trạng thái tương đối ổn định.
10. Stretch goal chỉ bắt đầu sau khi MVP và experiment bắt buộc đã ổn.
