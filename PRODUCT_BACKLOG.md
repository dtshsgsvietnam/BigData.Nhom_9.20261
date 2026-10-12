# Product Backlog — Wikipedia Attention Observatory
> Nhóm 3 thành viên • 12 tuần  
> Không dùng Priority; quản lý bằng **Status + Epic + Assignee + Week**.

## Quy ước
- **A** — Infrastructure / Platform
- **B** — Data / Processing
- **C** — Analytics / Serving
- Workflow: `Todo → In Progress → In Review → Done`

## Setup

| ID | Issue | Owner | Tuần |
|---|---|---|---|
| PB-001 | Khởi tạo repository và cấu trúc thư mục | A | W1 |
| PB-002 | Hoàn thiện README mô tả project | C | W1 |
| PB-003 | Thiết lập GitHub Project board và workflow | A | W1 |
| PB-004 | Chốt phạm vi dữ liệu: enwiki, viwiki và khoảng thời gian | B+C | W1 |
| PB-005 | Thiết kế architecture v1 | A+C | W2 |
| PB-006 | Lập kế hoạch benchmark và fault-tolerance experiment | A+B+C | W2 |

## Ingestion

| ID | Issue | Owner | Tuần |
|---|---|---|---|
| PB-101 | Tải thử Wikimedia hourly pageview sample và khảo sát format | B | W1 |
| PB-102 | Viết script download dữ liệu theo ngày/giờ | B | W1-W2 |
| PB-103 | Viết parser cho Wikimedia pageview records | B | W2 |
| PB-104 | Xác định rule lọc Wikipedia tiếng Anh và tiếng Việt | B | W2 |
| PB-105 | Thiết kế cấu trúc lưu raw data | A+B | W2-W4 |
| PB-106 | Bổ sung log và metadata cho ingestion | B | W4 |
| PB-107 | Kiểm tra malformed/invalid records | B | W4-W5 |
| PB-108 | Ước lượng dung lượng, số record và dataset size cuối | B+C | W2 |

## Infrastructure

| ID | Issue | Owner | Tuần |
|---|---|---|---|
| PB-201 | Chuẩn bị Kubernetes hoặc cloud environment | A | W3 |
| PB-202 | Thiết lập distributed/object storage | A | W3 |
| PB-203 | Triển khai Spark cluster nhiều worker | A | W3 |
| PB-204 | Kiểm tra Spark job chạy phân tán trên nhiều worker | A+B | W3 |

## Storage

| ID | Issue | Owner | Tuần |
|---|---|---|---|
| PB-205 | Thiết kế raw / curated / aggregate layers | A+B | W3 |
| PB-206 | Tạo curated storage bằng Parquet | B | W5 |
| PB-207 | Thiết kế partition layout A | B | W5 |
| PB-208 | Thiết kế partition layout B để benchmark | B | W5 |
| PB-209 | Viết data quality report | B+C | W5 |

## Batch

| ID | Issue | Owner | Tuần |
|---|---|---|---|
| PB-301 | Tính top pages theo giờ | B | W6 |
| PB-302 | Tính top pages theo ngày | B | W6 |
| PB-303 | Tính hourly/daily/weekly traffic patterns | B | W6 |
| PB-304 | Phân tích long-tail distribution | C | W6 |
| PB-305 | So sánh selected language editions | C | W6 |
| PB-306 | Tạo aggregate tables cho serving/dashboard | B+C | W6 |

## Attention

| ID | Issue | Owner | Tuần |
|---|---|---|---|
| PB-401 | Xây historical baseline cho pageviews | C | W7 |
| PB-402 | Xây attention/burst score | C | W7 |
| PB-403 | Phát hiện rapid-growth pages | C | W7 |
| PB-404 | Tính burst duration | C | W7 |
| PB-405 | Phân loại short-lived / sustained / recurring attention | C | W7 |
| PB-406 | Chọn ít nhất một historical attention burst để minh họa | C | W7 |
| PB-407 | Viết attention-metric specification | C | W7 |

## Streaming

| ID | Issue | Owner | Tuần |
|---|---|---|---|
| PB-501 | Triển khai Kafka trên environment được chọn | A | W8 |
| PB-502 | Viết historical replay producer | B | W8 |
| PB-503 | Thiết kế Kafka topic/schema | A+B | W8 |
| PB-504 | Xây Spark Structured Streaming consumer | B | W8 |
| PB-505 | Thiết lập checkpointing | A+B | W8 |
| PB-506 | Tính streaming window aggregates | B | W8 |
| PB-507 | Kiểm tra batch-stream consistency trên sample | B+C | W8 |

## Serving

| ID | Issue | Owner | Tuần |
|---|---|---|---|
| PB-601 | Triển khai ClickHouse hoặc OLAP serving layer tương đương | A+C | W9 |
| PB-602 | Thiết kế analytical tables | C | W9 |
| PB-603 | Nạp batch aggregates vào serving layer | B+C | W9 |
| PB-604 | Nạp streaming aggregates vào serving layer | B+C | W9 |

## Dashboard

| ID | Issue | Owner | Tuần |
|---|---|---|---|
| PB-605 | Xây dashboard Overview | C | W9 |
| PB-606 | Xây dashboard Trending / Attention | C | W9 |
| PB-607 | Xây dashboard Historical Trends | C | W9 |
| PB-608 | Xây dashboard Language Comparison | C | W9 |

## Experiment

| ID | Issue | Owner | Tuần |
|---|---|---|---|
| PB-701 | Benchmark raw query vs pre-aggregated query | B+C | W10 |
| PB-702 | Benchmark partition layout A vs B | B | W10 |
| PB-703 | Benchmark batch với 1 worker | A+B | W10 |
| PB-704 | Benchmark batch với 2 workers | A+B | W10 |
| PB-705 | Benchmark batch với 4 workers | A+B | W10 |
| PB-706 | Tính speedup và phân tích scaling behavior | A+C | W10 |
| PB-707 | Replay stream ở nhiều tốc độ khác nhau | A+B | W10 |
| PB-708 | Đo throughput, processing rate, lag và latency | A+B | W10 |
| PB-709 | Xác định điểm hệ thống bắt đầu falling behind | A+C | W10 |

## Fault Tolerance

| ID | Issue | Owner | Tuần |
|---|---|---|---|
| PB-801 | Chuẩn bị fault-injection scenario | A | W11 |
| PB-802 | Terminate một worker/executor khi job đang chạy | A | W11 |
| PB-803 | Đo recovery time | A | W11 |
| PB-804 | Kiểm tra missing windows và duplicate records | B | W11 |
| PB-805 | Phân tích checkpoint/recovery behavior | A+B | W11 |
| PB-806 | Viết fault-tolerance experiment report | A+C | W11 |

## Documentation

| ID | Issue | Owner | Tuần |
|---|---|---|---|
| PB-901 | Hoàn thiện architecture diagram | A | W12 |
| PB-902 | Hoàn thiện setup/reproducibility guide | A+B | W12 |
| PB-903 | Tổng hợp experiment logs và bảng kết quả | C | W12 |
| PB-904 | Viết final technical report | A+B+C | W12 |

## Demo

| ID | Issue | Owner | Tuần |
|---|---|---|---|
| PB-905 | Chuẩn bị end-to-end demo | A+B+C | W12 |

## Presentation

| ID | Issue | Owner | Tuần |
|---|---|---|---|
| PB-906 | Chuẩn bị slide 15–20 phút | C+A+B | W12 |
| PB-907 | Rehearsal và test demo từ đầu | A+B+C | W12 |

## Stretch

| ID | Issue | Owner | Tuần |
|---|---|---|---|
| PB-S01 | Tích hợp Wikimedia Recent Changes | B | Sau MVP |
| PB-S02 | Multilingual attention correlation | C | Sau MVP |
| PB-S03 | Watermark / late-data experiment | B | Sau MVP |
| PB-S04 | So sánh ClickHouse và Druid | A+C | Sau MVP |
| PB-S05 | Monitoring bằng Prometheus/Grafana | A | Sau MVP |
| PB-S06 | Automated burst alerts | C | Sau MVP |

## Definition of Done chung

- [ ] Hoàn thành công việc mô tả trong issue.
- [ ] Đã chạy thử/kiểm chứng kết quả.
- [ ] Có log/output/tài liệu nếu task yêu cầu.
- [ ] Không chứa secret/password.
- [ ] Nếu có code: đã tạo Pull Request và có người khác review.
- [ ] Nếu ảnh hưởng kiến trúc/cách chạy: README/docs đã được cập nhật.

## Quy ước branch
```text
feature/PB-xxx-description
fix/PB-xxx-description
experiment/PB-xxx-description
docs/PB-xxx-description
```

## Quy ước commit
```text
feat(PB-102): add Wikimedia hourly downloader
fix(PB-103): handle malformed records
experiment(PB-703): add one-worker benchmark
docs(PB-902): update setup guide
```