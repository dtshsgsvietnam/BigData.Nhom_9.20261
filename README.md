# Wikipedia Attention Observatory

> Học phần: Big Data Storage and Processing — Project 5

## 1. Tổng quan

**Wikipedia Attention Observatory** là một nền tảng Big Data phân tán dùng để phân tích cách mức độ chú ý của cộng đồng thay đổi theo thời gian dựa trên dữ liệu pageview theo giờ của Wikimedia.

Hệ thống kết hợp:

- Xử lý batch trên dữ liệu lịch sử
- Xử lý streaming bằng cách replay dữ liệu lịch sử
- Lưu trữ và xử lý phân tán
- OLAP serving cho truy vấn phân tích
- Dashboard tương tác
- Thí nghiệm về khả năng mở rộng và khả năng chịu lỗi

Mục tiêu của project không chỉ là phân tích dữ liệu Wikipedia, mà còn là xây dựng và đánh giá một hệ thống Big Data hoàn chỉnh.

---

## 2. Câu hỏi nghiên cứu

> Làm thế nào để xử lý hiệu quả luồng dữ liệu pageview Wikipedia quy mô lớn nhằm phát hiện các thay đổi nhanh, các đợt tăng chú ý ngắn hạn, sự chú ý kéo dài và các mẫu chú ý lặp lại theo thời gian?

---

## 3. Dữ liệu

Nguồn dữ liệu chính:

**Wikimedia hourly pageview dumps**

Phạm vi dự kiến:

- Wikipedia tiếng Anh
- Wikipedia tiếng Việt
- Khoảng 1–3 năm dữ liệu lịch sử, tùy khả năng hạ tầng

Trong giai đoạn phát triển, nhóm nên tăng quy mô dần:

```text
1–3 ngày
    ↓
1 tháng
    ↓
Vài tháng
    ↓
Dataset lớn cuối cùng
```

Các trường dữ liệu dự kiến sau khi làm sạch:

```text
timestamp
project
page_title
views
content_size
year
month
day
hour
```

---

## 4. Kiến trúc đề xuất

```text
                  Wikimedia Pageview Dumps
                            |
               +------------+------------+
               |                         |
               v                         v
          Downloader                 Kafka Replay
               |                         |
               v                         v
        MinIO / Object Storage     Spark Structured
               |                    Streaming
               v                         |
          Spark Batch                    |
               |                         |
               +------------+------------+
                            |
                            v
                    Parquet / Iceberg
                            |
                            v
                       ClickHouse
                            |
                            v
                    Apache Superset
                            |
                            v
                        Dashboard
```

### Công nghệ dự kiến

| Thành phần | Công nghệ |
|---|---|
| Nguồn dữ liệu | Wikimedia Pageview Dumps |
| Ingestion | Python |
| Streaming | Apache Kafka |
| Object Storage | MinIO |
| Định dạng file/table | Parquet / Apache Iceberg |
| Batch Processing | Apache Spark |
| Stream Processing | Spark Structured Streaming |
| OLAP / Serving | ClickHouse |
| Dashboard | Apache Superset |
| Deployment | Kubernetes |
| Orchestration | Airflow — tùy chọn |
| Monitoring | Prometheus / Grafana — tùy chọn |

---

## 5. Các phân tích chính

### Top Pages

- Top page theo giờ
- Top page theo ngày
- Top page theo tuần

### Mẫu truy cập theo thời gian

- Lưu lượng theo giờ
- Lưu lượng theo ngày
- Mẫu theo tuần
- Phân phối long-tail của pageview

### Phát hiện mức độ chú ý

Phát hiện:

- Trang tăng trưởng nhanh
- Attention burst
- Thời lượng burst
- Short-lived attention
- Sustained attention
- Recurring attention

### So sánh ngôn ngữ

So sánh khi có ý nghĩa giữa:

```text
Wikipedia tiếng Việt
vs
Wikipedia tiếng Anh
```

---

## 6. Attention Metric

Một baseline ban đầu có thể sử dụng:

```text
                  current_views - historical_mean
attention_score = --------------------------------
                    historical_std + epsilon
```

Sau đó có thể thử:

- Rolling average
- Z-score
- Relative growth
- Exponential moving average
- Baseline theo giờ trong ngày
- Baseline theo ngày trong tuần

Metric cuối cùng cần:

- Dễ giải thích
- Minh bạch
- Có thể kiểm chứng
- Có khả năng tính toán trên dữ liệu lớn

---

## 7. Các thí nghiệm hệ thống

### Thí nghiệm 1 — Raw vs Pre-aggregated Query

So sánh truy vấn trên:

```text
Raw pageview data
vs
Bảng aggregate theo giờ/ngày
```

Đo:

- Query latency
- Bytes scanned
- CPU usage
- Memory usage

---

### Thí nghiệm 2 — Partitioning

So sánh ít nhất hai cách partition.

Ví dụ:

```text
Layout A:
year/month/day

Layout B:
year/month/day/hour
```

Đo:

- Query latency
- Số file được đọc
- Dữ liệu được scan
- Tổng số file
- Ảnh hưởng của over-partitioning

---

### Thí nghiệm 3 — Scale-out

Chạy cùng một workload Spark với:

```text
1 worker
2 workers
4 workers
```

Đo:

- Runtime
- Throughput
- Speedup
- CPU utilization
- Memory utilization

---

### Thí nghiệm 4 — Streaming Throughput

Replay dữ liệu lịch sử với tốc độ tăng dần:

```text
1x
5x
10x
20x
...
```

Đo:

- Input records/s
- Processing records/s
- Kafka lag
- End-to-end latency
- CPU / memory usage

Xác định thời điểm:

```text
input rate > processing capacity
```

và hệ thống bắt đầu không theo kịp dữ liệu đầu vào.

---

### Thí nghiệm 5 — Fault Tolerance

Trong khi một job phân tán đang chạy:

1. Dừng một Spark worker/executor.
2. Quan sát hệ thống phục hồi.
3. Đo recovery time.
4. Kiểm tra checkpoint.
5. Kiểm tra missing windows.
6. Kiểm tra duplicate records.
7. Phân tích processing semantics sau khi phục hồi.

---

## 8. Dashboard

### Overview

- Tổng pageview
- Số trang đang hoạt động
- Top page hiện tại
- Số attention burst đang xảy ra

### Trending

- Trang tăng trưởng nhanh
- Attention score
- Burst duration

### Historical Trends

- Traffic theo giờ
- Traffic theo ngày
- Traffic theo tuần

### Page Detail

Với từng page:

- Pageview time series
- Attention score
- Historical baseline
- Burst intervals

### Language Comparison

- Wikipedia tiếng Việt
- Wikipedia tiếng Anh
- Một số xu hướng có thể so sánh giữa hai ngôn ngữ

### System Monitoring — tùy chọn

- Kafka lag
- Spark processing rate
- Streaming latency
- Resource utilization

---

## 9. Cấu trúc repository

```text
wikipedia-attention-observatory/
│
├── README.md
├── .gitignore
│
├── docs/
│   ├── architecture.md
│   ├── data_model.md
│   ├── attention_metric.md
│   └── experiments.md
│
├── infra/
│   ├── kubernetes/
│   ├── kafka/
│   ├── spark/
│   ├── minio/
│   ├── clickhouse/
│   └── superset/
│
├── ingestion/
│   ├── downloader/
│   └── replay/
│
├── processing/
│   ├── batch/
│   ├── streaming/
│   └── common/
│
├── sql/
│   ├── ddl/
│   ├── analytics/
│   └── experiments/
│
├── dashboard/
│
├── experiments/
│   ├── raw_vs_aggregate/
│   ├── partitioning/
│   ├── scale_out/
│   ├── streaming_rate/
│   └── fault_tolerance/
│
├── tests/
│
├── scripts/
│
├── config/
│
└── reports/
    └── results/
```

---

## 10. Roadmap 12 tuần

### Tuần 1 — Khảo sát dữ liệu

- Tìm hiểu format Wikimedia pageview
- Download một số file mẫu
- Chốt phạm vi project
- Ước lượng dung lượng dữ liệu
- Tạo GitHub repository
- Xác định schema ban đầu

### Tuần 2 — Thiết kế kiến trúc

- Chốt architecture
- Thiết kế raw / curated / aggregate layer
- Chọn các phương án partition
- Thiết kế các experiment
- Định nghĩa attention metric ban đầu

### Tuần 3 — Môi trường phân tán

- Thiết lập Kubernetes
- Deploy MinIO
- Deploy Spark
- Kiểm tra Spark chạy với nhiều worker

### Tuần 4 — Ingestion

- Viết Wikimedia downloader
- Giữ nguyên raw data
- Lưu raw data vào MinIO
- Thêm log và metadata cho ingestion

### Tuần 5 — Curated Storage

- Parse raw records
- Lọc các Wikimedia project cần dùng
- Làm sạch và chuẩn hóa dữ liệu
- Ghi Parquet / Iceberg
- Thiết kế partition

### Tuần 6 — Batch Processing

Thực hiện:

- Top pages
- Hourly aggregates
- Daily aggregates
- Weekly statistics
- Long-tail analysis

### Tuần 7 — Attention Analytics

Thực hiện:

- Historical baseline
- Rapid-growth detection
- Burst detection
- Burst duration
- Short-lived / sustained / recurring attention

### Tuần 8 — Streaming

- Deploy Kafka
- Replay dữ liệu lịch sử
- Xây Spark Structured Streaming
- Thiết lập checkpoints
- Sinh streaming aggregates

### Tuần 9 — Serving và Dashboard

- Deploy ClickHouse
- Tạo analytical tables
- Kết nối Superset
- Xây dashboard
- Tích hợp batch và streaming

### Tuần 10 — Scalability

Chạy:

- Raw vs aggregate benchmark
- Partitioning benchmark
- Benchmark 1 / 2 / 4 worker
- Streaming replay-rate benchmark

### Tuần 11 — Fault Tolerance

- Kill một worker/executor
- Quan sát quá trình phục hồi
- Phân tích checkpoint
- Kiểm tra missing/duplicate data
- Ghi lại recovery metrics

### Tuần 12 — Hoàn thiện

- Dọn repository
- Kiểm tra reproducibility
- Hoàn thiện dashboard
- Tổng hợp experiment results
- Viết technical report
- Chuẩn bị presentation
- Chuẩn bị end-to-end demo

---

## 11. Phân chia công việc nhóm

### Thành viên 1 — Infrastructure / Platform

Phụ trách:

- Kubernetes
- MinIO
- Kafka
- Spark deployment
- Monitoring
- Fault-tolerance experiments

### Thành viên 2 — Data / Processing

Phụ trách:

- Wikimedia downloader
- Parsing và cleaning
- Schema design
- Spark Batch
- Spark Structured Streaming
- Partitioning

### Thành viên 3 — Analytics / Serving

Phụ trách:

- Attention metric
- ClickHouse
- Analytical SQL
- Superset
- Dashboard
- Phân tích kết quả

Tất cả thành viên cần hiểu toàn bộ pipeline và cùng tham gia integration, experiment, report và presentation.

---

## 12. MVP

MVP đầu tiên nên là:

```text
Wikimedia Dumps
       ↓
     MinIO
       ↓
   Spark Batch
       ↓
     Parquet
       ↓
   ClickHouse
       ↓
    Superset
```

Chỉ sau khi pipeline trên chạy ổn định mới thêm:

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Checkpoints
  ↓
Iceberg
  ↓
Scalability Experiments
  ↓
Fault-Injection Experiments
```

---

## 13. Stretch Goals

Sau khi baseline hoàn chỉnh:

- Wikimedia Recent Changes
- Multilingual attention correlation
- ClickHouse vs Druid
- Late-data handling
- Watermark experiments
- Incremental materialized views
- Automated burst alerts
- Anomaly detection nâng cao

Machine Learning chỉ nên là phần mở rộng, không thay thế mục tiêu về Big Data systems.

---

## 14. Definition of Done

- [ ] Hoàn thiện cấu trúc repository
- [ ] Wikimedia ingestion chạy tự động
- [ ] Raw data được lưu nguyên vẹn
- [ ] Curated storage hoạt động
- [ ] Spark chạy trên nhiều worker
- [ ] Batch analytics hoạt động
- [ ] Attention metric hoạt động
- [ ] Kafka replay hoạt động
- [ ] Spark Structured Streaming hoạt động
- [ ] Checkpointing hoạt động
- [ ] ClickHouse phục vụ truy vấn
- [ ] Superset dashboard hoạt động
- [ ] Hoàn thành raw vs aggregate benchmark
- [ ] Hoàn thành partitioning benchmark
- [ ] Hoàn thành 1 / 2 / 4 worker experiment
- [ ] Hoàn thành streaming throughput experiment
- [ ] Hoàn thành fault-recovery experiment
- [ ] Experiment có thể tái lập
- [ ] Hoàn thành technical report
- [ ] Hoàn thành presentation 15–20 phút
- [ ] End-to-end demo sẵn sàng

---

## 15. Nhóm

| Thành viên | Vai trò |
|---|---|
| Member 1 | Infrastructure / Platform |
| Member 2 | Data / Processing |
| Member 3 | Analytics / Serving |

---

## 16. Học phần

**Big Data Storage and Processing**

Hanoi University of Science and Technology  
Project Topics 2026
