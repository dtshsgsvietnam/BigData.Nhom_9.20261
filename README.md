# Wikipedia Attention Observatory

> Big Data Storage and Processing — Project 5

## 1. Overview

**Wikipedia Attention Observatory** is a distributed Big Data platform for analyzing how public attention changes over time using Wikimedia hourly pageview data.

The system combines:

- Historical batch processing
- Replay-based streaming processing
- Distributed storage and computation
- OLAP analytical serving
- Interactive visualization
- Scalability and fault-tolerance experiments

The project focuses on Big Data system design rather than only data analysis or dashboard development.

---

## 2. Research Question

> How can large-scale Wikipedia page-view streams be processed efficiently to identify rapid changes, short-lived bursts, sustained attention, and recurring temporal patterns?

---

## 3. Dataset

We use **Wikimedia hourly pageview dumps**.

Initial target:

- English Wikipedia
- Vietnamese Wikipedia
- Approximately 1–3 years of historical data depending on available infrastructure

During development, smaller subsets will be used first:

```text
1–3 days
    ↓
1 month
    ↓
Several months
    ↓
Final large-scale dataset
```

Possible curated fields:

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

## 4. Proposed Architecture

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

### Technology Stack

| Layer | Technology |
|---|---|
| Data Source | Wikimedia Pageview Dumps |
| Ingestion | Python |
| Streaming | Apache Kafka |
| Object Storage | MinIO |
| File/Table Format | Parquet / Apache Iceberg |
| Batch Processing | Apache Spark |
| Stream Processing | Spark Structured Streaming |
| OLAP / Serving | ClickHouse |
| Dashboard | Apache Superset |
| Deployment | Kubernetes |
| Orchestration | Airflow — optional |
| Monitoring | Prometheus / Grafana — optional |

---

## 5. Main Analytics

### Top Pages

- Top pages by hour
- Top pages by day
- Top pages by week

### Traffic Patterns

- Hourly traffic
- Daily traffic
- Weekly patterns
- Long-tail pageview distribution

### Attention Detection

Identify:

- Rapid-growth pages
- Attention bursts
- Burst duration
- Short-lived attention
- Sustained attention
- Recurring attention

### Language Comparison

Compare attention patterns between:

```text
Vietnamese Wikipedia
vs
English Wikipedia
```

where meaningful.

---

## 6. Attention Metric

An initial baseline can use a standardized attention score:

```text
                  current_views - historical_mean
attention_score = --------------------------------
                    historical_std + epsilon
```

This may later be improved using:

- Rolling averages
- Z-score
- Relative growth
- Exponential moving averages
- Hour-of-day baseline
- Day-of-week baseline

The final metric should remain:

- Transparent
- Explainable
- Scalable
- Easy to validate

---

## 7. System Experiments

### Experiment 1 — Raw vs Pre-aggregated Queries

Compare queries executed on:

```text
Raw pageview data
vs
Pre-aggregated hourly/daily tables
```

Measure:

- Query latency
- Bytes scanned
- CPU usage
- Memory usage

---

### Experiment 2 — Partitioning

Compare at least two layouts.

Example:

```text
Layout A:
year/month/day

Layout B:
year/month/day/hour
```

Measure:

- Query latency
- Files scanned
- Bytes scanned
- Number of files
- Over-partitioning effects

---

### Experiment 3 — Scale-out

Run the same Spark workload with:

```text
1 worker
2 workers
4 workers
```

Measure:

- Runtime
- Throughput
- Speedup
- CPU utilization
- Memory utilization

---

### Experiment 4 — Streaming Throughput

Replay historical data at increasing rates:

```text
1x
5x
10x
20x
...
```

Measure:

- Input records/s
- Processing records/s
- Kafka lag
- End-to-end latency
- CPU / memory usage

Determine when:

```text
input rate > processing capacity
```

and the system begins falling behind.

---

### Experiment 5 — Fault Tolerance

During a running distributed job:

1. Terminate one Spark worker/executor.
2. Observe system recovery.
3. Measure recovery time.
4. Inspect checkpoints.
5. Check for missing windows.
6. Check for duplicate records.
7. Analyze recovery and processing semantics.

---

## 8. Dashboard

The dashboard is planned to contain the following views.

### Overview

- Total pageviews
- Active pages
- Current top pages
- Active attention bursts

### Trending

- Rapid-growth pages
- Attention scores
- Burst duration

### Historical Trends

- Hourly traffic
- Daily traffic
- Weekly traffic

### Page Detail

For each selected page:

- Pageview time series
- Attention score
- Historical baseline
- Burst intervals

### Language Comparison

- Vietnamese Wikipedia
- English Wikipedia
- Selected cross-language trends

### System Monitoring — Optional

- Kafka lag
- Spark processing rate
- Streaming latency
- Resource utilization

---

## 9. Repository Structure

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

## 10. Development Roadmap

### Week 1 — Dataset Exploration

- Study Wikimedia pageview format
- Download sample files
- Define project scope
- Estimate data volume
- Create GitHub repository
- Define initial schema

### Week 2 — Architecture Design

- Finalize architecture
- Design raw / curated / aggregate layers
- Define partitioning candidates
- Define experiments
- Define initial attention metric

### Week 3 — Distributed Environment

- Set up Kubernetes
- Deploy MinIO
- Deploy Spark
- Test multi-worker Spark processing

### Week 4 — Ingestion

- Build automatic Wikimedia downloader
- Preserve immutable raw data
- Store files in MinIO
- Add ingestion metadata and logs

### Week 5 — Curated Storage

- Parse raw records
- Filter selected Wikimedia projects
- Clean and normalize data
- Write Parquet / Iceberg
- Implement partitioning

### Week 6 — Batch Processing

Implement:

- Top pages
- Hourly aggregates
- Daily aggregates
- Weekly statistics
- Long-tail analysis

### Week 7 — Attention Analytics

Implement:

- Historical baseline
- Rapid-growth detection
- Burst detection
- Burst duration
- Short-lived / sustained / recurring attention

### Week 8 — Streaming

- Deploy Kafka
- Replay historical data
- Implement Spark Structured Streaming
- Configure checkpoints
- Produce streaming aggregates

### Week 9 — Serving and Dashboard

- Deploy ClickHouse
- Create analytical tables
- Connect Superset
- Build dashboard
- Integrate batch and streaming results

### Week 10 — Scalability

Run:

- Raw vs aggregate benchmark
- Partitioning benchmark
- 1 / 2 / 4 worker benchmark
- Streaming replay-rate benchmark

### Week 11 — Fault Tolerance

- Kill a worker/executor
- Observe recovery
- Analyze checkpoints
- Check missing and duplicate data
- Record recovery metrics

### Week 12 — Finalization

- Clean repository
- Verify reproducibility
- Finalize dashboard
- Finalize experiment results
- Write technical report
- Prepare presentation
- Prepare end-to-end demo

---

## 11. Team Responsibilities

### Member 1 — Infrastructure / Platform

Responsible for:

- Kubernetes
- MinIO
- Kafka
- Spark deployment
- Monitoring
- Fault-tolerance experiments

### Member 2 — Data / Processing

Responsible for:

- Wikimedia downloader
- Parsing and cleaning
- Schema design
- Spark Batch
- Spark Structured Streaming
- Partitioning

### Member 3 — Analytics / Serving

Responsible for:

- Attention metric
- ClickHouse
- Analytical SQL
- Superset
- Dashboard
- Result interpretation

All members should understand the complete architecture and participate in integration, experiments, report writing, and presentation.

---

## 12. MVP

The first end-to-end MVP should be:

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

Only after this pipeline works reliably:

```text
Add Kafka
    ↓
Add Structured Streaming
    ↓
Add Checkpoints
    ↓
Add Iceberg
    ↓
Run Scalability Experiments
    ↓
Run Fault-Injection Experiments
```

---

## 13. Stretch Goals

After the baseline system is complete:

- Wikimedia Recent Changes integration
- Multilingual attention correlation
- ClickHouse vs Druid
- Late-data handling
- Watermark experiments
- Incremental materialized views
- Automated burst alerts
- More advanced anomaly detection

Machine learning is optional and should not replace the distributed-system objectives.

---

## 14. Definition of Done

- [ ] Repository structure completed
- [ ] Automated Wikimedia ingestion works
- [ ] Immutable raw data is preserved
- [ ] Curated storage is generated
- [ ] Spark runs on multiple workers
- [ ] Batch analytics works
- [ ] Attention metric works
- [ ] Kafka replay works
- [ ] Spark Structured Streaming works
- [ ] Checkpointing works
- [ ] ClickHouse serves analytical queries
- [ ] Superset dashboard works
- [ ] Raw vs aggregate benchmark completed
- [ ] Partitioning benchmark completed
- [ ] 1 / 2 / 4 worker experiment completed
- [ ] Streaming throughput experiment completed
- [ ] Fault-recovery experiment completed
- [ ] Experiment results are reproducible
- [ ] Final technical report completed
- [ ] 15–20 minute presentation completed
- [ ] End-to-end demo ready

---

## 15. Team

| Member | Role |
|---|---|
| Member 1 | Infrastructure / Platform |
| Member 2 | Data / Processing |
| Member 3 | Analytics / Serving |

---

## 16. Course

**Big Data Storage and Processing**

Hanoi University of Science and Technology  
Project Topics 2026
