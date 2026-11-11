# DATA role: downloader, parser, Spark Batch

This implementation covers the DATA-owned path in the README: Wikimedia hourly dump -> raw snapshot/manifest -> shared Python and Spark SQL parsing contract -> Spark Batch -> curated, quality rejects and history Parquet outputs. `parse_raw_lines` is Spark SQL based so it can also be reused by replay/stream processing without a Python UDF.

## 1. Install

Use Python 3.10+ and Java 17 for the pinned Spark version:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-data.txt
```

## 2. Download a sample

The interval is UTC and `--end` is exclusive; each timestamp names the dump hour and is recorded as `hour_end`. The command keeps compressed raw files locally and writes one JSON object per requested hour to the manifest. Missing hours and failed downloads are recorded as such; they are never converted to zero views.

```powershell
python -m ingestion.downloader.download_pageviews `
  --start 2026-01-01T00:00:00Z `
  --end 2026-01-02T00:00:00Z
```

Optional MinIO upload (raw objects and the accumulated manifest are uploaded; local files remain available for inspection):

```powershell
$env:AWS_ACCESS_KEY_ID = "minioadmin"
$env:AWS_SECRET_ACCESS_KEY = "minioadmin"
python -m ingestion.downloader.download_pageviews `
  --start 2026-01-01T00:00:00Z `
  --end 2026-01-02T00:00:00Z `
  --bucket pageviews --endpoint-url http://localhost:9000
```

## 3. Run Spark Batch

```powershell
python -m processing.batch.pageviews_batch `
  --input "data/raw/pageviews-*.gz" `
  --output "data/processed/pageviews"
```

For MinIO, pass an `s3a://bucket/raw/.../pageviews-*.gz` input and configure Hadoop S3A endpoint/credentials in the Spark session/environment. Outputs are `curated/`, `quality/rejected/`, `history/hourly/`, `history/daily/`, `history/top_pages_hourly/`, and `history/attention/` Parquet datasets.

## Data contract and limits

- The shared parser keeps only `en`, `en.m`, `vi`, and `vi.m`, preserving `domain_code` and desktop/mobile variant. It parses the source's 3-field and 4-field variants; the fourth field is not treated as article size.
- Dump timestamp is interpreted as `hour_end`; `hour_start` is one hour earlier and all timestamps are UTC.
- Aggregates use `SUM(views)`. Batch outputs are partitioned by project and hour for curated detail.
- Desktop and mobile remain separate in aggregates and attention output; combine them only in a downstream query with an explicit decision.
- Attention is a first baseline implementation: trailing `--baseline-hours` observed page-hours, excluding the current hour, population standard deviation and a 1.0 score denominator floor. It emits a score and history count, not final burst labels. Tune these parameters and metric version before reporting results.
- Downloader writes the manifest in append mode. Use a fresh manifest per dataset snapshot, or archive/clear it before a new independent download.
- Malformed rows for the selected en/vi projects are written to `quality/rejected/` with a reason. Other projects are excluded by the project whitelist.
- Spark generates `source_line` within a run; it can change if input partitioning changes. Keep the input snapshot and partitioning fixed for replay comparison, or replace this field with a stable source byte offset when cross-run identity is required.
