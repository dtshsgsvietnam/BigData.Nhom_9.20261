# DATA role: download, profile and Spark Batch

See [data-contract.md](data-contract.md) for schema, UTC/project rules, raw layout,
manifest semantics and capacity policy. See [data-issues-progress.md](data-issues-progress.md)
for the fifteen-issue delivery and its remaining acceptance checks.

## Install

Use Python 3.10 or 3.11 and Java 17 for the pinned PySpark 3.5.6 environment.
The current machine's Python 3.14 environment has not been validated for this Spark version.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-data.txt
```

## Download a real sample (PB-101, PB-102)

Times must include a timezone and align to UTC hours; end is exclusive.
The range selects filename hour-end timestamps. This downloads one complete
UTC day of source intervals on January 1 (ending January 2 at midnight):

```powershell
python -m ingestion.downloader.download_pageviews `
  --start 2026-01-01T01:00:00Z --end 2026-01-02T01:00:00Z `
  --output data/raw --manifest data/manifests/sample-20260101.jsonl `
  --retries 3 --log-file data/ingestion.log
```

Default local layout is `data/raw/YYYY/MM/DD/FILENAME`; use `--layout flat` for
legacy flat storage. Use a fresh manifest per independent snapshot. Missing/failed
hours produce manifest entries and exit code 1; do not submit their files to batch.
Check the manifest before processing a glob, especially after a failed replacement.

Optional MinIO upload adds `--bucket pageviews --endpoint-url http://localhost:9000`.
Create the bucket and set AWS credentials in the environment first. Raw objects
use `raw/YYYY/MM/DD/FILENAME`. Spark S3A use additionally needs compatible Hadoop
S3A jars and endpoint/credential configuration on the cluster.

## Survey, data quality and capacity (PB-107, PB-108, PB-209)

For the one-day download above the last source hour is under January 2, so use a
multi-date glob to include the midnight-ending file. Use a directory dedicated to
this snapshot so unrelated hours are not included.

```powershell
python -m processing.common.profile_pageviews `
  --input "data/raw/*/*/*/pageviews-*.gz" `
  --target-hours 720 --output data/reports/sample-profile.json
```

The streaming profiler records source hours, checksums, raw/compressed bytes,
field counts, retained/excluded/rejected rows and views. `--max-rows 10000` enables
a quick format survey; capacity extrapolation is omitted for a truncated sample.
Final dataset sizing requires real representative hours and measured Parquet size.

## Reproducible synthetic smoke sample

This sample is hand-authored test data, not a downloaded Wikimedia dump. It has
four sparse source hours crossing day/year/week boundaries, duplicates, mobile
variants, malformed rows, and excluded projects.

```powershell
python -m scripts.data_smoke
python -m scripts.data_smoke --spark
```

The first command needs only the Python standard library and produces
`data/smoke/profile.json`; the second also runs Spark with both Parquet layouts.

## Run Spark Batch (PB-206 through PB-209, PB-301 through PB-303)

```powershell
python -m processing.batch.pageviews_batch `
  --input "data/raw/*/*/*/pageviews-*.gz" `
  --output data/processed/sample-20260101 --layout both --top-k 10
```

For a single machine, configure `--master local[2]` through `spark-submit`, or set
Spark's master in the execution environment. Batch outputs are overwritten on
rerun. Use a new output root for independent snapshots and preserve the input
manifest. Writes are per-dataset, not one transaction across all outputs; only
consume a run after the entire command succeeds.

Outputs:

- `curated/layout_a`: project/date partitions.
- `curated/layout_b`: date/hour/project partitions with identical rows.
- `quality/rejected`: original malformed selected-project rows and reason.
- `quality/summary`, `source_counts`, `reject_reasons`, `duplicates`: JSON datasets.
- `history/hourly`, `daily`, `weekly`: traffic Parquet datasets.
- `history/top_pages_hourly`, `top_pages_daily`: top-K page rankings.
- `history/attention`: existing trailing observed-page-hour baseline (not final burst labels).

Single-layout runs (`--layout a` or `b`) write directly under `curated`.
Desktop/mobile remain separate. Daily/weekly traffic reports observed and expected
hours; gaps are not filled with zeros. Top pages aggregate views before ranking,
with title ascending as tie-breaker. The attention baseline uses previous observed
page-hours, excludes the current hour, and applies a 1.0 standard-deviation floor;
`--baseline-hours` does not imply a dense calendar-hour baseline.

## Compare layouts

Run both layouts on the same snapshot first, then compare the same workload:

```powershell
python -m experiments.layout_benchmark `
  --layout-a data/processed/sample-20260101/curated/layout_a `
  --layout-b data/processed/sample-20260101/curated/layout_b `
  --project en --day 2026-01-01 --repeats 3 `
  --output reports/results/layout-sample-20260101.json
```

The command verifies equal query results, excludes warm-up, alternates layout
order and reports repeated timings/median and cache policy. OS/object-store cache
is uncontrolled, and no speed advantage is claimed until real measurements exist.

## Verification

```powershell
python -m unittest discover -s tests -v
python -m scripts.validate_data --require-spark --output reports/results/data-validation.json
```

The unittest command skips Spark integration tests if PySpark is missing. The
strict validation command fails on any skip, suitable for acceptance checks in
a fully provisioned Spark environment. It covers Python/Spark parser parity,
stable identity, int64/ANSI behavior, actual Parquet round trips across layouts,
view totals, duplicate handling, top pages and UTC week boundaries.
