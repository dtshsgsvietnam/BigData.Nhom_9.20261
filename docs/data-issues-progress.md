# Fifteen DATA issues: implementation and evidence

This is a local implementation report, not a claim that all acceptance criteria
or GitHub review/closure steps are complete. No GitHub issue was closed.

| Issue | Delivered | Validation / remaining work |
|---|---|---|
| PB-101 | Download CLI, survey profiler, synthetic smoke fixture | Real sample request attempted; failed with network error. Real dump survey remains pending. |
| PB-102 | Hour-range download, retries, dated layout, gzip validation, failure exit code | Mocked network/CLI tests passed; live download pending. |
| PB-103 | Python/Spark parsing contract v3, lineage and preserved legacy field | Python contract tests passed; Spark parity test added, not executed. |
| PB-104 | Exact en/vi desktop/mobile whitelist and title rules | Python whitelist tests passed; Spark parity pending. |
| PB-105 | Raw paths, snapshot/manifest, retry and checksum contract | Documented in data-contract.md; local storage tests passed, MinIO validation pending. |
| PB-106 | Persistent logs, attempt/outcome, duration, timestamps, SHA-256, JSONL flush | CLI failure/continuation and metadata tests passed. |
| PB-107 | Stable reject reasons, int64 bounds, malformed fixtures | Python invalid-record tests passed; Spark ANSI-mode parity pending. |
| PB-108 | Streaming capacity report, full-sample projection guard, dataset sizing policy | Synthetic projection test passed; representative real sample and final capacity decision pending. |
| PB-206 | Curated Parquet writer with schema v3 and cleanup | Implemented; Parquet round-trip test requires Spark. |
| PB-207 | Layout A: project/date | Implemented; layout parity test requires Spark. |
| PB-208 | Layout B: date/hour/project, same-workload benchmark command | Implemented; actual layout measurements require Spark. |
| PB-209 | Local profile and Spark quality outputs incl. counts/reasons/duplicates | Synthetic local report verified; Spark output validation pending. |
| PB-301 | Hourly top K, sum duplicate views, deterministic ties | Spark integration assertions added, not executed. |
| PB-302 | Daily top K per project/access | Spark integration assertions added, not executed. |
| PB-303 | Hourly/daily/weekly traffic with UTC boundaries and coverage fields | Spark totals/week-boundary assertions added, not executed. |

## Observed validation

- `python -m unittest discover -s tests -v`: 24 tests discovered, 18 passed,
  6 skipped because PySpark is not installed.
- `python -m scripts.data_smoke`: produced a synthetic fixture profile. Its
  10 valid rows total 64 views, with 5 rejected and 2 excluded rows. This sample
  deliberately contains duplicate records and missing hours.
- `python -m compileall -q ingestion processing scripts experiments tests`:
  syntax verification; no claim of Spark runtime validation.
- Live sample command used one hour ending `2026-01-01T00:00:00Z` with one retry.
  The manifest records failure, not a zero-view hour. The current environment
  cannot reach dumps.wikimedia.org; downloading Python/Spark prerequisites also
  failed with a socket-permission error.

See [data-role.md](data-role.md) for runnable commands and
[data-contract.md](data-contract.md) for the exact semantics.
