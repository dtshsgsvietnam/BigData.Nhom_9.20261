# DATA contract (schema version 3)

Applies to PB-101 through PB-108, PB-206 through PB-209, and PB-301 through PB-303.

## Source and scope

Source: [Wikimedia's pageviews specification](https://wikitech.wikimedia.org/wiki/Data_Platform/Data_Lake/Traffic/Pageviews).
The filename timestamp is the **end** of the UTC aggregation period. For example,
`pageviews-20260101-000000.gz` represents `[2025-12-31T23:00Z, 2026-01-01T00:00Z)`.
Each record is an hourly aggregate, so traffic always uses `sum(views)`.

The selected domains are exactly `en`, `en.m`, `vi`, `vi.m`. They map to projects
`en`/`vi` and desktop/mobile access respectively. Codes such as `enwiki`, `en.d`,
`en.m.v` and `commons.m` are excluded. Mobile refers to the mobile site, not a
guaranteed device category. The original title is preserved, including percent
escapes and underscores; no second URL decoding or cross-language title matching.

The source specifies four space-separated fields. For compatibility the parser
also accepts three fields. `legacy_response_size` preserves the fourth field as
text when present, but is never used as article size or an attention metric.
Views must match `-?[0-9]+`, fit in signed int64, and be nonnegative. Decimal,
plus-prefixed, underscore-separated, and overflowing values are rejected.
Repeated spaces can create an empty title or extra fields and are not silently
normalized. Excluded projects are counted separately from malformed selected
projects. Python rejects invalid UTF-8 during profiling; Spark's text reader can
replace invalid bytes, so the raw profiler should be run before batch ingestion
when byte-level validation is required.

## Curated schema and identity

| Column | Type | Rule |
|---|---|---|
| hour_start, hour_end | UTC timestamp | One-hour half-open interval |
| domain_code, project, access_variant | string | Exact whitelist mapping |
| page_title | string | Original nonempty source title |
| views | long | Nonnegative source aggregate |
| legacy_response_size | nullable string | Preserved fourth field |
| source_file | string | Original basename, unique within snapshot |
| source_line | long | One-based physical line number before filtering |
| event_id | string | SHA-256 of file, line, domain, title separated by U+001F |
| schema_version | string | `3` |
| event_date, event_hour | date, integer | Derived from hour_start in UTC |

Raw duplicate records keep distinct event IDs. They are reported in quality
output and their views are summed in analytics. Replay must carry the same
filename and original line number. Schema 3 retains stable identity from v2,
adds the legacy field, and tightens parser validation; regenerate older curated
snapshots before combining them with new outputs.

## Raw storage and manifests

CLI default: `data/raw/YYYY/MM/DD/pageviews-YYYYMMDD-HH0000.gz` (date of hour_end).
`--layout flat` retains the previous flat directory option. Object storage uses
`s3a://BUCKET/raw/YYYY/MM/DD/FILENAME`. Both keep the original compressed bytes.

Downloads validate every gzip member through EOF/CRC, including cached files.
Fresh content is written to `.part` and replaces the final path only after
validation. Failed/missing hours have no usable raw URI and never become zeros.
The CLI continues to later hours, flushes a JSONL manifest entry per hour, and
returns exit code 1 if any hour failed or was missing. Invalid arguments exit 2.

Manifest fields: manifest_version, recorded_at, hour_start, hour_end, source_url,
raw_uri, status, duration_seconds; successful rows add checksum_sha256 and
size_bytes. Failed rows add error. Logs contain attempt and outcome details.
Local upload exceptions also produce failed entries. Manifest upload failure
leaves the local manifest available and makes the command fail.

Use a **new manifest name for every independent snapshot**. Repeated runs append
attempt history; select only the final successful attempt per hour when building
a downstream dataset. Do not feed a glob containing known corrupt cached files
to Spark. Files retained after a failed replacement are not valid inputs.
The upload key is `manifests/<manifest basename>`; use unique snapshot basenames.
No remote bucket is created automatically. Credentials and Hadoop S3A jars must
be configured on the execution environment.

## Layout and analytics

Layout A partitions by `(project, event_date)`; layout B by
`(event_date, event_hour, project)`. Both contain identical curated rows/columns.
`--layout both` writes `curated/layout_a` and `curated/layout_b`; choosing a single
layout writes directly under `curated`. Compare both on the same input snapshot.
Layout B trades finer hourly pruning for more directories and potentially small
files. Neither is declared faster without measured results.

History outputs include hourly/daily/weekly traffic, hourly/daily top pages, and
the existing attention baseline. Rankings first sum duplicate page records and
then sort by views descending, title ascending for deterministic ties. Access
variants remain separate. A week starts Monday UTC. Daily and weekly traffic
include observed_hours and expected_hours (24/168); partial periods are kept,
and missing hours are never filled with zeros. Observed hours mean that at least
one selected row was present for that project/access, not proof of source-file
completeness. The manifest remains the authority for missing source hours.

Quality outputs include rejected rows/reasons, source-level and overall valid /
rejected / excluded counts, and duplicate `(hour_start, domain_code, page_title)`
groups. The local profiler additionally reports field counts, domain distribution,
raw/compressed bytes, checksums and reason counts without collecting full dumps.

## Capacity policy

Begin with a one-day sample, then expand after measuring compressed bytes,
decompressed bytes, retained rows and actual Parquet bytes. The profiler projects
full inspected file averages to `--target-hours`; a limited row survey suppresses
capacity projection. These estimates assume representative hours and are **not**
measurements of a final dataset or predictions of Parquet compression. A final
20-100 GB processing dataset remains unselected until representative real data
and available cluster/storage capacity have been measured.
