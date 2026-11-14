"""Stream local raw files to produce a reproducible quality/capacity report."""

from __future__ import annotations

import argparse
from collections import Counter
import glob
import gzip
import hashlib
import json
from pathlib import Path

from processing.common.pageviews import RecordError, SCHEMA_VERSION, parse_line, source_hours


def profile_files(paths, target_hours=24, max_rows=None):
    if target_hours < 1 or (max_rows is not None and max_rows < 1):
        raise ValueError('target_hours and max_rows must be positive')
    paths = sorted(Path(p) for p in paths)
    if not paths:
        raise ValueError('No input files')
    names = [p.name for p in paths]
    if len(set(names)) != len(names):
        raise ValueError('Duplicate source filenames')
    files = []
    total_domains, total_rejects = Counter(), Counter()
    totals = Counter()
    for path in paths:
        start, end = source_hours(path.name)
        rows = valid = excluded = raw_bytes = views = 0
        domains, rejects, shapes = Counter(), Counter(), Counter()
        digest = hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b''):
                digest.update(chunk)
        opener = gzip.open if path.suffix == '.gz' else open
        complete = True
        with opener(path, 'rb') as source:
            for payload in source:
                if max_rows is not None and rows == max_rows:
                    complete = False
                    break
                rows += 1
                raw_bytes += len(payload)
                try:
                    line = payload.decode('utf-8')
                    shapes[len(line.rstrip('\r\n').split(' '))] += 1
                    record = parse_line(line)
                    if record is None:
                        excluded += 1
                        continue
                    valid += 1
                    views += record.views
                    domains[record.domain_code] += 1
                except UnicodeDecodeError:
                    rejects['invalid_utf8'] += 1
                except RecordError as exc:
                    rejects[exc.reason] += 1
        files.append(dict(source_file=path.name, hour_start=start.isoformat(), hour_end=end.isoformat(),
                          checksum_sha256=digest.hexdigest(), compressed_bytes=path.stat().st_size,
                          inspected_raw_bytes=raw_bytes, rows=rows, valid_rows=valid,
                          excluded_rows=excluded, rejected_rows=sum(rejects.values()),
                          total_views=views, complete=complete, field_counts=dict(shapes),
                          domains=dict(domains), reject_reasons=dict(rejects)))
        totals.update(rows=rows, valid_rows=valid, excluded_rows=excluded,
                      rejected_rows=sum(rejects.values()), total_views=views,
                      compressed_bytes=path.stat().st_size, inspected_raw_bytes=raw_bytes)
        total_domains.update(domains)
        total_rejects.update(rejects)
    complete = all(f['complete'] for f in files)
    # Only full hour files can be extrapolated to an hourly capacity estimate.
    estimate = None
    if complete:
        estimate = {key: totals[key] / len(files) * target_hours
                    for key in ('compressed_bytes', 'inspected_raw_bytes', 'rows', 'valid_rows')}
    return dict(schema_version=SCHEMA_VERSION, files=files, totals=dict(totals),
                domains=dict(total_domains), reject_reasons=dict(total_rejects),
                target_hours=target_hours, estimated_target=estimate,
                estimate_warning='Linear sample extrapolation; not a measured final dataset or Parquet size.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True, help='Local raw file glob')
    parser.add_argument('--output', required=True)
    parser.add_argument('--target-hours', type=int, default=24)
    parser.add_argument('--max-rows', type=int, help='Survey only; disables capacity extrapolation if truncated')
    args = parser.parse_args()
    try:
        report = profile_files(glob.glob(args.input), args.target_hours, args.max_rows)
    except (ValueError, OSError, EOFError) as exc:
        parser.error(str(exc))
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


if __name__ == '__main__':
    main()
