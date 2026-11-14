"""Generate a SYNTHETIC smoke dataset; optionally execute the Spark job."""

import argparse
import gzip
import json
from pathlib import Path
import subprocess
import sys

from processing.common.profile_pageviews import profile_files


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', default='data/smoke')
    parser.add_argument('--spark', action='store_true')
    args = parser.parse_args()
    root = Path(args.output)
    raw = root / 'raw'
    raw.mkdir(parents=True, exist_ok=True)
    fixtures = Path(__file__).resolve().parents[1] / 'tests/fixtures/pageviews'
    paths = []
    for source in sorted(fixtures.glob('pageviews-*')):
        path = raw / (source.name + '.gz')
        path.write_bytes(gzip.compress(source.read_bytes(), mtime=0))
        paths.append(path)
    report = profile_files(paths)
    report['data_origin'] = 'synthetic test fixture; not downloaded Wikimedia data'
    (root / 'profile.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    if args.spark:
        subprocess.run([sys.executable, '-m', 'processing.batch.pageviews_batch', '--input',
                        str(raw / '*.gz'), '--output', str(root / 'processed'), '--layout', 'both'], check=True)
    print(root / 'profile.json')


if __name__ == '__main__':
    main()
