from datetime import datetime, timezone
import gzip
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from ingestion.downloader.download_pageviews import download_hour, main


class IngestionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.hour = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def test_missing_hour_is_not_a_zero_view_file(self):
        missing = HTTPError('https://example.invalid', 404, 'Not Found', {}, None)
        self.addCleanup(missing.close)
        with patch('ingestion.downloader.download_pageviews.urlopen', side_effect=missing):
            entry = download_hour(self.hour, self.root)
        self.assertEqual(entry['status'], 'missing')
        self.assertIsNone(entry['raw_uri'])
        self.assertEqual(list(self.root.iterdir()), [])

    def test_transient_failure_retries_and_records_checksum(self):
        response = gzip.compress(b'en Page 1\n')
        with patch('ingestion.downloader.download_pageviews.urlopen', side_effect=[URLError('temporary'), io.BytesIO(response)]) as network, patch('ingestion.downloader.download_pageviews.time.sleep'):
            entry = download_hour(self.hour, self.root, retries=2)
        self.assertEqual(network.call_count, 2)
        self.assertEqual(entry['status'], 'downloaded')
        self.assertEqual(entry['size_bytes'], len(response))
        self.assertEqual(len(entry['checksum_sha256']), 64)
        self.assertEqual(entry['hour_start'], '2025-12-31T23:00:00+00:00')

    def test_cli_keeps_manifest_on_upload_failure_and_continues(self):
        manifest = self.root / 'snapshot.jsonl'
        args = ['download', '--start', '2026-01-01T00:00:00Z', '--end', '2026-01-01T02:00:00Z',
                '--output', str(self.root / 'raw'), '--manifest', str(manifest)]
        success = {'hour_end': '2026-01-01T01:00:00+00:00', 'status': 'downloaded'}
        with patch('sys.argv', args), patch('sys.stdout', io.StringIO()), patch('logging.basicConfig'), self.assertLogs('ingestion.downloader.download_pageviews', level='INFO'), patch('ingestion.downloader.download_pageviews.download_hour', side_effect=[RuntimeError('storage unavailable'), success]) as download:
            with self.assertRaises(SystemExit) as error:
                main()
        self.assertEqual(error.exception.code, 1)
        entries = [json.loads(line) for line in manifest.read_text().splitlines()]
        self.assertEqual([r['status'] for r in entries], ['failed', 'downloaded'])
        self.assertIn('duration_seconds', entries[0])
        self.assertEqual(download.call_count, 2)
        self.assertEqual(download.call_args.args[1], self.root / 'raw/2026/01/01')

    def test_invalid_range_fails_before_creating_manifest(self):
        manifest = self.root / 'invalid.jsonl'
        args = ['download', '--start', '2026-01-02T00:00:00Z', '--end', '2026-01-01T00:00:00Z', '--manifest', str(manifest)]
        with patch('sys.argv', args), patch('sys.stderr', io.StringIO()), patch('logging.basicConfig'):
            with self.assertRaises(SystemExit) as error:
                main()
        self.assertEqual(error.exception.code, 2)
        self.assertFalse(manifest.exists())


if __name__ == '__main__':
    unittest.main()
