import csv
import gzip
import importlib.util
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, timezone

from ingestion.downloader.download_pageviews import download_hour


class DownloadTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.hour = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.path = self.root / 'pageviews-20260101-000000.gz'
        self.payload = gzip.compress(b'en Example 42\n' * 10000)

    def download(self, response):
        with patch('ingestion.downloader.download_pageviews.urlopen', return_value=io.BytesIO(response)):
            return download_hour(self.hour, self.root, retries=1)

    def test_truncated_response_is_rejected(self):
        result = self.download(self.payload[:-8])
        self.assertEqual(result['status'], 'failed')
        self.assertFalse(self.path.exists())
        self.assertFalse(list(self.root.glob('*.part')))

    def test_corrupt_cache_is_replaced(self):
        self.path.write_bytes(self.payload[:-8])
        result = self.download(self.payload)
        self.assertEqual(result['status'], 'downloaded')
        self.assertEqual(self.path.read_bytes(), self.payload)

    def test_valid_cache_is_reused_without_network(self):
        self.path.write_bytes(self.payload)
        with patch('ingestion.downloader.download_pageviews.urlopen') as network:
            result = download_hour(self.hour, self.root)
        network.assert_not_called()
        self.assertEqual(result['status'], 'already_present')

    def test_failed_replacement_is_not_uploaded(self):
        self.path.write_bytes(self.payload[:-8])
        with patch('ingestion.downloader.download_pageviews.urlopen', return_value=io.BytesIO(self.payload[:-8])), patch('ingestion.downloader.download_pageviews._s3_client') as s3:
            result = download_hour(self.hour, self.root, bucket='test', retries=1)
        self.assertEqual(result['status'], 'failed')
        s3.assert_not_called()

    def test_invalid_retry_count(self):
        with self.assertRaises(ValueError):
            download_hour(self.hour, self.root, retries=0)


class BacklogTests(unittest.TestCase):
    def test_markdown_matches_import_csv(self):
        root = Path(__file__).resolve().parents[1]
        markdown = (root / 'PRODUCT_BACKLOG.md').read_text(encoding='utf-8')
        with (root / 'BACKLOG_ISSUES_NO_PRIORITY.csv').open(encoding='utf-8') as source:
            rows = list(csv.DictReader(source))
        self.assertEqual(len(rows), 79)
        for row in rows:
            self.assertIn(row['OwnerRole'], {'DATA', 'STREAM', 'PLATFORM'})
            self.assertIn(f"| {row['ID']} | {row['Title']} | {row['OwnerRole']} | {row['Week']} |", markdown)


@unittest.skipUnless(importlib.util.find_spec('pyspark'), 'PySpark is not installed')
class SparkIdentityTests(unittest.TestCase):
    def test_identity_survives_input_partition_changes(self):
        from pyspark.sql import SparkSession
        from processing.common.spark_pageviews import read_raw_files, parse_raw_lines
        spark = SparkSession.builder.master('local[2]').appName('identity-regression').getOrCreate()
        self.addCleanup(spark.stop)
        spark.conf.set('spark.sql.session.timeZone', 'UTC')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'pageviews-20260101-000000.gz'
            with gzip.open(path, 'wt', encoding='utf-8') as out:
                out.write('en Same 5\nen Same 5\nfr Other 3\nvi Test 7\n')
            results = []
            for partitions in (1, 4):
                raw = read_raw_files(spark, path.as_uri()).repartition(partitions)
                valid, _ = parse_raw_lines(raw)
                results.append(sorted((r.source_line, r.event_id) for r in valid.collect()))
            self.assertEqual(results[0], results[1])
            self.assertEqual([r[0] for r in results[0]], [1, 2, 4])
            self.assertEqual(len({r[1] for r in results[0]}), 3)


if __name__ == '__main__':
    unittest.main()
