from datetime import datetime, timezone, timedelta
import gzip
import importlib.util
from pathlib import Path
import tempfile
import unittest

from ingestion.downloader.download_pageviews import dump_url, iter_hours
from processing.common.pageviews import RecordError, event_id, parse_line, source_hours
from processing.common.profile_pageviews import profile_files

FIXTURES = Path(__file__).resolve().parent / 'fixtures/pageviews'


class ContractTests(unittest.TestCase):
    def test_project_whitelist_preserves_variants(self):
        for domain, project, variant in [('en', 'en', 'desktop'), ('vi', 'vi', 'desktop'),
                                          ('en.m', 'en', 'mobile'), ('vi.m', 'vi', 'mobile')]:
            for suffix in ('', ' 0'):
                with self.subTest(domain=domain, suffix=suffix):
                    record = parse_line(f'{domain} Encoded_%25_Title 42{suffix}\r\n')
                    self.assertEqual((record.project, record.access_variant, record.views), (project, variant, 42))
                    self.assertEqual(record.page_title, 'Encoded_%25_Title')
                    self.assertEqual(record.legacy_response_size, '0' if suffix else None)
        for domain in ('enwiki', 'viwiki', 'en.d', 'en.m.v', 'fr', 'commons.m', ''):
            self.assertIsNone(parse_line(f'{domain} Example garbage extra extra'))

    def test_reject_reasons_and_int64_bounds(self):
        cases = {'en': 'invalid_field_count', 'en A 1 0 extra': 'invalid_field_count',
                 'en  1': 'empty_title', 'en A -1': 'negative_views', 'en A 1.5': 'invalid_views',
                 'en A +1': 'invalid_views', 'en A 1_000': 'invalid_views',
                 'en A 9223372036854775808': 'invalid_views', 'en A ' + '9' * 5000: 'invalid_views'}
        for line, reason in cases.items():
            with self.subTest(reason=reason, line=line[:40]):
                with self.assertRaises(RecordError) as error:
                    parse_line(line)
                self.assertEqual(error.exception.reason, reason)
        self.assertEqual(parse_line('en A 9223372036854775807').views, 2**63 - 1)
        self.assertEqual(parse_line('en A 0000').views, 0)

    def test_midnight_year_boundary(self):
        start, end = source_hours('pageviews-20260101-000000.gz')
        self.assertEqual(start.isoformat(), '2025-12-31T23:00:00+00:00')
        self.assertEqual(end - start, timedelta(hours=1))
        for name in ('pageviews-20260230-000000.gz', 'pageviews-20260101-240000.gz', 'xpageviews-20260101-000000.gz'):
            with self.assertRaises(RecordError):
                source_hours(name)

    def test_download_time_contract(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(list(iter_hours(start, start + timedelta(hours=2))), [start, start + timedelta(hours=1)])
        local = start.astimezone(timezone(timedelta(hours=7)))
        self.assertEqual(dump_url(local), dump_url(start))
        for begin, end in [(start, start), (start, start - timedelta(hours=1)),
                           (start.replace(tzinfo=None), start), (start, start + timedelta(minutes=30))]:
            with self.assertRaises(ValueError):
                list(iter_hours(begin, end))

    def test_identity_preserves_duplicate_raw_records(self):
        args = ('pageviews-20260101-000000.gz', 1, 'en', 'Alpha')
        self.assertEqual(event_id(*args), event_id(*args))
        self.assertNotEqual(event_id(*args), event_id(args[0], 2, *args[2:]))

    def test_profile_fixture_and_capacity(self):
        report = profile_files(FIXTURES.glob('pageviews-*'), target_hours=24)
        totals = report['totals']
        self.assertEqual(totals['valid_rows'], 10)
        self.assertEqual(totals['total_views'], 64)
        self.assertEqual(totals['rejected_rows'], 5)
        self.assertEqual(totals['rows'], totals['valid_rows'] + totals['rejected_rows'] + totals['excluded_rows'])
        self.assertEqual(report['estimated_target']['valid_rows'], 60)

    def test_limited_survey_is_not_a_capacity_estimate(self):
        report = profile_files([FIXTURES / 'pageviews-20260101-000000'], max_rows=1)
        self.assertIsNone(report['estimated_target'])
        self.assertFalse(report['files'][0]['complete'])

    def test_profile_checks_compressed_input(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'pageviews-20260101-000000.gz'
            path.write_bytes(gzip.compress(b'en A 2\n'))
            self.assertEqual(profile_files([path])['totals']['total_views'], 2)
            path.write_bytes(path.read_bytes()[:-8])
            with self.assertRaises(EOFError):
                profile_files([path])


@unittest.skipUnless(importlib.util.find_spec('pyspark'), 'PySpark is not installed')
class SparkDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        from processing.common.spark_pageviews import parse_raw_lines, read_raw_files
        from processing.batch.pageviews_batch import build_tables, curated_columns, quality_tables
        cls.spark = SparkSession.builder.master('local[2]').appName('data-contract').getOrCreate()
        cls.spark.conf.set('spark.sql.session.timeZone', 'UTC')
        cls.spark.conf.set('spark.sql.shuffle.partitions', '2')
        cls.spark.conf.set('spark.sql.ansi.enabled', 'true')
        cls.raw = read_raw_files(cls.spark, FIXTURES.as_uri() + '/pageviews-*')
        cls.valid, cls.rejected = parse_raw_lines(cls.raw)
        cls.curated = curated_columns(cls.valid).cache()
        cls.tables = build_tables(cls.curated, top_k=2)
        cls.quality = quality_tables(cls.raw, cls.curated, cls.rejected)

    @classmethod
    def tearDownClass(cls):
        cls.curated.unpersist()
        cls.spark.stop()

    def test_python_spark_parser_parity_and_identity(self):
        expected = {}
        for path in FIXTURES.glob('pageviews-*'):
            for number, line in enumerate(path.read_text().splitlines(), 1):
                try:
                    record = parse_line(line)
                except RecordError:
                    continue
                if record:
                    expected[event_id(path.name, number, record.domain_code, record.page_title)] = record.views
        self.assertEqual({r.event_id: r.views for r in self.valid.collect()}, expected)
        self.assertEqual(self.rejected.count(), 5)

    def test_totals_and_partial_periods(self):
        for name in ('hourly', 'daily', 'weekly'):
            rows = self.tables[name].collect()
            self.assertEqual(sum(r.total_views for r in rows), 64)
            if name != 'hourly':
                self.assertTrue(all(r.observed_hours < r.expected_hours for r in rows))
        weeks = {str(r.week_start) for r in self.tables['weekly'].collect()}
        self.assertEqual(weeks, {'2025-12-29', '2026-01-05'})

    def test_rankings_sum_duplicates_before_ranking(self):
        from processing.batch.pageviews_batch import build_tables
        rows = self.tables['top_pages_hourly'].filter("project = 'en' and access_variant = 'desktop'").collect()
        first = sorted((r.rank, r.page_title, r.views) for r in rows if str(r.hour_start) == '2025-12-31 23:00:00')
        self.assertEqual(first, [(1, 'Alpha', 12), (2, 'Beta', 10)])
        self.assertEqual(self.tables['top_pages_daily'].filter("project = 'en' and day = '2025-12-31' and rank = 1").first().views, 12)
        # Exclude the duplicate Alpha=2 record to construct an exact 10/10 tie.
        tied = self.curated.filter("not (page_title = 'Alpha' and views = 2)")
        ranked = build_tables(tied, top_k=2)['top_pages_hourly'].filter(
            "project = 'en' and access_variant = 'desktop' and hour_start = '2025-12-31 23:00:00'")
        self.assertEqual(sorted((r.rank, r.page_title) for r in ranked.collect()), [(1, 'Alpha'), (2, 'Beta')])

    def test_quality_accounts_for_every_row(self):
        counts = {r.status: r.rows for r in self.quality['summary'].collect()}
        self.assertEqual(counts['valid'], 10)
        self.assertEqual(counts['rejected'], 5)
        self.assertEqual(sum(counts.values()), self.raw.count())
        self.assertEqual(self.quality['duplicates'].count(), 1)

    def test_parquet_layouts_have_identical_content(self):
        from processing.batch.pageviews_batch import LAYOUTS
        with tempfile.TemporaryDirectory() as directory:
            loaded = []
            for name, columns in LAYOUTS.items():
                target = Path(directory, name).as_uri()
                self.curated.write.partitionBy(*columns).parquet(target)
                loaded.append(self.spark.read.parquet(target).select(*self.curated.columns))
            self.assertEqual(loaded[0].exceptAll(loaded[1]).count(), 0)
            self.assertEqual(loaded[1].exceptAll(loaded[0]).count(), 0)
            self.assertEqual(loaded[0].count(), 10)


if __name__ == '__main__':
    unittest.main()
