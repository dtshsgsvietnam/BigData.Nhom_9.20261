"""Create curated, quality, ranking and traffic datasets from hourly dumps."""

from __future__ import annotations

import argparse
from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from processing.common.pageviews import ALLOWED_DOMAINS
from processing.common.spark_pageviews import parse_raw_lines, read_raw_files

LAYOUTS = {'a': ('project', 'event_date'), 'b': ('event_date', 'event_hour', 'project')}


def curated_columns(curated):
    return (curated.withColumn('event_date', F.to_date('hour_start'))
            .withColumn('event_hour', F.hour('hour_start')))


def build_tables(curated, baseline_hours=168, top_k=10):
    """Build tables without collecting page-level data on the driver."""
    if baseline_hours < 1 or top_k < 1:
        raise ValueError('baseline_hours and top_k must be positive')
    dimensions = ['project', 'access_variant']
    pages_hourly = curated.groupBy('hour_start', *dimensions, 'page_title').agg(F.sum('views').alias('views'))
    pages_daily = (pages_hourly.withColumn('day', F.to_date('hour_start'))
                   .groupBy('day', *dimensions, 'page_title').agg(F.sum('views').alias('views')))
    hourly = curated.groupBy('hour_start', 'hour_end', *dimensions).agg(
        F.sum('views').alias('total_views'), F.count('*').alias('page_count'),
        F.countDistinct('page_title').alias('unique_pages'))
    daily = (curated.withColumn('day', F.to_date('hour_start')).groupBy('day', *dimensions)
             .agg(F.sum('views').alias('total_views'), F.countDistinct('page_title').alias('unique_pages'),
                  F.countDistinct('hour_start').alias('observed_hours'))
             .withColumn('expected_hours', F.lit(24)))
    weekly = (curated.withColumn('week_start', F.to_date(F.date_trunc('week', 'hour_start')))
              .groupBy('week_start', *dimensions)
              .agg(F.sum('views').alias('total_views'), F.countDistinct('page_title').alias('unique_pages'),
                   F.countDistinct('hour_start').alias('observed_hours'))
              .withColumn('expected_hours', F.lit(168)))

    def rank(frame, period):
        window = Window.partitionBy(period, *dimensions).orderBy(F.desc('views'), F.asc('page_title'))
        return frame.withColumn('rank', F.row_number().over(window)).filter(F.col('rank') <= top_k)

    history = (Window.partitionBy(*dimensions, 'page_title').orderBy('hour_start')
               .rowsBetween(-baseline_hours, -1))
    attention = (pages_hourly.withColumn('historical_mean', F.avg('views').over(history))
                 .withColumn('historical_std', F.stddev_pop('views').over(history))
                 .withColumn('history_hours', F.count('views').over(history))
                 .withColumn('score', (F.col('views') - F.col('historical_mean')) /
                             F.greatest(F.coalesce('historical_std', F.lit(0.0)), F.lit(1.0)))
                 .withColumn('metric_version', F.lit(f'zscore-{baseline_hours}-observations-v1')))
    return {'hourly': hourly, 'daily': daily, 'weekly': weekly,
            'top_pages_hourly': rank(pages_hourly, 'hour_start'),
            'top_pages_daily': rank(pages_daily, 'day'), 'attention': attention}


def quality_tables(raw, curated, rejected):
    """Account for every input row; missing hours are reported by manifests."""
    excluded = raw.filter(~F.split('raw_line', ' ').getItem(0).isin(*sorted(ALLOWED_DOMAINS)))
    counts = []
    for frame, status in ((curated, 'valid'), (rejected, 'rejected'), (excluded, 'excluded')):
        counts.append(frame.groupBy('source_file').count().withColumn('status', F.lit(status)))
    counts = counts[0].unionByName(counts[1]).unionByName(counts[2])
    return {
        'source_counts': counts,
        'summary': counts.groupBy('status').agg(F.sum('count').alias('rows')),
        'reject_reasons': rejected.groupBy('source_file', 'reject_reason').count(),
        'duplicates': curated.groupBy('hour_start', 'domain_code', 'page_title').count().filter(F.col('count') > 1),
    }


def run(input_glob, output_root, baseline_hours=168, layout='a', top_k=10):
    if layout not in (*LAYOUTS, 'both') or baseline_hours < 1 or top_k < 1:
        raise ValueError('Invalid layout, baseline_hours or top_k')
    if output_root.rstrip('/\\') == input_glob.rstrip('/\\'):
        raise ValueError('Input and output must be different paths')
    spark = SparkSession.builder.appName('wikipedia-pageviews-batch').getOrCreate()
    raw = curated = rejected = None
    try:
        spark.conf.set('spark.sql.session.timeZone', 'UTC')
        raw = read_raw_files(spark, input_glob).persist(StorageLevel.MEMORY_AND_DISK)
        curated, rejected = parse_raw_lines(raw)
        curated = curated_columns(curated).persist(StorageLevel.MEMORY_AND_DISK)
        rejected = rejected.persist(StorageLevel.MEMORY_AND_DISK)
        layouts = ('a', 'b') if layout == 'both' else (layout,)
        for selected in layouts:
            suffix = f'/layout_{selected}' if layout == 'both' else ''
            (curated.write.mode('overwrite').partitionBy(*LAYOUTS[selected])
             .parquet(f'{output_root}/curated{suffix}'))
        rejected.write.mode('overwrite').parquet(f'{output_root}/quality/rejected')
        for name, frame in quality_tables(raw, curated, rejected).items():
            frame.write.mode('overwrite').json(f'{output_root}/quality/{name}')
        for name, frame in build_tables(curated, baseline_hours, top_k).items():
            frame.write.mode('overwrite').parquet(f'{output_root}/history/{name}')
    finally:
        for frame in (raw, curated, rejected):
            if frame is not None:
                frame.unpersist()
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True, help='Raw file glob, local or s3a://')
    parser.add_argument('--output', required=True)
    parser.add_argument('--baseline-hours', type=int, default=168, help='Previous observed page-hours')
    parser.add_argument('--top-k', type=int, default=10)
    parser.add_argument('--layout', choices=('a', 'b', 'both'), default='a')
    args = parser.parse_args()
    if args.baseline_hours < 1 or args.top_k < 1:
        parser.error('--baseline-hours and --top-k must be >= 1')
    run(args.input, args.output, args.baseline_hours, args.layout, args.top_k)


if __name__ == '__main__':
    main()
