"""Compare two curated layouts with the same filtered aggregation workload."""

import argparse
import json
from pathlib import Path
from statistics import median
import time

from pyspark.sql import SparkSession, functions as F


def benchmark(spark, paths, project, day, repeats):
    if repeats < 2:
        raise ValueError('Use at least two measured repetitions')
    timings = {name: [] for name in paths}
    expected = None
    # Alternate layout order to reduce fixed first-run bias. Clear Spark cache;
    # operating-system/object-store caches are explicitly not controlled here.
    for iteration in range(repeats + 1):
        order = list(paths) if iteration % 2 == 0 else list(reversed(paths))
        for name in order:
            spark.catalog.clearCache()
            started = time.perf_counter()
            result = (spark.read.parquet(paths[name])
                      .filter((F.col('project') == project) & (F.col('event_date') == day))
                      .agg(F.count('*').alias('rows'), F.sum('views').alias('views')).first().asDict())
            seconds = time.perf_counter() - started
            if expected is None:
                expected = result
            if result != expected:
                raise ValueError('Layout query results differ; benchmark is invalid')
            if iteration:
                timings[name].append(seconds)
    return {'query': {'project': project, 'event_date': day, 'aggregation': 'count(*), sum(views)'},
            'result': expected, 'repeats': repeats, 'warmup_runs_per_layout': 1,
            'cache_policy': 'Spark cache cleared; OS and object-store caches uncontrolled',
            'spark_version': spark.version, 'master': spark.sparkContext.master,
            'resources': {key: spark.sparkContext.getConf().get(key, 'not explicitly set')
                          for key in ('spark.executor.instances', 'spark.executor.cores',
                                      'spark.executor.memory', 'spark.driver.memory')},
            'paths': paths, 'timings_seconds': timings,
            'median_seconds': {name: median(values) for name, values in timings.items()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--layout-a', required=True)
    parser.add_argument('--layout-b', required=True)
    parser.add_argument('--project', choices=('en', 'vi'), default='en')
    parser.add_argument('--day', required=True)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    if args.repeats < 2:
        parser.error('--repeats must be >= 2')
    spark = SparkSession.builder.appName('pageviews-layout-benchmark').getOrCreate()
    try:
        spark.conf.set('spark.sql.session.timeZone', 'UTC')
        report = benchmark(spark, {'a': args.layout_a, 'b': args.layout_b}, args.project, args.day, args.repeats)
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
