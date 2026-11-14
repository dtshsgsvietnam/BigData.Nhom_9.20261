"""Create curated pageviews and history aggregates from hourly gzip dumps."""

from __future__ import annotations

import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from processing.common.spark_pageviews import parse_raw_lines, read_raw_files


def run(input_glob: str, output_root: str, baseline_hours: int = 168) -> None:
    spark = SparkSession.builder.appName("wikipedia-pageviews-batch").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # Spark's text reader decompresses .gz inputs and retains the source object path.
    raw = read_raw_files(spark, input_glob)
    curated, rejected = parse_raw_lines(raw)
    curated.write.mode("overwrite").partitionBy("project", "hour_start").parquet(f"{output_root}/curated")
    rejected.write.mode("overwrite").parquet(f"{output_root}/quality/rejected")

    hourly = curated.groupBy("hour_start", "hour_end", "project", "access_variant").agg(
        F.sum("views").alias("total_views"),
        F.count(F.lit(1)).alias("page_count"),
        F.countDistinct("page_title").alias("unique_pages"),
    )
    daily = (
        curated.withColumn("day", F.to_date("hour_start"))
        .groupBy("day", "project", "access_variant")
        .agg(F.sum("views").alias("total_views"), F.countDistinct("page_title").alias("unique_pages"))
    )
    top_pages = (
        curated.groupBy("hour_start", "project", "access_variant", "page_title")
        .agg(F.sum("views").alias("views"))
        .withColumn("rank", F.row_number().over(Window.partitionBy("hour_start", "project", "access_variant").orderBy(F.desc("views"))))
        .filter(F.col("rank") <= 10)
    )

    history = Window.partitionBy("project", "access_variant", "page_title").orderBy("hour_start").rowsBetween(-baseline_hours, -1)
    attention = (
        curated.groupBy("hour_start", "project", "access_variant", "page_title")
        .agg(F.sum("views").alias("views"))
        .withColumn("historical_mean", F.avg("views").over(history))
        .withColumn("historical_std", F.stddev_pop("views").over(history))
        .withColumn("history_hours", F.count("views").over(history))
        .withColumn("score", (F.col("views") - F.col("historical_mean")) / F.greatest(F.coalesce("historical_std", F.lit(0.0)), F.lit(1.0)))
        .withColumn("metric_version", F.lit(f"zscore-{baseline_hours}h-v1"))
    )

    hourly.write.mode("overwrite").parquet(f"{output_root}/history/hourly")
    daily.write.mode("overwrite").parquet(f"{output_root}/history/daily")
    top_pages.write.mode("overwrite").parquet(f"{output_root}/history/top_pages_hourly")
    attention.write.mode("overwrite").parquet(f"{output_root}/history/attention")
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Glob for raw .gz files; supports s3a:// with Hadoop S3A config")
    parser.add_argument("--output", required=True, help="Output root for Parquet datasets")
    parser.add_argument("--baseline-hours", type=int, default=168)
    args = parser.parse_args()
    if args.baseline_hours < 1:
        parser.error("--baseline-hours must be >= 1")
    run(args.input, args.output, args.baseline_hours)


if __name__ == "__main__":
    main()
