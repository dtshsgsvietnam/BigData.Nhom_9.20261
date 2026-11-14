"""Spark SQL implementation of the shared Wikimedia record contract.

Keeping this in Spark SQL (rather than a Python UDF) lets batch and replay use
the same normalization without moving each record through Python.
"""

from pyspark.sql import functions as F

from processing.common.pageviews import ALLOWED_DOMAINS, SCHEMA_VERSION


def read_raw_files(spark, input_glob):
    """Read each source in order and attach a stable, one-based line number.

    zipWithIndex counts records in file order, independent of task partition IDs.
    Paths must name files (local or Hadoop-supported storage), not directories.
    """
    jvm = spark.sparkContext._jvm
    pattern = jvm.org.apache.hadoop.fs.Path(input_glob)
    fs = pattern.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    matches = fs.globStatus(pattern)
    paths = sorted(str(item.getPath()) for item in (matches or []) if item.isFile())
    if not paths:
        raise ValueError(f"No input files matched {input_glob!r}")
    names = [path.rsplit("/", 1)[-1] for path in paths]
    if len(set(names)) != len(names):
        raise ValueError("Source filenames must be unique within a snapshot")
    sources = []
    for path, name in zip(paths, names):
        sources.append(
            spark.sparkContext.textFile(path).zipWithIndex().map(
                lambda pair, source=name: (pair[0], source, pair[1] + 1)
            )
        )
    return spark.createDataFrame(
        spark.sparkContext.union(sources),
        "raw_line string, source_file string, source_line long",
    )


def parse_raw_lines(raw):
    """Return (valid, rejected) DataFrames; input includes original source_line."""
    if "source_line" not in raw.columns:
        raise ValueError("raw input must include a stable source_line from the source reader")
    staged = raw
    fields = F.split(F.col("raw_line"), " ")
    staged = (
        staged.withColumn("field_count", F.size(fields))
        .withColumn("domain_code", fields.getItem(0))
        .withColumn("page_title", F.get(fields, 1))
        .withColumn("raw_views", F.get(fields, 2))
        .withColumn("legacy_response_size", F.get(fields, 3))
        .withColumn("views", F.when(F.col('raw_views').rlike(r'^-?[0-9]+$'), F.expr('try_cast(raw_views as bigint)')))
        .withColumn("hour_end_text", F.regexp_extract("source_file", r"^pageviews-(\d{8})-(\d{2})0000(?:\.gz)?$", 0))
        .withColumn(
            "hour_end",
            F.try_to_timestamp(
                F.regexp_replace("hour_end_text", r"pageviews-(\d{8})-(\d{2})0000(?:\.gz)?", "$1$2"),
                F.lit("yyyyMMddHH"),
            ),
        )
        .withColumn("hour_start", F.expr("hour_end - INTERVAL 1 HOUR"))
        .withColumn("project", F.split("domain_code", r"\.").getItem(0))
        .withColumn(
            "access_variant",
            F.when(F.col("domain_code").endswith(".m"), F.lit("mobile")).otherwise(F.lit("desktop")),
        )
        .withColumn("schema_version", F.lit(SCHEMA_VERSION))
        .withColumn(
            "event_id",
            F.sha2(F.concat_ws("\u001f", "source_file", "source_line", "domain_code", "page_title"), 256),
        )
    )
    valid_shape = F.col("field_count").isin(3, 4)
    valid_project = F.col("domain_code").isin(*sorted(ALLOWED_DOMAINS))
    valid_values = (
        F.col("page_title").isNotNull()
        & (F.length("page_title") > 0)
        & F.col("views").isNotNull()
        & (F.col("views") >= 0)
        & F.col("hour_end").isNotNull()
    )
    valid = staged.filter(valid_shape & valid_project & valid_values).select(
        "hour_start", "hour_end", "domain_code", "project", "access_variant", "page_title",
        "views", "legacy_response_size", "source_file", "source_line", "event_id", "schema_version",
    )
    rejected = (
        staged.filter(valid_project & (~valid_shape | ~valid_values))
        .withColumn(
            "reject_reason",
            F.when(~valid_shape, F.lit("invalid_field_count"))
            .when(F.col("page_title").isNull() | (F.length("page_title") == 0), F.lit("empty_title"))
            .when(F.col("views").isNull(), F.lit("invalid_views"))
            .when(F.col("views") < 0, F.lit("negative_views"))
            .when(F.col("hour_end").isNull(), F.lit("unrecognized_source_filename"))
            .otherwise(F.lit("invalid_record")),
        )
        .select("source_file", "source_line", "raw_line", "reject_reason")
    )
    return valid, rejected
