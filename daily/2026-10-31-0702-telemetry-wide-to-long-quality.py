"""
Daily Data Engineering Practice | 2026-10-31 07:02 IST
Topic: Wide-to-Long Telemetry Normalization, Threshold Validation & Pivoting

Run:
    spark-submit daily/2026-10-31-0702-telemetry-wide-to-long-quality.py

Requirements:
    PySpark 3.3+

Scenario:
A device platform receives a wide telemetry record with one column per metric.
Normalize it into a metric-grain fact table, validate values against a small
reference/threshold dimension, calculate event-time quality summaries, and
reconstruct the original wide shape for downstream consumers.

This script uses deterministic in-memory data; no external files or services.
"""

from datetime import datetime

from pyspark.sql import SparkSession, functions as F, types as T


METRICS = ["cpu_pct", "memory_pct", "disk_pct", "network_kbps"]


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("TelemetryWideToLongQualityPractice")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # 1. Realistic source data: one row per device event, metrics are wide.
    # A null memory reading is intentional and must remain observable.
    # ------------------------------------------------------------------
    source_schema = T.StructType([
        T.StructField("device_id", T.StringType(), False),
        T.StructField("event_ts", T.TimestampType(), False),
        T.StructField("cpu_pct", T.DoubleType(), True),
        T.StructField("memory_pct", T.DoubleType(), True),
        T.StructField("disk_pct", T.DoubleType(), True),
        T.StructField("network_kbps", T.DoubleType(), True),
    ])

    source_rows = [
        ("device-A", datetime(2026, 10, 31, 8, 5), 75.0, 82.0, 91.0, 1200.0),
        ("device-A", datetime(2026, 10, 31, 8, 35), 105.0, None, 88.0, 1300.0),
        ("device-B", datetime(2026, 10, 31, 9, 10), 42.0, 65.0, 101.0, 500.0),
    ]
    wide_df = spark.createDataFrame(source_rows, source_schema)

    print("\n=== SOURCE: WIDE DEVICE EVENTS ===")
    wide_df.orderBy("device_id", "event_ts").show(truncate=False)

    # ------------------------------------------------------------------
    # 2. Unpivot using an array of structs + explode.
    # Unlike filtering nulls, this preserves missing measurements as rows.
    # The metric list is explicit so the output contract is deterministic.
    # ------------------------------------------------------------------
    metric_structs = [
        F.struct(
            F.lit(metric).alias("metric"),
            F.col(metric).cast("double").alias("value"),
        )
        for metric in METRICS
    ]

    long_df = (
        wide_df
        .select(
            "device_id",
            "event_ts",
            F.explode(F.array(*metric_structs)).alias("metric_record"),
        )
        .select(
            "device_id",
            "event_ts",
            F.col("metric_record.metric").alias("metric"),
            F.col("metric_record.value").alias("value"),
        )
    )

    assert long_df.count() == len(source_rows) * len(METRICS) == 12
    print("\n=== NORMALIZED: ONE ROW PER DEVICE + EVENT + METRIC ===")
    long_df.orderBy("device_id", "event_ts", "metric").show(30, truncate=False)

    # ------------------------------------------------------------------
    # 3. Reference-driven validation.
    # Thresholds are data, not hard-coded branches in the transformation.
    # A missing value receives MISSING rather than disappearing from metrics.
    # ------------------------------------------------------------------
    threshold_schema = T.StructType([
        T.StructField("metric", T.StringType(), False),
        T.StructField("lower_bound", T.DoubleType(), False),
        T.StructField("upper_bound", T.DoubleType(), False),
        T.StructField("unit", T.StringType(), False),
    ])
    threshold_rows = [
        ("cpu_pct", 0.0, 100.0, "percent"),
        ("memory_pct", 0.0, 95.0, "percent"),
        ("disk_pct", 0.0, 100.0, "percent"),
        ("network_kbps", 0.0, 10000.0, "kbps"),
    ]
    thresholds_df = spark.createDataFrame(threshold_rows, threshold_schema)

    validated_df = (
        long_df
        .join(F.broadcast(thresholds_df), on="metric", how="left")
        .withColumn(
            "quality_status",
            F.when(F.col("lower_bound").isNull(), F.lit("UNKNOWN_METRIC"))
             .when(F.col("value").isNull(), F.lit("MISSING"))
             .when(F.col("value") < F.col("lower_bound"), F.lit("BELOW_RANGE"))
             .when(F.col("value") > F.col("upper_bound"), F.lit("ABOVE_RANGE"))
             .otherwise(F.lit("OK")),
        )
    )

    status_counts = {
        row["quality_status"]: row["count"]
        for row in validated_df.groupBy("quality_status").count().collect()
    }
    expected_status_counts = {"OK": 9, "MISSING": 1, "ABOVE_RANGE": 2}
    assert status_counts == expected_status_counts, status_counts
    assert validated_df.count() == 12

    print("\n=== VALIDATED METRIC FACT ===")
    validated_df.orderBy("device_id", "event_ts", "metric").show(30, truncate=False)
    print("Quality status counts:", status_counts)

    # ------------------------------------------------------------------
    # 4. Device-event completeness: compare observed metric rows with the
    # expected metric contract. This is safer than dropping null measurements.
    # ------------------------------------------------------------------
    event_quality_df = (
        validated_df
        .groupBy("device_id", "event_ts")
        .agg(
            F.count(F.lit(1)).alias("expected_metric_count"),
            F.sum(F.when(F.col("value").isNotNull(), 1).otherwise(0))
             .alias("present_metric_count"),
            F.sum(F.when(F.col("quality_status") == "MISSING", 1).otherwise(0))
             .alias("missing_metric_count"),
        )
        .withColumn(
            "completeness_pct",
            F.round(
                F.col("present_metric_count") * F.lit(100.0)
                / F.col("expected_metric_count"),
                2,
            ),
        )
    )

    incomplete = event_quality_df.filter(F.col("completeness_pct") < 100)
    assert incomplete.count() == 1
    assert incomplete.first()["missing_metric_count"] == 1

    print("\n=== EVENT-LEVEL COMPLETENESS ===")
    event_quality_df.orderBy("device_id", "event_ts").show(truncate=False)

    # ------------------------------------------------------------------
    # 5. Event-time rollups by device, hour, and metric.
    # Windowing happens after normalization, so adding a metric does not
    # require duplicating aggregation logic.
    # ------------------------------------------------------------------
    hourly_metrics_df = (
        validated_df
        .groupBy(
            "device_id",
            F.window("event_ts", "1 hour").alias("hour_window"),
            "metric",
        )
        .agg(
            F.avg("value").alias("avg_value"),
            F.max("value").alias("max_value"),
            F.count(F.lit(1)).alias("sample_count"),
            F.sum(F.when(F.col("value").isNull(), 1).otherwise(0))
             .alias("missing_count"),
            F.sum(F.when(F.col("quality_status") != "OK", 1).otherwise(0))
             .alias("non_ok_count"),
        )
    )

    assert hourly_metrics_df.count() == 8  # 4 metrics for each device-hour.
    print("\n=== DEVICE-HOUR METRIC QUALITY ===")
    hourly_metrics_df.orderBy("device_id", "hour_window.start", "metric").show(
        30, truncate=False
    )

    # ------------------------------------------------------------------
    # 6. Pivot back to the original wide contract.
    # The explicit pivot values avoid Spark's distinct-value discovery job.
    # first(..., ignorenulls=False) preserves a null measurement as null.
    # ------------------------------------------------------------------
    reconstructed_df = (
        long_df
        .groupBy("device_id", "event_ts")
        .pivot("metric", METRICS)
        .agg(F.first("value", ignorenulls=False))
    )

    assert reconstructed_df.count() == 3
    second_a = reconstructed_df.filter(
        (F.col("device_id") == "device-A")
        & (F.col("event_ts") == F.lit(datetime(2026, 10, 31, 8, 35)))
    ).first()
    assert second_a["cpu_pct"] == 105.0
    assert second_a["memory_pct"] is None

    print("\n=== RECONSTRUCTED WIDE CONTRACT ===")
    reconstructed_df.orderBy("device_id", "event_ts").show(truncate=False)

    # ------------------------------------------------------------------
    # 7. Compact rule metrics for monitoring / alerting.
    # ------------------------------------------------------------------
    rule_metrics_df = (
        validated_df
        .groupBy("metric")
        .agg(
            F.count(F.lit(1)).alias("received_samples"),
            F.sum(F.when(F.col("quality_status") == "OK", 1).otherwise(0))
             .alias("ok_samples"),
            F.sum(F.when(F.col("quality_status") == "MISSING", 1).otherwise(0))
             .alias("missing_samples"),
            F.sum(F.when(F.col("quality_status") == "ABOVE_RANGE", 1).otherwise(0))
             .alias("above_range_samples"),
            F.round(F.avg("value"), 2).alias("average_value"),
        )
        .withColumn(
            "ok_rate_pct",
            F.round(F.col("ok_samples") * 100.0 / F.col("received_samples"), 2),
        )
    )

    print("\n=== METRIC-LEVEL QUALITY SCORECARD ===")
    rule_metrics_df.orderBy("metric").show(truncate=False)

    # Physical-plan inspection: look for the broadcast threshold join and
    # exchanges introduced by grouping/pivoting.
    print("\n=== PHYSICAL PLAN: VALIDATION ===")
    validated_df.explain(mode="formatted")

    # Interview / correctness checks.
    assert set(status_counts) == {"OK", "MISSING", "ABOVE_RANGE"}
    assert long_df.select("device_id", "event_ts", "metric").distinct().count() == 12
    print("\nALL ASSERTIONS PASSED")

    spark.stop()


if __name__ == "__main__":
    main()


# ----------------------------------------------------------------------
# OPTIMIZATION GUIDANCE
# 1. Use explode(array(struct(...))) for a portable, explicit unpivot; retain
#    null values when missingness is a data-quality signal.
# 2. Broadcast only a genuinely small metric-dimension table. Check the plan
#    rather than assuming the hint is always beneficial.
# 3. Keep the metric contract explicit. For rapidly changing schemas, generate
#    the struct array from governed metadata and validate unknown columns.
# 4. Pivot with an explicit value list to avoid a separate distinct-value scan.
# 5. Avoid repeated count()/show() actions in production. Persist only if the
#    normalized frame is reused enough to offset cache cost.
# 6. For high-volume telemetry, partition storage by a coarse event date and
#    cluster/order by device or metric according to dominant query patterns.
# 7. Set the Spark session time zone deliberately (for example UTC) before
#    event-time windowing; convert source timestamps at ingestion boundaries.
#
# INTERVIEW FOLLOW-UPS
# 1. Why preserve null metric rows instead of filtering them before explode?
# 2. When would stack() be preferable to array(struct(...))?
# 3. How would you handle a metric introduced by a new producer version?
# 4. How would you keep the threshold dimension effective-dated?
# 5. What are the memory and shuffle implications of a wide pivot?
# 6. How would you store the normalized fact in Delta and evolve its schema?
# 7. How would you separate late telemetry from invalid telemetry?
