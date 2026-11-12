"""
Daily PySpark Interview Practice — 2026-11-12
Topic: Skew-aware two-phase aggregation with deterministic salting

Problem statement
-----------------
A clickstream table is heavily skewed: one tenant contributes most events.
Build event counts and gross revenue by tenant and event type. First implement
the straightforward groupBy, then reduce hot-key reducer pressure with a
two-phase salted aggregation. Both paths must return exactly the same result.

Run:
    spark-submit daily/2026-11-12-skew-aware-aggregations.py

Compatible with Spark 3.x. The sample is generated in memory; no external files
or services are required.
"""
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


SALT_BUCKETS = 8


def build_sample_events(spark):
    """Create a deliberately skewed but deterministic clickstream sample."""
    schema = StructType(
        [
            StructField("event_seq", LongType(), nullable=False),
            StructField("event_id", StringType(), nullable=False),
            StructField("tenant_id", StringType(), nullable=False),
            StructField("event_type", StringType(), nullable=False),
            StructField("amount_cents", LongType(), nullable=False),
            StructField("event_ts", TimestampType(), nullable=False),
        ]
    )

    rows = []
    sequence = 0
    start = datetime(2026, 11, 12, 8, 0, 0)

    # 90% of the rows belong to TENANT_HOT to simulate a dominant customer.
    for tenant_id, row_count in [
        ("TENANT_HOT", 900),
        ("TENANT_A", 50),
        ("TENANT_B", 50),
    ]:
        for i in range(row_count):
            is_purchase = i % 4 == 0
            event_type = "purchase" if is_purchase else "view"
            # Views have no revenue; purchase values are stored in integer cents.
            amount_cents = 100 + (i % 11) * 10 if is_purchase else 0
            rows.append(
                (
                    sequence,
                    f"{tenant_id}-{i:04d}",
                    tenant_id,
                    event_type,
                    amount_cents,
                    start + timedelta(seconds=sequence),
                )
            )
            sequence += 1

    return spark.createDataFrame(rows, schema)


def aggregate_events(df):
    """Baseline: one aggregation at the requested business grain."""
    return (
        df.groupBy("tenant_id", "event_type")
        .agg(
            F.count(F.lit(1)).alias("event_count"),
            F.sum("amount_cents").cast("long").alias("gross_revenue_cents"),
        )
    )


def salted_aggregate_events(df):
    """
    Optimized path:
      1. Assign each event deterministically to one of N salt buckets.
      2. Aggregate by tenant, event type, and salt.
      3. Merge the small partial aggregates back to the business grain.
    """
    salted = df.withColumn(
        "salt",
        F.pmod(F.xxhash64("event_id"), F.lit(SALT_BUCKETS)).cast("int"),
    )

    partial = (
        salted.groupBy("tenant_id", "event_type", "salt")
        .agg(
            F.count(F.lit(1)).alias("partial_event_count"),
            F.sum("amount_cents").cast("long").alias("partial_revenue_cents"),
        )
    )

    final = (
        partial.groupBy("tenant_id", "event_type")
        .agg(
            F.sum("partial_event_count").cast("long").alias("event_count"),
            F.sum("partial_revenue_cents")
            .cast("long")
            .alias("gross_revenue_cents"),
        )
    )
    return salted, partial, final


def main():
    spark = (
        SparkSession.builder
        .appName("SkewAwareTwoPhaseAggregation")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    events = build_sample_events(spark)

    # Q1. Quantify the input skew before choosing an optimization.
    tenant_sizes = (
        events.groupBy("tenant_id")
        .count()
        .orderBy(F.desc("count"), "tenant_id")
    )
    tenant_sizes.show(truncate=False)

    hot_tenant_rows = (
        events.where(F.col("tenant_id") == "TENANT_HOT").count()
    )
    total_rows = events.count()
    assert total_rows == 1000
    assert hot_tenant_rows == 900
    assert hot_tenant_rows / total_rows == 0.90

    # Q2. Baseline aggregation.
    baseline = aggregate_events(events)

    # Q3. Salt the hot-key workload and merge partial aggregates.
    salted_events, partial, optimized = salted_aggregate_events(events)

    # Q4. Verify that the salt key actually spreads the hot tenant in this sample.
    hot_salt_distribution = (
        salted_events.where(F.col("tenant_id") == "TENANT_HOT")
        .groupBy("salt")
        .count()
        .orderBy("salt")
    )
    hot_salt_distribution.show(truncate=False)
    used_salt_buckets = hot_salt_distribution.count()
    largest_salt_bucket = (
        hot_salt_distribution.agg(F.max("count").alias("max_bucket_rows"))
        .first()["max_bucket_rows"]
    )
    assert used_salt_buckets > 1
    assert largest_salt_bucket < hot_tenant_rows

    # Q5. Exact equivalence: counts and integer-cents sums must match both ways.
    compare_columns = [
        "tenant_id",
        "event_type",
        "event_count",
        "gross_revenue_cents",
    ]
    baseline_result = baseline.select(*compare_columns)
    optimized_result = optimized.select(*compare_columns)

    assert baseline_result.exceptAll(optimized_result).count() == 0
    assert optimized_result.exceptAll(baseline_result).count() == 0

    # Q6. Validate independently known expected aggregates from the sample design.
    expected = {
        ("TENANT_HOT", "purchase"): (225, 33680),
        ("TENANT_HOT", "view"): (675, 0),
        ("TENANT_A", "purchase"): (13, 1890),
        ("TENANT_A", "view"): (37, 0),
        ("TENANT_B", "purchase"): (13, 1890),
        ("TENANT_B", "view"): (37, 0),
    }
    actual = {
        (row["tenant_id"], row["event_type"]): (
            row["event_count"],
            row["gross_revenue_cents"],
        )
        for row in optimized_result.collect()
    }
    assert actual == expected, f"Unexpected aggregates: {actual}"

    # Average is derived from additive measures; do not average partial averages.
    display_result = optimized_result.withColumn(
        "average_cents_per_event",
        F.round(
            F.col("gross_revenue_cents") / F.col("event_count"),
            2,
        ),
    )

    print("Baseline aggregation:")
    baseline_result.orderBy("tenant_id", "event_type").show(truncate=False)

    print("Optimized salted aggregation:")
    display_result.orderBy("tenant_id", "event_type").show(truncate=False)

    print("Partial aggregation rows:", partial.count())
    print("Hot-tenant salt buckets used:", used_salt_buckets)
    print("Largest hot-tenant salt bucket:", largest_salt_bucket)

    # Compare physical plans to inspect exchanges and aggregation stages.
    print("Baseline physical plan:")
    baseline_result.explain("formatted")

    print("Salted aggregation physical plan:")
    display_result.explain("formatted")

    print("\nOptimization notes:")
    print("1. A hot (tenant_id, event_type) key can concentrate final aggregation work.")
    print("2. Salting increases the first-stage grouping cardinality but distributes")
    print("   the hot tenant across multiple (tenant, event_type, salt) keys.")
    print("3. The second stage merges only the compact partial aggregates.")
    print("4. Use stable, high-cardinality identifiers for deterministic salt assignment.")
    print("5. Choose the bucket count from observed skew and cluster parallelism;")
    print("   excessive buckets add shuffle and merge overhead.")
    print("6. Spark already performs map-side partial aggregation. Salting targets")
    print("   the remaining hot reducer key; it is not automatically faster.")
    print("7. AQE helps with several skewed join and shuffle scenarios, but do not")
    print("   assume it will split every single hot aggregation key for you.")
    print("8. Counts and sums are safely mergeable. Exact distinct counts need a")
    print("   different merge strategy; never sum per-bucket distinct counts blindly.")

    print("\nInterview follow-ups:")
    print("1. How would you identify a skewed aggregation from Spark UI task metrics?")
    print("2. Why is average-of-averages incorrect when bucket sizes differ?")
    print("3. Which aggregate functions can be safely combined in two stages?")
    print("4. How would you choose salt bucket count for a 2 TB fact table?")
    print("5. When would AQE, repartitioning, or salting be the better first option?")
    print("6. How would you handle exact countDistinct for a hot tenant?")
    print("7. What correctness risks arise if the salt expression is nondeterministic?")

    print("\nAll expected-result and baseline-equivalence assertions passed.")
    spark.stop()


if __name__ == "__main__":
    main()
