"""
Daily PySpark Interview Practice — 2026-11-11
Topic: Effective-dated temporal joins (point-in-time dimension lookup)

Problem
-------
An order event must be priced using the product price that was valid at the
event timestamp. Price history is an SCD-style dimension with half-open
validity intervals: [valid_from, valid_to). A NULL valid_to means the version
is still current. Preserve events that have no matching price.

Run:
    spark-submit daily/2026-11-11-effective-dated-temporal-joins.py

Compatible with Spark 3.x. No external files or services are required.
"""
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)


def build_sample_data(spark):
    """Create a small order-event fact and an effective-dated price dimension."""
    event_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("product_id", StringType(), True),
        StructField("event_ts", TimestampType(), True),
        StructField("quantity", IntegerType(), False),
    ])
    event_rows = [
        ("E001", "P100", datetime(2026, 11, 4, 12, 0, 0), 2),
        ("E002", "P100", datetime(2026, 11, 5, 0, 0, 0), 1),  # exact boundary
        ("E003", "P100", datetime(2026, 11, 10, 0, 0, 0), 3), # exact boundary
        ("E004", "P200", datetime(2026, 11, 7, 18, 30, 0), 4),
        ("E005", "P200", datetime(2026, 11, 9, 9, 15, 0), 2),
        ("E006", "P300", datetime(2026, 11, 9, 10, 0, 0), 1), # no history
        ("E007", "P100", None, 5),                            # invalid event time
    ]
    events = spark.createDataFrame(event_rows, event_schema)

    price_schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("price_version", IntegerType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("valid_from", TimestampType(), False),
        StructField("valid_to", TimestampType(), True),
    ])
    price_rows = [
        ("P100", 1, 10.00, datetime(2026, 11, 1), datetime(2026, 11, 5)),
        ("P100", 2, 11.00, datetime(2026, 11, 5), datetime(2026, 11, 10)),
        ("P100", 3, 12.00, datetime(2026, 11, 10), None),
        ("P200", 1, 25.00, datetime(2026, 11, 1), datetime(2026, 11, 8)),
        ("P200", 2, 24.00, datetime(2026, 11, 8), None),
    ]
    prices = spark.createDataFrame(price_rows, price_schema)
    return events, prices


def temporal_join(events, prices):
    """Readable baseline: equi-key plus a point-in-interval predicate."""
    e = events.alias("e")
    p = prices.alias("p")
    condition = (
        (F.col("e.product_id") == F.col("p.product_id"))
        & F.col("e.event_ts").isNotNull()
        & (F.col("e.event_ts") >= F.col("p.valid_from"))
        & (
            F.col("p.valid_to").isNull()
            | (F.col("e.event_ts") < F.col("p.valid_to"))
        )
    )
    return (
        e.join(p, condition, "left")
        .select(
            F.col("e.event_id"),
            F.col("e.product_id"),
            F.col("e.event_ts"),
            F.col("e.quantity"),
            F.col("p.price_version"),
            F.col("p.unit_price"),
            (F.col("e.quantity") * F.col("p.unit_price")).alias("order_value"),
        )
    )


def optimized_temporal_join(events, prices):
    """
    Optimized for a genuinely small price dimension:
    project only required columns and broadcast it. The product equality
    enables a broadcast hash join; the timestamp range remains a residual
    condition. Do not force broadcast when the dimension is large.
    """
    event_projection = events.select(
        "event_id", "product_id", "event_ts", "quantity"
    )
    price_projection = prices.select(
        "product_id", "price_version", "unit_price", "valid_from", "valid_to"
    )
    e = event_projection.alias("e")
    p = F.broadcast(price_projection).alias("p")

    condition = (
        (F.col("e.product_id") == F.col("p.product_id"))
        & F.col("e.event_ts").isNotNull()
        & (F.col("e.event_ts") >= F.col("p.valid_from"))
        & (
            F.col("p.valid_to").isNull()
            | (F.col("e.event_ts") < F.col("p.valid_to"))
        )
    )
    return (
        e.join(p, condition, "left")
        .select(
            F.col("e.event_id"),
            F.col("e.product_id"),
            F.col("e.event_ts"),
            F.col("e.quantity"),
            F.col("p.price_version"),
            F.col("p.unit_price"),
            (F.col("e.quantity") * F.col("p.unit_price")).alias("order_value"),
        )
    )


def main():
    spark = (
        SparkSession.builder
        .appName("EffectiveDatedTemporalJoins")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    events, prices = build_sample_data(spark)

    # Q1. Validate basic dimension constraints before joining.
    invalid_intervals = prices.where(
        F.col("valid_to").isNotNull()
        & (F.col("valid_to") <= F.col("valid_from"))
    )
    duplicate_versions = (
        prices.groupBy("product_id", "price_version")
        .count()
        .where(F.col("count") > 1)
    )
    assert invalid_intervals.count() == 0
    assert duplicate_versions.count() == 0

    # Q2. Perform the baseline point-in-time lookup.
    baseline = temporal_join(events, prices)

    # Q3. Apply the broadcast/projection optimization where the dimension is small.
    optimized = optimized_temporal_join(events, prices)

    # Q4. Prove that the optimized path preserves baseline results.
    compare_cols = ["event_id", "price_version", "unit_price"]
    assert (
        baseline.select(*compare_cols)
        .exceptAll(optimized.select(*compare_cols))
        .count()
        == 0
    )
    assert (
        optimized.select(*compare_cols)
        .exceptAll(baseline.select(*compare_cols))
        .count()
        == 0
    )

    # Q5. Assert one output row per event. This also detects overlapping
    # validity intervals that happen to match the supplied event timestamps.
    assert optimized.count() == events.count()
    assert optimized.groupBy("event_id").count().where(
        F.col("count") > 1
    ).count() == 0

    actual = {
        row["event_id"]: (
            row["price_version"],
            None if row["unit_price"] is None else round(row["unit_price"], 2),
        )
        for row in optimized.select(
            "event_id", "price_version", "unit_price"
        ).collect()
    }
    expected = {
        "E001": (1, 10.00),
        "E002": (2, 11.00),
        "E003": (3, 12.00),
        "E004": (1, 25.00),
        "E005": (2, 24.00),
        "E006": (None, None),
        "E007": (None, None),
    }
    assert actual == expected, f"Unexpected temporal lookup results: {actual}"

    print("Point-in-time price lookup:")
    optimized.orderBy("event_id").show(truncate=False)

    print("Optimized physical plan:")
    optimized.explain("formatted")

    print("\nInterview follow-ups:")
    print("1. Why use [valid_from, valid_to) instead of inclusive bounds on both ends?")
    print("2. How would you detect all overlapping intervals, not only overlaps hit by sample events?")
    print("3. What changes if the effective-dated dimension is too large to broadcast?")
    print("4. How would you handle late-arriving corrections to historical price versions?")
    print("5. How would you make a daily point-in-time enrichment reproducible?")
    print("6. Which metrics would you monitor for unmatched events and interval quality?")

    spark.stop()


if __name__ == "__main__":
    main()
