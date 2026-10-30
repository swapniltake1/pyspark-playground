"""
Skew-aware joins with deterministic salting
===========================================

Practice scenario:
A transaction fact table is heavily concentrated on one merchant key. Compare
a regular join with a salting strategy that spreads the hot key across several
join buckets while preserving the result.

Run:
    spark-submit daily/2026-10-30-2300-skew-aware-joins.py

The in-memory data is intentionally small. It validates correctness, not
production performance. On a small dimension, a broadcast join is often the
best first choice; salting is useful when neither side can be broadcast and a
few keys dominate a shuffle join.
"""

from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SALT_BUCKETS = 8
HOT_KEY_MIN_ROWS = 10


def build_inputs(spark):
    transactions_data = [
        ("T001", "M_HOT", Decimal("120.00")),
        ("T002", "M_HOT", Decimal("80.00")),
        ("T003", "M_HOT", Decimal("45.00")),
        ("T004", "M_HOT", Decimal("250.00")),
        ("T005", "M_HOT", Decimal("60.00")),
        ("T006", "M_HOT", Decimal("90.00")),
        ("T007", "M_HOT", Decimal("30.00")),
        ("T008", "M_HOT", Decimal("110.00")),
        ("T009", "M_HOT", Decimal("75.00")),
        ("T010", "M_HOT", Decimal("140.00")),
        ("T011", "M_HOT", Decimal("55.00")),
        ("T012", "M_HOT", Decimal("200.00")),
        ("T013", "M_HOT", Decimal("35.00")),
        ("T014", "M_HOT", Decimal("65.00")),
        ("T015", "M_02", Decimal("20.00")),
        ("T016", "M_02", Decimal("30.00")),
        ("T017", "M_02", Decimal("25.00")),
        ("T018", "M_03", Decimal("50.00")),
        ("T019", "M_03", Decimal("40.00")),
        ("T020", "M_03", Decimal("60.00")),
    ]
    merchant_data = [
        ("M_HOT", "MegaMart", "HIGH"),
        ("M_02", "Corner Shop", "LOW"),
        ("M_03", "HealthPlus", "MEDIUM"),
    ]

    transactions = spark.createDataFrame(
        transactions_data, ["txn_id", "merchant_id", "amount"]
    )
    merchants = spark.createDataFrame(
        merchant_data, ["merchant_id", "merchant_name", "risk_band"]
    )
    return transactions, merchants


def aggregate_by_risk_band(joined):
    return (
        joined.groupBy("risk_band")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("gross_amount"),
        )
        .orderBy("risk_band")
    )


def main():
    spark = (
        SparkSession.builder
        .appName("SkewAwareJoinSaltingPractice")
        .getOrCreate()
    )

    transactions, merchants = build_inputs(spark)

    # A dimension key must be unique; otherwise either join can multiply facts.
    duplicate_dimension_keys = (
        merchants.groupBy("merchant_id")
        .agg(F.count("*").alias("key_count"))
        .filter(F.col("key_count") > 1)
    )
    assert duplicate_dimension_keys.limit(1).count() == 0

    # 1) Measure key distribution. In production, persist these metrics and
    #    derive thresholds from data volume/percentiles rather than a constant.
    merchant_frequency = (
        transactions.groupBy("merchant_id")
        .agg(F.count("*").alias("row_count"))
        .orderBy(F.desc("row_count"))
    )
    print("Fact rows by merchant:")
    merchant_frequency.show(truncate=False)

    hot_keys = [
        row["merchant_id"]
        for row in merchant_frequency
        .filter(F.col("row_count") >= HOT_KEY_MIN_ROWS)
        .select("merchant_id")
        .collect()
    ]
    assert hot_keys == ["M_HOT"], f"Unexpected hot-key detection: {hot_keys}"

    # 2) Baseline shuffle join. Inspect the plan and note that the hot key can
    #    send a disproportionate amount of work to one reducer/partition.
    baseline_join = transactions.join(merchants, "merchant_id", "inner")
    baseline_metrics = aggregate_by_risk_band(baseline_join)

    print("Baseline join physical plan:")
    baseline_join.explain(mode="formatted")

    # 3) Add a deterministic salt to fact rows for hot keys only.
    #    xxhash64(txn_id) gives stable distribution across retries.
    salted_facts = transactions.withColumn(
        "salt",
        F.when(
            F.col("merchant_id").isin(hot_keys),
            F.pmod(F.xxhash64("txn_id"), F.lit(SALT_BUCKETS)),
        )
        .otherwise(F.lit(0))
        .cast("int"),
    )

    # 4) Replicate only hot dimension rows across every salt bucket. Normal
    #    keys retain salt=0 and are not unnecessarily expanded.
    salted_merchants = (
        merchants.withColumn(
            "salt_values",
            F.when(
                F.col("merchant_id").isin(hot_keys),
                F.sequence(F.lit(0), F.lit(SALT_BUCKETS - 1)),
            ).otherwise(F.array(F.lit(0))),
        )
        .withColumn("salt", F.explode("salt_values"))
        .drop("salt_values")
    )

    salted_join = (
        salted_facts.join(
            salted_merchants,
            on=["merchant_id", "salt"],
            how="inner",
        )
        .drop("salt")
    )
    salted_metrics = aggregate_by_risk_band(salted_join)

    print("Salted join physical plan:")
    salted_join.explain(mode="formatted")

    # 5) Correctness checks: salting changes distribution, never business output.
    assert baseline_join.count() == transactions.count()
    assert salted_join.count() == transactions.count()
    assert baseline_metrics.collect() == salted_metrics.collect()

    observed_counts = {
        row["risk_band"]: row["txn_count"]
        for row in salted_metrics.collect()
    }
    expected_counts = {"HIGH": 14, "LOW": 3, "MEDIUM": 3}
    assert observed_counts == expected_counts, (
        f"Expected {expected_counts}, received {observed_counts}"
    )

    print("Validated aggregate output (baseline and salted results match):")
    salted_metrics.show(truncate=False)
    print("All skew-aware join assertions passed.")

    # 6) Decision guidance:
    #    - If the dimension comfortably fits executor memory, test a broadcast
    #      join first; it avoids shuffling the large fact side.
    #    - For large-large joins, inspect Spark UI task-duration and shuffle
    #      read distributions, then consider AQE skew-join handling.
    #    - Salting adds dimension rows and key complexity. Apply it only to
    #      measured hot keys, and use a deterministic salt expression.
    #    - Keep the salt bucket count configurable; too few buckets leave skew,
    #      while too many increase replication and scheduling overhead.
    #    - Compare runtime, spill, shuffle bytes, and max-vs-median task time
    #      on representative data. A small local run cannot prove a speedup.

    spark.stop()


if __name__ == "__main__":
    main()
