"""
Daily PySpark Interview Practice — 2026-11-13
Topic: Spark accumulators vs DataFrame-native data-quality metrics

Problem statement
-----------------
A payments ingestion pipeline must report how many rows violate each quality
rule and how many rows should be quarantined. Build the same metrics in two
ways:

1. A diagnostic RDD pass that updates Spark accumulators.
2. A DataFrame-native solution that calculates all rule counts in one
   aggregation and produces valid/quarantined records.

Explain why accumulators are useful for operational diagnostics but should
not be the source of truth for financial or data-quality totals.

Run:
    spark-submit daily/2026-11-13-accumulators-vs-dataframe-quality-metrics.py

Requirements:
    PySpark 3.x; no external files or services.
"""

from datetime import datetime

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


SUPPORTED_CURRENCIES = ("INR", "USD")


def build_sample_data(spark):
    schema = StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("account_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("event_ts", TimestampType(), True),
            StructField("source_system", StringType(), False),
        ]
    )

    rows = [
        ("E001", "A100", 125.50, "INR", datetime(2026, 11, 13, 8, 0), "mobile"),
        ("E002", "A101", 0.00, "INR", datetime(2026, 11, 13, 8, 1), "web"),
        ("E003", None, 50.00, "INR", datetime(2026, 11, 13, 8, 2), "partner"),
        ("E004", "A103", None, "USD", datetime(2026, 11, 13, 8, 3), "mobile"),
        ("E005", "A104", 22.00, "EUR", datetime(2026, 11, 13, 8, 4), "web"),
        ("E006", "A105", 14.00, "INR", datetime(2026, 11, 13, 8, 5), "mobile"),
        ("E007", "A106", 18.50, None, datetime(2026, 11, 13, 8, 6), "partner"),
        ("E008", "A107", -4.00, "USD", datetime(2026, 11, 13, 8, 7), "web"),
        ("E009", "A108", 35.00, "USD", datetime(2026, 11, 13, 8, 8), "mobile"),
        ("E010", "A109", 8.00, "GBP", datetime(2026, 11, 13, 8, 9), "partner"),
        # One row violates all three rules; per-rule counts therefore overlap.
        ("E011", None, None, None, datetime(2026, 11, 13, 8, 10), "legacy"),
    ]

    return spark.createDataFrame(rows, schema)


def add_quality_flags(df):
    missing_account = F.col("account_id").isNull()
    invalid_amount = F.col("amount").isNull() | (F.col("amount") <= F.lit(0.0))
    unsupported_currency = (
        F.col("currency").isNull()
        | (~F.col("currency").isin(*SUPPORTED_CURRENCIES))
    )

    return (
        df.withColumn("missing_account", missing_account)
        .withColumn("invalid_amount", invalid_amount)
        .withColumn("unsupported_currency", unsupported_currency)
        .withColumn(
            "is_valid",
            ~(
                F.col("missing_account")
                | F.col("invalid_amount")
                | F.col("unsupported_currency")
            ),
        )
    )


def dataframe_native_solution(source_df):
    """
    Preferred production pattern:
    - express rules with native Catalyst-visible Column expressions;
    - aggregate every metric in one pass;
    - persist the classified DataFrame only because several downstream
      outputs (metrics, valid rows, quarantine rows) reuse it.
    """
    classified = add_quality_flags(source_df).persist(StorageLevel.MEMORY_AND_DISK)

    try:
        metrics = classified.agg(
            F.count("*").alias("total_rows"),
            F.sum(F.col("missing_account").cast("long")).alias("missing_account_rows"),
            F.sum(F.col("invalid_amount").cast("long")).alias("invalid_amount_rows"),
            F.sum(F.col("unsupported_currency").cast("long")).alias(
                "unsupported_currency_rows"
            ),
            F.sum((~F.col("is_valid")).cast("long")).alias("rejected_rows"),
            F.sum(F.col("is_valid").cast("long")).alias("valid_rows"),
        ).first()

        valid_ids = [
            row.event_id
            for row in (
                classified.where(F.col("is_valid"))
                .select("event_id")
                .orderBy("event_id")
                .collect()
            )
        ]

        quarantine = (
            classified.where(~F.col("is_valid"))
            .select(
                "event_id",
                "account_id",
                "amount",
                "currency",
                "missing_account",
                "invalid_amount",
                "unsupported_currency",
            )
            .orderBy("event_id")
        )

        print("DataFrame-native quality metrics:")
        print(metrics.asDict())
        print("Valid event IDs:", valid_ids)
        print("Quarantine records and rule flags:")
        quarantine.show(truncate=False)

        expected = {
            "total_rows": 11,
            "missing_account_rows": 2,
            "invalid_amount_rows": 4,
            "unsupported_currency_rows": 4,
            "rejected_rows": 8,
            "valid_rows": 3,
        }
        actual = metrics.asDict()
        for name, expected_value in expected.items():
            assert actual[name] == expected_value, (
                f"{name}: expected {expected_value}, got {actual[name]}"
            )

        assert valid_ids == ["E001", "E006", "E009"]
        assert actual["valid_rows"] + actual["rejected_rows"] == actual["total_rows"]

        # Per-rule totals intentionally exceed rejected_rows because E011
        # violates three rules. Never add overlapping rule counts to get
        # the number of distinct rejected records.
        assert (
            actual["missing_account_rows"]
            + actual["invalid_amount_rows"]
            + actual["unsupported_currency_rows"]
            > actual["rejected_rows"]
        )

        print("Optimized DataFrame assertions passed.")
        print("Physical plan for the classified DataFrame:")
        classified.explain("formatted")

        return actual
    finally:
        classified.unpersist()


def naive_multiple_actions(source_df):
    """
    Baseline for comparison. Each count is a separate action and can rescan
    the input. This is easy to read but scales poorly for many rules.
    """
    missing_account = F.col("account_id").isNull()
    invalid_amount = F.col("amount").isNull() | (F.col("amount") <= 0)
    unsupported_currency = (
        F.col("currency").isNull()
        | (~F.col("currency").isin(*SUPPORTED_CURRENCIES))
    )
    rejected = missing_account | invalid_amount | unsupported_currency

    result = {
        "missing_account_rows": source_df.where(missing_account).count(),
        "invalid_amount_rows": source_df.where(invalid_amount).count(),
        "unsupported_currency_rows": source_df.where(unsupported_currency).count(),
        "rejected_rows": source_df.where(rejected).count(),
    }
    print("Baseline metrics from separate count actions:")
    print(result)
    return result


def accumulator_diagnostic_pass(source_df):
    """
    Demonstration only. Accumulators are handy for lightweight diagnostics
    inside RDD transformations, but task retries/recomputation can make
    transformation-side updates unsuitable as authoritative business counts.
    """
    sc = source_df.sparkSession.sparkContext
    missing_account_acc = sc.accumulator(0)
    invalid_amount_acc = sc.accumulator(0)

    def inspect_partition(rows):
        for row in rows:
            if row.account_id is None:
                missing_account_acc.add(1)
            if row.amount is None or row.amount <= 0:
                invalid_amount_acc.add(1)
            yield row.event_id

    processed_rows = (
        source_df.select("event_id", "account_id", "amount")
        .rdd.mapPartitions(inspect_partition)
        .count()
    )

    diagnostic = {
        "processed_rows": processed_rows,
        "missing_account_accumulator": missing_account_acc.value,
        "invalid_amount_accumulator": invalid_amount_acc.value,
    }
    print("Accumulator diagnostic snapshot (one successful local pass):")
    print(diagnostic)

    assert processed_rows == 11
    # Do not use these accumulator values as persisted financial totals.
    # The DataFrame aggregation above is the authoritative result.
    return diagnostic


def main():
    spark = (
        SparkSession.builder
        .appName("AccumulatorsVsDataFrameQualityMetrics")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        source_df = build_sample_data(spark)

        print("Input payments:")
        source_df.orderBy("event_id").show(truncate=False)

        baseline = naive_multiple_actions(source_df)
        optimized = dataframe_native_solution(source_df)

        for metric in (
            "missing_account_rows",
            "invalid_amount_rows",
            "unsupported_currency_rows",
            "rejected_rows",
        ):
            assert baseline[metric] == optimized[metric]

        accumulator_diagnostic_pass(source_df)

        print("\nInterview takeaways:")
        print("1. Why can accumulator updates in transformations be repeated?")
        print("2. Why are DataFrame aggregations safer for authoritative quality metrics?")
        print("3. How can one aggregation compute many rule counts in a single scan?")
        print("4. When does persist help, and when does it add unnecessary memory pressure?")
        print("5. How would you write valid and quarantined outputs atomically?")
        print("6. How would you add rule-level metrics without double-counting rejected rows?")
        print("7. What changes when the same quality rules run in Structured Streaming?")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
