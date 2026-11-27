"""
Daily PySpark Interview Practice — 2026-11-27
Topic: Multi-source settlement reconciliation with union-based aggregation

Problem
-------
A payment platform receives transaction-level records from its internal ledger
and a payment processor. Each feed can contain multiple lines for the same
(settlement_id, currency). Reconcile the two feeds without creating a
many-to-many join, classify missing/mismatched settlements, and quarantine
records that do not have a usable business key or amount.

Run
---
spark-submit daily/2026-11-27-multi-source-settlement-reconciliation.py

Compatible with PySpark 3.x. The exercise uses only in-memory DataFrames.
"""

from decimal import Decimal

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
)


TOLERANCE = Decimal("0.01")
MONEY_TYPE = DecimalType(18, 2)
DIFFERENCE_TYPE = DecimalType(38, 2)
KEY_COLUMNS = ["settlement_id", "currency"]


def create_sample_data(spark):
    """Create realistic transaction-level ledger and processor feeds."""
    schema = StructType(
        [
            StructField("settlement_id", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("amount", MONEY_TYPE, True),
        ]
    )

    ledger_rows = [
        ("S-100", "INR", Decimal("100.00")),
        ("S-100", "INR", Decimal("50.00")),
        ("S-101", "USD", Decimal("80.00")),
        ("S-102", "INR", Decimal("40.00")),
        ("S-103", "EUR", Decimal("25.00")),
        (None, "INR", Decimal("9.00")),       # Missing business key
        ("S-107", "INR", None),               # Missing amount
    ]

    processor_rows = [
        ("S-100", "INR", Decimal("149.99")),
        ("S-100", "INR", Decimal("0.01")),
        ("S-101", "USD", Decimal("79.50")),
        ("S-103", "EUR", Decimal("25.00")),
        ("S-105", "INR", Decimal("12.00")),   # Processor-only settlement
        ("S-106", "INR", Decimal("7.00")),    # Processor-only settlement
        (None, "INR", Decimal("5.00")),       # Missing business key
        ("S-108", "INR", None),               # Missing amount
    ]

    return (
        spark.createDataFrame(ledger_rows, schema),
        spark.createDataFrame(processor_rows, schema),
    )


def split_valid_and_quarantine(df, feed_name):
    """
    Keep only rows with a complete reconciliation key and amount.
    Retain rejected rows with explicit reason codes for investigation.
    """
    invalid_condition = (
        F.col("settlement_id").isNull()
        | F.col("currency").isNull()
        | F.col("amount").isNull()
    )

    reason = F.concat_ws(
        "|",
        F.when(
            F.col("settlement_id").isNull(),
            F.lit("MISSING_SETTLEMENT_ID"),
        ),
        F.when(F.col("currency").isNull(), F.lit("MISSING_CURRENCY")),
        F.when(F.col("amount").isNull(), F.lit("MISSING_AMOUNT")),
    )

    valid = df.filter(~invalid_condition).select(*KEY_COLUMNS, "amount")

    quarantine = (
        df.filter(invalid_condition)
        .withColumn("feed", F.lit(feed_name))
        .withColumn("reason", reason)
    )

    return valid, quarantine


def aggregate_one_feed(df, amount_alias, count_alias):
    """Aggregate transaction lines to one row per reconciliation key."""
    return df.groupBy(*KEY_COLUMNS).agg(
        F.sum("amount").alias(amount_alias),
        F.count(F.lit(1)).alias(count_alias),
    )


def baseline_full_outer_reconciliation(ledger_valid, processor_valid):
    """
    Readable baseline:
      1. Aggregate each feed independently.
      2. Full-outer-join the compact results on the composite business key.

    Aggregating before joining prevents duplicate lines on both sides from
    multiplying each other and inflating totals.
    """
    ledger_agg = aggregate_one_feed(
        ledger_valid, "ledger_total", "ledger_line_count"
    ).alias("l")
    processor_agg = aggregate_one_feed(
        processor_valid, "processor_total", "processor_line_count"
    ).alias("p")

    join_condition = (
        (F.col("l.settlement_id") == F.col("p.settlement_id"))
        & (F.col("l.currency") == F.col("p.currency"))
    )

    return (
        ledger_agg.join(processor_agg, join_condition, "full_outer")
        .select(
            F.coalesce(
                F.col("l.settlement_id"), F.col("p.settlement_id")
            ).alias("settlement_id"),
            F.coalesce(
                F.col("l.currency"), F.col("p.currency")
            ).alias("currency"),
            F.col("l.ledger_total"),
            F.col("p.processor_total"),
            F.coalesce(
                F.col("l.ledger_line_count"), F.lit(0)
            ).cast("long").alias("ledger_line_count"),
            F.coalesce(
                F.col("p.processor_line_count"), F.lit(0)
            ).cast("long").alias("processor_line_count"),
        )
    )


def union_based_reconciliation(ledger_valid, processor_valid):
    """
    Optimized alternative:
      1. Project only the business key and amount from each feed.
      2. Tag each row and union the feeds.
      3. Perform one grouped aggregation with conditional sums/counts.

    This avoids two independent aggregate shuffles followed by a full-outer
    join shuffle in the common case. Benchmark with representative data:
    the best plan depends on input sizes, partitioning, and data distribution.
    """
    ledger_tagged = ledger_valid.select(
        *KEY_COLUMNS,
        "amount",
        F.lit("LEDGER").alias("record_source"),
    )
    processor_tagged = processor_valid.select(
        *KEY_COLUMNS,
        "amount",
        F.lit("PROCESSOR").alias("record_source"),
    )

    combined = ledger_tagged.unionByName(processor_tagged)

    return combined.groupBy(*KEY_COLUMNS).agg(
        F.sum(
            F.when(
                F.col("record_source") == "LEDGER",
                F.col("amount"),
            )
        ).alias("ledger_total"),
        F.sum(
            F.when(
                F.col("record_source") == "PROCESSOR",
                F.col("amount"),
            )
        ).alias("processor_total"),
        F.sum(
            F.when(F.col("record_source") == "LEDGER", F.lit(1))
            .otherwise(F.lit(0))
        ).cast("long").alias("ledger_line_count"),
        F.sum(
            F.when(F.col("record_source") == "PROCESSOR", F.lit(1))
            .otherwise(F.lit(0))
        ).cast("long").alias("processor_line_count"),
    )


def classify_reconciliation(aggregated):
    """Classify each key while preserving one-sided totals as NULL."""
    both_sides_present = (
        (F.col("ledger_line_count") > 0)
        & (F.col("processor_line_count") > 0)
    )

    difference = F.when(
        both_sides_present,
        F.abs(F.col("ledger_total") - F.col("processor_total")),
    ).otherwise(F.lit(None).cast(DIFFERENCE_TYPE))

    return (
        aggregated.withColumn("difference", difference)
        .withColumn(
            "status",
            F.when(
                F.col("ledger_line_count") == 0,
                F.lit("PROCESSOR_ONLY"),
            )
            .when(
                F.col("processor_line_count") == 0,
                F.lit("LEDGER_ONLY"),
            )
            .when(
                F.col("difference") <= F.lit(TOLERANCE).cast(DIFFERENCE_TYPE),
                F.lit("MATCH"),
            )
            .otherwise(F.lit("AMOUNT_MISMATCH")),
        )
        .select(
            "settlement_id",
            "currency",
            "ledger_total",
            "processor_total",
            "ledger_line_count",
            "processor_line_count",
            "difference",
            "status",
        )
    )


def canonical_rows(df):
    """Collect a stable representation for deterministic sample assertions."""
    rows = [
        (
            row["settlement_id"],
            row["currency"],
            row["ledger_total"],
            row["processor_total"],
            row["ledger_line_count"],
            row["processor_line_count"],
            row["difference"],
            row["status"],
        )
        for row in df.collect()
    ]
    return sorted(rows, key=lambda row: (row[0], row[1]))


def main():
    spark = (
        SparkSession.builder
        .appName("MultiSourceSettlementReconciliation")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    try:
        ledger_raw, processor_raw = create_sample_data(spark)

        ledger_valid, ledger_quarantine = split_valid_and_quarantine(
            ledger_raw, "LEDGER"
        )
        processor_valid, processor_quarantine = split_valid_and_quarantine(
            processor_raw, "PROCESSOR"
        )

        # Baseline: two per-feed aggregates followed by a full outer join.
        baseline = classify_reconciliation(
            baseline_full_outer_reconciliation(
                ledger_valid, processor_valid
            )
        )

        # Optimized: union the tagged feeds and aggregate once by business key.
        optimized = classify_reconciliation(
            union_based_reconciliation(ledger_valid, processor_valid)
        )

        # Both implementations must produce identical business results.
        assert canonical_rows(baseline) == canonical_rows(optimized)

        expected_status = {
            "S-100": "MATCH",
            "S-101": "AMOUNT_MISMATCH",
            "S-102": "LEDGER_ONLY",
            "S-103": "MATCH",
            "S-105": "PROCESSOR_ONLY",
            "S-106": "PROCESSOR_ONLY",
        }
        actual_status = {
            row["settlement_id"]: row["status"]
            for row in optimized.select("settlement_id", "status").collect()
        }
        assert actual_status == expected_status, actual_status

        expected_totals = {
            "S-100": (Decimal("150.00"), Decimal("150.00")),
            "S-101": (Decimal("80.00"), Decimal("79.50")),
            "S-102": (Decimal("40.00"), None),
            "S-103": (Decimal("25.00"), Decimal("25.00")),
            "S-105": (None, Decimal("12.00")),
            "S-106": (None, Decimal("7.00")),
        }
        actual_totals = {
            row["settlement_id"]: (
                row["ledger_total"],
                row["processor_total"],
            )
            for row in optimized.select(
                "settlement_id", "ledger_total", "processor_total"
            ).collect()
        }
        assert actual_totals == expected_totals, actual_totals

        status_counts = {
            row["status"]: row["count"]
            for row in optimized.groupBy("status").count().collect()
        }
        assert status_counts == {
            "MATCH": 2,
            "AMOUNT_MISMATCH": 1,
            "LEDGER_ONLY": 1,
            "PROCESSOR_ONLY": 2,
        }, status_counts

        assert ledger_quarantine.count() == 2
        assert processor_quarantine.count() == 2
        assert optimized.count() == 6

        print("Reconciliation results:")
        optimized.orderBy("settlement_id").show(truncate=False)

        print("Ledger quarantine:")
        ledger_quarantine.select(
            "settlement_id", "currency", "amount", "reason"
        ).show(truncate=False)

        print("Processor quarantine:")
        processor_quarantine.select(
            "settlement_id", "currency", "amount", "reason"
        ).show(truncate=False)

        print("Optimized physical plan:")
        optimized.explain("formatted")

        print("All reconciliation assertions passed.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()


"""
Transformation and optimization notes
-------------------------------------
1. Validate the composite key (settlement_id, currency) and amount before
   aggregation. Invalid records are retained in a separate quarantine output.
2. Aggregate each feed before a join. Joining raw duplicate lines directly can
   create a many-to-many multiplication and inflate financial totals.
3. The baseline uses a full outer join so ledger-only and processor-only keys
   are not lost.
4. The optimized path tags and unions the narrow feeds, then calculates both
   sides' sums and line counts in one groupBy. This can remove the separate
   aggregate exchanges and full-outer-join exchange.
5. A full outer join is not a good candidate for a forced broadcast hash join
   in standard Spark join strategies. Do not add a broadcast hint blindly.
6. For very large feeds, inspect the formatted plan and Spark UI. Consider
   AQE, appropriate shuffle partition sizing, and upstream partition layout.
   Explicit repartitioning can itself cause a shuffle, so benchmark it rather
   than assuming it is faster.
7. The tolerance is an explicit business rule. Currency-specific tolerances,
   FX normalization, settlement-date keys, and reversal handling may be needed
   in a production reconciliation contract.

Expected sample result
---------------------
Key     Ledger     Processor   Status
S-100   150.00     150.00      MATCH
S-101    80.00      79.50      AMOUNT_MISMATCH
S-102    40.00       NULL      LEDGER_ONLY
S-103    25.00      25.00      MATCH
S-105     NULL      12.00      PROCESSOR_ONLY
S-106     NULL       7.00      PROCESSOR_ONLY

Status counts: MATCH=2, AMOUNT_MISMATCH=1, LEDGER_ONLY=1, PROCESSOR_ONLY=2
Quarantine counts: ledger=2, processor=2

Interview follow-up questions
-----------------------------
1. Why must each feed be aggregated before joining when duplicate business keys
   are possible on both sides?
2. How can union plus conditional aggregation reduce shuffle stages compared
   with two aggregates followed by a full outer join?
3. When would the full-outer-join baseline outperform the union approach?
4. Why is a broadcast hash join generally not applicable to a full outer join?
5. How would you reconcile currencies when the two feeds use different units
   or FX rates?
6. How would you handle reversals, chargebacks, late-arriving settlement lines,
   and reruns without double-counting?
7. Which metrics and source controls would you persist for auditability?
8. How would you scale the pipeline when a small number of settlement keys
   contain a disproportionate number of lines?
"""
