"""
PySpark Interview Practice — Decimal Precision & FX Conversion
==============================================================

Problem
-------
Convert settled multi-currency payment events into INR using the FX rate
for the transaction date. The pipeline must:
  * preserve financial precision (never use FloatType/DoubleType for money),
  * choose one deterministic rate when providers publish duplicates,
  * quarantine missing amounts and missing FX rates,
  * apply an explicit per-transaction rounding policy, and
  * produce correct daily and overall totals.

Requirements
------------
PySpark 3.x. No external dataset or third-party package is required.

Run
---
spark-submit daily/2026-11-23-decimal-precision-and-fx-conversion.py

Interview focus
---------------
DecimalType, deterministic deduplication, joins, broadcast joins, null handling,
rounding policy, aggregation correctness, and financial data quality.
"""

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def create_sample_data(spark):
    """Create payment events and provider FX quotes with explicit schemas."""
    payment_schema = StructType([
        StructField("payment_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("txn_date", DateType(), False),
        StructField("currency", StringType(), False),
        StructField("amount", DecimalType(18, 4), True),
        StructField("status", StringType(), False),
        StructField("event_type", StringType(), False),
    ])

    payment_rows = [
        ("P001", "C01", date(2026, 11, 20), "USD", Decimal("100.0000"), "SETTLED", "SALE"),
        ("P002", "C02", date(2026, 11, 20), "EUR", Decimal("50.0000"), "SETTLED", "SALE"),
        ("P003", "C03", date(2026, 11, 20), "USD", Decimal("-10.0000"), "SETTLED", "REFUND"),
        ("P004", "C04", date(2026, 11, 20), "GBP", Decimal("25.0000"), "SETTLED", "SALE"),
        ("P005", "C05", date(2026, 11, 20), "INR", Decimal("999.9900"), "SETTLED", "SALE"),
        ("P006", "C06", date(2026, 11, 21), "USD", Decimal("12.3456"), "SETTLED", "SALE"),
        ("P007", "C07", date(2026, 11, 21), "EUR", None, "SETTLED", "SALE"),
        ("P008", "C08", date(2026, 11, 20), "USD", Decimal("100.0000"), "CANCELLED", "SALE"),
        ("P009", "C09", date(2026, 11, 21), "EUR", Decimal("0.1000"), "SETTLED", "SALE"),
    ]

    fx_schema = StructType([
        StructField("fx_date", DateType(), False),
        StructField("currency", StringType(), False),
        StructField("inr_per_unit", DecimalType(18, 8), False),
        StructField("provider", StringType(), False),
        StructField("source_priority", IntegerType(), False),
        StructField("ingested_at", TimestampType(), False),
    ])

    fx_rows = [
        # Lower source_priority wins; ingested_at breaks ties deterministically.
        (date(2026, 11, 20), "USD", Decimal("83.25000000"), "PRIMARY", 1, datetime(2026, 11, 20, 9, 0, 0)),
        (date(2026, 11, 20), "USD", Decimal("83.20000000"), "SECONDARY", 2, datetime(2026, 11, 20, 9, 5, 0)),
        (date(2026, 11, 20), "EUR", Decimal("90.12500000"), "PRIMARY", 1, datetime(2026, 11, 20, 9, 0, 0)),
        (date(2026, 11, 21), "USD", Decimal("83.33333333"), "PRIMARY", 1, datetime(2026, 11, 21, 9, 0, 0)),
        (date(2026, 11, 21), "EUR", Decimal("90.00000000"), "PRIMARY", 1, datetime(2026, 11, 21, 9, 0, 0)),
        (date(2026, 11, 20), "INR", Decimal("1.00000000"), "INTERNAL", 1, datetime(2026, 11, 20, 0, 0, 0)),
        (date(2026, 11, 21), "INR", Decimal("1.00000000"), "INTERNAL", 1, datetime(2026, 11, 21, 0, 0, 0)),
        # No GBP quote is intentional: P004 must be quarantined.
    ]

    return (
        spark.createDataFrame(payment_rows, payment_schema),
        spark.createDataFrame(fx_rows, fx_schema),
    )


def select_preferred_fx_rates(fx_rates):
    """
    Keep one rate per (fx_date, currency).

    The source contract states that lower source_priority is preferred.
    If two records have the same priority, the latest ingestion timestamp wins;
    provider is a final stable tie-breaker.
    """
    rate_window = (
        Window
        .partitionBy("fx_date", "currency")
        .orderBy(
            F.col("source_priority").asc(),
            F.col("ingested_at").desc(),
            F.col("provider").asc(),
        )
    )

    return (
        fx_rates
        .withColumn("_rate_rank", F.row_number().over(rate_window))
        .where(F.col("_rate_rank") == 1)
        .drop("_rate_rank")
    )


def convert_payments(payments, preferred_rates, use_broadcast=False):
    """
    Return (converted_payments, quarantined_payments).

    Baseline:
      regular left join to the deduplicated FX dimension.

    Optimized:
      filter non-settled and null-amount rows before the join, project only
      required columns, and broadcast the FX dimension when it is genuinely
      small enough to fit safely in executor memory.
    """
    settled = payments.where(F.col("status") == "SETTLED")

    missing_amounts = (
        settled
        .where(F.col("amount").isNull())
        .select(
            "payment_id", "customer_id", "txn_date", "currency", "amount",
            F.lit("MISSING_AMOUNT").alias("quarantine_reason"),
        )
    )

    # The baseline sends null-amount settled rows through the join; the optimized
    # path removes them first because they cannot be converted.
    join_source = settled
    if use_broadcast:
        join_source = join_source.where(F.col("amount").isNotNull())

    join_input = (
        join_source
        .select("payment_id", "customer_id", "txn_date", "currency", "amount", "event_type")
        .alias("p")
    )

    rate_dim = preferred_rates.select(
        "fx_date", "currency", "inr_per_unit", "provider"
    )
    if use_broadcast:
        rate_dim = F.broadcast(rate_dim)
    rate_dim = rate_dim.alias("r")

    joined = join_input.join(
        rate_dim,
        (F.col("p.txn_date") == F.col("r.fx_date"))
        & (F.col("p.currency") == F.col("r.currency")),
        "left",
    ).select(
        F.col("p.payment_id").alias("payment_id"),
        F.col("p.customer_id").alias("customer_id"),
        F.col("p.txn_date").alias("txn_date"),
        F.col("p.currency").alias("currency"),
        F.col("p.amount").alias("amount"),
        F.col("p.event_type").alias("event_type"),
        F.col("r.inr_per_unit").alias("inr_per_unit"),
        F.col("r.provider").alias("fx_provider"),
    )

    missing_rates = (
        joined
        .where(F.col("amount").isNotNull() & F.col("inr_per_unit").isNull())
        .select(
            "payment_id", "customer_id", "txn_date", "currency", "amount",
            F.lit("MISSING_FX_RATE").alias("quarantine_reason"),
        )
    )

    converted = (
        joined
        .where(F.col("amount").isNotNull() & F.col("inr_per_unit").isNotNull())
        # Explicit policy: round each converted payment/refund to 2 INR decimals.
        .withColumn(
            "inr_amount",
            F.round(F.col("amount") * F.col("inr_per_unit"), 2),
        )
        .select(
            "payment_id", "customer_id", "txn_date", "currency", "amount",
            "event_type", "inr_per_unit", "fx_provider", "inr_amount",
        )
    )

    quarantine = missing_amounts.unionByName(missing_rates)
    return converted, quarantine


def main():
    spark = (
        SparkSession.builder
        .appName("DecimalPrecisionAndFxConversion")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    payments, fx_rates = create_sample_data(spark)
    preferred_rates = select_preferred_fx_rates(fx_rates)

    # Baseline: ordinary left join against the canonical FX dimension.
    baseline_converted, baseline_quarantine = convert_payments(
        payments, preferred_rates, use_broadcast=False
    )

    # Optimized: narrow broadcast dimension plus early removal of invalid amounts.
    optimized_converted, optimized_quarantine = convert_payments(
        payments, preferred_rates, use_broadcast=True
    )

    # Verify that the preferred USD quote is the PRIMARY provider's rate.
    selected_usd_rate = (
        preferred_rates
        .where(
            (F.col("fx_date") == F.lit(date(2026, 11, 20)))
            & (F.col("currency") == "USD")
        )
        .select("provider", "inr_per_unit")
        .first()
    )
    assert selected_usd_rate["provider"] == "PRIMARY"
    assert selected_usd_rate["inr_per_unit"] == Decimal("83.25000000")

    # Ensure the dimension is unique at its declared business key.
    duplicate_rate_keys = (
        preferred_rates
        .groupBy("fx_date", "currency")
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    assert duplicate_rate_keys == 0

    # Compare both implementations in both directions (multiset equality).
    comparison_columns = [
        "payment_id", "customer_id", "txn_date", "currency", "amount",
        "event_type", "inr_per_unit", "fx_provider", "inr_amount",
    ]
    assert (
        baseline_converted.select(*comparison_columns)
        .exceptAll(optimized_converted.select(*comparison_columns))
        .count() == 0
    )
    assert (
        optimized_converted.select(*comparison_columns)
        .exceptAll(baseline_converted.select(*comparison_columns))
        .count() == 0
    )

    # Expected accepted and quarantined records.
    assert optimized_converted.count() == 6
    assert optimized_quarantine.count() == 2

    quarantine_counts = {
        row["quarantine_reason"]: row["count"]
        for row in optimized_quarantine.groupBy("quarantine_reason").count().collect()
    }
    assert quarantine_counts == {
        "MISSING_AMOUNT": 1,
        "MISSING_FX_RATE": 1,
    }

    # Financial control total: refunds remain negative and currencies are
    # converted before aggregation, so no unlike currencies are added together.
    total_inr = (
        optimized_converted
        .agg(F.sum("inr_amount").alias("total_inr"))
        .first()["total_inr"]
    )
    assert total_inr == Decimal("14036.54")

    daily_totals = (
        optimized_converted
        .groupBy("txn_date")
        .agg(
            F.sum("inr_amount").alias("total_inr"),
            F.count("*").alias("payment_count"),
        )
        .orderBy("txn_date")
    )
    daily_rows = daily_totals.collect()
    assert daily_rows[0]["total_inr"] == Decimal("12998.74")
    assert daily_rows[1]["total_inr"] == Decimal("1037.80")

    # Demonstrate why the rounding point is a business/accounting decision.
    rounding_schema = StructType([
        StructField("line_id", StringType(), False),
        StructField("amount", DecimalType(18, 4), False),
        StructField("rate", DecimalType(18, 8), False),
    ])
    rounding_rows = [
        ("R1", Decimal("0.0050"), Decimal("1.00000000")),
        ("R2", Decimal("0.0050"), Decimal("1.00000000")),
    ]
    rounding_demo = spark.createDataFrame(rounding_rows, rounding_schema)
    rounding_result = rounding_demo.agg(
        F.sum(F.round(F.col("amount") * F.col("rate"), 2)).alias("round_each_line"),
        F.round(F.sum(F.col("amount") * F.col("rate")), 2).alias("round_after_sum"),
    ).first()
    assert rounding_result["round_each_line"] == Decimal("0.02")
    assert rounding_result["round_after_sum"] == Decimal("0.01")

    print("Converted payments (INR):")
    optimized_converted.orderBy("payment_id").show(truncate=False)

    print("Quarantined payments:")
    optimized_quarantine.orderBy("payment_id").show(truncate=False)

    print("Daily INR totals:")
    daily_totals.show(truncate=False)

    print("Rounding policy comparison:")
    print(
        "Round each line =", rounding_result["round_each_line"],
        "| round after aggregation =", rounding_result["round_after_sum"],
    )

    print("Baseline physical plan:")
    baseline_converted.explain("formatted")

    print("Optimized physical plan:")
    optimized_converted.explain("formatted")

    print("All assertions passed.")

    print("\\nInterview follow-up questions:")
    print("1. How would you handle currencies whose minor unit is not two decimals, such as JPY?")
    print("2. What should happen when the exact transaction-date FX rate is missing?")
    print("3. How would you extend this equality join to effective-dated FX intervals?")
    print("4. Why is DecimalType safer than DoubleType for financial amounts?")
    print("5. When is broadcasting an FX dimension unsafe, and how can AQE help?")
    print("6. How would you detect decimal overflow and define precision/scale contracts?")
    print("7. Should rounding happen per line, per invoice, or only after aggregation?")

    spark.stop()


if __name__ == "__main__":
    main()
