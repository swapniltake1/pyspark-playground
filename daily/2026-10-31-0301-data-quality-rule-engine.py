"""
Topic: Reusable Data-Quality Rule Engine with Row-Level Quarantine
Runtime: PySpark 3.3+
Run: spark-submit daily/2026-10-31-0301-data-quality-rule-engine.py

The example validates a customer batch against field, domain, range, chronology,
and uniqueness rules. Invalid records are retained with all violation codes.
"""

from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, TimestampType
)
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel


def build_source_df(spark):
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("credit_limit", DecimalType(12, 2), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("source_file", StringType(), False),
    ])

    rows = [
        ("C001", "asha@example.com", "US", Decimal("250.00"),
         datetime(2026, 10, 1, 9, 0), datetime(2026, 10, 2, 9, 0), "customers_001.csv"),
        ("C002", "not-an-email", "IN", Decimal("-50.00"),
         datetime(2026, 10, 2, 9, 0), datetime(2026, 10, 3, 9, 0), "customers_001.csv"),
        ("C003", "li@example.com", "ZZ", Decimal("100.00"),
         datetime(2026, 10, 3, 9, 0), datetime(2026, 10, 3, 10, 0), "customers_001.csv"),
        ("C004", "raj@example.com", "GB", Decimal("100.00"),
         datetime(2026, 10, 4, 9, 0), datetime(2026, 10, 4, 10, 0), "customers_001.csv"),
        ("C004", "raj@example.com", "GB", Decimal("120.00"),
         datetime(2026, 10, 4, 9, 0), datetime(2026, 10, 5, 10, 0), "customers_002.csv"),
        (None, None, "IN", Decimal("20.00"),
         datetime(2026, 10, 5, 9, 0), datetime(2026, 10, 5, 10, 0), "customers_002.csv"),
        ("C006", "meera@example.com", "US", Decimal("500.00"),
         datetime(2026, 10, 8, 12, 0), datetime(2026, 10, 7, 12, 0), "customers_002.csv"),
        ("C007", "dev@example.com", "IN", Decimal("750.00"),
         datetime(2026, 10, 9, 8, 0), datetime(2026, 10, 9, 8, 30), "customers_002.csv"),
    ]
    return spark.createDataFrame(rows, schema)


def build_country_reference(spark):
    # In production this should be a governed reference/dimension table.
    return spark.createDataFrame(
        [("US",), ("IN",), ("GB",)],
        ["country_code"]
    )


def apply_quality_rules(source_df, country_df):
    # Count all occurrences of a non-null business key so every member of a
    # duplicate group is quarantined, not just the second and later records.
    id_window = Window.partitionBy("customer_id")
    keyed = source_df.withColumn(
        "_customer_id_count", F.count(F.lit(1)).over(id_window)
    )

    # Broadcast only when the reference set is genuinely small.
    country_lookup = country_df.withColumn("_country_found", F.lit(True))
    enriched = keyed.join(
        F.broadcast(country_lookup),
        on="country_code",
        how="left"
    )

    email_pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

    # Declarative rule list: each condition yields a stable, machine-readable code.
    violations = F.array(
        F.when(F.col("customer_id").isNull(), F.lit("CUSTOMER_ID_REQUIRED")),
        F.when(F.col("email").isNull() | (F.length(F.trim("email")) == 0),
               F.lit("EMAIL_REQUIRED")),
        F.when(
            F.col("email").isNotNull()
            & (F.length(F.trim("email")) > 0)
            & (~F.trim("email").rlike(email_pattern)),
            F.lit("INVALID_EMAIL")
        ),
        F.when(
            F.col("country_code").isNull() | (F.length(F.trim("country_code")) == 0),
            F.lit("COUNTRY_REQUIRED")
        ),
        F.when(
            F.col("country_code").isNotNull()
            & (F.length(F.trim("country_code")) > 0)
            & F.col("_country_found").isNull(),
            F.lit("COUNTRY_UNKNOWN")
        ),
        F.when(
            F.col("credit_limit").isNull()
            | (F.col("credit_limit") < F.lit(0).cast("decimal(12,2)"))
            | (F.col("credit_limit") > F.lit(1000000).cast("decimal(12,2)")),
            F.lit("CREDIT_LIMIT_OUT_OF_RANGE")
        ),
        F.when(
            F.col("created_at").isNull()
            | F.col("updated_at").isNull()
            | (F.col("created_at") > F.col("updated_at")),
            F.lit("INVALID_TIMESTAMP_ORDER")
        ),
        F.when(
            F.col("customer_id").isNotNull()
            & (F.col("_customer_id_count") > 1),
            F.lit("DUPLICATE_CUSTOMER_ID")
        ),
    )

    checked = (
        enriched
        .withColumn(
            "dq_errors",
            F.filter(violations, lambda code: code.isNotNull())
        )
        .withColumn("dq_error_count", F.size("dq_errors"))
        .withColumn("dq_error_codes", F.array_join("dq_errors", "|"))
        .withColumn("is_valid", F.col("dq_error_count") == 0)
    )

    return checked


def main():
    spark = (
        SparkSession.builder
        .appName("ReusableDataQualityRuleEngine")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    source_df = build_source_df(spark)
    country_df = build_country_reference(spark)

    checked = apply_quality_rules(source_df, country_df).persist(
        StorageLevel.MEMORY_AND_DISK
    )

    try:
        valid_df = checked.filter(F.col("is_valid")).drop(
            "_customer_id_count", "_country_found"
        )
        quarantine_df = checked.filter(~F.col("is_valid")).drop(
            "_customer_id_count", "_country_found"
        )

        # One row per rule code per failed record; useful for quality dashboards.
        rule_metrics = (
            quarantine_df
            .select(F.explode("dq_errors").alias("rule_code"))
            .groupBy("rule_code")
            .agg(F.count("*").alias("failed_record_count"))
            .orderBy("rule_code")
        )

        total_rows = checked.count()
        valid_count = valid_df.count()
        quarantine_count = quarantine_df.count()

        print("=== Valid records ===")
        valid_df.select(
            "customer_id", "email", "country_code", "credit_limit"
        ).orderBy("customer_id").show(truncate=False)

        print("=== Quarantined records with reason codes ===")
        quarantine_df.select(
            "customer_id", "source_file", "dq_error_codes"
        ).orderBy("source_file", "customer_id").show(truncate=False)

        print("=== Rule-level quality metrics ===")
        rule_metrics.show(truncate=False)

        # Expected results for the fixed sample batch.
        assert total_rows == 8, f"Expected 8 source rows, got {total_rows}"
        assert valid_count == 2, f"Expected 2 valid rows, got {valid_count}"
        assert quarantine_count == 6, (
            f"Expected 6 quarantined rows, got {quarantine_count}"
        )

        valid_ids = {
            row["customer_id"]
            for row in valid_df.select("customer_id").collect()
        }
        assert valid_ids == {"C001", "C007"}, (
            f"Unexpected valid customer IDs: {valid_ids}"
        )

        metrics = {
            row["rule_code"]: row["failed_record_count"]
            for row in rule_metrics.collect()
        }
        expected_metrics = {
            "CUSTOMER_ID_REQUIRED": 1,
            "CREDIT_LIMIT_OUT_OF_RANGE": 1,
            "COUNTRY_UNKNOWN": 1,
            "DUPLICATE_CUSTOMER_ID": 2,
            "EMAIL_REQUIRED": 1,
            "INVALID_EMAIL": 1,
            "INVALID_TIMESTAMP_ORDER": 1,
        }
        assert metrics == expected_metrics, (
            f"Rule metrics mismatch. Expected {expected_metrics}, got {metrics}"
        )

        print(
            "PASS: source rows=8, valid rows=2, quarantined rows=6, "
            f"rule violations={sum(metrics.values())}"
        )
    finally:
        checked.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()


# Optimization and production design notes
# ----------------------------------------
# 1. The customer_id window introduces a shuffle. If uniqueness is enforced
#    upstream or by a Delta/SQL constraint, avoid recalculating this check here.
# 2. Broadcast the country reference only if it is small enough for executor
#    memory; otherwise let Spark choose a join strategy or use a managed lookup.
# 3. Project only columns required by rules before expensive joins/windows.
# 4. Persist the checked DataFrame only because this example performs several
#    actions (counts, display, and metrics); unpersist it after those actions.
# 5. Keep quarantine records immutable with source_file, ingestion_run_id,
#    ingestion_timestamp, raw payload, and rule-version metadata in production.
# 6. Track rule metrics by source, batch, and rule version; alert on failure-rate
#    thresholds rather than treating every invalid row as a pipeline failure.
# 7. For large batches, consider a two-stage design: cheap row-local checks first,
#    then reference/uniqueness checks on the reduced candidate set.
#
# Interview follow-ups
# --------------------
# - Why quarantine every occurrence of a duplicate key rather than selecting one?
# - When is a broadcast join unsafe, and how would you validate its size?
# - How would you make rule definitions configurable without building unsafe SQL?
# - How would you persist quarantine data and replay corrected records idempotently?
# - How would you distinguish record-level quality failures from batch-level failures?
# - How would you handle rules that depend on historical state or cross-row aggregates?
