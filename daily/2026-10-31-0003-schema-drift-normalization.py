"""
Schema Drift Normalization and Row-Level Quarantine
===================================================

Scenario
--------
Three upstream customer feeds publish different schema versions:
* v1 uses an integer customer_id and the field name "name".
* v2 changes customer_id to a string and adds country.
* v3 uses a long customer_id and adds loyalty_tier.

Build a stable Silver contract without silently losing malformed records:
1. Normalize each version to one explicit target shape.
2. Union the normalized DataFrames.
3. Route invalid rows to a quarantine DataFrame with reason codes.
4. Select the latest valid customer state deterministically.
5. Assert expected results.

Run:
    spark-submit daily/2026-10-31-0003-schema-drift-normalization.py

Compatible with Spark 3.2+ (try_cast is used for safe identifier parsing).
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, LongType, StringType
)


def build_spark():
    return (
        SparkSession.builder
        .appName("SchemaDriftNormalizationPractice")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def create_source_dataframes(spark):
    """Create three realistic source versions with intentionally different schemas."""

    v1_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("ingestion_ts", StringType(), True),
    ])
    v1_rows = [
        (101, "Asha Rao", "ASHA@example.com", "2026-10-29 09:00:00", "2026-10-29 09:05:00"),
        (102, "Bharat Shah", "bharat@example.com", "2026-10-29 10:00:00", "2026-10-29 10:03:00"),
    ]

    v2_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("ingested_at", StringType(), True),
    ])
    v2_rows = [
        ("101", "Asha Rao", "asha.rao@example.com", "IN", "2026-10-30 08:00:00", "2026-10-30 08:02:00"),
        ("103", "Chitra Iyer", "chitra@example.com", "IN", "2026-10-29 11:00:00", "2026-10-29 11:04:00"),
        ("bad-id", "Broken Identifier", "broken@example.com", "IN", "2026-10-30 09:00:00", "2026-10-30 09:01:00"),
        ("104", "Deepa Nair", "not-an-email", "IN", "2026-10-29 12:00:00", "2026-10-29 12:02:00"),
    ]

    v3_schema = StructType([
        StructField("customer_id", LongType(), True),
        StructField("full_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("loyalty_tier", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("ingested_at", StringType(), True),
    ])
    v3_rows = [
        (103, "Chitra Iyer", "chitra.iyer@example.com", "IN", "GOLD",
         "2026-10-31 00:01:00", "2026-10-31 00:02:00"),
        (104, "Deepa Nair", "deepa@example.com", "IN", "SILVER",
         "2026-10-30 10:00:00", "2026-10-30 10:03:00"),
    ]

    return (
        spark.createDataFrame(v1_rows, v1_schema),
        spark.createDataFrame(v2_rows, v2_schema),
        spark.createDataFrame(v3_rows, v3_schema),
    )


def normalize_v1(df):
    """Map v1 field names/types to the canonical Silver contract."""
    return df.select(
        F.expr("try_cast(customer_id AS BIGINT)").alias("customer_id"),
        F.trim(F.col("name")).alias("full_name"),
        F.lower(F.trim(F.col("email"))).alias("email"),
        F.lit(None).cast("string").alias("country"),
        F.lit(None).cast("string").alias("loyalty_tier"),
        F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss").alias("source_updated_at"),
        F.to_timestamp("ingestion_ts", "yyyy-MM-dd HH:mm:ss").alias("ingested_at"),
        F.lit("v1").alias("source_version"),
    )


def normalize_v2(df):
    """Map v2 field names/types to the canonical Silver contract."""
    return df.select(
        F.expr("try_cast(customer_id AS BIGINT)").alias("customer_id"),
        F.trim(F.col("full_name")).alias("full_name"),
        F.lower(F.trim(F.col("email"))).alias("email"),
        F.upper(F.trim(F.col("country"))).alias("country"),
        F.lit(None).cast("string").alias("loyalty_tier"),
        F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss").alias("source_updated_at"),
        F.to_timestamp("ingested_at", "yyyy-MM-dd HH:mm:ss").alias("ingested_at"),
        F.lit("v2").alias("source_version"),
    )


def normalize_v3(df):
    """Map v3 field names/types to the canonical Silver contract."""
    return df.select(
        F.expr("try_cast(customer_id AS BIGINT)").alias("customer_id"),
        F.trim(F.col("full_name")).alias("full_name"),
        F.lower(F.trim(F.col("email"))).alias("email"),
        F.upper(F.trim(F.col("country"))).alias("country"),
        F.upper(F.trim(F.col("loyalty_tier"))).alias("loyalty_tier"),
        F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss").alias("source_updated_at"),
        F.to_timestamp("ingested_at", "yyyy-MM-dd HH:mm:ss").alias("ingested_at"),
        F.lit("v3").alias("source_version"),
    )


def apply_quality_rules(df):
    """
    Add row-level reason codes. concat_ws skips null values, so a valid row has
    an empty violation_reason while a bad row can carry multiple reasons.
    """
    email_pattern = r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+$"

    checked = (
        df
        .withColumn(
            "violation_reason",
            F.concat_ws(
                "|",
                F.when(
                    F.col("customer_id").isNull() | (F.col("customer_id") <= 0),
                    F.lit("INVALID_CUSTOMER_ID"),
                ),
                F.when(
                    F.col("full_name").isNull() | (F.length(F.trim("full_name")) == 0),
                    F.lit("MISSING_FULL_NAME"),
                ),
                F.when(
                    F.col("email").isNull() | (~F.col("email").rlike(email_pattern)),
                    F.lit("INVALID_EMAIL"),
                ),
                F.when(
                    F.col("source_updated_at").isNull(),
                    F.lit("INVALID_SOURCE_UPDATED_AT"),
                ),
                F.when(
                    F.col("ingested_at").isNull(),
                    F.lit("INVALID_INGESTED_AT"),
                ),
            ),
        )
        .withColumn("is_valid", F.col("violation_reason") == "")
    )
    return checked


def build_silver(valid_df):
    """Keep the latest valid state per customer with deterministic tie-breaks."""
    latest_per_customer = Window.partitionBy("customer_id").orderBy(
        F.col("source_updated_at").desc_nulls_last(),
        F.col("ingested_at").desc_nulls_last(),
        F.col("source_version").desc(),
    )

    return (
        valid_df
        .withColumn("row_num", F.row_number().over(latest_per_customer))
        .filter(F.col("row_num") == 1)
        .drop("row_num", "violation_reason", "is_valid")
    )


def main():
    spark = build_spark()
    try:
        source_v1, source_v2, source_v3 = create_source_dataframes(spark)

        # Normalize before union: column names, order and data types now match.
        normalized = (
            normalize_v1(source_v1)
            .unionByName(normalize_v2(source_v2))
            .unionByName(normalize_v3(source_v3))
            .cache()
        )

        checked = apply_quality_rules(normalized).cache()
        quarantine = checked.filter(~F.col("is_valid"))
        valid = checked.filter(F.col("is_valid"))
        silver = build_silver(valid).cache()

        # Metrics are actions; materialize once because the same branches are
        # inspected more than once in this small practice pipeline.
        normalized_count = normalized.count()
        quarantine_count = quarantine.count()
        valid_count = valid.count()
        silver_count = silver.count()

        print("\n=== Normalized source rows ===")
        normalized.orderBy("source_version", "customer_id").show(truncate=False)

        print("\n=== Quarantine rows and reason codes ===")
        quarantine.select(
            "customer_id", "full_name", "email", "source_version", "violation_reason"
        ).show(truncate=False)

        print("\n=== Silver customer state ===")
        silver.orderBy("customer_id").show(truncate=False)

        print("\n=== Data quality metrics ===")
        print({
            "normalized_rows": normalized_count,
            "valid_rows": valid_count,
            "quarantined_rows": quarantine_count,
            "silver_customers": silver_count,
        })

        # Expected results for the supplied sample.
        assert normalized_count == 8, f"Expected 8 input rows, got {normalized_count}"
        assert quarantine_count == 2, f"Expected 2 quarantined rows, got {quarantine_count}"
        assert valid_count == 6, f"Expected 6 valid source rows, got {valid_count}"
        assert silver_count == 4, f"Expected 4 latest customer states, got {silver_count}"

        customer_ids = {
            row["customer_id"] for row in silver.select("customer_id").collect()
        }
        assert customer_ids == {101, 102, 103, 104}, customer_ids

        latest_by_id = {
            row["customer_id"]: row
            for row in silver.select(
                "customer_id", "email", "source_version", "loyalty_tier"
            ).collect()
        }
        assert latest_by_id[101]["email"] == "asha.rao@example.com"
        assert latest_by_id[103]["source_version"] == "v3"
        assert latest_by_id[104]["email"] == "deepa@example.com"
        assert latest_by_id[104]["loyalty_tier"] == "SILVER"

        print("\nAll schema-drift, quarantine, and latest-state assertions passed.")

        # In production, write valid rows to a Delta Silver table and quarantine
        # rows to a separate Delta table with batch_id/source_file metadata.
        # Example:
        # silver.write.format("delta").mode("append").saveAsTable("silver.customers")
        # quarantine.write.format("delta").mode("append").saveAsTable("ops.customer_quarantine")

        # Interview follow-up: how would you add a schema registry, source-file
        # lineage, replay-safe batch IDs, and alert thresholds for quarantine rate?

        silver.unpersist()
        checked.unpersist()
        normalized.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()


# ============================================================================
# Optimization and production notes
# ============================================================================
# 1. Normalize each source to an explicit target projection before unioning.
#    This avoids accidental positional alignment and makes type changes visible.
# 2. Use safe casts (try_cast) so malformed identifiers become quarantine data
#    instead of terminating the complete ingestion batch.
# 3. Keep a source_version and both source_updated_at and ingested_at. They answer
#    different questions and provide deterministic ordering for replay handling.
# 4. Use row_number with a stable tie-breaker instead of dropDuplicates when the
#    requirement is "latest record wins".
# 5. Cache only because this demo reuses the normalized and quality-checked
#    branches for multiple actions. In a one-pass production flow, avoid caching.
# 6. Do not partition a small customer table by high-cardinality customer_id.
#    For large Delta tables, choose partitioning based on common filters and
#    table size; consider liquid clustering where supported.
# 7. Track quarantine count and quarantine percentage by source/version. A sudden
#    increase is often an upstream contract change, not random bad data.
# 8. Preserve original raw payload and file/batch identifiers in a real Bronze
#    layer so rejected records can be diagnosed and safely replayed.
#
# Interview follow-ups:
# - How do you support a breaking rename while old and new producers overlap?
# - What is the difference between missing, explicit NULL, and default values?
# - How would you make a replay idempotent across multiple ingestion batches?
# - When would you use Delta MERGE instead of append + downstream deduplication?
# ============================================================================
