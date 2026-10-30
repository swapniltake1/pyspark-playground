"""
Practice: Conservative Customer Entity Resolution & Survivorship
Runtime: PySpark 3.3+
Run: spark-submit daily/2026-10-31-0458-customer-entity-resolution.py

Scenario
--------
CRM, billing, and support systems contain overlapping customer records.
Build conservative deterministic match keys from normalized email or phone,
resolve field-level survivorship, and surface conflicts for human review.

Matching policy for this exercise:
1. Prefer normalized email when present.
2. Otherwise use a normalized 10-digit phone.
3. If neither exists, retain a source-record-specific singleton.
This intentionally avoids fuzzy matching and does not merge a phone-only row
into an email-keyed entity. A production identity service needs a governed
crosswalk/merge workflow for those ambiguous cases.
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)


def build_source_records(spark):
    schema = StructType([
        StructField("source_system", StringType(), False),
        StructField("source_record_id", StringType(), False),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("legal_name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("source_priority", IntegerType(), False),
        StructField("updated_at", StringType(), False),
    ])

    rows = [
        # Three systems refer to Alice by the same email but disagree on phone.
        ("CRM", "C-100", "Alice@Example.com", None,
         "Alice Morgan", None, 1, "2026-10-30 09:00:00"),
        ("BILLING", "B-900", "alice@example.com", "+1 (415) 555-0101",
         None, "12 Market St", 2, "2026-10-31 01:00:00"),
        ("SUPPORT", "S-200", " alice@example.com ", "+1-646-555-0199",
         "Alice M.", None, 3, "2026-10-31 02:00:00"),

        # Bob is a separate email-keyed entity but shares Alice's 415 number.
        ("CRM", "C-200", "bob@example.com", "+1 415 555 0101",
         "Bob Jones", "99 Pine St", 1, "2026-10-30 10:00:00"),

        # Carol has no email; two phone formats should normalize to one key.
        ("CRM", "C-300", None, "+1 212 555 0144",
         "Carol Lee", None, 1, "2026-10-30 11:00:00"),
        ("BILLING", "B-901", "  ", "212-555-0144",
         None, "5 Pine St", 2, "2026-10-31 03:00:00"),

        # No usable contact key: preserve as a source-specific singleton.
        ("SUPPORT", "S-999", None, None,
         "Unknown Contact", None, 3, "2026-10-31 04:00:00"),
    ]
    return spark.createDataFrame(rows, schema)


def normalize_and_key(source_df):
    normalized = (
        source_df
        .withColumn(
            "email_norm",
            F.when(
                F.length(F.trim(F.col("email"))) > 0,
                F.lower(F.trim(F.col("email")))
            )
        )
        .withColumn(
            "phone_digits",
            F.regexp_replace(F.coalesce(F.col("phone"), F.lit("")), r"[^0-9]", "")
        )
        .withColumn(
            "phone_norm",
            F.when(
                (F.length(F.col("phone_digits")) == 11)
                & (F.substring(F.col("phone_digits"), 1, 1) == "1"),
                F.substring(F.col("phone_digits"), 2, 10)
            ).when(
                F.length(F.col("phone_digits")) == 10,
                F.col("phone_digits")
            )
        )
        .withColumn("updated_ts", F.to_timestamp("updated_at"))
        .withColumn(
            "match_key",
            F.when(
                F.col("email_norm").isNotNull(),
                F.concat(F.lit("email:"), F.col("email_norm"))
            )
            .when(
                F.col("phone_norm").isNotNull(),
                F.concat(F.lit("phone:"), F.col("phone_norm"))
            )
            .otherwise(
                F.concat(
                    F.lit("source:"),
                    F.col("source_system"),
                    F.lit(":"),
                    F.col("source_record_id")
                )
            )
        )
        # Stable for this exercise. See production caveat below before using
        # a plain hash of PII as a real enterprise identifier.
        .withColumn("entity_id", F.sha2(F.col("match_key"), 256))
    )
    return normalized


def resolve_entities(normalized):
    # Lower source_priority wins; within a source tier, newest update wins.
    order = [
        F.col("source_priority").asc(),
        F.col("updated_ts").desc(),
        F.col("source_system").asc(),
        F.col("source_record_id").asc(),
    ]
    w_all = (
        Window.partitionBy("entity_id")
        .orderBy(*order)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    w_rank = Window.partitionBy("entity_id").orderBy(*order)

    # Field-level first(non-null) means a high-priority null does not erase a
    # populated value from a lower-priority source.
    enriched = (
        normalized
        .withColumn("survivor_email", F.first("email_norm", ignorenulls=True).over(w_all))
        .withColumn("survivor_phone", F.first("phone_norm", ignorenulls=True).over(w_all))
        .withColumn("survivor_name", F.first("legal_name", ignorenulls=True).over(w_all))
        .withColumn("survivor_address", F.first("address", ignorenulls=True).over(w_all))
        .withColumn("record_rank", F.row_number().over(w_rank))
    )

    metrics = normalized.groupBy("entity_id").agg(
        F.count("*").alias("source_record_count"),
        F.countDistinct("email_norm").alias("distinct_email_count"),
        F.countDistinct("phone_norm").alias("distinct_phone_count"),
    )

    # A phone attached to more than one email-derived entity is a collision,
    # not proof that those entities should be merged.
    shared_phones = (
        normalized
        .filter(F.col("phone_norm").isNotNull())
        .groupBy("phone_norm")
        .agg(F.countDistinct("entity_id").alias("entity_count"))
        .filter(F.col("entity_count") > 1)
        .select("phone_norm")
    )
    collision_by_entity = (
        normalized
        .join(shared_phones, on="phone_norm", how="inner")
        .groupBy("entity_id")
        .agg(F.max(F.lit(1)).cast("boolean").alias("phone_collision"))
    )

    canonical = (
        enriched
        .filter(F.col("record_rank") == 1)
        .select(
            "entity_id", "match_key",
            F.col("survivor_email").alias("email"),
            F.col("survivor_phone").alias("phone"),
            F.col("survivor_name").alias("legal_name"),
            F.col("survivor_address").alias("address"),
        )
        .join(metrics, on="entity_id", how="inner")
        .join(collision_by_entity, on="entity_id", how="left")
        .fillna({"phone_collision": False})
        .withColumn(
            "phone_conflict",
            F.col("distinct_phone_count") > 1
        )
        .withColumn(
            "email_conflict",
            F.col("distinct_email_count") > 1
        )
    )
    return canonical


def main():
    spark = (
        SparkSession.builder
        .appName("CustomerEntityResolutionSurvivorship")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    source_df = build_source_records(spark)
    normalized = normalize_and_key(source_df)
    canonical = resolve_entities(normalized)

    print("\nNormalized source records:")
    normalized.select(
        "source_system", "source_record_id", "email_norm", "phone_norm",
        "match_key", "entity_id"
    ).orderBy("match_key", "source_system").show(truncate=False)

    print("\nCanonical entities and stewardship flags:")
    canonical.orderBy("match_key").show(truncate=False)

    # ---------------------------- Assertions ----------------------------
    rows = {row["match_key"]: row.asDict() for row in canonical.collect()}
    assert len(rows) == 4, f"Expected 4 conservative entities, got {len(rows)}"

    alice = rows["email:alice@example.com"]
    assert alice["source_record_count"] == 3
    assert alice["legal_name"] == "Alice Morgan"       # priority 1 source
    assert alice["phone"] == "4155550101"              # first non-null by priority
    assert alice["address"] == "12 Market St"          # filled from billing
    assert alice["phone_conflict"] is True              # 415 and 646 disagree
    assert alice["phone_collision"] is True             # 415 is also Bob's phone

    bob = rows["email:bob@example.com"]
    assert bob["phone_collision"] is True

    carol = rows["phone:2125550144"]
    assert carol["source_record_count"] == 2
    assert carol["legal_name"] == "Carol Lee"
    assert carol["address"] == "5 Pine St"
    assert carol["phone_conflict"] is False

    unknown_key = "source:SUPPORT:S-999"
    assert unknown_key in rows
    assert rows[unknown_key]["source_record_count"] == 1

    assert canonical.select("entity_id").distinct().count() == canonical.count()
    print("PASS: entity count, survivorship, conflict flags, and uniqueness checks")

    # Inspect the plan. Expect exchanges for window/grouping operations.
    canonical.explain(mode="formatted")
    spark.stop()


if __name__ == "__main__":
    main()


# Optimization and production notes
# ---------------------------------
# 1. Normalize once in a curated staging layer. Persist only if the normalized
#    frame is reused by multiple expensive branches and fits the storage budget.
# 2. Exact blocking keys (email/phone) avoid an all-pairs O(n^2) comparison.
#    Fuzzy matching should generate bounded candidates within blocks, then use
#    scored evidence and a steward review threshold.
# 3. The windows and countDistinct aggregations shuffle by entity_id. Project
#    only required columns before them and inspect the physical plan.
# 4. Do not collect production-scale entity data to the driver. The collect()
#    above is safe only because the exercise intentionally has seven rows.
# 5. A plain SHA-256 hash of email/phone is not a privacy boundary: common
#    values can be guessed. Use a governed token service or keyed HMAC, with
#    secret management outside source code.
# 6. Hashing the match key is deterministic for the exercise, but a corrected
#    email changes the key. Production systems should assign a durable
#    enterprise_entity_id and maintain a reviewed source-key crosswalk.
# 7. Source priority, tie-breaking, survivorship, merge/unmerge, and retention
#    policies should be versioned data contracts, not hard-coded assumptions.

# Interview follow-ups
# --------------------
# 1. How would you resolve transitive matches when A matches B by email and
#    B matches C by phone, but A and C share neither key?
# 2. How would you prevent two concurrent batches from assigning conflicting
#    canonical IDs to the same newly observed person?
# 3. When should a phone collision create a quarantine versus a stewardship
#    task, and how would you measure false-positive merges?
# 4. How would you handle an email correction without changing the durable
#    enterprise customer ID?
# 5. What are the trade-offs between deterministic rules, probabilistic
#    matching, and human-in-the-loop entity resolution?
