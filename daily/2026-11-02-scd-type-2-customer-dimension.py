"""
PySpark Interview Practice — SCD Type 2 Customer Dimension
=========================================================

Problem
-------
A customer change feed arrives each day. Build a Type 2 dimension that:
1. detects new customers and attribute changes,
2. expires the previous current version,
3. inserts the new current version,
4. leaves unchanged customers untouched, and
5. preserves all prior history.

This example uses half-open validity intervals:
    valid_from <= event_time < valid_to

The source is a PARTIAL CHANGE FEED, not a complete snapshot. A customer
missing from the feed must not be interpreted as deleted.

Run:
    spark-submit daily/2026-11-02-scd-type-2-customer-dimension.py

Compatibility: PySpark 3.3+
"""

from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.storagelevel import StorageLevel


OPEN_END = datetime(2099, 12, 31, 23, 59, 59)


def build_spark():
    return (
        SparkSession.builder
        .appName("SCD-Type-2-Customer-Dimension-Interview")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def assert_unique(df, key, label):
    duplicates = (
        df.groupBy(key)
        .count()
        .filter(F.col("count") > 1)
    )
    assert duplicates.limit(1).count() == 0, (
        f"{label} must contain one row per {key}"
    )


def detect_changes(updates_df, active_dimension_df, broadcast_dimension=False):
    """
    Return one row for each new or changed business key.

    The optimized variant broadcasts the current dimension only because
    this interview fixture is intentionally small. Do not force broadcast
    in production unless the filtered dimension safely fits executor memory.
    """
    dimension_side = (
        F.broadcast(active_dimension_df)
        if broadcast_dimension
        else active_dimension_df
    )

    joined = updates_df.alias("s").join(
        dimension_side.alias("d"),
        F.col("s.customer_id") == F.col("d.customer_id"),
        "left",
    )

    is_new = F.col("d.customer_id").isNull()
    attributes_changed = (
        (~F.col("s.customer_name").eqNullSafe(F.col("d.customer_name")))
        | (~F.col("s.city").eqNullSafe(F.col("d.city")))
        | (~F.col("s.segment").eqNullSafe(F.col("d.segment")))
    )

    return (
        joined
        .filter(is_new | attributes_changed)
        .select(
            F.col("s.customer_id").alias("customer_id"),
            F.col("s.customer_name").alias("customer_name"),
            F.col("s.city").alias("city"),
            F.col("s.segment").alias("segment"),
            F.col("s.change_ts").alias("effective_from"),
        )
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # 1. Realistic source and target data
    # ------------------------------------------------------------------
    dimension_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("city", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("valid_from", TimestampType(), False),
        StructField("valid_to", TimestampType(), False),
        StructField("is_current", BooleanType(), False),
    ])

    dimension_rows = [
        # Older historical version for C001.
        ("C001", "Asha Rao", "Pune", "Bronze",
         datetime(2025, 6, 1, 0, 0), datetime(2026, 1, 1, 0, 0), False),

        # Current versions before today's feed.
        ("C001", "Asha Rao", "Pune", "Silver",
         datetime(2026, 1, 1, 0, 0), OPEN_END, True),
        ("C002", "Rohan Shah", "Mumbai", "Gold",
         datetime(2026, 2, 1, 0, 0), OPEN_END, True),
        ("C003", "Meera Iyer", "Nashik", "Bronze",
         datetime(2026, 1, 15, 0, 0), OPEN_END, True),
        ("C004", "Kabir Desai", "Pune", "Gold",
         datetime(2026, 3, 1, 0, 0), OPEN_END, True),
    ]

    update_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("city", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("change_ts", TimestampType(), False),
    ])

    # C002 is unchanged. C004 is absent because this is not a full snapshot.
    update_rows = [
        ("C001", "Asha Rao", "Pune", "Gold", datetime(2026, 11, 2, 9, 0)),
        ("C002", "Rohan Shah", "Mumbai", "Gold", datetime(2026, 11, 2, 9, 0)),
        ("C003", "Meera Iyer", "Ahmednagar", "Bronze",
         datetime(2026, 11, 2, 9, 5)),
        ("C005", "Neha Kulkarni", "Pune", "Silver",
         datetime(2026, 11, 2, 9, 10)),
    ]

    dimension_df = spark.createDataFrame(dimension_rows, dimension_schema)
    updates_df = spark.createDataFrame(update_rows, update_schema)

    assert_unique(
        dimension_df.filter(F.col("is_current")),
        "customer_id",
        "Current dimension",
    )
    assert_unique(updates_df, "customer_id", "Incoming change feed")

    active_dimension = (
        dimension_df
        .filter(F.col("is_current"))
        .select(
            "customer_id",
            "customer_name",
            "city",
            "segment",
            "valid_from",
        )
    )

    # This simple Type 2 algorithm expects changes later than the current
    # version's start. Backdated events require interval-splitting logic.
    temporal_conflicts = (
        updates_df.alias("s")
        .join(
            active_dimension.alias("d"),
            F.col("s.customer_id") == F.col("d.customer_id"),
            "inner",
        )
        .filter(F.col("s.change_ts") <= F.col("d.valid_from"))
    )
    assert temporal_conflicts.limit(1).count() == 0, (
        "Backdated/equal-effective-time changes need a separate correction path"
    )

    # ------------------------------------------------------------------
    # 2. Baseline change detection and optimized change detection
    # ------------------------------------------------------------------
    baseline_changes = detect_changes(
        updates_df,
        active_dimension,
        broadcast_dimension=False,
    )

    optimized_changes = detect_changes(
        updates_df,
        active_dimension,
        broadcast_dimension=True,
    ).persist(StorageLevel.MEMORY_AND_DISK)

    # Both implementations must produce the same change set.
    assert baseline_changes.exceptAll(optimized_changes).count() == 0
    assert optimized_changes.exceptAll(baseline_changes).count() == 0
    assert optimized_changes.count() == 3  # C001, C003, and new C005

    # Cache is useful here because the change set is consumed by both the
    # close-old-version and insert-new-version branches.
    changed_keys = optimized_changes.select("customer_id", "effective_from")

    # ------------------------------------------------------------------
    # 3. Expire only the previous CURRENT version for changed keys
    # ------------------------------------------------------------------
    closed_current = (
        dimension_df.alias("d")
        .join(
            F.broadcast(changed_keys).alias("c"),
            F.col("d.customer_id") == F.col("c.customer_id"),
            "inner",
        )
        .filter(F.col("d.is_current"))
        .select(
            F.col("d.customer_id").alias("customer_id"),
            F.col("d.customer_name").alias("customer_name"),
            F.col("d.city").alias("city"),
            F.col("d.segment").alias("segment"),
            F.col("d.valid_from").alias("valid_from"),
            # Half-open interval: old version ends exactly when new starts.
            F.col("c.effective_from").alias("valid_to"),
            F.lit(False).alias("is_current"),
        )
    )

    # Keep every already-historical row, including C001's Bronze version.
    existing_history = dimension_df.filter(~F.col("is_current"))

    # Keep current rows whose business keys did not change.
    unchanged_current = (
        dimension_df
        .filter(F.col("is_current"))
        .join(
            changed_keys.select("customer_id").distinct(),
            on="customer_id",
            how="left_anti",
        )
    )

    # ------------------------------------------------------------------
    # 4. Insert the new current version for changed and new customers
    # ------------------------------------------------------------------
    new_versions = optimized_changes.select(
        "customer_id",
        "customer_name",
        "city",
        "segment",
        F.col("effective_from").alias("valid_from"),
        F.lit(OPEN_END).cast("timestamp").alias("valid_to"),
        F.lit(True).alias("is_current"),
    )

    scd2_result = (
        existing_history
        .unionByName(unchanged_current)
        .unionByName(closed_current)
        .unionByName(new_versions)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # ------------------------------------------------------------------
    # 5. Expected output and integrity assertions
    # ------------------------------------------------------------------
    assert scd2_result.count() == 8
    current_rows = scd2_result.filter(F.col("is_current"))
    assert current_rows.count() == 5

    expected_current = {
        "C001": ("Asha Rao", "Pune", "Gold"),
        "C002": ("Rohan Shah", "Mumbai", "Gold"),
        "C003": ("Meera Iyer", "Ahmednagar", "Bronze"),
        "C004": ("Kabir Desai", "Pune", "Gold"),
        "C005": ("Neha Kulkarni", "Pune", "Silver"),
    }

    actual_current = {
        row["customer_id"]: (
            row["customer_name"],
            row["city"],
            row["segment"],
        )
        for row in current_rows.select(
            "customer_id", "customer_name", "city", "segment"
        ).collect()
    }
    assert actual_current == expected_current

    duplicate_current = (
        current_rows.groupBy("customer_id")
        .count()
        .filter(F.col("count") != 1)
    )
    assert duplicate_current.limit(1).count() == 0

    # Check that validity intervals do not overlap. Adjacent half-open
    # intervals are valid when previous.valid_to == next.valid_from.
    version_window = Window.partitionBy("customer_id").orderBy("valid_from")
    interval_violations = (
        scd2_result
        .withColumn("previous_valid_to", F.lag("valid_to").over(version_window))
        .filter(
            F.col("previous_valid_to").isNotNull()
            & (F.col("previous_valid_to") > F.col("valid_from"))
        )
    )
    assert interval_violations.limit(1).count() == 0

    c001_history = (
        scd2_result
        .filter((F.col("customer_id") == "C001") & (~F.col("is_current")))
        .orderBy("valid_from")
        .collect()
    )
    assert len(c001_history) == 2
    assert c001_history[0]["segment"] == "Bronze"
    assert c001_history[1]["segment"] == "Silver"
    assert c001_history[1]["valid_to"] == datetime(2026, 11, 2, 9, 0)

    c003_closed = (
        scd2_result
        .filter(
            (F.col("customer_id") == "C003")
            & (F.col("valid_from") == datetime(2026, 1, 15, 0, 0))
        )
        .select("valid_to")
        .first()
    )
    assert c003_closed["valid_to"] == datetime(2026, 11, 2, 9, 5)

    print("SCD Type 2 result (ordered by business key and valid_from):")
    scd2_result.orderBy("customer_id", "valid_from").show(truncate=False)

    print("Current customer state:")
    current_rows.orderBy("customer_id").show(truncate=False)

    print("Optimized change-detection physical plan:")
    optimized_changes.explain(mode="formatted")

    print("All SCD Type 2 assertions passed.")

    scd2_result.unpersist()
    optimized_changes.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()


# ----------------------------------------------------------------------
# EXPLANATION AND OPTIMIZATION NOTES
# ----------------------------------------------------------------------
# 1. Type 2 behavior:
#    - Unchanged keys retain their existing current row.
#    - Changed keys have their old current row closed and a new row inserted.
#    - New keys receive their first current row.
#    - Existing historical rows are never overwritten.
#
# 2. Null-safe comparisons:
#    eqNullSafe treats NULL-to-NULL as unchanged and NULL-to-value (or the
#    reverse) as a real attribute change. A normal != comparison would yield
#    NULL for some comparisons and can silently miss changes.
#
# 3. Half-open intervals:
#    [valid_from, valid_to) avoids subtracting one millisecond/microsecond.
#    A point-in-time lookup is:
#      event_ts >= valid_from AND event_ts < valid_to
#
# 4. Join and shuffle choices:
#    - Filter the target to is_current before joining.
#    - Project only the business key and compared attributes.
#    - Broadcast only a genuinely small side; otherwise let AQE choose.
#    - The anti join keeps unchanged current keys without collecting IDs.
#
# 5. Reuse:
#    The changed-key DataFrame feeds both close and insert branches, so it is
#    persisted in this example. Cache only when the same expensive result is
#    reused; unpersist after the dependent actions complete.
#
# 6. Production implementation:
#    Persist the staged change set with a stable batch ID, validate one
#    deterministic source row per business key, and apply close + insert in
#    an ACID-capable table transaction (for example, a carefully staged
#    Delta Lake MERGE/write workflow). Make retries idempotent.
#
# 7. Important edge cases:
#    - A partial feed's missing key is not a deletion.
#    - Multiple changes for one key need deterministic event ordering.
#    - Backdated changes require splitting/rebuilding validity intervals.
#    - A delete event should close the current version without inserting a
#      new active version, or insert a tombstone according to the contract.
#    - Do not broadcast a large dimension just to force a join strategy.
#
# INTERVIEW FOLLOW-UPS
# --------------------
# 1. How would you implement this with Delta Lake MERGE and guarantee retries?
# 2. How would you process two source changes for the same customer in one batch?
# 3. How would a late-arriving change with an earlier effective timestamp work?
# 4. How would you model deletes and reactivations?
# 5. When is a broadcast join unsafe, and how do you verify the plan?
# 6. Why use half-open intervals instead of subtracting one millisecond?
# 7. How would you detect and repair overlapping SCD intervals at scale?
# 8. How would you handle schema evolution in the source change feed?
