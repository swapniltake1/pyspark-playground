"""
Daily PySpark Interview Practice — 2026-11-06
Topic: CDC Upserts, Deletes, Deterministic Ordering, and Idempotent Snapshots

Run:
    spark-submit daily/2026-11-06-cdc-upsert-delete-idempotency.py

Compatible with PySpark 3.3+. Uses in-memory DataFrames only.
"""

from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)


def build_frames(spark):
    event_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("operation", StringType(), False),  # I, U, D
        StructField("source_version", IntegerType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True),
    ])
    events = [
        ("e1", "C001", "I", 1, datetime(2026, 11, 5, 9, 0), datetime(2026, 11, 5, 9, 1), "a@example.com", "Pune"),
        ("e2", "C002", "I", 1, datetime(2026, 11, 5, 9, 2), datetime(2026, 11, 5, 9, 3), "b@example.com", "Mumbai"),
        ("e3", "C001", "U", 2, datetime(2026, 11, 5, 10, 0), datetime(2026, 11, 5, 10, 1), "a@example.com", "Nashik"),
        # A replay of e3: same logical version, later ingestion timestamp.
        ("e3-replay", "C001", "U", 2, datetime(2026, 11, 5, 10, 0), datetime(2026, 11, 5, 10, 5), "a@example.com", "Nashik"),
        ("e4", "C002", "D", 2, datetime(2026, 11, 5, 11, 0), datetime(2026, 11, 5, 11, 1), None, None),
        ("e5", "C003", "I", 1, datetime(2026, 11, 5, 11, 5), datetime(2026, 11, 5, 11, 6), "c@example.com", "Pune"),
        # Older version arrives late; it must not overwrite version 2.
        ("e6-late", "C001", "U", 1, datetime(2026, 11, 5, 9, 30), datetime(2026, 11, 5, 12, 0), "old@example.com", "Delhi"),
    ]
    current_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("source_version", IntegerType(), False),
        StructField("is_deleted", StringType(), False),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True),
    ])
    current = [
        ("C001", 1, "false", "a@example.com", "Pune"),
        ("C002", 1, "false", "b@example.com", "Mumbai"),
    ]
    return (
        spark.createDataFrame(events, event_schema),
        spark.createDataFrame(current, current_schema)
    )


def latest_event_per_customer(events):
    # Deterministic ordering: source version first; event time and event ID
    # break ties. In production, source sequence/LSN is preferable to timestamps.
    w = Window.partitionBy("customer_id").orderBy(
        F.col("source_version").desc(),
        F.col("event_ts").desc(),
        F.col("event_id").desc()
    )
    return (
        events.withColumn("rn", F.row_number().over(w))
              .filter(F.col("rn") == 1)
              .drop("rn")
    )


def apply_snapshot(current, latest):
    # A snapshot includes tombstones so a repeated delete remains idempotent.
    c = current.select(
        "customer_id",
        F.col("source_version").alias("target_version"),
        F.col("is_deleted").cast("boolean").alias("target_deleted"),
        F.col("email").alias("target_email"),
        F.col("city").alias("target_city")
    )
    joined = latest.alias("s").join(c.alias("t"), "customer_id", "full_outer")
    result = joined.select(
        F.coalesce(F.col("s.customer_id"), F.col("t.customer_id")).alias("customer_id"),
        F.coalesce(F.col("s.source_version"), F.col("t.target_version")).alias("source_version"),
        F.when(F.col("s.customer_id").isNull(), F.col("t.target_deleted"))
         .otherwise(F.col("s.operation") == "D").alias("is_deleted"),
        F.when(F.col("s.customer_id").isNull(), F.col("t.target_email"))
         .otherwise(F.col("s.email")).alias("email"),
        F.when(F.col("s.customer_id").isNull(), F.col("t.target_city"))
         .otherwise(F.col("s.city")).alias("city"),
        F.col("s.source_version").alias("incoming_version"),
        F.col("t.target_version")
    )
    # Ignore stale source versions. Keep the target unchanged in that case.
    return result.select(
        "customer_id",
        F.when(
            F.col("incoming_version").isNotNull()
            & F.col("target_version").isNotNull()
            & (F.col("incoming_version") < F.col("target_version")),
            F.col("target_version")
        ).otherwise(F.col("source_version")).alias("source_version"),
        F.when(
            F.col("incoming_version").isNotNull()
            & F.col("target_version").isNotNull()
            & (F.col("incoming_version") < F.col("target_version")),
            F.col("target_version").isNotNull() & F.lit(False)
        ).otherwise(F.col("is_deleted")).alias("_stale_placeholder"),
        F.when(
            F.col("incoming_version").isNotNull()
            & F.col("target_version").isNotNull()
            & (F.col("incoming_version") < F.col("target_version")),
            F.lit(None).cast("string")
        ).otherwise(F.col("email")).alias("email"),
        F.when(
            F.col("incoming_version").isNotNull()
            & F.col("target_version").isNotNull()
            & (F.col("incoming_version") < F.col("target_version")),
            F.lit(None).cast("string")
        ).otherwise(F.col("city")).alias("city"),
        F.col("target_version")
    )


def main():
    spark = SparkSession.builder.appName("CDCUpsertDeleteIdempotency").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    events, current = build_frames(spark)

    # First collapse transport replays for the same event/version.
    event_window = Window.partitionBy("customer_id", "source_version").orderBy(
        F.col("ingested_at").desc(), F.col("event_id").desc()
    )
    canonical = (
        events.withColumn("event_rank", F.row_number().over(event_window))
              .filter(F.col("event_rank") == 1)
              .drop("event_rank")
    )
    latest = latest_event_per_customer(canonical)

    print("Canonical latest source event per customer:")
    latest.orderBy("customer_id").show(truncate=False)

    # More explicit merge logic: only accept a newer source version.
    source = latest.alias("s")
    target = current.alias("t")
    merged = source.join(target, "customer_id", "full_outer")

    final_state = merged.select(
        F.coalesce(F.col("s.customer_id"), F.col("t.customer_id")).alias("customer_id"),
        F.when(F.col("s.customer_id").isNull(), F.col("t.source_version"))
         .when(F.col("t.customer_id").isNull(), F.col("s.source_version"))
         .when(F.col("s.source_version") > F.col("t.source_version"), F.col("s.source_version"))
         .otherwise(F.col("t.source_version")).alias("source_version"),
        F.when(F.col("s.customer_id").isNull(), F.col("t.is_deleted").cast("boolean"))
         .when(F.col("t.customer_id").isNull(), F.col("s.operation") == "D")
         .when(F.col("s.source_version") > F.col("t.source_version"), F.col("s.operation") == "D")
         .otherwise(F.col("t.is_deleted").cast("boolean")).alias("is_deleted"),
        F.when(F.col("s.customer_id").isNull(), F.col("t.email"))
         .when(F.col("t.customer_id").isNull(), F.col("s.email"))
         .when(F.col("s.source_version") > F.col("t.source_version"), F.col("s.email"))
         .otherwise(F.col("t.email")).alias("email"),
        F.when(F.col("s.customer_id").isNull(), F.col("t.city"))
         .when(F.col("t.customer_id").isNull(), F.col("s.city"))
         .when(F.col("s.source_version") > F.col("t.source_version"), F.col("s.city"))
         .otherwise(F.col("t.city")).alias("city")
    )

    print("Merged target snapshot (including tombstones):")
    final_state.orderBy("customer_id").show(truncate=False)

    actual = {
        row["customer_id"]: (row["source_version"], row["is_deleted"], row["city"])
        for row in final_state.collect()
    }
    expected = {
        "C001": (2, False, "Nashik"),
        "C002": (2, True, None),
        "C003": (1, False, "Pune"),
    }
    assert actual == expected, f"Unexpected final state: {actual}"

    # Idempotency check: applying the same latest batch again must not change state.
    replay = final_state.alias("t").join(latest.alias("s"), "customer_id", "left")
    replayed = replay.select(
        F.col("t.customer_id"),
        F.when(
            F.col("s.source_version").isNotNull()
            & (F.col("s.source_version") > F.col("t.source_version")),
            F.col("s.source_version")
        ).otherwise(F.col("t.source_version")).alias("source_version"),
        F.when(
            F.col("s.source_version").isNotNull()
            & (F.col("s.source_version") > F.col("t.source_version")),
            F.col("s.operation") == "D"
        ).otherwise(F.col("t.is_deleted")).alias("is_deleted"),
        F.when(
            F.col("s.source_version").isNotNull()
            & (F.col("s.source_version") > F.col("t.source_version")),
            F.col("s.city")
        ).otherwise(F.col("t.city")).alias("city")
    )
    replay_actual = {
        row["customer_id"]: (row["source_version"], row["is_deleted"], row["city"])
        for row in replayed.collect()
    }
    assert replay_actual == expected, "Reapplying the same CDC batch changed the target"
    print("PASS: latest-version, delete tombstone, late-event, and idempotency assertions")

    # Interview follow-ups:
    # 1. Why should the source LSN/sequence outrank ingestion timestamp?
    # 2. How would you persist tombstones and apply retention safely?
    # 3. How do you handle two events with the same version but conflicting payloads?
    # 4. How would this translate to Delta MERGE with a version predicate?
    # 5. How do you make checkpoint/watermark advancement atomic with the write?
    # 6. What changes if the source emits before/after images rather than full rows?

    spark.stop()


if __name__ == "__main__":
    main()
