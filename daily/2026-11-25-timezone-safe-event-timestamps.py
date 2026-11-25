"""
PySpark Interview Practice: Time-Zone-Safe Event Timestamp Normalization
========================================================================

Problem
-------
An event platform receives ISO-8601 timestamps from services in several
regions. Normalize every offset-aware timestamp to a canonical UTC instant,
derive the region's local business date, and quarantine malformed timestamps
or regions without a configured IANA time zone.

The sample deliberately includes the 2026 New York daylight-saving fall-back:
01:30 occurs twice, once at -04:00 and once at -05:00. The explicit offsets
identify two different instants even though the local wall-clock text matches.

Run
---
    spark-submit daily/2026-11-25-timezone-safe-event-timestamps.py

Compatibility: PySpark 3.x
"""

from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX"

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("region", StringType(), nullable=False),
        StructField("event_ts_iso", StringType(), nullable=True),
    ]
)

TIME_ZONE_SCHEMA = StructType(
    [
        StructField("region", StringType(), nullable=False),
        StructField("time_zone", StringType(), nullable=False),
    ]
)

OUTPUT_COLUMNS = [
    "event_id",
    "region",
    "event_ts_iso",
    "event_ts_utc",
    "time_zone",
    "utc_date",
    "local_business_date",
    "reject_reason",
]


def build_sample_data(spark):
    """Create deterministic event and region-to-IANA-time-zone DataFrames."""
    event_rows = [
        ("IN-001", "IN", "2026-11-25T09:30:00+05:30"),
        ("US-001", "US", "2026-11-25T09:00:00-05:00"),
        ("DE-001", "DE", "2026-11-25T15:00:00+01:00"),
        # New York's repeated 01:30 during the fall-back transition.
        ("US-DST-EDT", "US", "2026-11-01T01:30:00-04:00"),
        ("US-DST-EST", "US", "2026-11-01T01:30:00-05:00"),
        # Local midnight is still the previous UTC calendar date.
        ("IN-MIDNIGHT", "IN", "2026-11-01T00:15:00+05:30"),
        ("JP-MIDNIGHT", "JP", "2026-11-01T00:15:00+09:00"),
        ("BAD-TS", "IN", "not-a-timestamp"),
        ("MISSING-TS", "DE", None),
        ("UNKNOWN-TZ", "XX", "2026-11-25T10:00:00+00:00"),
    ]

    time_zone_rows = [
        ("IN", "Asia/Kolkata"),
        ("US", "America/New_York"),
        ("DE", "Europe/Berlin"),
        ("JP", "Asia/Tokyo"),
    ]

    events = spark.createDataFrame(event_rows, EVENT_SCHEMA)
    time_zones = spark.createDataFrame(time_zone_rows, TIME_ZONE_SCHEMA)
    return events, time_zones


def baseline_solution(events, time_zones):
    """
    Readable baseline:
    1. Parse the source's explicit UTC offset into a Spark timestamp.
    2. Join the region time-zone reference.
    3. Derive UTC and local business dates.
    4. Retain invalid rows with a quarantine reason.
    """
    parsed = events.withColumn(
        "event_ts_utc",
        F.to_timestamp(F.col("event_ts_iso"), TIMESTAMP_FORMAT),
    )

    joined = parsed.join(time_zones, on="region", how="left")

    return (
        joined
        .withColumn("utc_date", F.to_date(F.col("event_ts_utc")))
        .withColumn(
            "local_business_date",
            F.to_date(
                F.from_utc_timestamp(
                    F.col("event_ts_utc"),
                    F.col("time_zone"),
                )
            ),
        )
        .withColumn(
            "reject_reason",
            F.when(
                F.col("event_ts_utc").isNull(),
                F.lit("INVALID_OR_MISSING_TIMESTAMP"),
            ).when(
                F.col("time_zone").isNull(),
                F.lit("UNKNOWN_REGION_TIME_ZONE"),
            ),
        )
        .select(*OUTPUT_COLUMNS)
    )


def optimized_solution(events, time_zones):
    """
    Optimized path for a genuinely small region reference:
    - Project only the columns needed by the pipeline.
    - Parse the source timestamp once.
    - Broadcast the small time-zone lookup to avoid a shuffle join.
    - Keep all parsing and date derivation in native Spark expressions.
    """
    projected = (
        events
        .select("event_id", "region", "event_ts_iso")
        .withColumn(
            "event_ts_utc",
            F.to_timestamp(F.col("event_ts_iso"), TIMESTAMP_FORMAT),
        )
    )

    joined = projected.join(
        F.broadcast(time_zones.select("region", "time_zone")),
        on="region",
        how="left",
    )

    return joined.select(
        "event_id",
        "region",
        "event_ts_iso",
        "event_ts_utc",
        "time_zone",
        F.to_date(F.col("event_ts_utc")).alias("utc_date"),
        F.to_date(
            F.from_utc_timestamp(
                F.col("event_ts_utc"),
                F.col("time_zone"),
            )
        ).alias("local_business_date"),
        F.when(
            F.col("event_ts_utc").isNull(),
            F.lit("INVALID_OR_MISSING_TIMESTAMP"),
        ).when(
            F.col("time_zone").isNull(),
            F.lit("UNKNOWN_REGION_TIME_ZONE"),
        ).alias("reject_reason"),
    )


def validate_results(result):
    accepted = result.filter(F.col("reject_reason").isNull())
    quarantine = result.filter(F.col("reject_reason").isNotNull())

    assert accepted.count() == 7, "Expected seven accepted events"
    assert quarantine.count() == 3, "Expected three quarantined events"

    # The two repeated New York wall-clock times are distinct UTC instants.
    actual = {
        row["event_id"]: row
        for row in (
            accepted
            .select(
                "event_id",
                F.date_format(
                    F.col("event_ts_utc"),
                    "yyyy-MM-dd HH:mm:ss",
                ).alias("utc_text"),
                "utc_date",
                "local_business_date",
            )
            .collect()
        )
    }

    assert actual["US-DST-EDT"]["utc_text"] == "2026-11-01 05:30:00"
    assert actual["US-DST-EST"]["utc_text"] == "2026-11-01 06:30:00"
    assert actual["US-DST-EDT"]["utc_text"] != actual["US-DST-EST"]["utc_text"]

    # A local business date can differ from the UTC calendar date.
    assert actual["IN-MIDNIGHT"]["utc_date"] == date(2026, 10, 31)
    assert actual["IN-MIDNIGHT"]["local_business_date"] == date(2026, 11, 1)
    assert actual["JP-MIDNIGHT"]["utc_date"] == date(2026, 10, 31)
    assert actual["JP-MIDNIGHT"]["local_business_date"] == date(2026, 11, 1)

    reason_counts = {
        row["reject_reason"]: row["count"]
        for row in quarantine.groupBy("reject_reason").count().collect()
    }
    assert reason_counts == {
        "INVALID_OR_MISSING_TIMESTAMP": 2,
        "UNKNOWN_REGION_TIME_ZONE": 1,
    }, reason_counts

    daily_counts = {
        (row["region"], row["business_date"].isoformat()): row["event_count"]
        for row in (
            accepted
            .groupBy("region", F.col("local_business_date").alias("business_date"))
            .agg(F.count("*").alias("event_count"))
            .collect()
        )
    }

    expected_daily_counts = {
        ("IN", "2026-11-25"): 1,
        ("US", "2026-11-25"): 1,
        ("DE", "2026-11-25"): 1,
        ("US", "2026-11-01"): 2,
        ("IN", "2026-11-01"): 1,
        ("JP", "2026-11-01"): 1,
    }
    assert daily_counts == expected_daily_counts, daily_counts

    print("Accepted events:", accepted.count())
    print("Quarantined events:", quarantine.count())
    print("All timestamp, DST, local-date, and quarantine assertions passed.")

    print("\nDaily counts by region-local business date:")
    (
        accepted
        .groupBy("region", "local_business_date")
        .agg(F.count("*").alias("event_count"))
        .orderBy("local_business_date", "region")
        .show(truncate=False)
    )

    print("Quarantine records:")
    (
        quarantine
        .select("event_id", "region", "event_ts_iso", "reject_reason")
        .orderBy("event_id")
        .show(truncate=False)
    )


def main():
    spark = (
        SparkSession.builder
        .appName("TimezoneSafeEventTimestampPractice")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Set UTC before parsing or deriving dates so display and date extraction
    # are deterministic across machines and developer environments.
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    events, time_zones = build_sample_data(spark)

    baseline = baseline_solution(events, time_zones)
    optimized = optimized_solution(events, time_zones)

    # Both implementations must produce the same rows, including quarantines.
    baseline_only = baseline.exceptAll(optimized).count()
    optimized_only = optimized.exceptAll(baseline).count()
    assert baseline_only == 0 and optimized_only == 0

    print("Baseline and optimized outputs are equivalent.")
    validate_results(optimized)

    print("\nOptimized physical plan:")
    optimized.explain(mode="formatted")

    spark.stop()


if __name__ == "__main__":
    main()


"""
Expected checks
---------------
Accepted events: 7
Quarantined events: 3

US-DST-EDT -> 2026-11-01 05:30:00 UTC
US-DST-EST -> 2026-11-01 06:30:00 UTC
IN-MIDNIGHT -> UTC date 2026-10-31, local business date 2026-11-01
JP-MIDNIGHT -> UTC date 2026-10-31, local business date 2026-11-01

Quarantine reasons:
- INVALID_OR_MISSING_TIMESTAMP: 2
- UNKNOWN_REGION_TIME_ZONE: 1

Optimization notes
------------------
- Timestamp strings contain explicit offsets. Parsing them with XXX identifies
  an instant; do not apply the source offset a second time with
  to_utc_timestamp().
- A Spark TimestampType represents an instant. The session time zone affects
  rendering and date extraction, so use UTC for canonical processing.
- from_utc_timestamp() is used only to derive a region-local wall-clock date.
- The region-to-time-zone table is tiny in this exercise, so a broadcast join
  avoids a shuffle. Do not broadcast an unbounded or unexpectedly large table.
- Native Spark expressions avoid Python serialization and keep the work visible
  to Catalyst. Keep the original ISO string for audit and replay.

Important production caveats
----------------------------
- Offset-bearing timestamps disambiguate the repeated hour during a DST
  fall-back. A naive local timestamp such as 2026-11-01 01:30:00 in New York
  does not identify a unique instant.
- Define an explicit policy for nonexistent local times during spring-forward
  and for ambiguous naive timestamps; do not silently guess.
- Validate that source offsets agree with the configured IANA region when the
  source contract requires that guarantee.
- Choose UTC dates or local business dates deliberately for partitioning and
  reporting; they answer different questions.

Interview follow-up questions
-----------------------------
1. What is the difference between TimestampType and TimestampNTZType?
2. Why should a distributed pipeline standardize its session time zone?
3. Why are the two New York 01:30 timestamps different instants?
4. How would you handle a naive local timestamp during a DST gap or overlap?
5. How could you validate that a supplied offset matches the region's IANA zone?
6. When is a broadcast join unsafe, and how can you confirm the chosen strategy?
7. Should a global event table be partitioned by UTC date or local business date?
"""
