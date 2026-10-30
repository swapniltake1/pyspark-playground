"""
Daily PySpark Interview Practice
Topic: Event-time sessionization with late-arriving events

Problem
-------
Group customer events into sessions. A new session starts only when the gap
between consecutive event timestamps is GREATER THAN 30 minutes. Input rows
are deliberately out of event-time order: one event arrives after a later
event and changes the session boundary. Reject records missing a customer ID,
event ID, or event timestamp.

Run locally:
    spark-submit daily/2026-10-30-event-time-sessionization.py

The sample is self-contained and uses only in-memory DataFrames.
"""

from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)


SESSION_GAP_SECONDS = 30 * 60


def timestamp(hour, minute, second=0):
    """Create a timestamp on the practice date."""
    return datetime(2026, 10, 30, hour, minute, second)


def sessionize_basic(events, gap_seconds=SESSION_GAP_SECONDS):
    """Straightforward implementation that retains the complete input row."""
    event_order = (
        Window.partitionBy("customer_id")
        .orderBy(F.col("event_ts"), F.col("event_id"))
    )
    cumulative_rows = event_order.rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )

    with_previous = events.withColumn(
        "previous_event_ts", F.lag("event_ts").over(event_order)
    )
    marked = with_previous.withColumn(
        "is_new_session",
        F.when(
            F.col("previous_event_ts").isNull()
            | (
                F.col("event_ts").cast("long")
                - F.col("previous_event_ts").cast("long")
                > F.lit(gap_seconds)
            ),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )

    return marked.withColumn(
        "session_number", F.sum("is_new_session").over(cumulative_rows)
    )


def sessionize_optimized(events, gap_seconds=SESSION_GAP_SECONDS):
    """
    Production-oriented variant.

    Project only the columns required for session assignment before the
    partitioning/sort work. Built-in Spark expressions avoid Python UDF
    serialization and keep the calculation in Catalyst/Tungsten execution.
    """
    narrow_events = events.select("customer_id", "event_id", "event_ts")

    event_order = (
        Window.partitionBy("customer_id")
        .orderBy(F.col("event_ts"), F.col("event_id"))
    )
    cumulative_rows = event_order.rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )

    lagged = narrow_events.withColumn(
        "previous_event_ts", F.lag("event_ts").over(event_order)
    )

    marked = lagged.select(
        "customer_id",
        "event_id",
        "event_ts",
        F.when(
            F.col("previous_event_ts").isNull()
            | (
                F.col("event_ts").cast("long")
                - F.col("previous_event_ts").cast("long")
                > F.lit(gap_seconds)
            ),
            F.lit(1),
        ).otherwise(F.lit(0)).alias("is_new_session"),
    )

    return marked.withColumn(
        "session_number", F.sum("is_new_session").over(cumulative_rows)
    ).select("customer_id", "event_id", "event_ts", "session_number")


def main():
    spark = (
        SparkSession.builder
        .appName("DailyPractice-EventTimeSessionization")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    schema = StructType(
        [
            StructField("customer_id", StringType(), True),
            StructField("event_id", StringType(), True),
            StructField("event_ts", TimestampType(), True),
            StructField("event_type", StringType(), True),
            StructField("ingested_at", TimestampType(), True),
        ]
    )

    rows = [
        # Customer C101: three sessions because the gaps are 35 and 50 min.
        ("C101", "e-001", timestamp(9, 0), "page_view", timestamp(9, 0, 5)),
        ("C101", "e-002", timestamp(9, 10), "add_to_cart", timestamp(9, 10, 5)),
        ("C101", "e-003", timestamp(9, 45), "purchase", timestamp(9, 45, 2)),
        ("C101", "e-004", timestamp(10, 0), "page_view", timestamp(10, 0, 5)),
        ("C101", "e-005", timestamp(10, 50), "checkout", timestamp(10, 50, 1)),
        ("C101", "e-006", timestamp(10, 55), "purchase", timestamp(10, 55, 3)),
        # Customer C202: e-010 is ingested late. In event time it belongs
        # between e-008 and e-009, joining what would otherwise be two sessions.
        ("C202", "e-007", timestamp(11, 0), "page_view", timestamp(11, 0, 5)),
        ("C202", "e-008", timestamp(11, 30), "search", timestamp(11, 30, 4)),
        ("C202", "e-009", timestamp(12, 1), "purchase", timestamp(12, 5, 0)),
        ("C202", "e-010", timestamp(11, 50), "add_to_cart", timestamp(12, 10, 0)),
        # Invalid rows are quarantined by the data-quality filter.
        (None, "e-011", timestamp(13, 0), "page_view", timestamp(13, 0, 1)),
        ("C303", "e-012", None, "purchase", timestamp(13, 1, 0)),
    ]

    raw_events = spark.createDataFrame(rows, schema)

    valid_condition = (
        F.col("customer_id").isNotNull()
        & F.col("event_id").isNotNull()
        & F.col("event_ts").isNotNull()
    )
    invalid_events = raw_events.filter(~valid_condition)
    valid_events = raw_events.filter(valid_condition)

    invalid_count = invalid_events.count()
    assert invalid_count == 2, f"Expected 2 invalid rows, got {invalid_count}"

    # Compare the straightforward and narrow-projection implementations.
    basic = sessionize_basic(valid_events).select(
        "customer_id", "event_id", "event_ts", "session_number"
    )
    optimized = sessionize_optimized(valid_events)

    assert basic.exceptAll(optimized).count() == 0
    assert optimized.exceptAll(basic).count() == 0

    # Session-level output: one row per customer and session.
    session_summary = (
        optimized.groupBy("customer_id", "session_number")
        .agg(
            F.min("event_ts").alias("session_start"),
            F.max("event_ts").alias("session_end"),
            F.count("*").alias("event_count"),
        )
        .orderBy("customer_id", "session_number")
    )

    actual = [
        (
            row["customer_id"],
            int(row["session_number"]),
            int(row["event_count"]),
            row["session_start"].strftime("%H:%M"),
            row["session_end"].strftime("%H:%M"),
        )
        for row in session_summary.collect()
    ]

    expected = [
        ("C101", 1, 2, "09:00", "09:10"),
        ("C101", 2, 2, "09:45", "10:00"),
        ("C101", 3, 2, "10:50", "10:55"),
        ("C202", 1, 4, "11:00", "12:01"),
    ]
    assert actual == expected, f"Unexpected session summary: {actual}"

    # Prove the late-arriving event changes the session boundary.
    without_late_event = valid_events.filter(F.col("event_id") != "e-010")
    sessions_before_late_event = (
        sessionize_optimized(without_late_event)
        .filter(F.col("customer_id") == "C202")
        .select("session_number")
        .distinct()
        .count()
    )
    sessions_after_late_event = (
        optimized.filter(F.col("customer_id") == "C202")
        .select("session_number")
        .distinct()
        .count()
    )
    assert sessions_before_late_event == 2
    assert sessions_after_late_event == 1

    print("\nSession summary:")
    session_summary.show(truncate=False)

    print("Event-level session assignment:")
    optimized.orderBy("customer_id", "event_ts", "event_id").show(truncate=False)

    print(
        f"\nAll assertions passed. Invalid rows quarantined: {invalid_count}; "
        f"C202 sessions before/after late event: "
        f"{sessions_before_late_event}/{sessions_after_late_event}."
    )

    # Inspect the physical plan when discussing shuffle and sort costs.
    print("\nPhysical plan:")
    optimized.explain("formatted")

    spark.stop()


if __name__ == "__main__":
    main()


"""
Interview discussion and optimization notes
--------------------------------------------

1. Why use event_ts rather than ingested_at?
   Session boundaries are based on when the activity happened. ingested_at
   records arrival/processing time and is useful for measuring lateness, not
   ordering user activity.

2. Why is event_id part of the ordering?
   A timestamp alone may not uniquely order events. A stable tie-breaker makes
   the result deterministic when two events share the same event timestamp.
   In production, use a source sequence or another stable unique key if event_id
   is not guaranteed to be unique.

3. Why is the comparison greater-than, not greater-than-or-equal?
   The business rule says a new session starts only when the gap exceeds
   30 minutes. Therefore, an event exactly 30 minutes after the previous event
   remains in the same session.

4. What makes the optimized variant preferable?
   It drops unused event_type and ingested_at columns before the window shuffle,
   uses native Spark expressions instead of Python UDFs, and defines an
   explicit ROWS frame for the running session number. The two window
   expressions share the same partition and ordering keys, allowing Spark to
   plan compatible window work together. Confirm the actual plan with explain()
   rather than assuming a particular physical plan.

5. What is the main scalability bottleneck?
   Window.partitionBy(customer_id).orderBy(event_ts, event_id) requires data
   to be colocated and sorted by customer. A very active customer can create a
   hot partition. Do not salt customer_id naively: splitting one customer's
   timeline can break session continuity. A scalable design needs boundary
   reconciliation between time chunks or a stateful streaming strategy.

6. How should streaming differ?
   Use event-time processing with a watermark and stateful session windows
   (for example, groupBy(customer_id, session_window(event_ts, "30 minutes"))).
   The watermark controls how long late data can update state. Events arriving
   beyond the allowed lateness may be dropped from stateful results. Exact
   behavior depends on the output mode and Spark version.

7. What would you add in production?
   - Deterministic deduplication by event_id before sessionization.
   - A documented late-arrival SLA and replay/lookback strategy.
   - Data-quality metrics for rejected records and timestamp anomalies.
   - Idempotent writes keyed by customer/session or a stable session identity.
   - Tests for daylight-saving/time-zone handling and events at the exact gap.
   - Monitoring for skew, shuffle spill, task duration, and state growth.
"""
