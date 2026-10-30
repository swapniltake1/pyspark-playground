"""
Topic: Order Lifecycle State-Machine Validation with PySpark
Runtime: PySpark 3.3+
Run: spark-submit daily/2026-10-31-0200-order-state-machine-validation.py

Scenario
--------
Order events arrive from multiple services and may be replayed, delayed, or
recorded in an invalid order. Build a deterministic event timeline, remove
replayed event IDs, validate allowed state transitions, and calculate lifecycle
durations without silently accepting impossible transitions.

The script uses in-memory data only; no external services are required.
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("OrderLifecycleStateMachineValidation")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("status", StringType(), False),
        StructField("event_ts_text", StringType(), False),
        StructField("event_sequence", IntegerType(), False),
        StructField("ingest_ts_text", StringType(), False),
    ])

    rows = [
        # A complete successful lifecycle.
        ("100-e1", "100", "CREATED",   "2026-10-31 09:00:00", 1, "2026-10-31 09:00:02"),
        ("100-e2", "100", "PAID",      "2026-10-31 09:10:00", 2, "2026-10-31 09:10:03"),
        ("100-e3", "100", "PICKED",    "2026-10-31 10:00:00", 3, "2026-10-31 10:00:05"),
        ("100-e4", "100", "SHIPPED",   "2026-10-31 12:00:00", 4, "2026-10-31 12:05:00"),
        ("100-e5", "100", "DELIVERED", "2026-10-31 14:00:00", 5, "2026-10-31 14:01:00"),

        # A valid cancellation after payment.
        ("101-e1", "101", "CREATED",   "2026-10-31 09:02:00", 1, "2026-10-31 09:02:01"),
        ("101-e2", "101", "PAID",      "2026-10-31 09:05:00", 2, "2026-10-31 09:05:01"),
        ("101-e3", "101", "CANCELLED", "2026-10-31 09:20:00", 3, "2026-10-31 09:20:02"),

        # Invalid jump: CREATED cannot transition directly to SHIPPED.
        # The following SHIPPED -> DELIVERED edge is valid relative to the
        # source event stream, but the earlier invalid jump remains flagged.
        ("102-e1", "102", "CREATED",   "2026-10-31 09:30:00", 1, "2026-10-31 09:30:01"),
        ("102-e2", "102", "SHIPPED",   "2026-10-31 09:45:00", 2, "2026-10-31 09:45:03"),
        ("102-e3", "102", "DELIVERED", "2026-10-31 10:30:00", 3, "2026-10-31 10:30:01"),

        # Replayed event ID: keep the copy with the latest ingestion timestamp.
        ("103-e1", "103", "CREATED",   "2026-10-31 10:00:00", 1, "2026-10-31 10:00:01"),
        ("103-e2", "103", "PAID",      "2026-10-31 10:15:00", 2, "2026-10-31 10:15:02"),
        ("103-e2", "103", "PAID",      "2026-10-31 10:15:00", 2, "2026-10-31 10:16:30"),
        ("103-e3", "103", "PICKED",    "2026-10-31 11:00:00", 3, "2026-10-31 11:00:02"),

        # An in-progress order: no terminal event yet.
        ("104-e1", "104", "CREATED",   "2026-10-31 11:00:00", 1, "2026-10-31 11:00:01"),
        ("104-e2", "104", "PAID",      "2026-10-31 11:10:00", 2, "2026-10-31 11:10:02"),
        ("104-e3", "104", "PICKED",    "2026-10-31 11:40:00", 3, "2026-10-31 11:40:02"),
        ("104-e4", "104", "SHIPPED",   "2026-10-31 12:30:00", 4, "2026-10-31 12:30:03"),
    ]

    raw = (
        spark.createDataFrame(rows, schema)
        .withColumn("event_ts", F.to_timestamp("event_ts_text"))
        .withColumn("ingest_ts", F.to_timestamp("ingest_ts_text"))
        .drop("event_ts_text", "ingest_ts_text")
    )

    # ------------------------------------------------------------------
    # Q1. Replay-safe event deduplication
    # Problem: Keep one record per event_id, choosing the latest ingestion
    # timestamp. Add event_sequence as a deterministic tie-breaker.
    # ------------------------------------------------------------------
    replay_window = (
        Window.partitionBy("event_id")
        .orderBy(
            F.col("ingest_ts").desc(),
            F.col("event_sequence").desc(),
        )
    )

    deduplicated = (
        raw.withColumn("_replay_rank", F.row_number().over(replay_window))
        .filter(F.col("_replay_rank") == 1)
        .drop("_replay_rank")
    )

    # ------------------------------------------------------------------
    # Q2. Deterministic per-order event timeline
    # Problem: Order events by event time, source sequence, and event ID.
    # The event timestamp is the business ordering field; ingestion time is
    # retained for lineage and replay selection.
    # ------------------------------------------------------------------
    order_window = (
        Window.partitionBy("order_id")
        .orderBy("event_ts", "event_sequence", "event_id")
    )

    timeline = (
        deduplicated
        .withColumn("previous_status", F.lag("status").over(order_window))
        .withColumn("previous_event_ts", F.lag("event_ts").over(order_window))
        .withColumn("event_number", F.row_number().over(order_window))
    )

    # ------------------------------------------------------------------
    # Q3. Validate the transition graph
    # Allowed edges:
    #   START -> CREATED
    #   CREATED -> PAID | CANCELLED
    #   PAID -> PICKED | CANCELLED
    #   PICKED -> SHIPPED
    #   SHIPPED -> DELIVERED
    # All other edges are rejected for investigation.
    # ------------------------------------------------------------------
    is_valid = (
        (F.col("previous_status").isNull() & (F.col("status") == "CREATED"))
        | ((F.col("previous_status") == "CREATED") & F.col("status").isin("PAID", "CANCELLED"))
        | ((F.col("previous_status") == "PAID") & F.col("status").isin("PICKED", "CANCELLED"))
        | ((F.col("previous_status") == "PICKED") & (F.col("status") == "SHIPPED"))
        | ((F.col("previous_status") == "SHIPPED") & (F.col("status") == "DELIVERED"))
    )

    validated = (
        timeline
        .withColumn("is_valid_transition", F.coalesce(is_valid, F.lit(False)))
        .withColumn(
            "transition_seconds",
            F.when(
                F.col("previous_event_ts").isNotNull(),
                F.unix_timestamp("event_ts") - F.unix_timestamp("previous_event_ts"),
            ),
        )
        .withColumn(
            "validation_reason",
            F.when(F.col("is_valid_transition"), F.lit(None).cast("string"))
             .otherwise(F.lit("INVALID_ORDER_STATE_TRANSITION")),
        )
    )

    invalid_transitions = (
        validated
        .filter(~F.col("is_valid_transition"))
        .select(
            "order_id", "event_id", "previous_status", "status",
            "event_ts", "validation_reason",
        )
    )

    # ------------------------------------------------------------------
    # Q4. Latest observed state per order
    # Problem: Produce the current source-observed state and identify whether
    # an order is terminal. This is an observation, not an assertion that all
    # preceding transitions were valid.
    # ------------------------------------------------------------------
    latest_window = (
        Window.partitionBy("order_id")
        .orderBy(
            F.col("event_ts").desc(),
            F.col("event_sequence").desc(),
            F.col("event_id").desc(),
        )
    )

    latest_state = (
        validated
        .withColumn("_latest_rank", F.row_number().over(latest_window))
        .filter(F.col("_latest_rank") == 1)
        .select(
            "order_id",
            F.col("status").alias("latest_status"),
            F.col("event_ts").alias("latest_event_ts"),
        )
        .withColumn(
            "is_terminal",
            F.col("latest_status").isin("DELIVERED", "CANCELLED"),
        )
    )

    # ------------------------------------------------------------------
    # Q5. Lifecycle duration metrics
    # Problem: Calculate creation-to-delivery duration only for delivered
    # orders. Do not assign a delivery duration to cancelled/in-progress orders.
    # ------------------------------------------------------------------
    lifecycle = (
        validated.groupBy("order_id")
        .agg(
            F.min(F.when(F.col("status") == "CREATED", F.col("event_ts"))).alias("created_at"),
            F.max(F.when(F.col("status") == "DELIVERED", F.col("event_ts"))).alias("delivered_at"),
            F.sum(F.when(~F.col("is_valid_transition"), 1).otherwise(0)).alias("invalid_event_count"),
            F.count("*").alias("deduplicated_event_count"),
        )
        .withColumn(
            "fulfillment_seconds",
            F.when(
                F.col("delivered_at").isNotNull() & F.col("created_at").isNotNull(),
                F.unix_timestamp("delivered_at") - F.unix_timestamp("created_at"),
            ),
        )
        .join(latest_state, "order_id", "inner")
    )

    # ------------------------------------------------------------------
    # Q6. Quality summary for monitoring
    # Problem: Return event counts and invalid-transition counts by order.
    # ------------------------------------------------------------------
    quality_summary = (
        validated.groupBy("order_id")
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("is_valid_transition"), 0).otherwise(1)).alias("invalid_transition_count"),
        )
        .orderBy("order_id")
    )

    # Display compact outputs for an interview walkthrough.
    print("=== Deduplicated event count ===")
    print(f"raw={raw.count()}, deduplicated={deduplicated.count()}")

    print("=== Invalid transitions ===")
    invalid_transitions.orderBy("order_id", "event_ts").show(truncate=False)

    print("=== Latest state ===")
    latest_state.orderBy("order_id").show(truncate=False)

    print("=== Lifecycle metrics ===")
    lifecycle.orderBy("order_id").show(truncate=False)

    print("=== Quality summary ===")
    quality_summary.show(truncate=False)

    # ------------------------------------------------------------------
    # Q7. Expected-result assertions
    # ------------------------------------------------------------------
    assert raw.count() == 19, "Unexpected raw input row count"
    assert deduplicated.count() == 18, "Replay deduplication did not remove one duplicate"
    assert (
        deduplicated.filter(F.col("event_id") == "103-e2").count() == 1
    ), "Duplicate event ID remains after deduplication"

    replay_ingest = (
        deduplicated.filter(F.col("event_id") == "103-e2")
        .select("ingest_ts")
        .first()[0]
    )
    assert str(replay_ingest) == "2026-10-31 10:16:30", "Latest replay copy was not retained"

    order_100_state = (
        latest_state.filter(F.col("order_id") == "100")
        .select("latest_status")
        .first()[0]
    )
    assert order_100_state == "DELIVERED"

    order_101_state = (
        latest_state.filter(F.col("order_id") == "101")
        .select("latest_status")
        .first()[0]
    )
    assert order_101_state == "CANCELLED"

    invalid_102 = (
        invalid_transitions
        .filter((F.col("order_id") == "102") & (F.col("status") == "SHIPPED"))
        .count()
    )
    assert invalid_102 == 1, "Expected CREATED -> SHIPPED transition to be quarantined"

    delivered_seconds = (
        lifecycle.filter(F.col("order_id") == "100")
        .select("fulfillment_seconds")
        .first()[0]
    )
    assert delivered_seconds == 18000, "Order 100 should take five hours to deliver"

    in_progress = (
        latest_state.filter((F.col("order_id") == "104") & (~F.col("is_terminal")))
        .count()
    )
    assert in_progress == 1, "Order 104 should remain non-terminal"

    print("All order lifecycle state-machine assertions passed.")

    # ------------------------------------------------------------------
    # Optimization guidance
    # ------------------------------------------------------------------
    print("=== Physical plan for transition validation ===")
    validated.explain(mode="formatted")

    # 1. Deduplicate with a narrow projection before the window if raw payloads
    #    are wide; avoid shuffling large JSON/string columns unnecessarily.
    # 2. Window operations shuffle by order_id. Repartitioning by order_id can
    #    help when the same partitioning is reused, but benchmark the extra
    #    shuffle against the workload.
    # 3. Keep event_id and event_sequence stable across retries. A deterministic
    #    ordering is essential for repeatable latest-record selection.
    # 4. For very large streams, use Structured Streaming with event-time
    #    watermarks and state TTL where business lateness bounds are defined.
    #    Do not apply an arbitrary watermark to a batch history replay.
    # 5. Persist validated only when multiple downstream actions justify the
    #    storage cost; otherwise Spark may recompute the lineage.
    # 6. Separate invalid transitions into a quarantine output with source
    #    offsets, event IDs, and reason codes for replay and remediation.

    # Interview follow-ups
    # * How would you handle a late event that changes an already-computed state?
    # * Should invalid transitions be dropped, quarantined, or trigger rollback?
    # * How would you represent allowed transitions as reference data?
    # * How would you make the pipeline idempotent across micro-batch retries?
    # * How would you measure order-state SLA by channel and fulfillment centre?

    spark.stop()


if __name__ == "__main__":
    main()
