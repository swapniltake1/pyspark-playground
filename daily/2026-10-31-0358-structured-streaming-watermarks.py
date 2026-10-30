"""
Structured Streaming: event-time windows, watermarking, and bounded deduplication.

Run the deterministic batch exercise:
    spark-submit daily/2026-10-31-0358-structured-streaming-watermarks.py

Run the streaming pipeline against a directory of newline-delimited JSON files:
    spark-submit daily/2026-10-31-0358-structured-streaming-watermarks.py \
      --run-stream \
      --input-path /tmp/device-events/incoming \
      --output-path /tmp/device-events/aggregates \
      --checkpoint-path /tmp/device-events/checkpoint

The streaming mode expects JSON fields matching EVENT_SCHEMA. Keep the checkpoint
directory stable across restarts of the same query.
"""

import argparse
import os
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("device_id", StringType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("source_seq", IntegerType(), False),
        StructField("temperature", DoubleType(), True),
    ]
)


def make_sample_events(spark):
    """Create deterministic events, including one replay and one late event."""
    rows = [
        ("e-001", "device-A", datetime(2026, 10, 31, 10, 0, 0),
         datetime(2026, 10, 31, 10, 0, 5), 1, 20.0),
        # Same event_id replayed later: latest ingestion metadata wins.
        ("e-001", "device-A", datetime(2026, 10, 31, 10, 0, 0),
         datetime(2026, 10, 31, 10, 2, 0), 2, 20.0),
        ("e-002", "device-A", datetime(2026, 10, 31, 10, 2, 0),
         datetime(2026, 10, 31, 10, 2, 5), 1, 22.0),
        ("e-003", "device-A", datetime(2026, 10, 31, 10, 6, 0),
         datetime(2026, 10, 31, 10, 6, 5), 1, 24.0),
        ("e-004", "device-B", datetime(2026, 10, 31, 10, 4, 0),
         datetime(2026, 10, 31, 10, 4, 5), 1, 18.0),
        # Arrived much later than its event time; batch mode retains it.
        ("e-005", "device-A", datetime(2026, 10, 31, 9, 50, 0),
         datetime(2026, 10, 31, 10, 10, 5), 1, 19.0),
    ]
    return spark.createDataFrame(rows, schema=EVENT_SCHEMA)


def deterministic_batch_demo(spark):
    """
    1) Resolve replayed event IDs deterministically.
    2) Aggregate by five-minute event-time windows and device.
    3) Assert exact counts and averages for the sample.
    """
    events = make_sample_events(spark)

    latest_delivery = Window.partitionBy("event_id").orderBy(
        F.col("ingested_at").desc(),
        F.col("source_seq").desc(),
    )
    deduped = (
        events.withColumn("_delivery_rank", F.row_number().over(latest_delivery))
        .filter(F.col("_delivery_rank") == 1)
        .drop("_delivery_rank")
    )

    assert deduped.count() == 5, "Replay should collapse to five unique events"

    aggregates = (
        deduped.groupBy(
            F.window(F.col("event_ts"), "5 minutes").alias("event_window"),
            F.col("device_id"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.round(F.avg("temperature"), 2).alias("avg_temperature"),
        )
    )

    actual = {}
    for row in aggregates.collect():
        window_start = row["event_window"]["start"].strftime("%H:%M")
        actual[(row["device_id"], window_start)] = (
            row["event_count"],
            float(row["avg_temperature"]),
        )

    expected = {
        ("device-A", "09:50"): (1, 19.0),
        ("device-A", "10:00"): (2, 21.0),
        ("device-B", "10:00"): (1, 18.0),
        ("device-A", "10:05"): (1, 24.0),
    }
    assert actual == expected, f"Unexpected window aggregates: {actual}"

    print("Batch replay-deduplication and event-window assertions passed.")
    aggregates.orderBy("device_id", "event_window").show(truncate=False)

    print("Physical plan for the batch aggregation:")
    aggregates.explain("formatted")


def run_streaming_pipeline(spark, args):
    """
    Process JSON files as micro-batches.

    A ten-minute watermark bounds state used by streaming deduplication and
    event-time aggregation. Append mode emits a window after Spark considers
    that window complete relative to the advancing watermark.
    """
    # The file source must exist. Let Spark create the sink and checkpoint
    # directories; pre-creating the sink can trigger file-sink path-exists errors.
    os.makedirs(args.input_path, exist_ok=True)

    incoming = (
        spark.readStream
        .schema(EVENT_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .json(args.input_path)
    )

    # The composite key includes event_ts: an event_id reused with a different
    # event timestamp is treated as a distinct event. Use a stronger source
    # identity contract if event IDs are globally unique and immutable.
    deduped = (
        incoming.withWatermark("event_ts", "10 minutes")
        .dropDuplicates(["event_id", "event_ts"])
    )

    windowed = (
        deduped.groupBy(
            F.window(F.col("event_ts"), "5 minutes"),
            F.col("device_id"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.round(F.avg("temperature"), 2).alias("avg_temperature"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "device_id",
            "event_count",
            "avg_temperature",
        )
    )

    query = (
        windowed.writeStream
        .queryName("device_event_time_5m_rollup")
        .outputMode("append")
        .format("parquet")
        .option("path", args.output_path)
        .option("checkpointLocation", args.checkpoint_path)
        .start()
    )

    print(f"Streaming query started: {query.id}")
    query.awaitTermination()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-stream", action="store_true")
    parser.add_argument("--input-path", default="/tmp/device-events/incoming")
    parser.add_argument("--output-path", default="/tmp/device-events/aggregates")
    parser.add_argument("--checkpoint-path", default="/tmp/device-events/checkpoint")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName("StructuredStreamingWatermarkPractice")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        if args.run_stream:
            run_streaming_pipeline(spark, args)
        else:
            deterministic_batch_demo(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()


# Optimization and operational guidance:
# * Watermark delay is a correctness/latency/state-size trade-off, not a promise
#   that every event older than exactly ten minutes is always dropped immediately.
# * Watermarks advance from observed event time; a quiet source may delay window
#   finalization. Monitor input rows, state rows, watermark, and batch duration.
# * Deduplication keys must reflect the source's real event identity contract.
# * Use durable checkpoint storage (ADLS/S3/DBFS volume) in production, not /tmp.
# * Keep micro-batches and output files reasonably sized; compact small files later.
# * Avoid collecting streaming data to the driver. Keep transformations distributed.
# * Inspect the Spark UI and explain plans; verify state-store and shuffle pressure.
#
# Interview follow-ups:
# 1. How does event time differ from processing time?
# 2. What happens when an event arrives later than the configured watermark?
# 3. Why is a watermark required for stateful streaming deduplication?
# 4. How would you handle two devices with very different event volumes?
# 5. What is the role of the checkpoint location after a driver restart?
# 6. When would you choose update mode instead of append mode?
# 7. How would you make downstream file or Delta writes idempotent?
