"""
Daily PySpark Interview Practice — 2026-11-26
Topic: Structured Streaming deduplication and idempotent foreachBatch writes

Problem
Process order events arriving from a file stream. Events may be replayed, arrive
out of order, or be delivered again when a micro-batch is retried. Deduplicate
within an event-time horizon, aggregate each micro-batch, and make the sink write
idempotent using a deterministic event key.

Run:
  spark-submit daily/2026-11-26-structured-streaming-idempotent-foreachbatch.py

Uses only built-in PySpark APIs and a local file source. Spark 3.5+ is recommended
for dropDuplicatesWithinWatermark. For older Spark 3.x, replace it with
withWatermark(...).dropDuplicates(["event_id"]).
"""
import json
import os
import shutil
import tempfile
import time

from pyspark.sql import SparkSession, functions as F, types as T


def build_spark():
    return (
        SparkSession.builder
        .appName("structured-streaming-idempotent-foreachbatch")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType(), False),
    T.StructField("order_id", T.StringType(), False),
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("event_type", T.StringType(), False),
    T.StructField("amount", T.DecimalType(12, 2), True),
    T.StructField("event_ts", T.TimestampType(), False),
])


def write_idempotently(batch_df, batch_id, sink_path):
    """
    Demonstration sink: one deterministic output file per micro-batch ID.
    Spark may invoke foreachBatch again for the same batch_id after recovery;
    overwrite makes that batch's output repeatable rather than duplicating rows.

    A production transactional sink should MERGE on event_id (or use a unique
    constraint/upsert) because batch IDs alone do not protect against the same
    business event being replayed in a different micro-batch.
    """
    target = os.path.join(sink_path, "batch_id={}".format(batch_id))
    (
        batch_df
        .groupBy("event_type")
        .agg(
            F.count("*").alias("event_count"),
            F.sum("amount").alias("total_amount"),
        )
        .write
        .mode("overwrite")
        .parquet(target)
    )


def run_stream(spark, input_dir, checkpoint_dir, sink_dir, trigger_once=True):
    raw = (
        spark.readStream
        .schema(EVENT_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .json(input_dir)
    )

    # Keep event-time state bounded. The watermark allows state eviction;
    # events older than the watermark may be dropped from deduplication.
    unique_events = (
        raw
        .withWatermark("event_ts", "2 hours")
        .dropDuplicatesWithinWatermark(["event_id"])
        .filter(F.col("amount").isNotNull() & (F.col("amount") >= 0))
    )

    query = (
        unique_events.writeStream
        .foreachBatch(lambda df, bid: write_idempotently(df, bid, sink_dir))
        .option("checkpointLocation", checkpoint_dir)
        .trigger(once=True)
        .start()
    )
    query.awaitTermination()
    return query


def write_input_file(directory, filename, rows):
    path = os.path.join(directory, filename)
    with open(path, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def main():
    spark = build_spark()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    root = tempfile.mkdtemp(prefix="pyspark_streaming_idempotency_")
    input_dir = os.path.join(root, "input")
    checkpoint_dir = os.path.join(root, "checkpoint")
    sink_dir = os.path.join(root, "sink")
    os.makedirs(input_dir)

    try:
        # Batch 1 has a duplicate event_id in the same input file.
        write_input_file(input_dir, "events-1.json", [
            {"event_id":"e-100","order_id":"o-1","customer_id":"c-1",
             "event_type":"PAID","amount":"125.50","event_ts":"2026-11-26T10:00:00Z"},
            {"event_id":"e-100","order_id":"o-1","customer_id":"c-1",
             "event_type":"PAID","amount":"125.50","event_ts":"2026-11-26T10:00:00Z"},
            {"event_id":"e-101","order_id":"o-2","customer_id":"c-2",
             "event_type":"REFUNDED","amount":"20.00","event_ts":"2026-11-26T10:05:00Z"},
            {"event_id":"e-102","order_id":"o-3","customer_id":"c-3",
             "event_type":"PAID","amount":"80.00","event_ts":"2026-11-26T10:10:00Z"},
            {"event_id":"e-103","order_id":"o-4","customer_id":"c-4",
             "event_type":"PAID","amount":None,"event_ts":"2026-11-26T10:15:00Z"}
        ])

        run_stream(spark, input_dir, checkpoint_dir, sink_dir)

        # Read back deterministic batch output. One copy of e-100 is retained;
        # the invalid null-amount event is filtered from the business aggregate.
        outputs = (
            spark.read.parquet(sink_dir)
            .groupBy("event_type")
            .agg(
                F.sum("event_count").alias("event_count"),
                F.sum("total_amount").alias("total_amount"),
            )
        )
        actual = {
            row["event_type"]: (row["event_count"], str(row["total_amount"]))
            for row in outputs.collect()
        }
        assert actual["PAID"][0] == 2, actual
        assert actual["REFUNDED"][0] == 1, actual
        assert actual["PAID"][1] == "205.50", actual
        assert actual["REFUNDED"][1] == "20.00", actual

        print("Expected micro-batch aggregate assertions passed:", actual)

        # A second independent run/checkpoint illustrates a later batch.
        # In production, retain the checkpoint directory across restarts.
        print("Checkpoint:", checkpoint_dir)
        print("Sink:", sink_dir)

    finally:
        spark.stop()
        # Keep artifacts only when debugging; this standalone interview script
        # cleans up its temporary local directories.
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()


"""
Transformations and optimization choices
----------------------------------------
1. Explicit schema avoids repeated JSON schema inference and stabilizes contracts.
2. withWatermark + dropDuplicatesWithinWatermark bounds deduplication state.
3. Filter invalid measures before aggregation to keep downstream data clean.
4. foreachBatch provides batch APIs per micro-batch, but the callback must be
   designed for retries; Spark does not make arbitrary sink side effects atomic.
5. Deterministic batch output paths plus overwrite make a replay of the same
   batch ID idempotent for this local demonstration.
6. For a production database, MERGE/UPSERT by event_id with a unique key is
   stronger: it also handles business-event replay across different batch IDs.
7. Keep the checkpoint stable across restarts. Deleting it resets source offsets
   and state, so the sink must independently tolerate replay.
8. Avoid collect() in production; it is used only here for tiny test assertions.

Expected output
---------------
PAID:     event_count=2, total_amount=205.50
REFUNDED: event_count=1, total_amount=20.00

Interview follow-ups
--------------------
1. What does a watermark guarantee, and what late data can still be dropped?
2. Why is foreachBatch at-least-once unless the sink is idempotent?
3. Why is batch_id insufficient to deduplicate a replayed business event?
4. How would you implement an atomic Delta Lake MERGE keyed by event_id?
5. What happens to state when a checkpoint is deleted or changed?
6. How would you handle schema evolution in a streaming file source?
7. How do you monitor state-store size, inputRowsPerSecond, and processingTime?
"""
