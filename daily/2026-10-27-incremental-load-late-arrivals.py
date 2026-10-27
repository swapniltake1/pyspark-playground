"""
Daily PySpark Interview Practice — 2026-10-27
Topic: Incremental loads, late-arriving records, and deterministic deduplication.

Run locally:
    spark-submit daily/2026-10-27-incremental-load-late-arrivals.py

Requires PySpark 3.x. This example uses ordinary DataFrames, so it does not
require Delta Lake or a Databricks cluster.
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def build_spark():
    return (
        SparkSession.builder
        .appName("IncrementalLoadLateArrivalsInterview")
        .master("local[*]")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    target_schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", StringType(), False),
        StructField("status", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("updated_at", StringType(), False),
    ])

    incoming_schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", StringType(), False),
        StructField("status", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("updated_at", StringType(), False),
        StructField("ingested_at", StringType(), False),
    ])

    # Existing target snapshot. In a real pipeline this would be read from
    # a durable table or a Delta Lake target.
    target_raw = [
        (1001, "C01", "PAID", 120.0, "2026-10-20 10:00:00"),
        (1002, "C02", "PENDING", 80.0, "2026-10-21 09:00:00"),
        (1003, "C03", "SHIPPED", 45.0, "2026-10-22 15:00:00"),
    ]

    # Incoming batch includes a replay, updates, a late-arriving event, and
    # two versions of order 1004. The latest business update should win.
    incoming_raw = [
        (1001, "C01", "PAID", 120.0, "2026-10-20 10:00:00", "2026-10-27 07:00:00"),
        (1002, "C02", "PAID", 80.0, "2026-10-26 11:00:00", "2026-10-27 07:05:00"),
        (1003, "C03", "DELIVERED", 45.0, "2026-10-25 16:00:00", "2026-10-27 07:10:00"),
        (1004, "C04", "PENDING", 210.0, "2026-10-27 08:00:00", "2026-10-27 07:15:00"),
        (1004, "C04", "PAID", 210.0, "2026-10-27 09:00:00", "2026-10-27 07:16:00"),
        (1005, "C05", "CANCELLED", 30.0, "2026-10-23 12:00:00", "2026-10-27 07:20:00"),
    ]

    target = (
        spark.createDataFrame(target_raw, target_schema)
        .withColumn("updated_at", F.to_timestamp("updated_at"))
    )

    incoming = (
        spark.createDataFrame(incoming_raw, incoming_schema)
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
    )

    # Interview problem:
    # Produce the latest state for every order using the existing target and
    # the incoming batch. Do not let replayed or out-of-order records overwrite
    # a newer state. Include late-arriving events and make reruns idempotent.
    #
    # ----------------------------------------------------------------------
    # Solution 1 — straightforward approach
    # ----------------------------------------------------------------------
    # Union the full incoming batch with the target and keep the newest row
    # per order. This is easy to reason about but may process unnecessary
    # source rows and columns on a large workload.

    target_for_union = target.select(
        "order_id", "customer_id", "status", "amount", "updated_at"
    )
    incoming_for_union = incoming.select(
        "order_id", "customer_id", "status", "amount", "updated_at"
    )

    full_union = target_for_union.unionByName(incoming_for_union)

    latest_order_window = Window.partitionBy("order_id").orderBy(
        F.col("updated_at").desc(),
        # Stable secondary ordering when two versions share the same update
        # timestamp. In production, prefer a source sequence/version column.
        F.col("status").desc(),
        F.col("amount").desc(),
    )

    straightforward_result = (
        full_union
        .withColumn("_row_num", F.row_number().over(latest_order_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    # ----------------------------------------------------------------------
    # Solution 2 — optimized incremental candidate processing
    # ----------------------------------------------------------------------
    # Use an ingestion-time watermark/lookback to select only the candidate
    # batch, project only required columns before the shuffle, and union it
    # with the target snapshot. Filtering on ingested_at is deliberate:
    # filtering only on updated_at can miss an event that arrived late with
    # an old business timestamp.
    #
    # The example uses a fixed lower bound to keep the exercise reproducible.
    # Production code should derive this bound from a persisted ingestion
    # watermark and advance that watermark only after a successful commit.

    lookback_start = "2026-10-24 00:00:00"

    candidate_batch = (
        incoming
        .filter(
            F.col("ingested_at")
            >= F.to_timestamp(F.lit(lookback_start))
        )
        .select("order_id", "customer_id", "status", "amount", "updated_at")
    )

    optimized_union = target_for_union.unionByName(candidate_batch)

    optimized_result = (
        optimized_union
        .withColumn("_row_num", F.row_number().over(latest_order_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    # Expected final state:
    # 1001 PAID       (replay does not change the existing state)
    # 1002 PAID       (newer update wins)
    # 1003 DELIVERED  (late-arriving update wins by updated_at)
    # 1004 PAID       (latest of two versions in this batch wins)
    # 1005 CANCELLED  (new order is inserted)

    expected = [
        (1001, "PAID", 120.0),
        (1002, "PAID", 80.0),
        (1003, "DELIVERED", 45.0),
        (1004, "PAID", 210.0),
        (1005, "CANCELLED", 30.0),
    ]

    def compact_rows(df):
        return [
            (row["order_id"], row["status"], row["amount"])
            for row in (
                df.select("order_id", "status", "amount")
                .orderBy("order_id")
                .collect()
            )
        ]

    straightforward_rows = compact_rows(straightforward_result)
    optimized_rows = compact_rows(optimized_result)

    assert straightforward_rows == expected, straightforward_rows
    assert optimized_rows == expected, optimized_rows
    assert optimized_result.count() == 5

    print("Optimized final order snapshot:")
    optimized_result.orderBy("order_id").show(truncate=False)
    print("Assertions passed: both strategies produced the expected 5 orders.")

    # Interview follow-up questions:
    # 1. Why can updated_at alone be an unsafe incremental watermark?
    # 2. How would you persist and advance an ingestion watermark safely?
    # 3. How would you implement the final write with Delta MERGE INTO?
    # 4. What source sequence/version field would make tie-breaking reliable?
    # 5. How would you handle deletes/tombstones in a CDC stream?
    # 6. What changes if the target is too large to union and window each run?

    spark.stop()


if __name__ == "__main__":
    main()
