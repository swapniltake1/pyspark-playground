"""
Topic: Partition-Aware Writes and Small-File Mitigation
Runtime: Apache Spark / PySpark 3.3+
Run: spark-submit daily/2026-10-31-0104-partitioned-writes-small-files.py

Scenario:
A daily transaction dataset is written as Parquet partitioned by business_date.
A naive repartition can scatter the same date across many tasks, creating many
small files inside each date directory. This exercise compares that layout with
a write distribution aligned to the output partition key.
"""

from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def parquet_part_files(root: Path) -> list[Path]:
    """Return only data files; ignore _SUCCESS and other metadata files."""
    return sorted(root.rglob("part-*.parquet"))


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("PartitionAwareWritesSmallFiles")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    try:
        schema = StructType(
            [
                StructField("transaction_id", IntegerType(), nullable=False),
                StructField("business_date", StringType(), nullable=False),
                StructField("region", StringType(), nullable=False),
                StructField("amount", DoubleType(), nullable=False),
            ]
        )

        # Reproducible transaction sample: 1,200 rows across three dates.
        # The sample is intentionally small enough to run on a laptop.
        dates = ["2026-10-28", "2026-10-29", "2026-10-30"]
        regions = ["WEST", "NORTH", "SOUTH", "EAST"]
        rows = [
            (
                transaction_id,
                dates[(transaction_id - 1) % len(dates)],
                regions[(transaction_id - 1) % len(regions)],
                round(100.0 + ((transaction_id * 17) % 900) / 10.0, 2),
            )
            for transaction_id in range(1, 1201)
        ]

        transactions = spark.createDataFrame(rows, schema)
        source_count = transactions.count()
        date_count = transactions.select("business_date").distinct().count()

        print(f"Source rows: {source_count}")
        print(f"Output date partitions: {date_count}")
        transactions.groupBy("business_date").count().orderBy("business_date").show()

        with TemporaryDirectory(prefix="spark-small-files-") as temp_dir:
            root = Path(temp_dir)
            naive_path = root / "naive_partitioned_output"
            optimized_path = root / "date_aligned_output"

            # -----------------------------------------------------------------
            # Q1. Baseline: repartition without considering the output partition.
            # Each task can encounter the same business_date, producing multiple
            # part files under that date directory.
            # -----------------------------------------------------------------
            (
                transactions
                .repartition(8)
                .write
                .mode("overwrite")
                .option("maxRecordsPerFile", 10000)
                .partitionBy("business_date")
                .parquet(str(naive_path))
            )

            naive_files = parquet_part_files(naive_path)
            print(f"Q1 baseline Parquet data files: {len(naive_files)}")

            # -----------------------------------------------------------------
            # Q2. Optimized for this partition layout:
            # Hash repartition by the same key used by partitionBy. All rows for
            # a date are sent to one shuffle partition, so a date is normally
            # written by one task instead of many tasks.
            # -----------------------------------------------------------------
            (
                transactions
                .repartition(4, "business_date")
                .write
                .mode("overwrite")
                .option("maxRecordsPerFile", 10000)
                .partitionBy("business_date")
                .parquet(str(optimized_path))
            )

            optimized_files = parquet_part_files(optimized_path)
            print(f"Q2 date-aligned Parquet data files: {len(optimized_files)}")

            # Read-back correctness checks.
            round_trip = spark.read.parquet(str(optimized_path))
            assert round_trip.count() == source_count
            assert round_trip.select("transaction_id").distinct().count() == source_count
            assert (
                round_trip.select("business_date").distinct().count() == date_count
            )

            # With the date key as the repartition key and no max-record split,
            # each non-empty date should be emitted by a single task.
            assert 0 < optimized_files.__len__() <= date_count, (
                f"Expected at most one data file per date, found {len(optimized_files)} "
                f"for {date_count} dates"
            )

            print("Q2 assertions passed: row count, unique IDs, and date partitions.")
            print(
                "Files per output date (optimized):",
                {
                    date: len(list((optimized_path / f"business_date={date}").glob(
                        "part-*.parquet"
                    )))
                    for date in dates
                },
            )

            # -----------------------------------------------------------------
            # Q3. Inspect the physical distribution plans.
            # Look for Exchange / hashpartitioning on business_date in the
            # optimized plan. Explain output is diagnostic, not an assertion.
            # -----------------------------------------------------------------
            print("Baseline repartition plan:")
            transactions.repartition(8).explain(mode="formatted")

            print("Date-aligned repartition plan:")
            transactions.repartition(4, "business_date").explain(mode="formatted")

            # -----------------------------------------------------------------
            # Q4. Optional single-file export for a small downstream extract.
            # Do not use coalesce(1) for a large production dataset: it funnels
            # all output through one task and can become a severe bottleneck.
            # -----------------------------------------------------------------
            # small_extract_path = root / "small_control_extract"
            # (
            #     transactions
            #     .filter(F.col("business_date") == "2026-10-30")
            #     .coalesce(1)
            #     .write.mode("overwrite")
            #     .parquet(str(small_extract_path))
            # )

            print(
                {
                    "source_rows": source_count,
                    "date_partitions": date_count,
                    "baseline_files": len(naive_files),
                    "date_aligned_files": len(optimized_files),
                }
            )

        print("All deterministic correctness assertions passed.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()


# Optimization guidance:
# 1. Align repartition keys with the physical output partition columns when it
#    reduces duplicate files per directory.
# 2. Do not blindly repartition by every column; high-cardinality partition
#    columns create excessive directories and metadata overhead.
# 3. Estimate target file size from compressed output volume and storage/query
#    characteristics; avoid tiny files and avoid oversized files.
# 4. maxRecordsPerFile is a row-count guardrail, not a byte-size target.
# 5. Repartition adds a shuffle. Use it when improved distribution/file layout
#    justifies the network and sort/hash cost.
# 6. coalesce reduces partitions without a full shuffle but cannot rebalance
#    skewed data; coalesce(1) is appropriate only for small extracts.
# 7. On Delta Lake, evaluate OPTIMIZE/compaction and auto-optimize features
#    supported by the target runtime. Compaction is not a substitute for a
#    sensible initial write layout.
# 8. Track file count, median/percentile file size, task duration, and bytes
#    written as pipeline metrics rather than relying on row count alone.

# Interview follow-ups:
# * Why can repartition(8) create multiple files in one partition directory?
# * What trade-off appears when the partition key has only two or three values?
# * How would you size files for a multi-terabyte daily ingestion?
# * When is coalesce safer than repartition, and what does it not solve?
# * How do Delta OPTIMIZE and liquid clustering differ from partitionBy?
