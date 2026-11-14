"""
Daily PySpark Interview Practice — 2026-11-14
Topic: Partition pruning, column pruning, and small-file-aware Parquet layout

Problem
-------
An e-commerce analytics team repeatedly queries recent sales by month. Compare
an unpartitioned Parquet layout with a layout partitioned by order_month. Prove
that both return the same business result, inspect the optimized physical plan,
and discuss how to avoid creating thousands of tiny files.

Run:
    spark-submit daily/2026-11-14-partition-pruning-and-small-files.py

Compatible with PySpark 3.x. Uses only local temporary directories and
in-memory sample data; no external dataset is required.
"""
from datetime import date
from decimal import Decimal
import os
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)


TARGET_MONTH = "2026-10"


def build_sales(spark):
    schema = StructType([
        StructField("order_id", LongType(), nullable=False),
        StructField("order_date", DateType(), nullable=False),
        StructField("region", StringType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("net_amount", DecimalType(12, 2), nullable=False),
    ])
    rows = [
        (1001, date(2026, 9, 2),  "west",  "electronics", Decimal("120.00")),
        (1002, date(2026, 9, 12), "east",  "grocery",     Decimal("80.00")),
        (1003, date(2026, 9, 30), "west",  "home",        Decimal("50.00")),
        (1004, date(2026, 10, 1), "east",  "electronics", Decimal("200.00")),
        (1005, date(2026, 10, 8), "west",  "grocery",     Decimal("125.00")),
        (1006, date(2026, 10, 18),"east",  "home",        Decimal("75.00")),
        (1007, date(2026, 10, 31),"south", "electronics", Decimal("300.00")),
        (1008, date(2026, 11, 1), "west",  "grocery",     Decimal("90.00")),
        (1009, date(2026, 11, 2), "east",  "electronics", Decimal("110.00")),
        (1010, date(2026, 11, 5), "south", "home",        Decimal("45.00")),
    ]
    return (
        spark.createDataFrame(rows, schema)
        .withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
    )


def selected_rows(df):
    return [
        (row["order_id"], row["region"], row["net_amount"])
        for row in df.orderBy("order_id").collect()
    ]


def main():
    spark = (
        SparkSession.builder
        .appName("PartitionPruningAndSmallFiles")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        sales = build_sales(spark)

        with tempfile.TemporaryDirectory(prefix="pyspark_partition_practice_") as root:
            baseline_path = os.path.join(root, "unpartitioned_sales")
            partitioned_path = os.path.join(root, "sales_by_month")

            # Baseline: all months share the same directory layout.
            sales.write.mode("overwrite").parquet(baseline_path)

            # Optimized layout: order_month becomes a directory partition key.
            # Do not add repartition(1) blindly in production: it serializes the
            # write through one task and can become a throughput bottleneck.
            sales.write.mode("overwrite").partitionBy("order_month").parquet(
                partitioned_path
            )

            baseline = spark.read.parquet(baseline_path)
            partitioned = spark.read.parquet(partitioned_path)

            # Q1. Filter the unpartitioned dataset and retain only needed columns.
            baseline_october = (
                baseline
                .where(F.col("order_month") == F.lit(TARGET_MONTH))
                .select("order_id", "region", "net_amount")
            )

            # Q2. Apply the same direct predicate to the partitioned dataset.
            # Spark can eliminate unrelated order_month directories at read time.
            # The narrow projection also enables Parquet column pruning.
            optimized_october = (
                partitioned
                .where(F.col("order_month") == F.lit(TARGET_MONTH))
                .select("order_id", "region", "net_amount")
            )

            baseline_result = selected_rows(baseline_october)
            optimized_result = selected_rows(optimized_october)

            # Q3. Prove business-result equivalence and expected October contents.
            expected = [
                (1004, "east",  Decimal("200.00")),
                (1005, "west",  Decimal("125.00")),
                (1006, "east",  Decimal("75.00")),
                (1007, "south", Decimal("300.00")),
            ]
            assert baseline_result == expected, baseline_result
            assert optimized_result == expected, optimized_result
            assert baseline_result == optimized_result

            # Q4. Validate an aggregate commonly used by monthly reporting.
            summary = optimized_october.agg(
                F.count("*").alias("order_count"),
                F.sum("net_amount").alias("net_sales"),
            ).first()
            assert summary["order_count"] == 4
            assert summary["net_sales"] == Decimal("700.00")

            # Q5. Confirm that the target month has its own physical directory.
            october_partition = os.path.join(
                partitioned_path, "order_month=" + TARGET_MONTH
            )
            assert os.path.isdir(october_partition), october_partition

            parquet_files = []
            for current_root, _, filenames in os.walk(partitioned_path):
                parquet_files.extend(
                    os.path.join(current_root, name)
                    for name in filenames
                    if name.endswith(".parquet")
                )

            print("Expected October rows:")
            optimized_october.orderBy("order_id").show(truncate=False)
            print("October summary:")
            print(summary.asDict())
            print("Partition directories:")
            for name in sorted(os.listdir(partitioned_path)):
                if name.startswith("order_month="):
                    print("  " + name)
            print("Total Parquet data files:", len(parquet_files))

            print("\nBaseline physical plan (unpartitioned read):")
            baseline_october.explain("formatted")
            print("\nOptimized physical plan (partitioned read):")
            optimized_october.explain("formatted")

            print("\nAssertions passed: baseline and partitioned results match.")
            print("Expected October order count = 4; net sales = 700.00")

        # Q6. Compare partitioning choices conceptually:
        # - order_month is a low-cardinality, commonly filtered partition key.
        # - order_id/customer_id are high-cardinality keys and poor directory keys.
        # - Partition pruning skips unrelated directories; it does not make files
        #   inside the selected month free to scan.
        # - Keep partition columns in simple predicates (no function around the
        #   partition column) so the optimizer can recognize the partition filter.
        # - Too many tiny files increase listing, scheduling, and metadata overhead.
        #   Tune upstream task counts and target file sizes; compact incrementally.
        # - repartition() adds a shuffle but can increase write parallelism.
        #   coalesce() reduces partitions without a full shuffle, but can create
        #   uneven tasks. Choose based on data volume and observed file sizes.
        # - Avoid partitioning by high-cardinality values or creating a directory
        #   for every individual customer/order.

        print("\nInterview follow-ups:")
        print("1. How does Spark distinguish PartitionFilters from DataFilters?")
        print("2. Why can wrapping order_month in a UDF prevent partition pruning?")
        print("3. When would repartition(n, key) be preferable to coalesce(n)?")
        print("4. How do small files affect object-store listing and query planning?")
        print("5. How would you choose partition columns and target file sizes?")
        print("6. What changes when the table is managed by Delta Lake or Iceberg?")
        print("7. How would you safely compact one month without rewriting the table?")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
