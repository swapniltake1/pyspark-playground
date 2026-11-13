"""
Daily PySpark Interview Practice — 2026-11-13
Topic: Priority-Based Inventory Allocation with Cumulative Window Functions

Problem statement
-----------------
A fulfillment platform receives order lines for the same SKU at multiple
warehouses. Allocate each warehouse/SKU's available stock in deterministic
priority order. Higher-priority orders are served first; a line may be
partially fulfilled, and any remainder is reported as unfilled.

Business ordering:
  1. Lower priority number first.
  2. Earlier order timestamp first.
  3. line_id ascending as a stable tie-breaker.

Run:
    spark-submit daily/2026-11-13-priority-inventory-allocation.py

Compatible with PySpark 3.x. Uses only small in-memory sample DataFrames.
"""
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)


def build_sample_data(spark):
    order_schema = StructType([
        StructField("line_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("sku", StringType(), False),
        StructField("warehouse_id", StringType(), False),
        StructField("priority", IntegerType(), False),
        StructField("order_ts", TimestampType(), False),
        StructField("requested_qty", IntegerType(), False),
        StructField("channel", StringType(), False),
    ])
    order_rows = [
        ("L-001", "O-1001", "SKU-A", "W-1", 1, datetime(2026, 11, 13, 8, 0), 4, "app"),
        ("L-002", "O-1002", "SKU-A", "W-1", 2, datetime(2026, 11, 13, 8, 5), 5, "web"),
        ("L-003", "O-1003", "SKU-A", "W-1", 3, datetime(2026, 11, 13, 8, 10), 6, "marketplace"),
        ("L-004", "O-1004", "SKU-B", "W-1", 1, datetime(2026, 11, 13, 8, 1), 2, "web"),
        ("L-005", "O-1005", "SKU-B", "W-1", 2, datetime(2026, 11, 13, 8, 2), 4, "app"),
        ("L-006", "O-1006", "SKU-A", "W-2", 1, datetime(2026, 11, 13, 8, 3), 3, "web"),
        ("L-007", "O-1007", "SKU-A", "W-2", 2, datetime(2026, 11, 13, 8, 4), 8, "app"),
        ("L-008", "O-1008", "SKU-A", "W-3", 1, datetime(2026, 11, 13, 8, 5), 2, "marketplace"),
    ]

    inventory_schema = StructType([
        StructField("warehouse_id", StringType(), False),
        StructField("sku", StringType(), False),
        StructField("available_qty", IntegerType(), False),
    ])
    inventory_rows = [
        ("W-1", "SKU-A", 10),
        ("W-1", "SKU-B", 5),
        ("W-2", "SKU-A", 7),
        # W-3 / SKU-A is deliberately absent: its orders must receive zero stock.
    ]

    orders = spark.createDataFrame(order_rows, order_schema)
    inventory = spark.createDataFrame(inventory_rows, inventory_schema)
    return orders, inventory


def allocate_inventory(orders, inventory, broadcast_inventory=False):
    """
    Baseline and optimized paths share the exact allocation semantics.
    The optimized path broadcasts the small warehouse/SKU stock lookup.
    """
    stock = inventory.select("warehouse_id", "sku", "available_qty")
    if broadcast_inventory:
        stock = F.broadcast(stock)

    # A left join preserves orders even when no inventory record exists.
    joined = (
        orders.select(
            "line_id",
            "order_id",
            "sku",
            "warehouse_id",
            "priority",
            "order_ts",
            "requested_qty",
            "channel",
        )
        .join(stock, on=["warehouse_id", "sku"], how="left")
        .withColumn("available_qty", F.coalesce(F.col("available_qty"), F.lit(0)))
    )

    # The preceding frame excludes the current row, so each line sees only
    # the demand already allocated to higher-ranked lines in its stock group.
    prior_lines = (
        Window.partitionBy("warehouse_id", "sku")
        .orderBy(
            F.col("priority").asc(),
            F.col("order_ts").asc(),
            F.col("line_id").asc(),
        )
        .rowsBetween(Window.unboundedPreceding, -1)
    )

    staged = joined.withColumn(
        "prior_requested_qty",
        F.coalesce(F.sum("requested_qty").over(prior_lines), F.lit(0)),
    )

    remaining_before_line = F.greatest(
        F.col("available_qty") - F.col("prior_requested_qty"),
        F.lit(0),
    )
    result = (
        staged
        .withColumn(
            "allocated_qty",
            F.least(F.col("requested_qty"), remaining_before_line),
        )
        .withColumn(
            "unfilled_qty",
            F.col("requested_qty") - F.col("allocated_qty"),
        )
        .withColumn(
            "stock_after_line",
            F.greatest(
                F.col("available_qty")
                - F.col("prior_requested_qty")
                - F.col("allocated_qty"),
                F.lit(0),
            ),
        )
        .select(
            "line_id",
            "order_id",
            "sku",
            "warehouse_id",
            "priority",
            "order_ts",
            "requested_qty",
            "available_qty",
            "prior_requested_qty",
            "allocated_qty",
            "unfilled_qty",
            "stock_after_line",
            "channel",
        )
    )
    return result


def main():
    spark = (
        SparkSession.builder
        .appName("PriorityInventoryAllocation")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    orders, inventory = build_sample_data(spark)

    # Validate the join's one-row-per-key assumption before allocation.
    duplicate_stock_keys = (
        inventory.groupBy("warehouse_id", "sku")
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    assert duplicate_stock_keys == 0, "Inventory must have one row per warehouse/SKU"
    assert orders.where(F.col("requested_qty") < 0).count() == 0
    assert inventory.where(F.col("available_qty") < 0).count() == 0
    assert orders.select("line_id").distinct().count() == orders.count()

    # Baseline: ordinary left join followed by a cumulative window.
    baseline = allocate_inventory(orders, inventory, broadcast_inventory=False)

    # Optimized: project only the required stock columns and broadcast the
    # small inventory lookup to avoid shuffling the large order side for join.
    optimized = allocate_inventory(orders, inventory, broadcast_inventory=True)

    # The hint must not change business results.
    assert baseline.exceptAll(optimized).count() == 0
    assert optimized.exceptAll(baseline).count() == 0

    expected_allocations = {
        "L-001": 4,
        "L-002": 5,
        "L-003": 1,
        "L-004": 2,
        "L-005": 3,
        "L-006": 3,
        "L-007": 4,
        "L-008": 0,
    }
    actual_allocations = {
        row["line_id"]: row["allocated_qty"]
        for row in optimized.select("line_id", "allocated_qty").collect()
    }
    assert actual_allocations == expected_allocations, actual_allocations

    # No warehouse/SKU group may allocate more than its available stock.
    stock_checks = (
        optimized.groupBy("warehouse_id", "sku")
        .agg(
            F.sum("allocated_qty").alias("allocated_total"),
            F.max("available_qty").alias("available_qty"),
        )
        .collect()
    )
    assert all(
        row["allocated_total"] <= row["available_qty"]
        for row in stock_checks
    )

    expected_unfilled = {
        "L-001": 0,
        "L-002": 0,
        "L-003": 5,
        "L-004": 0,
        "L-005": 1,
        "L-006": 0,
        "L-007": 4,
        "L-008": 2,
    }
    actual_unfilled = {
        row["line_id"]: row["unfilled_qty"]
        for row in optimized.select("line_id", "unfilled_qty").collect()
    }
    assert actual_unfilled == expected_unfilled, actual_unfilled

    print("Priority-based allocation:")
    optimized.orderBy(
        "warehouse_id", "sku", "priority", "order_ts", "line_id"
    ).show(truncate=False)

    print("Baseline physical plan:")
    baseline.explain("formatted")
    print("Broadcast-optimized physical plan:")
    optimized.explain("formatted")

    print("\nExpected allocation summary:")
    print("W-1 / SKU-A: 10 of 15 units allocated")
    print("W-1 / SKU-B: 5 of 6 units allocated")
    print("W-2 / SKU-A: 7 of 11 units allocated")
    print("W-3 / SKU-A: 0 of 2 units allocated (no inventory row)")
    print("All baseline-equivalence, priority, and stock-capacity assertions passed.")

    print("\nOptimization notes:")
    print("1. Broadcast only when the inventory lookup is safely small for executor memory.")
    print("2. The window still requires repartitioning and sorting by warehouse/SKU and priority.")
    print("3. A broadcast join removes the join-side shuffle; it does not remove the window shuffle.")
    print("4. Keep the ordering deterministic with a unique final tie-breaker.")
    print("5. Do not add repartition() blindly; inspect the physical plan and partition sizes first.")
    print("6. For repeated downstream actions, consider caching the narrow allocation result only")
    print("   after measuring recomputation cost and executor memory pressure.")

    print("\nInterview follow-ups:")
    print("1. How would the allocation change if an order must be fulfilled all-or-nothing?")
    print("2. How would you prevent two concurrent jobs from allocating the same stock snapshot?")
    print("3. What happens if inventory contains duplicate warehouse/SKU keys?")
    print("4. When would a broadcast join be unsafe, and how would AQE affect the plan?")
    print("5. Why is rowsBetween used instead of rangeBetween for this priority sequence?")
    print("6. How would you persist allocations and make retries idempotent?")
    print("7. How would you extend the model to substitute SKUs or transfer stock across warehouses?")

    spark.stop()


if __name__ == "__main__":
    main()
