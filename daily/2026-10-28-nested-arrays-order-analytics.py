"""
Daily PySpark Interview Practice — 2026-10-28
Topic: Flattening nested arrays and avoiding unnecessary row expansion.

Problem:
An orders DataFrame stores line items in an array of structs. Produce:
1. One row per line item and revenue totals by SKU.
2. An order-level total that includes orders with empty or null item arrays.
3. A comparison between an explode-and-aggregate approach and a higher-order
   aggregate that computes order totals without expanding rows.

Requirements:
- Use explicit schemas and in-memory sample data.
- Handle empty and null arrays safely.
- Assert the expected results.
- Run with Spark 3.x: spark-submit daily/2026-10-28-nested-arrays-order-analytics.py
"""

from datetime import date

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def build_orders(spark):
    item_schema = StructType(
        [
            StructField("sku", StringType(), True),
            StructField("qty", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
        ]
    )
    order_schema = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("order_date", DateType(), False),
            StructField("items", ArrayType(item_schema, containsNull=True), True),
        ]
    )

    rows = [
        (
            "O100",
            "C001",
            date(2026, 10, 20),
            [("SKU-A", 2, 10.0), ("SKU-B", 1, 15.0)],
        ),
        ("O101", "C002", date(2026, 10, 21), []),
        (
            "O102",
            "C001",
            date(2026, 10, 22),
            [("SKU-C", 3, 7.5), ("SKU-A", 1, 10.0)],
        ),
        ("O103", "C003", date(2026, 10, 23), None),
    ]
    return spark.createDataFrame(rows, schema=order_schema)


def main():
    spark = (
        SparkSession.builder
        .appName("DailyNestedArrayOrderAnalytics")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        orders = build_orders(spark)
        print("Input orders:")
        orders.show(truncate=False)

        # Problem 1 — explode nested items for item-level analysis.
        # Exclude empty/null arrays before explode because SKU metrics do not
        # need a placeholder row for orders without line items.
        item_rows = (
            orders
            .filter(F.coalesce(F.size("items"), F.lit(0)) > 0)
            .select("order_id", F.explode("items").alias("item"))
            .select(
                "order_id",
                F.col("item.sku").alias("sku"),
                F.col("item.qty").alias("qty"),
                F.col("item.unit_price").alias("unit_price"),
                (F.col("item.qty") * F.col("item.unit_price")).alias("line_revenue"),
            )
        )

        sku_metrics = (
            item_rows
            .groupBy("sku")
            .agg(
                F.sum("qty").alias("units_sold"),
                F.sum("line_revenue").alias("revenue"),
            )
            .orderBy("sku")
        )

        print("Revenue by SKU:")
        sku_metrics.show(truncate=False)

        # Problem 2 — baseline order totals.
        # This is easy to understand, but expanding items and then grouping
        # creates additional rows and a shuffle. A left join restores orders
        # that had no line items.
        exploded_for_totals = orders.select(
            "order_id", F.explode_outer("items").alias("item")
        )
        baseline_line_totals = (
            exploded_for_totals
            .groupBy("order_id")
            .agg(
                F.sum(
                    F.coalesce(
                        F.col("item.qty") * F.col("item.unit_price"),
                        F.lit(0.0),
                    )
                ).alias("order_total")
            )
        )
        baseline_totals = (
            orders.select("order_id")
            .join(baseline_line_totals, on="order_id", how="left")
            .select(
                "order_id",
                F.coalesce(F.col("order_total"), F.lit(0.0)).alias("order_total"),
            )
            .orderBy("order_id")
        )

        print("Order totals — explode/groupBy baseline:")
        baseline_totals.show(truncate=False)

        # Problem 3 — optimized order totals.
        # Higher-order aggregate evaluates each order's array in place. It
        # avoids materializing one row per item and avoids the groupBy/join
        # needed solely to calculate a parent-level total.
        order_totals = (
            orders
            .select(
                "order_id",
                F.coalesce(
                    F.expr(
                        """
                        aggregate(
                            items,
                            CAST(0.0 AS DOUBLE),
                            (acc, item) ->
                                acc + coalesce(
                                    item.qty * item.unit_price,
                                    CAST(0.0 AS DOUBLE)
                                )
                        )
                        """
                    ),
                    F.lit(0.0),
                ).alias("order_total"),
            )
            .orderBy("order_id")
        )

        print("Order totals — higher-order aggregate:")
        order_totals.show(truncate=False)

        # Problem 4 — verify results, including empty and null arrays.
        expected_totals = {
            "O100": 35.0,
            "O101": 0.0,
            "O102": 32.5,
            "O103": 0.0,
        }
        actual_baseline = {
            row["order_id"]: float(row["order_total"])
            for row in baseline_totals.collect()
        }
        actual_optimized = {
            row["order_id"]: float(row["order_total"])
            for row in order_totals.collect()
        }
        assert actual_baseline.keys() == expected_totals.keys()
        assert actual_optimized.keys() == expected_totals.keys()
        for order_id, expected in expected_totals.items():
            assert abs(actual_baseline[order_id] - expected) < 1e-6
            assert abs(actual_optimized[order_id] - expected) < 1e-6

        expected_sku = {
            "SKU-A": (3, 30.0),
            "SKU-B": (1, 15.0),
            "SKU-C": (3, 22.5),
        }
        actual_sku = {
            row["sku"]: (int(row["units_sold"]), float(row["revenue"]))
            for row in sku_metrics.collect()
        }
        assert actual_sku.keys() == expected_sku.keys()
        for sku, (expected_units, expected_revenue) in expected_sku.items():
            units, revenue = actual_sku[sku]
            assert units == expected_units
            assert abs(revenue - expected_revenue) < 1e-6

        print("All assertions passed.")

        # Optional interview exercise: inspect plans and compare them on a
        # representative production-scale dataset, not just these tiny rows.
        print("Optimized order-total physical plan:")
        order_totals.explain(mode="formatted")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()


"""
Optimization notes:
- Use explode when the business question genuinely requires item-level rows,
  such as revenue by SKU. Filter out null/empty arrays first when those rows
  cannot contribute to the result.
- If only an order-level total is required, aggregate the array in place with
  Spark's higher-order aggregate function. This avoids row expansion and the
  groupBy shuffle/join in the baseline approach.
- The SKU groupBy still requires distributed aggregation; inspect its physical
  plan and measure on representative data before changing partition counts.
- Do not assume the higher-order expression is always faster. Benchmark with
  realistic array lengths, skew, input sizes, and the target Spark version.

Interview follow-ups:
1. What output does explode_outer produce for an empty array versus a null array?
2. Why can exploding two independent arrays multiply row counts?
3. Which operation in the SKU aggregation may trigger a shuffle?
4. How would you handle malformed items, negative quantities, or currency
   precision? (Consider explicit decimal types and data-quality rules.)
5. How would you benchmark both approaches using explain(), Spark UI, and
   representative data?
"""
