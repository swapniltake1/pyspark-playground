"""
PySpark Interview Practice: Multi-Dimensional Aggregations with rollup() and cube()

Problem statement
-----------------
A retail analytics team needs one consistent sales summary for dashboards:
- detail by region, channel, and product category;
- hierarchical subtotals (region/channel/category);
- arbitrary dimensional subtotals for interactive slicing;
- a grand total.

Build the summaries without confusing a real NULL category with a subtotal row.
Use grouping_id() to identify which dimensions Spark aggregated away.

Run:
    spark-submit daily/2026-10-31-multidimensional-rollups.py

Compatible with Apache Spark / PySpark 3.x.
"""

from decimal import Decimal

from pyspark.sql import SparkSession, functions as F, types as T


def build_sample_orders(spark):
    schema = T.StructType(
        [
            T.StructField("order_id", T.StringType(), False),
            T.StructField("region", T.StringType(), False),
            T.StructField("channel", T.StringType(), False),
            T.StructField("category", T.StringType(), True),
            T.StructField("net_amount", T.DecimalType(12, 2), False),
            T.StructField("order_status", T.StringType(), False),
        ]
    )

    rows = [
        ("O-1001", "North", "Web", "Electronics", Decimal("120.00"), "COMPLETED"),
        ("O-1002", "North", "Store", "Electronics", Decimal("80.00"), "COMPLETED"),
        ("O-1003", "North", "Web", "Grocery", Decimal("40.00"), "COMPLETED"),
        ("O-1004", "South", "Web", "Electronics", Decimal("200.00"), "COMPLETED"),
        ("O-1005", "South", "Store", "Grocery", Decimal("60.00"), "COMPLETED"),
        # A genuine NULL category: grouping_id distinguishes this detail row
        # from subtotal rows where category was removed by rollup/cube.
        ("O-1006", "South", "Web", None, Decimal("25.00"), "COMPLETED"),
        ("O-1007", "North", "Store", "Electronics", Decimal("20.00"), "COMPLETED"),
        ("O-1008", "North", "Web", "Electronics", Decimal("40.00"), "COMPLETED"),
        ("O-1009", "North", "Web", "Electronics", Decimal("999.00"), "CANCELLED"),
    ]
    return spark.createDataFrame(rows, schema)


def add_level_label(summary):
    """Explain the grouping_id bitmask for region, channel, category."""
    return summary.withColumn(
        "aggregation_level",
        F.when(F.col("grouping_id") == 0, F.lit("DETAIL"))
        .when(F.col("grouping_id") == 1, F.lit("REGION_CHANNEL_SUBTOTAL"))
        .when(F.col("grouping_id") == 2, F.lit("REGION_CATEGORY_SUBTOTAL"))
        .when(F.col("grouping_id") == 3, F.lit("REGION_SUBTOTAL"))
        .when(F.col("grouping_id") == 4, F.lit("CHANNEL_CATEGORY_SUBTOTAL"))
        .when(F.col("grouping_id") == 5, F.lit("CHANNEL_SUBTOTAL"))
        .when(F.col("grouping_id") == 6, F.lit("CATEGORY_SUBTOTAL"))
        .otherwise(F.lit("GRAND_TOTAL")),
    )


def main():
    spark = (
        SparkSession.builder
        .appName("MultidimensionalRollupsInterviewPractice")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    orders = build_sample_orders(spark)

    # 1) Filter early and project only the columns required by the aggregate.
    # This reduces data shuffled by the aggregation.
    completed = (
        orders
        .filter(F.col("order_status") == "COMPLETED")
        .select("region", "channel", "category", "net_amount")
    )

    # 2) Hierarchical report: detail -> region/channel subtotal -> region
    # subtotal -> grand total. The order of dimensions defines the hierarchy.
    rollup_summary = (
        completed
        .rollup("region", "channel", "category")
        .agg(
            F.sum("net_amount").alias("revenue"),
            F.count(F.lit(1)).alias("order_count"),
            F.grouping_id("region", "channel", "category").alias("grouping_id"),
        )
    )

    # 3) Optimized multi-dimensional report: one cube aggregation emits all
    # 2^3 grouping combinations. This replaces separate scans/aggregations
    # when a dashboard genuinely needs every dimensional subtotal.
    cube_summary = add_level_label(
        completed
        .cube("region", "channel", "category")
        .agg(
            F.sum("net_amount").alias("revenue"),
            F.count(F.lit(1)).alias("order_count"),
            F.grouping_id("region", "channel", "category").alias("grouping_id"),
        )
    )

    print("Hierarchical rollup (grouping_id 0, 1, 3, 7):")
    rollup_summary.orderBy("grouping_id", "region", "channel", "category").show(
        truncate=False
    )

    print("All cube levels:")
    cube_summary.orderBy("grouping_id", "region", "channel", "category").show(
        truncate=False
    )

    # 4) Assertions: totals, detail groups, and NULL-vs-subtotal semantics.
    north_region = (
        rollup_summary
        .filter((F.col("region") == "North") & (F.col("grouping_id") == 3))
        .first()
    )
    grand_total = (
        rollup_summary
        .filter(F.col("grouping_id") == 7)
        .first()
    )
    north_web_electronics = (
        cube_summary
        .filter(
            (F.col("region") == "North")
            & (F.col("channel") == "Web")
            & (F.col("category") == "Electronics")
            & (F.col("grouping_id") == 0)
        )
        .first()
    )
    genuine_null_detail = (
        cube_summary
        .filter(
            (F.col("region") == "South")
            & (F.col("channel") == "Web")
            & F.col("category").isNull()
            & (F.col("grouping_id") == 0)
        )
        .first()
    )

    assert north_region is not None
    assert north_region["revenue"] == Decimal("300.00")
    assert north_region["order_count"] == 5

    assert grand_total is not None
    assert grand_total["revenue"] == Decimal("585.00")
    assert grand_total["order_count"] == 8

    assert north_web_electronics is not None
    assert north_web_electronics["revenue"] == Decimal("160.00")
    assert north_web_electronics["order_count"] == 2

    assert genuine_null_detail is not None
    assert genuine_null_detail["revenue"] == Decimal("25.00")
    assert genuine_null_detail["grouping_id"] == 0

    # All eight grouping sets must be represented for this sample.
    grouping_levels = {
        row["grouping_id"]
        for row in cube_summary.select("grouping_id").distinct().collect()
    }
    assert grouping_levels == set(range(8)), grouping_levels

    # Six distinct detail combinations; the cancelled order is excluded.
    assert cube_summary.filter(F.col("grouping_id") == 0).count() == 6
    assert cube_summary.filter(F.col("grouping_id") == 7).count() == 1

    print("All assertions passed.")
    print("Expected checks:")
    print("  North subtotal: revenue=300.00, orders=5")
    print("  Grand total:    revenue=585.00, orders=8")
    print("  North/Web/Electronics: revenue=160.00, orders=2")
    print("  South/Web/NULL detail: revenue=25.00, grouping_id=0")

    print("Physical plan for the cube aggregation:")
    cube_summary.explain(mode="formatted")

    # In production, write the summary to a managed table using an explicit
    # overwrite/merge strategy appropriate to the reporting period.
    # Avoid collecting the full aggregate to the driver.
    # spark.stop() is omitted to remain notebook-friendly on Databricks.


if __name__ == "__main__":
    main()


"""
EXPLANATION
-----------
- groupBy(region, channel, category) produces only detail-level groups.
- rollup(region, channel, category) produces a hierarchy:
    (region, channel, category), (region, channel), (region), ().
- cube(region, channel, category) produces every combination of dimensions:
    2^3 = 8 grouping sets.
- grouping_id(region, channel, category) is a bitmask. A set bit means that
  the corresponding dimension was aggregated away. For this order:
    0 = all dimensions present (detail)
    1 = category omitted
    3 = channel and category omitted (region subtotal)
    7 = all dimensions omitted (grand total)
- A real NULL category at grouping_id=0 is different from a subtotal row
  whose category is NULL because Spark removed that dimension.

OPTIMIZATION GUIDANCE
--------------------
1. Filter cancelled/test records and select only required columns before
   rollup/cube so fewer bytes enter the shuffle.
2. Prefer rollup when the business report follows one hierarchy. Use cube
   only when consumers need arbitrary combinations of dimensions.
3. Cube cardinality grows exponentially: N dimensions can produce up to 2^N
   grouping sets. Keep the dimension list intentionally small.
4. Spark performs partial aggregation before shuffle for supported aggregates
   such as sum/count. Inspect explain(mode="formatted") for exchanges and
   aggregate stages rather than assuming the operation is shuffle-free.
5. If the same filtered base is reused for multiple expensive reports, consider
   persist() only after measuring recomputation and memory pressure; unpersist
   after the final action.
6. Avoid orderBy on a large production result unless the consumer requires
   global ordering. Sort only a bounded sample for debugging.
7. Partition output tables by a low-cardinality reporting date when that
   matches downstream filters; do not partition by high-cardinality order_id.

INTERVIEW FOLLOW-UPS
--------------------
1. When would you choose rollup over cube?
2. How does grouping_id distinguish a genuine NULL from a subtotal NULL?
3. Why can cube become expensive as dimensions increase?
4. What shuffle and partial-aggregation stages do you expect in the physical plan?
5. How would you incrementally refresh only one business date's summary?
6. How would you validate that detail rows reconcile to the grand total?
7. Would you cache the filtered base DataFrame? What metrics would guide you?
8. How would you publish these aggregates to Delta Lake without exposing a
   partially written reporting period?
"""
