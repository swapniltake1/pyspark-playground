"""
PySpark Interview Practice — Bill of Materials (BOM) Explosion & Cycle Detection
===============================================================================

Problem
-------
A manufacturer stores a multi-level bill of materials as parent/child edges.
For a requested root assembly, calculate the total quantity of each terminal
component required across every valid path. Reject malformed edges, detect
cycles without recursing forever, and retain the path that caused each result.

This exercise uses iterative DataFrame joins because Spark DataFrames do not
provide a portable recursive traversal API across Spark 3.x.

Run
---
    spark-submit daily/2026-11-28-bill-of-materials-explosion-and-cycle-detection.py

Compatible with PySpark 3.x. No external files or services are required.
"""

from functools import reduce

from pyspark.sql import SparkSession, functions as F
from pyspark.sql import types as T
from pyspark import StorageLevel


def union_or_empty(frames, schema, spark):
    """Union same-shaped DataFrames, returning a typed empty frame if needed."""
    if not frames:
        return spark.createDataFrame([], schema)
    return reduce(lambda left, right: left.unionByName(right), frames)


def explode_bom(roots, edges, broadcast_edges=False, max_depth=20):
    """
    Expand each root into component paths.

    A path-local array detects cycles. A cyclic edge is reported separately and
    is not traversed. Quantities multiply along a path.
    """
    join_edges = F.broadcast(edges) if broadcast_edges else edges

    expanded_schema = T.StructType([
        T.StructField("root_id", T.StringType(), False),
        T.StructField("parent_component", T.StringType(), False),
        T.StructField("current_component", T.StringType(), False),
        T.StructField("required_qty", T.LongType(), False),
        T.StructField("path", T.ArrayType(T.StringType()), False),
        T.StructField("depth", T.IntegerType(), False),
    ])
    cycle_schema = T.StructType([
        T.StructField("root_id", T.StringType(), False),
        T.StructField("parent_component", T.StringType(), False),
        T.StructField("child_id", T.StringType(), False),
        T.StructField("required_qty", T.LongType(), False),
        T.StructField("cycle_path", T.ArrayType(T.StringType()), False),
        T.StructField("depth", T.IntegerType(), False),
    ])

    frontier = (
        roots.select(
            F.col("root_id"),
            F.col("root_id").alias("current_component"),
        )
        .withColumn("required_qty", F.lit(1).cast("long"))
        .withColumn("path", F.array(F.col("root_id")))
        .withColumn("depth", F.lit(0).cast("int"))
    )

    expanded_levels = []
    cycle_levels = []
    completed = False

    for depth in range(1, max_depth + 1):
        joined = frontier.alias("f").join(
            join_edges.alias("e"),
            F.col("f.current_component") == F.col("e.parent_id"),
            "inner",
        )

        # Use a SQL expression so both sides of array_contains are columns.
        is_cycle = F.expr("array_contains(f.path, e.child_id)")

        cycle_rows = joined.filter(is_cycle).select(
            F.col("f.root_id").alias("root_id"),
            F.col("f.current_component").alias("parent_component"),
            F.col("e.child_id").alias("child_id"),
            (F.col("f.required_qty") * F.col("e.qty_per")).cast("long").alias("required_qty"),
            F.concat(F.col("f.path"), F.array(F.col("e.child_id"))).alias("cycle_path"),
            F.lit(depth).cast("int").alias("depth"),
        )
        cycle_levels.append(cycle_rows)

        next_frontier = joined.filter(~is_cycle).select(
            F.col("f.root_id").alias("root_id"),
            F.col("f.current_component").alias("parent_component"),
            F.col("e.child_id").alias("current_component"),
            (F.col("f.required_qty") * F.col("e.qty_per")).cast("long").alias("required_qty"),
            F.concat(F.col("f.path"), F.array(F.col("e.child_id"))).alias("path"),
            F.lit(depth).cast("int").alias("depth"),
        )

        # One bounded action per level lets the driver stop when the frontier
        # is empty. This is appropriate for a small, known-depth BOM exercise.
        if next_frontier.limit(1).count() == 0:
            completed = True
            break

        expanded_levels.append(next_frontier)
        frontier = next_frontier

    if not completed:
        # Do not silently return a partial explosion when max_depth is too low.
        has_more = (
            frontier.join(
                join_edges,
                frontier.current_component == join_edges.parent_id,
                "inner",
            )
            .limit(1)
            .count()
            > 0
        )
        if has_more:
            raise RuntimeError(
                f"BOM traversal exceeded max_depth={max_depth}; increase the limit "
                "or validate the graph before publishing results."
            )

    expanded = union_or_empty(expanded_levels, expanded_schema, roots.sparkSession)
    cycles = union_or_empty(cycle_levels, cycle_schema, roots.sparkSession)

    # A component is a terminal leaf when it was reached but never expanded
    # through a non-cyclic edge. A cyclic edge is quarantined from leaf logic.
    expanded_parents = expanded.select(
        F.col("parent_component").alias("component_id")
    ).distinct()

    leaf_paths = expanded.alias("x").join(
        expanded_parents.alias("p"),
        F.col("x.current_component") == F.col("p.component_id"),
        "left_anti",
    )

    leaf_totals = (
        leaf_paths.groupBy("root_id", "current_component")
        .agg(F.sum("required_qty").cast("long").alias("total_required_qty"))
    )

    return {
        "expanded": expanded,
        "cycles": cycles,
        "leaf_totals": leaf_totals,
    }


def leaf_result_map(df):
    return {
        (row["root_id"], row["current_component"]): int(row["total_required_qty"])
        for row in df.collect()
    }


def cycle_signatures(df):
    rows = (
        df.select(
            "root_id",
            "parent_component",
            "child_id",
            "required_qty",
            "cycle_path",
        )
        .orderBy("root_id", "parent_component", "child_id")
        .collect()
    )
    return [
        (
            row["root_id"],
            row["parent_component"],
            row["child_id"],
            int(row["required_qty"]),
            list(row["cycle_path"]),
        )
        for row in rows
    ]


def main():
    spark = (
        SparkSession.builder
        .appName("BOM Explosion and Cycle Detection")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "4")

    components_schema = T.StructType([
        T.StructField("component_id", T.StringType(), False),
        T.StructField("description", T.StringType(), False),
    ])
    edges_schema = T.StructType([
        T.StructField("parent_id", T.StringType(), False),
        T.StructField("child_id", T.StringType(), False),
        T.StructField("qty_per", T.IntegerType(), True),
    ])
    roots_schema = T.StructType([
        T.StructField("root_id", T.StringType(), False),
    ])

    components = spark.createDataFrame(
        [
            ("A100", "Main assembly"),
            ("B200", "Frame subassembly"),
            ("C300", "Control subassembly"),
            ("D400", "Fastener"),
            ("E500", "Power cable"),
            ("F600", "Wiring harness"),
            ("G700", "Sensor"),
        ],
        components_schema,
    )

    # G700 -> C300 deliberately creates a cycle reachable from A100.
    # The final two rows are invalid: one unknown child and one zero quantity.
    bom_edges = spark.createDataFrame(
        [
            ("A100", "B200", 2),
            ("A100", "C300", 3),
            ("B200", "D400", 4),
            ("B200", "E500", 1),
            ("C300", "D400", 2),
            ("C300", "F600", 5),
            ("F600", "G700", 2),
            ("G700", "C300", 1),       # cycle edge
            ("A100", "M999", 1),       # unknown child
            ("B200", "D400", 0),       # invalid quantity
        ],
        edges_schema,
    )

    roots = spark.createDataFrame([("A100",)], roots_schema)

    # Validate edge references and quantities before graph traversal.
    parent_lookup = (
        components.select(F.col("component_id").alias("parent_id"))
        .withColumn("_parent_found", F.lit(1))
    )
    child_lookup = (
        components.select(F.col("component_id").alias("child_id"))
        .withColumn("_child_found", F.lit(1))
    )

    edge_quality = (
        bom_edges
        .join(parent_lookup, "parent_id", "left")
        .join(child_lookup, "child_id", "left")
        .withColumn(
            "reject_reason",
            F.when(
                F.col("qty_per").isNull() | (F.col("qty_per") <= 0),
                F.lit("INVALID_QTY"),
            )
            .when(F.col("_parent_found").isNull(), F.lit("UNKNOWN_PARENT"))
            .when(F.col("_child_found").isNull(), F.lit("UNKNOWN_CHILD"))
            .otherwise(F.lit(None).cast("string")),
        )
    )

    quarantine_edges = edge_quality.filter(
        F.col("reject_reason").isNotNull()
    ).select("parent_id", "child_id", "qty_per", "reject_reason")

    valid_edges = (
        edge_quality.filter(F.col("reject_reason").isNull())
        .select(
            "parent_id",
            "child_id",
            F.col("qty_per").cast("long").alias("qty_per"),
        )
    )

    assert valid_edges.count() == 8
    assert quarantine_edges.count() == 2

    quarantine_reasons = {
        row["reject_reason"]: row["count"]
        for row in quarantine_edges.groupBy("reject_reason").count().collect()
    }
    assert quarantine_reasons == {"INVALID_QTY": 1, "UNKNOWN_CHILD": 1}

    # Baseline: iterative joins without explicit caching or broadcast hints.
    baseline = explode_bom(roots, valid_edges, broadcast_edges=False)

    # Optimized for a small reusable BOM edge set: materialize it once and
    # broadcast it for each level's join. Do not broadcast a large edge table.
    cached_edges = valid_edges.persist(StorageLevel.MEMORY_AND_DISK)
    cached_edges.count()
    try:
        optimized = explode_bom(
            roots,
            cached_edges,
            broadcast_edges=True,
        )

        expected_leaves = {
            ("A100", "D400"): 14,  # 2*4 through B200 plus 3*2 through C300
            ("A100", "E500"): 2,
            ("A100", "G700"): 30,  # 3*5*2 through C300 and F600
        }

        baseline_leaves = leaf_result_map(baseline["leaf_totals"])
        optimized_leaves = leaf_result_map(optimized["leaf_totals"])
        assert baseline_leaves == expected_leaves
        assert optimized_leaves == expected_leaves
        assert baseline_leaves == optimized_leaves

        expected_cycle = [
            (
                "A100",
                "G700",
                "C300",
                30,
                ["A100", "C300", "F600", "G700", "C300"],
            )
        ]
        baseline_cycles = cycle_signatures(baseline["cycles"])
        optimized_cycles = cycle_signatures(optimized["cycles"])
        assert baseline_cycles == expected_cycle
        assert optimized_cycles == expected_cycle
        assert baseline_cycles == optimized_cycles

        assert baseline["expanded"].count() == 7
        assert optimized["expanded"].count() == 7
        assert baseline["cycles"].count() == 1
        assert optimized["cycles"].count() == 1
        assert sum(expected_leaves.values()) == 46

        print("\nTerminal component requirements:")
        optimized["leaf_totals"].orderBy("current_component").show(truncate=False)

        print("Detected cycle edges:")
        optimized["cycles"].select(
            "root_id",
            "parent_component",
            "child_id",
            "required_qty",
            "cycle_path",
        ).show(truncate=False)

        print("Quarantined BOM edges:")
        quarantine_edges.orderBy("parent_id", "child_id").show(truncate=False)

        print("Optimized leaf aggregation physical plan:")
        optimized["leaf_totals"].explain(mode="formatted")

        print(
            "PASS: baseline and optimized outputs match; "
            "leaf quantities, cycle detection, and edge quarantine are correct."
        )
    finally:
        cached_edges.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()


"""
Transformation and optimization notes
--------------------------------------
1. Validate parent/child references and positive quantities before traversal.
2. Start with one frontier row per requested root. Each iteration joins the
   current component to BOM parent_id and multiplies the path quantity by
   qty_per.
3. Keep a path array per traversal branch. array_contains detects a cycle on
   that path; a cycle edge is recorded and not expanded.
4. Aggregate terminal components across converging paths. Here D400 is reached
   twice, so its requirement is 8 + 6 = 14.
5. The optimized path persists the narrow edge DataFrame because it is reused
   at every depth, and broadcasts it only because this sample edge set is
   small. A production implementation should verify table size and executor
   memory before applying a broadcast hint.
6. Iterative traversal still launches work per depth. For very deep or very
   large graphs, evaluate GraphFrames/GraphX, a precomputed closure table, or
   a graph-oriented service rather than forcing an oversized broadcast join.
7. Long quantities are used for this integer example. Manufacturing systems
   with fractional units should use an appropriate DecimalType and explicit
   rounding/overflow policy.

Interview follow-up questions
-----------------------------
1. Why must cycle detection be path-local rather than a global visited set when
   a component can be reached through multiple legitimate assembly paths?
2. How would you support multiple root assemblies in one run?
3. How would effective-dated BOM versions change the edge validation and join?
4. When is broadcasting the edge table unsafe, and what physical-plan evidence
   would you inspect?
5. How would you represent fractional component quantities and prevent numeric
   overflow?
6. What should the pipeline do when a cycle is found: quarantine the edge,
   fail the whole root, or fail the complete batch?
7. How could you incrementally recompute requirements after one BOM edge changes?
8. When would GraphFrames/GraphX or a precomputed transitive-closure table be a
   better fit than iterative DataFrame joins?
"""
