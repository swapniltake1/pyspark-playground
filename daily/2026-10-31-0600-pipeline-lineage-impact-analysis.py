"""
DATA ENGINEERING PRACTICE
Topic: Pipeline Lineage Graph Traversal & Downstream Impact Analysis
File: 2026-10-31-0600-pipeline-lineage-impact-analysis.py

Scenario
--------
A data platform has jobs connected by upstream -> downstream dependencies.
When a source or transformation changes, identify every impacted pipeline,
calculate the minimum dependency depth, and validate whether the dependency
graph is a DAG before scheduling execution.

Runtime: PySpark 3.3+
No external graph package is required.
"""

from functools import reduce

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)


NODE_SCHEMA = StructType(
    [
        StructField("job_id", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("owner", StringType(), False),
        StructField("criticality", StringType(), False),
    ]
)

EDGE_SCHEMA = StructType(
    [
        StructField("upstream_job_id", StringType(), False),
        StructField("downstream_job_id", StringType(), False),
    ]
)

SEED_SCHEMA = StructType(
    [
        StructField("job_id", StringType(), False),
        StructField("depth", IntegerType(), False),
    ]
)


def build_sample_graph(spark: SparkSession):
    """Create a realistic medallion-style pipeline dependency graph."""
    nodes = spark.createDataFrame(
        [
            ("raw_orders", "source", "commerce", "critical"),
            ("bronze_orders", "bronze", "data-platform", "high"),
            ("silver_orders", "silver", "data-platform", "critical"),
            ("raw_customers", "source", "crm", "high"),
            ("bronze_customers", "bronze", "data-platform", "medium"),
            ("silver_customer_360", "silver", "customer-analytics", "high"),
            ("raw_products", "source", "catalog", "medium"),
            ("bronze_products", "bronze", "data-platform", "low"),
            ("silver_products", "silver", "catalog-analytics", "medium"),
            ("gold_sales", "gold", "finance-analytics", "critical"),
            ("executive_kpis", "serving", "bi-platform", "critical"),
            ("fraud_features", "feature", "risk-analytics", "high"),
            ("fraud_alerts", "serving", "risk-analytics", "critical"),
        ],
        schema=NODE_SCHEMA,
    )

    edges = spark.createDataFrame(
        [
            ("raw_orders", "bronze_orders"),
            ("bronze_orders", "silver_orders"),
            ("raw_customers", "bronze_customers"),
            ("bronze_customers", "silver_customer_360"),
            ("raw_products", "bronze_products"),
            ("bronze_products", "silver_products"),
            ("silver_orders", "gold_sales"),
            ("silver_customer_360", "gold_sales"),
            ("silver_products", "gold_sales"),
            ("gold_sales", "executive_kpis"),
            ("silver_orders", "fraud_features"),
            ("fraud_features", "fraud_alerts"),
        ],
        schema=EDGE_SCHEMA,
    )

    return nodes, edges


def downstream_impact(
    spark: SparkSession,
    nodes: DataFrame,
    edges: DataFrame,
    changed_job_ids,
    max_depth: int = 50,
) -> DataFrame:
    """
    Breadth-first traversal using DataFrame joins.

    The visited set prevents repeated work when multiple paths converge and
    guarantees termination even if an unexpected cycle is present. Depth is
    the shortest number of dependency edges from any changed seed.
    """
    seeds = spark.createDataFrame(
        [(job_id, 0) for job_id in sorted(set(changed_job_ids))],
        schema=SEED_SCHEMA,
    ).dropDuplicates(["job_id"])

    visited = seeds
    frontier = seeds
    iteration = 0

    while iteration < max_depth:
        candidates = (
            frontier.alias("f")
            .join(
                edges.alias("e"),
                F.col("f.job_id") == F.col("e.upstream_job_id"),
                "inner",
            )
            .select(
                F.col("e.downstream_job_id").alias("job_id"),
                (F.col("f.depth") + F.lit(1)).cast("int").alias("depth"),
            )
        )

        next_frontier = (
            candidates.join(
                visited.select("job_id").distinct(),
                on="job_id",
                how="left_anti",
            )
            .groupBy("job_id")
            .agg(F.min("depth").cast("int").alias("depth"))
        )

        if next_frontier.limit(1).count() == 0:
            break

        visited = visited.unionByName(next_frontier)
        frontier = next_frontier
        iteration += 1

    if iteration == max_depth:
        raise RuntimeError(
            f"Traversal reached max_depth={max_depth}; inspect graph size "
            "or increase the explicit safety limit."
        )

    return (
        visited.join(nodes, on="job_id", how="inner")
        .select("job_id", "depth", "layer", "owner", "criticality")
        .orderBy("depth", "job_id")
    )


def topological_layers(nodes: DataFrame, edges: DataFrame):
    """
    Kahn's algorithm implemented with DataFrame anti-joins.

    Returns:
      layers: each job with its topological level
      blocked: nodes left after no zero-in-degree node can be found;
               non-empty output indicates a cycle (or a dependency problem).
    """
    remaining_nodes = nodes.select("job_id").distinct()
    remaining_edges = edges.select(
        "upstream_job_id", "downstream_job_id"
    ).distinct()

    layer_frames = []
    level = 0

    while remaining_nodes.limit(1).count() > 0:
        incoming = (
            remaining_edges.groupBy("downstream_job_id")
            .agg(F.count("*").alias("in_degree"))
        )

        ready = (
            remaining_nodes.alias("n")
            .join(
                incoming.alias("i"),
                F.col("n.job_id") == F.col("i.downstream_job_id"),
                "left",
            )
            .filter(F.coalesce(F.col("i.in_degree"), F.lit(0)) == 0)
            .select(F.col("n.job_id").alias("job_id"))
            .distinct()
        )

        if ready.limit(1).count() == 0:
            break

        layer_frames.append(
            ready.withColumn(
                "topological_level", F.lit(level).cast("int")
            )
        )

        remaining_nodes = remaining_nodes.join(
            ready, on="job_id", how="left_anti"
        )

        ready_upstream = ready.select(
            F.col("job_id").alias("upstream_job_id")
        )
        remaining_edges = remaining_edges.join(
            ready_upstream, on="upstream_job_id", how="left_anti"
        )

        level += 1

    if layer_frames:
        layers = reduce(
            lambda left, right: left.unionByName(right),
            layer_frames,
        )
    else:
        layers = (
            nodes.select("job_id")
            .limit(0)
            .withColumn(
                "topological_level", F.lit(0).cast("int")
            )
        )

    return layers.orderBy("topological_level", "job_id"), remaining_nodes


def validate_edge_references(nodes: DataFrame, edges: DataFrame):
    """Return dependencies whose upstream or downstream job is unknown."""
    known_upstream = nodes.select(
        F.col("job_id").alias("upstream_job_id")
    )
    known_downstream = nodes.select(
        F.col("job_id").alias("downstream_job_id")
    )

    missing_upstream = edges.join(
        known_upstream, on="upstream_job_id", how="left_anti"
    )
    missing_downstream = edges.join(
        known_downstream, on="downstream_job_id", how="left_anti"
    )
    return missing_upstream, missing_downstream


def main():
    spark = (
        SparkSession.builder
        .appName("PipelineLineageImpactAnalysis")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    nodes, edges = build_sample_graph(spark)

    # The same edge list is reused in breadth-first and topological passes.
    nodes = nodes.cache()
    edges = edges.cache()
    nodes.count()
    edges.count()

    print("\\n=== 1. Pipeline nodes and direct dependencies ===")
    nodes.orderBy("layer", "job_id").show(truncate=False)
    edges.orderBy("upstream_job_id", "downstream_job_id").show(truncate=False)

    print("\\n=== 2. Downstream impact when raw_orders changes ===")
    impact = downstream_impact(
        spark,
        nodes,
        edges,
        changed_job_ids=["raw_orders"],
    )
    impact.show(truncate=False)

    actual_depths = {
        row["job_id"]: row["depth"] for row in impact.collect()
    }
    expected_depths = {
        "raw_orders": 0,
        "bronze_orders": 1,
        "silver_orders": 2,
        "gold_sales": 3,
        "fraud_features": 3,
        "executive_kpis": 4,
        "fraud_alerts": 4,
    }
    assert actual_depths == expected_depths, (
        f"Unexpected impact map: {actual_depths}"
    )

    print("\\n=== 3. Topological execution layers ===")
    layers, blocked = topological_layers(nodes, edges)
    layers.show(truncate=False)

    assert blocked.count() == 0, (
        "The baseline dependency graph should be acyclic."
    )

    level_map = {
        row["job_id"]: row["topological_level"]
        for row in layers.collect()
    }
    assert level_map["raw_orders"] == 0
    assert level_map["bronze_orders"] == 1
    assert level_map["silver_orders"] == 2
    assert level_map["gold_sales"] == 3
    assert level_map["executive_kpis"] == 4

    print("\\n=== 4. Validate dependency references ===")
    missing_upstream, missing_downstream = validate_edge_references(
        nodes, edges
    )
    assert missing_upstream.count() == 0
    assert missing_downstream.count() == 0

    # A deliberately invalid edge proves that orphan detection is effective.
    invalid_edge = spark.createDataFrame(
        [("raw_missing", "gold_sales")],
        schema=EDGE_SCHEMA,
    )
    bad_edges = edges.unionByName(invalid_edge)
    bad_upstream, _ = validate_edge_references(nodes, bad_edges)
    bad_upstream.show(truncate=False)
    assert bad_upstream.count() == 1

    print("\\n=== 5. Cycle detection using a deliberately invalid edge ===")
    cycle_edge = spark.createDataFrame(
        [("fraud_alerts", "silver_orders")],
        schema=EDGE_SCHEMA,
    )
    cyclic_edges = edges.unionByName(cycle_edge)
    _, cycle_blocked = topological_layers(nodes, cyclic_edges)
    blocked_ids = {row["job_id"] for row in cycle_blocked.collect()}
    assert "silver_orders" in blocked_ids
    assert "fraud_alerts" in blocked_ids
    print(f"Cycle-blocked jobs: {sorted(blocked_ids)}")

    print("\\n=== 6. Physical plan for the impact result ===")
    impact.explain(mode="formatted")

    print("\\nAll pipeline lineage and graph-integrity assertions passed.")
    edges.unpersist()
    nodes.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()


"""
OPTIMIZATION GUIDANCE
--------------------
1. This iterative DataFrame implementation is intentionally transparent for
   interview practice. Each BFS level triggers Spark work; it is not a
   substitute for a graph engine on millions of vertices/edges.
2. Persist the node and edge dimensions when reused across multiple traversal
   passes. Unpersist them after the analysis to control executor memory.
3. Filter to the affected upstream neighborhood before traversal when lineage
   metadata is large. Keep edge records narrow (two IDs plus optional metadata).
4. Avoid Python UDFs for graph expansion. Native joins, anti-joins, aggregation,
   and projections remain visible to Catalyst.
5. For production-scale multi-hop traversal, evaluate GraphFrames or GraphX,
   or maintain a precomputed transitive-closure table when dependency changes
   are relatively infrequent.
6. Never collect a large graph to the driver. Persist impact rows to a lineage
   or orchestration-control table and apply owner/criticality filters there.
7. Kahn's loop is driver-orchestrated and performs repeated actions. For large
   DAG validation, use a graph library or a dedicated metadata service.

INTERVIEW FOLLOW-UPS
--------------------
- How would you calculate the critical path when every job has a duration?
- How would you handle a job with multiple upstream dependencies?
- How would you incrementally refresh lineage after one edge changes?
- What is the trade-off between on-demand traversal and transitive closure?
- How would you prioritize impacted jobs by SLA, owner, and business criticality?
- How would you represent cross-platform dependencies across ADF, Databricks,
  Airflow, and external APIs?
"""
