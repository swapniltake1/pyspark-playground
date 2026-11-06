"""
PySpark Interview Practice: Approximate Aggregations for API Observability
============================================================================

Problem statement
-----------------
A platform processes millions of API request events per day. Product and SRE
teams need daily request counts, approximate unique-user counts, latency
percentiles (P50/P95/P99), and error-rate indicators by service. Exact
distinct-count aggregations can be expensive at high cardinality, while
approximate metrics are often sufficient for dashboards and alert triage.

Run:
    spark-submit daily/2026-11-06-approximate-aggregations-api-observability.py

Compatibility: PySpark 3.3+ (uses approx_count_distinct and percentile_approx).

Learning objectives
-------------------
1. Build a realistic, deterministic event DataFrame with explicit types.
2. Compare exact countDistinct with HyperLogLog++-based approx_count_distinct.
3. Calculate grouped P50/P95/P99 with percentile_approx.
4. Use approxQuantile for a global distribution check.
5. Discover candidate heavy hitters with freqItems.
6. Explain accuracy, shuffle, memory, and operational trade-offs.

This is a self-contained batch exercise. No external files or services required.
"""

from pyspark.sql import SparkSession, functions as F


def build_sample_events(spark):
    """Create 10,000 deterministic API request events using Spark expressions."""
    source = spark.range(0, 10_000)

    events = source.select(
        F.col("id").cast("long").alias("event_id"),
        F.date_add(
            F.lit("2026-11-04").cast("date"),
            (F.col("id") % 3).cast("int"),
        ).alias("event_date"),
        F.when(F.col("id") % 4 == 0, F.lit("payments"))
        .when(F.col("id") % 4 == 1, F.lit("accounts"))
        .when(F.col("id") % 4 == 2, F.lit("cards"))
        .otherwise(F.lit("loans"))
        .alias("service"),
        F.when(F.col("id") % 3 == 0, F.lit("IN-WEST"))
        .when(F.col("id") % 3 == 1, F.lit("IN-SOUTH"))
        .otherwise(F.lit("IN-NORTH"))
        .alias("region"),
        F.when(F.col("id") % 5 == 0, F.lit("/payments/authorize"))
        .when(F.col("id") % 5 == 1, F.lit("/accounts/balance"))
        .when(F.col("id") % 5 == 2, F.lit("/cards/limits"))
        .when(F.col("id") % 5 == 3, F.lit("/loans/status"))
        .otherwise(F.lit("/customers/profile"))
        .alias("endpoint"),
        F.when(F.col("id") % 113 == 0, F.lit(None).cast("long"))
        .otherwise(((F.col("id") * 37) % 2_500).cast("long"))
        .alias("user_id"),
        F.when(F.col("id") % 97 == 0, F.lit(1_800))
        .otherwise((25 + (F.col("id") * 13) % 275).cast("int"))
        .alias("latency_ms"),
        F.when(F.col("id") % 29 == 0, F.lit(500))
        .when(F.col("id") % 17 == 0, F.lit(429))
        .otherwise(F.lit(200))
        .cast("int")
        .alias("status_code"),
    )

    return events


def main():
    spark = (
        SparkSession.builder
        .appName("ApproximateAggregationsApiObservability")
        .getOrCreate()
    )

    # Keep the local exercise lightweight. Production values should be selected
    # from measured input size, cluster parallelism, and task metrics.
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    events = build_sample_events(spark)

    print("\n=== Sample API events ===")
    events.show(8, truncate=False)
    events.printSchema()

    # ---------------------------------------------------------------------
    # Exercise 1: Validate the event population and understand null handling.
    # ---------------------------------------------------------------------
    total_rows = events.count()
    null_user_rows = events.filter(F.col("user_id").isNull()).count()

    assert total_rows == 10_000
    assert null_user_rows > 0

    print(f"Input rows: {total_rows:,}")
    print(f"Rows with unknown user_id: {null_user_rows:,}")

    # countDistinct and approx_count_distinct ignore null values. If unknown
    # users must be represented as a separate category, model that explicitly
    # rather than silently replacing null with a potentially real user ID.

    # ---------------------------------------------------------------------
    # Exercise 2: Exact distinct users by day and service (reference result).
    # ---------------------------------------------------------------------
    exact_users = (
        events.groupBy("event_date", "service")
        .agg(
            F.countDistinct("user_id").alias("exact_unique_users"),
            F.count("*").alias("request_count"),
        )
    )

    print("\n=== Exact distinct-user reference ===")
    exact_users.orderBy("event_date", "service").show(20, truncate=False)

    # Exact distinct aggregation can require substantial distributed state.
    # Use this as a correctness/reference calculation when exactness is needed.

    # ---------------------------------------------------------------------
    # Exercise 3: Optimized dashboard metrics with approximate aggregation.
    # ---------------------------------------------------------------------
    # rsd is the target relative standard deviation for the HLL++ estimate.
    # Lower rsd generally increases sketch size and computation cost.
    dashboard_metrics = (
        events.groupBy("event_date", "service")
        .agg(
            F.count("*").alias("request_count"),
            F.approx_count_distinct("user_id", rsd=0.02)
            .alias("estimated_unique_users"),
            F.percentile_approx(
                "latency_ms",
                [0.50, 0.95, 0.99],
                10_000,
            ).alias("latency_p50_p95_p99_ms"),
            F.avg("latency_ms").alias("avg_latency_ms"),
            (
                F.avg(
                    F.when(F.col("status_code") >= 500, F.lit(1.0))
                    .otherwise(F.lit(0.0))
                ) * F.lit(100.0)
            ).alias("server_error_pct"),
            (
                F.avg(
                    F.when(F.col("latency_ms") > 500, F.lit(1.0))
                    .otherwise(F.lit(0.0))
                ) * F.lit(100.0)
            ).alias("over_500ms_pct"),
        )
    )

    print("\n=== Approximate dashboard metrics ===")
    dashboard_metrics.orderBy("event_date", "service").show(20, truncate=False)

    # ---------------------------------------------------------------------
    # Exercise 4: Compare approximate counts with exact reference counts.
    # ---------------------------------------------------------------------
    comparison = (
        dashboard_metrics.select(
            "event_date", "service", "estimated_unique_users"
        )
        .join(exact_users, ["event_date", "service"], "inner")
        .withColumn(
            "relative_error_pct",
            F.when(
                F.col("exact_unique_users") > 0,
                F.abs(
                    F.col("estimated_unique_users")
                    - F.col("exact_unique_users")
                )
                / F.col("exact_unique_users")
                * 100.0,
            ).otherwise(F.lit(0.0)),
        )
    )

    print("\n=== Approximation error by day and service ===")
    comparison.orderBy("event_date", "service").show(20, truncate=False)

    # This deterministic sample is deliberately large enough for useful
    # approximate-count practice. The tolerance is a smoke test, not a promise
    # that every production group will stay within a fixed error bound.
    bad_estimates = comparison.filter(
        F.col("relative_error_pct") > 15.0
    ).count()
    assert bad_estimates == 0

    # ---------------------------------------------------------------------
    # Exercise 5: Global quantiles with DataFrame.approxQuantile.
    # ---------------------------------------------------------------------
    # approxQuantile returns a small Python list to the driver, making it useful
    # for a few global thresholds. Do not collect the source rows.
    global_quantiles = events.approxQuantile(
        "latency_ms",
        [0.50, 0.95, 0.99],
        0.01,  # relativeError: smaller means more accurate and more expensive
    )

    assert len(global_quantiles) == 3
    assert global_quantiles[0] <= global_quantiles[1] <= global_quantiles[2]

    print(
        "\nGlobal approximate latency quantiles "
        f"(P50, P95, P99): {global_quantiles}"
    )

    # ---------------------------------------------------------------------
    # Exercise 6: Candidate heavy hitters with frequent-items analysis.
    # ---------------------------------------------------------------------
    # freqItems is approximate and may return false positives. Treat results
    # as candidates, then calculate exact frequencies for those candidates
    # before making operational decisions.
    frequent_endpoint_candidates = (
        events.stat.freqItems(["endpoint"], support=0.15)
        .select(F.explode("endpoint_freqItems").alias("endpoint"))
        .distinct()
    )

    print("\n=== Candidate frequent endpoints ===")
    frequent_endpoint_candidates.show(truncate=False)

    exact_candidate_frequencies = (
        events.join(frequent_endpoint_candidates, "endpoint", "inner")
        .groupBy("endpoint")
        .agg(F.count("*").alias("request_count"))
        .orderBy(F.desc("request_count"))
    )

    print("\n=== Exact counts for candidate endpoints ===")
    exact_candidate_frequencies.show(truncate=False)

    # ---------------------------------------------------------------------
    # Exercise 7: Tune approximate distinct-count accuracy.
    # ---------------------------------------------------------------------
    accuracy_comparison = events.agg(
        F.countDistinct("user_id").alias("exact_users"),
        F.approx_count_distinct("user_id", rsd=0.05).alias("estimate_rsd_005"),
        F.approx_count_distinct("user_id", rsd=0.02).alias("estimate_rsd_002"),
        F.approx_count_distinct("user_id", rsd=0.01).alias("estimate_rsd_001"),
    )

    print("\n=== Accuracy / cost comparison (inspect estimates) ===")
    accuracy_comparison.show(truncate=False)

    # ---------------------------------------------------------------------
    # Exercise 8: Validate expected output shape and inspect the physical plan.
    # ---------------------------------------------------------------------
    exact_global_users = (
        events.agg(F.countDistinct("user_id").alias("n"))
        .first()["n"]
    )
    assert exact_global_users == 2_500

    assert dashboard_metrics.count() == 12  # 3 dates x 4 services
    assert (
        dashboard_metrics.agg(F.sum("request_count").alias("n")).first()["n"]
        == 10_000
    )
    assert (
        dashboard_metrics.filter(
            F.col("latency_p50_p95_p99_ms")[0]
            > F.col("latency_p50_p95_p99_ms")[1]
        ).count()
        == 0
    )
    assert (
        dashboard_metrics.filter(
            F.col("latency_p50_p95_p99_ms")[1]
            > F.col("latency_p50_p95_p99_ms")[2]
        ).count()
        == 0
    )

    print("\n=== Physical plan for dashboard aggregation ===")
    dashboard_metrics.explain("formatted")

    print("\nAll correctness assertions passed.")
    spark.stop()


if __name__ == "__main__":
    main()


"""
Transformation and optimization notes
--------------------------------------

1. approx_count_distinct
   Uses a HyperLogLog++-style sketch instead of maintaining the full exact
   distinct set. It is useful for high-cardinality dashboards where a small,
   controlled estimation error is acceptable. The rsd parameter controls the
   requested relative standard deviation; validate it against business needs.

2. percentile_approx
   Computes grouped latency percentiles without sorting and retaining every
   value in each group. The accuracy parameter controls the memory/accuracy
   trade-off. P95/P99 are often more actionable for latency SLOs than averages.

3. approxQuantile
   Returns only requested global quantiles to the driver. It is appropriate
   for a small list of thresholds, not for transferring source data to Python.

4. freqItems
   Produces candidate frequent values and may include false positives. Recount
   candidates exactly before using them for routing, alerting, or billing.

5. Shuffle and partitioning
   groupBy still redistributes rows by grouping keys. Approximate aggregates
   reduce aggregate-state pressure; they do not remove the groupBy shuffle.
   Choose shuffle partitions from data volume and observed task sizes, not a
   fixed universal number. Inspect the SQL UI for spill, skew, and task time.

6. Exactness and nulls
   Keep exact countDistinct for financial, billing, or regulatory metrics when
   approximation is not acceptable. Both exact and approximate distinct-count
   functions ignore nulls; define unknown-user semantics explicitly.

Interview follow-ups
--------------------
1. How does HyperLogLog++ estimate cardinality, and what does rsd mean?
2. Why can a lower rsd increase memory and CPU cost?
3. When would you choose countDistinct over approx_count_distinct?
4. How do percentile_approx and approxQuantile differ in grouped versus global
   use cases?
5. Why can averages hide tail-latency problems?
6. Does approx_count_distinct eliminate the shuffle from groupBy? Why not?
7. How would you validate approximate metrics before using them in an SLO?
8. What would you monitor in Spark UI to identify spill or skew during these
   aggregations?
"""
