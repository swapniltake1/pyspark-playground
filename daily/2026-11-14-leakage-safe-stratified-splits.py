"""
PySpark Interview Practice — Leakage-Safe, Deterministic Stratified Splits
Spark 3.x | Run: spark-submit daily/2026-11-14-leakage-safe-stratified-splits.py

Problem
-------
Create reproducible train/validation/test datasets for a churn model when the
source contains multiple event rows per customer. Every customer must belong to
exactly one split, and each split should preserve the churn-label distribution.

A naive event-level randomSplit can put the same customer in both training and
validation, leaking customer-specific information. A customer-level randomSplit
avoids that leakage but does not guarantee stratification or stable assignments
when input partitioning changes.

Approach
--------
1. Show the event-level randomSplit anti-pattern and measure customer overlap.
2. Build one assignment per customer using a deterministic hash ordering within
   each churn-label stratum.
3. Allocate exact 60/20/20 row counts per label using cumulative row numbers.
4. Join the small assignment map back to customer features and event facts.
5. Assert split disjointness, class counts, row preservation, and stability after
   repartitioning.

For time-dependent prediction problems, use a chronological split instead of a
random split. The hash strategy is for IID-style evaluation where random
stratification is appropriate.
"""

from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.functions import broadcast


SEED = 20261114
TRAIN_FRACTION = 0.60
VALIDATION_FRACTION = 0.20


def build_customer_splits(customer_df):
    """
    Assign one deterministic split to each customer, stratified by churn_label.

    floor(n * 0.60) customers go to train, floor(n * 0.80) cumulative
    customers go to train + validation, and the remainder go to test.
    The customer_id tie-break makes the ordering total even if hashes collide.
    """
    class_window = Window.partitionBy("churn_label")
    deterministic_order = Window.partitionBy("churn_label").orderBy(
        F.xxhash64(F.col("customer_id"), F.lit(SEED)).asc(),
        F.col("customer_id").asc(),
    )

    ranked = (
        customer_df
        .withColumn("_class_size", F.count(F.lit(1)).over(class_window))
        .withColumn("_class_rank", F.row_number().over(deterministic_order))
        .withColumn(
            "_train_end",
            F.floor(F.col("_class_size") * F.lit(TRAIN_FRACTION)).cast("int"),
        )
        .withColumn(
            "_validation_end",
            F.floor(
                F.col("_class_size")
                * F.lit(TRAIN_FRACTION + VALIDATION_FRACTION)
            ).cast("int"),
        )
    )

    return (
        ranked
        .withColumn(
            "split",
            F.when(F.col("_class_rank") <= F.col("_train_end"), F.lit("train"))
            .when(
                F.col("_class_rank") <= F.col("_validation_end"),
                F.lit("validation"),
            )
            .otherwise(F.lit("test")),
        )
        .select("customer_id", "churn_label", "split")
    )


def count_map(df, key_columns, value_column="count"):
    rows = (
        df.groupBy(*key_columns)
        .count()
        .collect()
    )
    return {
        tuple(row[column] for column in key_columns): row[value_column]
        for row in rows
    }


def main():
    spark = (
        SparkSession.builder
        .appName("LeakageSafeStratifiedSplits")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    customer_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("churn_label", IntegerType(), False),
        StructField("tenure_months", IntegerType(), False),
        StructField("monthly_spend", DoubleType(), False),
        StructField("region", StringType(), False),
    ])

    # Thirty customer entities: 15 retained and 15 churned.
    # Generated deterministically so the exercise has repeatable expected counts.
    regions = ["West", "North", "South", "East"]
    customer_rows = [
        (
            f"C{i:03d}",
            0 if i <= 15 else 1,
            3 + ((i * 5) % 60),
            float(25 + ((i * 11) % 180)),
            regions[(i - 1) % len(regions)],
        )
        for i in range(1, 31)
    ]
    customers = spark.createDataFrame(customer_rows, customer_schema)

    event_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_amount", DoubleType(), False),
    ])

    event_rows = []
    for i in range(1, 31):
        customer_id = f"C{i:03d}"
        base_ts = datetime(2026, 10, 1) + timedelta(days=(i % 20))
        event_rows.append(
            (f"E{i:03d}-L", customer_id, base_ts, "login", 0.0)
        )
        event_rows.append(
            (
                f"E{i:03d}-P",
                customer_id,
                base_ts + timedelta(hours=2),
                "purchase",
                float(15 + ((i * 7) % 100)),
            )
        )

    events = spark.createDataFrame(event_rows, event_schema)

    # Input contract checks: the split grain must really be one row per customer.
    duplicate_customer_ids = (
        customers.groupBy("customer_id")
        .count()
        .filter(F.col("count") != 1)
        .count()
    )
    assert duplicate_customer_ids == 0, "Customer dimension must be unique"
    assert customers.filter(~F.col("churn_label").isin(0, 1)).count() == 0

    # ------------------------------------------------------------------
    # Baseline / anti-pattern: randomSplit at event grain.
    # A customer's login and purchase events can land in different splits.
    # The measured overlap is diagnostic; its exact count is seed/partition
    # dependent and is deliberately not hard-coded as an expected result.
    # ------------------------------------------------------------------
    event_parts = events.randomSplit([0.60, 0.20, 0.20], seed=SEED)
    bad_event_splits = (
        event_parts[0].withColumn("split", F.lit("train"))
        .unionByName(event_parts[1].withColumn("split", F.lit("validation")))
        .unionByName(event_parts[2].withColumn("split", F.lit("test")))
    )
    leaked_customer_count = (
        bad_event_splits.groupBy("customer_id")
        .agg(F.countDistinct("split").alias("split_count"))
        .filter(F.col("split_count") > 1)
        .count()
    )
    print(f"Customers appearing in multiple event-level splits: {leaked_customer_count}")

    # A second baseline: splitting the customer table avoids entity leakage,
    # but it is not stratified and can vary with data/partition ordering.
    baseline_parts = customers.randomSplit([0.60, 0.20, 0.20], seed=SEED)
    baseline_assignments = (
        baseline_parts[0].select("customer_id", "churn_label")
        .withColumn("split", F.lit("train"))
        .unionByName(
            baseline_parts[1].select("customer_id", "churn_label")
            .withColumn("split", F.lit("validation"))
        )
        .unionByName(
            baseline_parts[2].select("customer_id", "churn_label")
            .withColumn("split", F.lit("test"))
        )
    )
    print("Baseline customer-level randomSplit class counts:")
    baseline_assignments.groupBy("churn_label", "split").count().orderBy(
        "churn_label", "split"
    ).show()

    # ------------------------------------------------------------------
    # Recommended path: deterministic, stratified, customer-grain assignment.
    # ------------------------------------------------------------------
    assignments = build_customer_splits(customers)

    expected_class_counts = {
        (0, "train"): 9,
        (0, "validation"): 3,
        (0, "test"): 3,
        (1, "train"): 9,
        (1, "validation"): 3,
        (1, "test"): 3,
    }
    actual_class_counts = count_map(
        assignments, ["churn_label", "split"]
    )
    assert actual_class_counts == expected_class_counts, (
        f"Unexpected class split counts: {actual_class_counts}"
    )

    # Assignment key must be unique and every customer must be assigned once.
    duplicate_assignments = (
        assignments.groupBy("customer_id")
        .count()
        .filter(F.col("count") != 1)
        .count()
    )
    assert duplicate_assignments == 0
    assert assignments.count() == customers.count()

    # Apply the same customer assignment to the feature table and event facts.
    # The sample assignment map is tiny, so broadcast avoids a shuffle on this
    # join. Do not force broadcast in production unless the map fits in executor
    # memory; otherwise let Spark/AQE choose a suitable join strategy.
    assignment_map = assignments.select("customer_id", "split")
    customer_training_rows = customers.join(
        broadcast(assignment_map), on="customer_id", how="inner"
    )
    assigned_events = events.join(
        broadcast(assignment_map), on="customer_id", how="inner"
    )

    assert customer_training_rows.count() == customers.count()
    assert assigned_events.count() == events.count()

    unassigned_event_count = (
        events.join(assignment_map, on="customer_id", how="left_anti").count()
    )
    assert unassigned_event_count == 0

    # No entity can cross train/validation/test, even though it has many events.
    cross_split_customers = (
        assigned_events.groupBy("customer_id")
        .agg(F.countDistinct("split").alias("split_count"))
        .filter(F.col("split_count") != 1)
        .count()
    )
    assert cross_split_customers == 0

    expected_event_counts = {
        ("train",): 36,
        ("validation",): 12,
        ("test",): 12,
    }
    actual_event_counts = count_map(assigned_events, ["split"])
    assert actual_event_counts == expected_event_counts, (
        f"Unexpected event split counts: {actual_event_counts}"
    )

    # Determinism check: changing input partitioning must not change assignment.
    repartitioned_assignments = build_customer_splits(customers.repartition(4))
    assignment_columns = ["customer_id", "churn_label", "split"]
    forward_difference = (
        assignments.select(*assignment_columns)
        .exceptAll(repartitioned_assignments.select(*assignment_columns))
        .count()
    )
    reverse_difference = (
        repartitioned_assignments.select(*assignment_columns)
        .exceptAll(assignments.select(*assignment_columns))
        .count()
    )
    assert forward_difference == 0 and reverse_difference == 0

    print("Expected stratified customer counts:")
    assignments.groupBy("churn_label", "split").count().orderBy(
        "churn_label", "split"
    ).show()
    print("Assigned event counts:")
    assigned_events.groupBy("split").count().orderBy("split").show()
    print("Sample training rows:")
    customer_training_rows.filter(F.col("split") == "train").orderBy(
        "customer_id"
    ).show(10, truncate=False)

    print("Optimized assignment/join physical plan:")
    assigned_events.explain("formatted")

    print("\nOptimization and design notes:")
    print("1. Split at the entity grain, not the event grain, to prevent leakage.")
    print("2. Hash ordering plus a stable key tie-break gives repeatable assignments.")
    print("3. Stratifying by label preserves class counts for this balanced sample.")
    print("4. Broadcast only the small assignment map; avoid forcing it when large.")
    print("5. Keep the assignment map as a reusable artifact for every feature table.")
    print("6. For time-dependent labels, prefer chronological or rolling-origin splits.")
    print("7. Tiny strata may produce empty splits with floor-based allocation; add")
    print("   minimum-per-split validation or a documented fallback for production.")

    print("\nInterview follow-ups:")
    print("1. Why can event-level randomSplit leak information across model splits?")
    print("2. How would you create exact stratified percentages for very small classes?")
    print("3. Why is xxhash64 preferable to Python's process-randomized hash() here?")
    print("4. How would you handle a customer whose churn label changes over time?")
    print("5. When should a chronological split replace a stratified random split?")
    print("6. At what assignment-map size would you stop using a broadcast join?")
    print("7. How would you persist and version split assignments across training runs?")

    print("\nAll leakage-safe stratified split assertions passed.")
    spark.stop()


if __name__ == "__main__":
    main()
