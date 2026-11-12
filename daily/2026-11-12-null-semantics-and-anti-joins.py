"""
PySpark Interview Practice — 2026-11-12
Topic: NULL semantics, NOT IN, and left anti joins

Problem statement
-----------------
A retail platform receives transactions and a suppression list of customer IDs.
Return transactions belonging to customers who are not suppressed. The feeds can
contain duplicate suppression keys and NULL customer IDs.

Explain why SQL NOT IN can unexpectedly return no rows when its subquery contains
NULL, why an equality-based left_anti join treats NULL differently, and how to
implement an explicit data-quality policy before optimizing the join.

Run:
    spark-submit daily/2026-11-12-null-semantics-and-anti-joins.py

Requires PySpark 3.x. All data is generated in memory.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)


def sorted_ids(df):
    return sorted(row.transaction_id for row in df.select("transaction_id").collect())


def main():
    spark = (
        SparkSession.builder
        .appName("NullSemanticsAndAntiJoins")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    transaction_schema = StructType([
        StructField("transaction_id", IntegerType(), False),
        StructField("customer_id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("amount", DoubleType(), False),
    ])
    suppression_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("reason", StringType(), False),
    ])

    transactions = spark.createDataFrame(
        [
            (1001, "C001", "IN", 120.00),
            (1002, "C002", "IN", 80.00),
            (1003, "C003", "US", 45.50),
            (1004, None,   "IN", 30.00),   # missing business key
            (1005, "C004", None, 50.00),   # country is optional for this rule
            (1006, "C005", "IN", 65.00),
            (1007, "C006", "US", 20.00),
            (1008, "C007", "IN", 99.00),
        ],
        transaction_schema,
    )

    suppression = spark.createDataFrame(
        [
            ("C002", "chargeback abuse"),
            ("C005", "account review"),
            ("C005", "duplicate source record"),
            (None,   "unresolved source identity"),
        ],
        suppression_schema,
    )

    transactions.createOrReplaceTempView("transactions")
    suppression.createOrReplaceTempView("suppression")

    print("\nInput transactions:")
    transactions.orderBy("transaction_id").show(truncate=False)
    print("Suppression keys (including duplicate and NULL):")
    suppression.show(truncate=False)

    # Q1. Demonstrate SQL NOT IN's three-valued logic.
    # A NULL returned by the subquery makes non-matching comparisons UNKNOWN.
    # WHERE keeps only TRUE, so the result is empty in this example.
    not_in_result = spark.sql("""
        SELECT transaction_id
        FROM transactions
        WHERE customer_id NOT IN (
            SELECT customer_id FROM suppression
        )
    """)
    not_in_ids = sorted_ids(not_in_result)
    assert not_in_ids == [], f"Unexpected NOT IN result: {not_in_ids}"

    # Q2. A normal equality-based left anti join has different NULL behavior.
    # NULL = NULL is UNKNOWN, not TRUE; therefore transaction 1004 survives.
    # Duplicate suppression keys do not duplicate left-side rows in an anti join.
    suppression_keys = suppression.select("customer_id").dropDuplicates()
    equality_anti = transactions.join(
        suppression_keys,
        on="customer_id",
        how="left_anti",
    )
    equality_anti_ids = sorted_ids(equality_anti)
    assert equality_anti_ids == [1001, 1003, 1004, 1005, 1007, 1008]

    # Q3. Define the business policy explicitly:
    #   * transactions with a NULL customer_id are invalid and quarantined;
    #   * NULL suppression keys are ignored because they cannot identify a customer;
    #   * duplicate suppression IDs are reduced to one key;
    #   * all other transactions are eligible only if no suppression key matches.
    quarantine = (
        transactions
        .filter(F.col("customer_id").isNull())
        .withColumn("quarantine_reason", F.lit("NULL_CUSTOMER_ID"))
    )
    valid_transactions = transactions.filter(F.col("customer_id").isNotNull())
    valid_suppression_keys = (
        suppression
        .filter(F.col("customer_id").isNotNull())
        .select("customer_id")
        .dropDuplicates()
    )

    eligible = valid_transactions.join(
        valid_suppression_keys,
        on="customer_id",
        how="left_anti",
    )
    eligible_ids = sorted_ids(eligible)
    quarantine_ids = sorted_ids(quarantine)

    assert eligible_ids == [1001, 1003, 1005, 1007, 1008]
    assert quarantine_ids == [1004]

    # Q4. Alternative semantics: eqNullSafe treats NULL as equal to NULL.
    # Use this only when the business rule intentionally considers two missing
    # keys equivalent. Because suppression contains a NULL row, transaction
    # 1004 is excluded by this null-safe anti join.
    null_safe_anti = (
        transactions.alias("t")
        .join(
            suppression.alias("s"),
            F.col("t.customer_id").eqNullSafe(F.col("s.customer_id")),
            "left_anti",
        )
        .select("t.*")
    )
    null_safe_ids = sorted_ids(null_safe_anti)
    assert null_safe_ids == [1001, 1003, 1005, 1007, 1008]

    # Q5. Optimized path for a genuinely small suppression dimension.
    # Filter invalid fact keys early, project only the join key, deduplicate it,
    # and broadcast it to avoid shuffling the larger transaction side.
    # Do not force broadcast when the reference set is large or unstable in size.
    small_suppression_keys = (
        suppression
        .filter(F.col("customer_id").isNotNull())
        .select("customer_id")
        .dropDuplicates()
    )
    optimized_eligible = (
        transactions
        .filter(F.col("customer_id").isNotNull())
        .join(
            F.broadcast(small_suppression_keys),
            on="customer_id",
            how="left_anti",
        )
    )

    optimized_ids = sorted_ids(optimized_eligible)
    assert optimized_ids == eligible_ids

    print("\nQ1 — SQL NOT IN result (empty because suppression contains NULL):")
    not_in_result.show()
    print("Q2 — Equality left_anti (NULL transaction key survives):")
    equality_anti.orderBy("transaction_id").show(truncate=False)
    print("Q3 — Recommended eligible transactions:")
    eligible.orderBy("transaction_id").show(truncate=False)
    print("Q3 — Quarantined transactions:")
    quarantine.orderBy("transaction_id").show(truncate=False)
    print("Q4 — NULL-safe left_anti result:")
    null_safe_anti.orderBy("transaction_id").show(truncate=False)

    print("\nOptimized physical plan:")
    optimized_eligible.explain("formatted")

    print("\nAssertions passed:")
    print(f"NOT IN rows: {len(not_in_ids)}")
    print(f"Equality anti-join transaction IDs: {equality_anti_ids}")
    print(f"Eligible transaction IDs: {eligible_ids}")
    print(f"Quarantined transaction IDs: {quarantine_ids}")
    print(f"NULL-safe anti-join transaction IDs: {null_safe_ids}")

    print("\nInterview follow-ups:")
    print("1. Explain TRUE, FALSE, and UNKNOWN in SQL three-valued logic.")
    print("2. Why can NOT IN behave differently from NOT EXISTS when NULL is present?")
    print("3. When should a data pipeline quarantine a NULL business key instead of matching it?")
    print("4. What changes when the anti-join uses a composite key with nullable columns?")
    print("5. When is a broadcast left anti join preferable to a shuffled hash or sort-merge join?")
    print("6. How can AQE change a join strategy, and how would you confirm it in the physical plan?")
    print("7. Why should duplicate reference keys be removed, and when might that deduplication cost more than it saves?")

    spark.stop()


if __name__ == "__main__":
    main()
