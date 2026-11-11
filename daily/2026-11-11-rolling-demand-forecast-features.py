"""
Daily PySpark Interview Practice — 2026-11-11
Topic: Rolling demand features and leakage-safe forecasting datasets

Run:
    spark-submit daily/2026-11-11-rolling-demand-forecast-features.py

Compatible with Spark 3.x. Uses only in-memory sample DataFrames.
"""
from datetime import date
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, IntegerType, DoubleType
)


def main():
    spark = (
        SparkSession.builder
        .appName("RollingDemandForecastFeatures")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("store_id", StringType(), False),
        StructField("sku", StringType(), False),
        StructField("ds", DateType(), False),
        StructField("units_sold", IntegerType(), False),
        StructField("price", DoubleType(), False),
        StructField("promotion", IntegerType(), False),
    ])
    rows = [
        ("S1","A",date(2026,11,1),10,100.0,0),
        ("S1","A",date(2026,11,2),12,100.0,0),
        ("S1","A",date(2026,11,3),18,90.0,1),
        ("S1","A",date(2026,11,4),15,90.0,1),
        ("S1","A",date(2026,11,5),11,100.0,0),
        ("S1","A",date(2026,11,6),13,100.0,0),
        ("S1","A",date(2026,11,7),14,100.0,0),
        ("S1","A",date(2026,11,8),16,95.0,0),
        ("S1","B",date(2026,11,1),4,50.0,0),
        ("S1","B",date(2026,11,2),5,50.0,0),
        ("S1","B",date(2026,11,3),7,45.0,1),
        ("S1","B",date(2026,11,4),6,45.0,1),
        ("S1","B",date(2026,11,5),5,50.0,0),
        ("S1","B",date(2026,11,6),6,50.0,0),
        ("S1","B",date(2026,11,7),8,50.0,0),
        ("S1","B",date(2026,11,8),7,50.0,0),
    ]
    sales = spark.createDataFrame(rows, schema)

    # Q1. Build a complete store/SKU/date grain and fill missing sales with zero.
    # Here the sample is complete; production pipelines should join to a calendar
    # and active-assortment table before imputing zero.
    # Q2. Calculate lag-1 demand and prior 3-observation mean, excluding today's target.
    history = (
        Window.partitionBy("store_id", "sku")
        .orderBy(F.col("ds").cast("timestamp").cast("long"))
    )
    prior_three = history.rowsBetween(-3, -1)
    features = (
        sales
        .withColumn("lag_1_units", F.lag("units_sold", 1).over(history))
        .withColumn("prior_3d_avg", F.avg("units_sold").over(prior_three))
        .withColumn("prior_3d_stddev", F.stddev_pop("units_sold").over(prior_three))
        .withColumn("prior_3d_count", F.count("units_sold").over(prior_three))
        .withColumn("price_change_pct",
                    F.col("price") / F.lag("price", 1).over(history) - F.lit(1.0))
        .withColumn("day_of_week", F.dayofweek("ds"))
        .withColumn("is_weekend", F.dayofweek("ds").isin([1, 7]).cast("int"))
    )

    # Q3. Create a one-step-ahead training label. Do not use it as an input feature.
    features = features.withColumn("label_next_day_units",
                                   F.lead("units_sold", 1).over(history))

    # Q4. Compute a simple historical baseline from prior observations only.
    features = features.withColumn(
        "baseline_next_day",
        F.round(F.col("prior_3d_avg"), 2)
    )

    # Q5. Flag unusually high demand relative to the prior 3-observation baseline.
    features = features.withColumn(
        "demand_spike",
        F.when(
            (F.col("prior_3d_count") >= 2)
            & (F.col("units_sold") > F.col("prior_3d_avg")
               + F.lit(2.0) * F.coalesce(F.col("prior_3d_stddev"), F.lit(0.0))),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    # Q6. Evaluate baseline MAE only where a future label exists.
    scored = (
        features
        .where(F.col("label_next_day_units").isNotNull()
               & F.col("baseline_next_day").isNotNull())
        .withColumn("absolute_error",
                    F.abs(F.col("label_next_day_units") - F.col("baseline_next_day")))
    )
    metrics = scored.agg(
        F.count("*").alias("evaluated_rows"),
        F.round(F.avg("absolute_error"), 3).alias("baseline_mae")
    )

    # Q7. Produce a per-SKU feature completeness report.
    completeness = features.groupBy("store_id", "sku").agg(
        F.count("*").alias("rows"),
        F.sum(F.col("prior_3d_avg").isNotNull().cast("int")).alias("rows_with_history"),
        F.sum(F.col("label_next_day_units").isNotNull().cast("int")).alias("rows_with_label")
    )

    # Q8. Assert ordering, partition isolation, and no same-day target leakage.
    first_a = (
        features.where((F.col("sku") == "A") & (F.col("ds") == F.lit(date(2026,11,1))))
        .select("lag_1_units", "prior_3d_avg", "label_next_day_units").first()
    )
    assert first_a["lag_1_units"] is None
    assert first_a["prior_3d_avg"] is None
    assert first_a["label_next_day_units"] == 12

    second_a = (
        features.where((F.col("sku") == "A") & (F.col("ds") == F.lit(date(2026,11,2))))
        .select("lag_1_units", "prior_3d_avg").first()
    )
    assert second_a["lag_1_units"] == 10
    assert second_a["prior_3d_avg"] == 10.0

    print("Daily demand features:")
    features.orderBy("store_id", "sku", "ds").show(32, truncate=False)
    print("Baseline evaluation:")
    metrics.show(truncate=False)
    print("Feature completeness:")
    completeness.orderBy("store_id", "sku").show(truncate=False)
    print("Physical plan:")
    scored.explain("formatted")

    print("\nInterview follow-ups:")
    print("1. How would you build a calendar spine for missing dates and inactive SKUs?")
    print("2. Why must rolling features exclude the current target row?")
    print("3. How would you split train/validation data using a time-based cutoff?")
    print("4. How do you handle stockouts, returns, and censored demand?")
    print("5. When would rangeBetween differ from rowsBetween for date windows?")
    print("6. How would you persist features in Delta and prevent training/serving skew?")

    spark.stop()


if __name__ == "__main__":
    main()
