"""
Advanced PySpark Interview Scenarios – Single File

Covers:
- AQE (Adaptive Query Execution)
- Join strategies
- Skew handling
- Small files problem
- Checkpointing
- Schema evolution
- Corrupt records
- Partition pruning
- Idempotent writes
- Data quality checks
- Accumulators
- Broadcast variables
- Explain plans

Dataset: BigMart Sales CSV
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# --------------------------------------------------
# Spark Session with AQE enabled
# --------------------------------------------------
spark = (
    SparkSession.builder
    .appName("Advanced-PySpark-Interview")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .getOrCreate()
)

# --------------------------------------------------
# Read Data
# --------------------------------------------------
df = spark.read.option("header", True).option("inferSchema", True) \
    .csv("/Volumes/raw-data/git/allfiles/BigMart Sales.csv")

# --------------------------------------------------
# 1. Data Quality Checks
# --------------------------------------------------
invalid_mrp = df.filter(col("Item_MRP") <= 0).count()
print(f"Invalid MRP records: {invalid_mrp}")

# --------------------------------------------------
# 2. Feature Engineering
# --------------------------------------------------
df = df.withColumn("Outlet_Age", year(current_date()) - col("Outlet_Establishment_Year"))

# --------------------------------------------------
# 3. AQE + Skew Handling (automatic)
# --------------------------------------------------
# dim_item = df.select("Item_Type").distinct()
dim_item = df.select("Item_Type").distinct()
df = df.join(broadcast(dim_item), "Item_Type")

# --------------------------------------------------
# 4. Manual Salting (Skew handling - legacy)
# --------------------------------------------------
df = df.withColumn(
    "salt_key",
    concat(col("Outlet_Type"), lit("_"), (rand() * 10).cast("int"))
)

# --------------------------------------------------
# 5. Window Function (Top-N)
# --------------------------------------------------
w = Window.partitionBy("Outlet_Type").orderBy(col("Item_Outlet_Sales").desc())
df = df.withColumn("rank", row_number().over(w))

# --------------------------------------------------
# 6. Cache vs Persist
# --------------------------------------------------
df.cache()
df.count()

# --------------------------------------------------
# 7. Explain Plan
# --------------------------------------------------
df.filter(col("Item_MRP") > 150).explain(True)

# --------------------------------------------------
# 8. Partition Pruning
# --------------------------------------------------
df.filter(col("Outlet_Location_Type") == "Tier 2").count()

# --------------------------------------------------
# 9. Accumulator Example
# --------------------------------------------------
bad_visibility = spark.sparkContext.accumulator(0)

def check_visibility(v):
    if v == 0:
        bad_visibility.add(1)
    return v

visibility_udf = udf(check_visibility, IntegerType())
df.withColumn("Item_Visibility", visibility_udf("Item_Visibility")).count()
print("Zero visibility count:", bad_visibility.value)

# --------------------------------------------------
# 10. Broadcast Variable
# --------------------------------------------------
tier_factor = spark.sparkContext.broadcast({"Tier 1": 1.2, "Tier 2": 1.1})

df = df.withColumn(
    "location_factor",
    tier_factor.value.get("Tier 1")
)

# --------------------------------------------------
# 11. Checkpointing
# --------------------------------------------------
spark.sparkContext.setCheckpointDir("/tmp/spark_checkpoints")
df = df.checkpoint()
df.count()

# --------------------------------------------------
# 12. Small Files Problem Mitigation
# --------------------------------------------------
spark.conf.set("spark.sql.files.maxRecordsPerFile", 1_000_000)

# --------------------------------------------------
# 13. Schema Evolution (Parquet)
# --------------------------------------------------
df.write.mode("overwrite").parquet("/tmp/bigmart_parquet")

spark.read.option("mergeSchema", "true").parquet("/tmp/bigmart_parquet").count()

# --------------------------------------------------
# 14. Idempotent Write
# --------------------------------------------------
df.write.mode("overwrite").partitionBy("Outlet_Type").parquet("/tmp/bigmart_final")

print("Advanced PySpark interview pipeline completed successfully.")
