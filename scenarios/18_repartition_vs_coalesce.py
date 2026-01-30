from common.spark_session import get_spark

spark = get_spark()
df = spark.read.option("header", True).option("inferSchema", True).csv("/Volumes/raw-data/git/allfiles/BigMart Sales.csv")

print(df.repartition(10).rdd.getNumPartitions())
print(df.coalesce(1).rdd.getNumPartitions())