from pyspark.sql.functions import when


df = spark.read.option("header", True).option("inferSchema", True)    .csv("/Volumes/raw-data/git/allfiles/BigMart Sales.csv")

df.withColumn(
    "MRP_Category",
    when(df.Item_MRP < 100, "Low")
    .when(df.Item_MRP < 200, "Medium")
    .otherwise("High")
).show(5)