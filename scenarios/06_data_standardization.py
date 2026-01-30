from pyspark.sql.functions import when
from common.spark_session import get_spark

spark = get_spark()

df = spark.read.option("header", True).option("inferSchema", True)    .csv("/Volumes/raw-data/git/allfiles/BigMart Sales.csv")

df = df.withColumn(
    "Item_Fat_Content",
    when(df.Item_Fat_Content.isin("low fat", "LF"), "Low Fat")
    .when(df.Item_Fat_Content == "reg", "Regular")
    .otherwise(df.Item_Fat_Content)
)

df.groupBy("Item_Fat_Content").count().show()