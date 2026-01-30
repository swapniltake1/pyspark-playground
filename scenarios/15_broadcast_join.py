from pyspark.sql.functions import broadcast
from common.spark_session import get_spark

spark = get_spark()
df = spark.read.option("header", True).option("inferSchema", True).csv("/Volumes/raw-data/git/allfiles/BigMart Sales.csv")

dim = df.select("Item_Type").distinct()
df.join(broadcast(dim), "Item_Type").show(5)