from pyspark.sql.functions import avg
from common.spark_session import get_spark

spark = get_spark()

df = spark.read.option("header", True).option("inferSchema", True)    .csv("/Volumes/raw-data/git/allfiles/BigMart Sales.csv")

df.groupBy("Item_Type")   .agg(avg("Item_Outlet_Sales").alias("avg_sales"))   .show()