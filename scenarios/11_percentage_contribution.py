from pyspark.sql.functions import sum
from common.spark_session import get_spark

spark = get_spark()
df = spark.read.option("header", True).option("inferSchema", True).csv("/Volumes/raw-data/git/allfiles/BigMart Sales.csv")

total_sales = df.agg(sum("Item_Outlet_Sales")).first()[0]

df.withColumn("sales_percentage", (df.Item_Outlet_Sales / total_sales) * 100).show(5)