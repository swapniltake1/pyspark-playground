from common.spark_session import get_spark

spark = get_spark()
df = spark.read.option("header", True).option("inferSchema", True).csv("/Volumes/raw-data/git/allfiles/BigMart Sales.csv")

df.filter(
    (df.Item_MRP > 150) &
    (df.Outlet_Location_Type == "Tier 1") &
    (df.Item_Outlet_Sales > 2000)
).show()