from common.spark_session import get_spark

spark = get_spark()
df = spark.read.option("header", True).option("inferSchema", True).csv("/Volumes/raw-data/git/allfiles/BigMart Sales.csv")

df.groupBy("Outlet_Type").pivot("Item_Fat_Content").avg("Item_Outlet_Sales").show()