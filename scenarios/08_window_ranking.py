from pyspark.sql.window import Window
from pyspark.sql.functions import rank
from common.spark_session import get_spark

spark = get_spark()

df = spark.read.option("header", True).option("inferSchema", True)    .csv("/Volumes/raw-data/git/allfiles/BigMart Sales.csv")

w = Window.partitionBy("Outlet_Type").orderBy(df.Item_Outlet_Sales.desc())

df.withColumn("rank", rank().over(w)).show(5)