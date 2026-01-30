from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from common.spark_session import get_spark

spark = get_spark()

df = spark.read.option("header", True).option("inferSchema", True)    .csv("/Volumes/raw-data/git/allfiles/BigMart Sales.csv")

w = Window.partitionBy("Outlet_Identifier").orderBy(df.Item_Outlet_Sales.desc())

df.withColumn("rn", row_number().over(w)).filter("rn <= 5").show()