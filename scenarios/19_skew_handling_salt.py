from pyspark.sql.functions import concat, lit, rand
from common.spark_session import get_spark

spark = get_spark()
df = spark.read.option("header", True).option("inferSchema", True).csv("/Volumes/raw-data/git/allfiles/BigMart Sales.csv")

df = df.withColumn(
    "salted_key",
    concat(df.Outlet_Type, lit("_"), (rand()*10).cast("int"))
)

df.groupBy("salted_key").count().show()