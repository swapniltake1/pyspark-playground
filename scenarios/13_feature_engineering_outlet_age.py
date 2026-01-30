from pyspark.sql.functions import lit
from common.spark_session import get_spark

spark = get_spark()
df = spark.read.option("header", True).option("inferSchema", True).csv("/Volumes/raw-data/git/allfiles/BigMart Sales.csv")

df.withColumn("Outlet_Age", lit(2025) - df.Outlet_Establishment_Year).show(5)