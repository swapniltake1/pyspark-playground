from pyspark.sql.functions import col, count
###from common.spark_session import get_spark

###spark = get_spark()

df = spark.read.option("header", True).option("inferSchema", True)    .csv("/Volumes/raw-data/git/allfiles/BigMart Sales.csv")

df.select([count(col(c)).alias(c) for c in df.columns]).show()