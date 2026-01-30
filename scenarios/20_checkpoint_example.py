from common.spark_session import get_spark

spark = get_spark()
spark.sparkContext.setCheckpointDir("/tmp/spark_checkpoints")

df = spark.read.option("header", True).option("inferSchema", True).csv("/Volumes/raw-data/git/allfiles/BigMart Sales.csv")

df.checkpoint().count()