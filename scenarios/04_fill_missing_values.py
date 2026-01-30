from pyspark.sql.functions import avg
from common.spark_session import get_spark

###spark = get_spark()

df = spark.read.option("header", True).option("inferSchema", True)    .csv("/Volumes/raw-data/git/allfiles/BigMart Sales.csv")

avg_weight = df.select(avg("Item_Weight")).first()[0]
df.fillna({"Item_Weight": avg_weight}).show(5)