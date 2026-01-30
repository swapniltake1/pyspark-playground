from pyspark.sql.types import *

# Use built-in SparkSession in Databricks
# spark is already available

schema = StructType([
    StructField("Item_Identifier", StringType(), True),
    StructField("Item_Weight", DoubleType(), True),
    StructField("Item_Fat_Content", StringType(), True),
    StructField("Item_Visibility", DoubleType(), True),
    StructField("Item_Type", StringType(), True),
    StructField("Item_MRP", DoubleType(), True),
    StructField("Outlet_Identifier", StringType(), True),
    StructField("Outlet_Establishment_Year", IntegerType(), True),
    StructField("Outlet_Size", StringType(), True),
    StructField("Outlet_Location_Type", StringType(), True),
    StructField("Outlet_Type", StringType(), True),
    StructField("Item_Outlet_Sales", DoubleType(), True)
])

df = spark.read.csv(
    "/Volumes/raw-data/git/allfiles/BigMart Sales.csv",
    header=True,
    schema=schema
)

df.show(5)
