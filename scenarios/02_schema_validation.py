# Use built-in SparkSession in Databricks
# spark is already available

df = spark.read.option("header", True).option("inferSchema", True)    .csv("/Volumes/raw-data/git/allfiles/BigMart Sales.csv")

df.printSchema()
