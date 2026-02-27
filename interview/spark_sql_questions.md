# Spark SQL Interview Questions and Answers

1. **What is Spark SQL and how does it relate to DataFrames?**

   Spark SQL is a module for working with structured data using SQL queries. It provides a higher-level abstraction, DataFrames, which represent distributed tables. DataFrames are optimized using the Catalyst optimizer and can be queried with standard SQL syntax or the DataFrame API.

2. **How do you register a DataFrame as a temporary view and query it using SQL?**

   ```python
   df.createOrReplaceTempView("sales")
   spark.sql("SELECT region, SUM(amount) FROM sales GROUP BY region").show()
   ```

3. **Explain how to perform joins in Spark SQL. Provide an example with broadcast hint.**

   ```sql
   SELECT /*+ BROADCAST(small) */ l.*, s.name
   FROM large_table l
   JOIN small_table s ON l.key = s.key;
   ```

4. **What is the difference between `createOrReplaceTempView` and `createGlobalTempView`?**

   `TempView` is tied to a SparkSession, while `GlobalTempView` is tied to the Spark application and accessible across sessions via the `global_temp` database.

5. **How can you enable the Hive support in Spark and run Hive SQL?**

   ```python
   spark = SparkSession.builder \
           .appName("HiveExample") \
           .enableHiveSupport() \
           .getOrCreate()
   spark.sql("SHOW DATABASES").show()
   ```

6. **Describe how Spark SQL handles UDFs and how to register one.**

   ```python
   from pyspark.sql.functions import udf
   from pyspark.sql.types import IntegerType

   def add_one(x):
       return x + 1

   add_one_udf = udf(add_one, IntegerType())
   spark.udf.register("addOne", add_one_udf)
   df.selectExpr("addOne(value) as value2").show()
   ```

7. **What are DataFrame partitions and how can you inspect them in Spark SQL?**

   Partitions are logical chunks of a DataFrame. You can use `df.rdd.getNumPartitions()` or run `EXPLAIN` to view the physical plan and see partitioning details.

8. **Explain how Catalyst optimizer uses cost-based optimization.**

   When `spark.sql.cbo.enabled` is true and table statistics are available, Catalyst assigns costs to different physical plans and chooses the one with the lowest estimated cost, considering join order and data size.

9. **How do you save the result of a Spark SQL query to Parquet with partitioning?**

   ```python
   result = spark.sql("SELECT * FROM my_view")
   result.write.partitionBy("year", "month").parquet("/path/output")
   ```

10. **How would you troubleshoot a slow Spark SQL query?**

    - Check the execution plan using `EXPLAIN` or the UI.
    - Ensure filters are pushed down and unnecessary columns are pruned.
    - Examine skews and repartition if needed.
    - Verify statistics are up to date and consider broadcasting small tables.
