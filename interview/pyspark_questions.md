# PySpark Interview Questions and Answers

**1. Explain the difference between `map` and `flatMap` transformations in PySpark. Provide a code example demonstrating each.**

`map` applies a function to each input element and returns exactly one output element per input. `flatMap` applies a function that returns a sequence (list, iterator) for each input and then flattens the results into a single RDD.

```python
rdd = sc.parallelize([1, 2, 3])
# map returns a list for each element
print(rdd.map(lambda x: [x, x * 2]).collect())
# [[1, 2], [2, 4], [3, 6]]

# flatMap flattens the lists
print(rdd.flatMap(lambda x: [x, x * 2]).collect())
# [1, 2, 2, 4, 3, 6]
```

**2. How does PySpark manage data partitioning? Write a script that reads a CSV file and repartitions the DataFrame to 8 partitions.**

Spark breaks data into partitions across the cluster to parallelize processing. You can control partition count with `repartition` / `coalesce`.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PartitionExample").getOrCreate()
df = spark.read.csv("/path/to/file.csv", header=True, inferSchema=True)
# show current number of partitions
print(df.rdd.getNumPartitions())

# repartition to 8
df8 = df.repartition(8)
print(df8.rdd.getNumPartitions())
```

**3. What is the difference between `cache()` and `persist()`? When would you use each?**

`cache()` is a shorthand for `persist(StorageLevel.MEMORY_ONLY)`. `persist()` allows choosing storage levels (e.g., `MEMORY_AND_DISK`, `DISK_ONLY`). Use `cache()` when memory is sufficient; use `persist()` to handle larger datasets or control durability.

**4. Write a PySpark job to calculate the top 5 products by sales from a sales dataset.**

```python
from pyspark.sql import functions as F

sales = spark.read.parquet("s3://bucket/sales")
top5 = (
    sales.groupBy("product_id")
         .agg(F.sum("amount").alias("total_sales"))
         .orderBy(F.desc("total_sales"))
         .limit(5)
)
top5.show()
```

**5. Describe how Spark's Catalyst optimizer works and why it's important for performance.**

Catalyst converts user queries into an optimized logical plan, applies rule-based transformations (constant folding, predicate pushdown, projection pruning), and generates a physical plan that minimizes data shuffles and I/O. It is critical because it automates complex optimizations and improves job execution speed without manual tuning.

**6. Demonstrate using window functions in PySpark to compute a running total column.**

```python
from pyspark.sql import Window
from pyspark.sql import functions as F

w = Window.partitionBy("category").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)

df.withColumn("running_total", F.sum("amount").over(w)).show()
```

**7. How can you handle skewed data in joins? Provide an example using broadcast joins.**

For a small table joined with a large skewed table, broadcast the small one so that the join avoids shuffling the large dataset.

```python
small = spark.read.parquet("/path/to/small")
large = spark.read.parquet("/path/to/large")

result = large.join(F.broadcast(small), "key")
```

**8. Write a PySpark DataFrame transformation that standardizes column names to lowercase and replaces spaces with underscores.**

```python
def normalize_cols(df):
    new_cols = [c.lower().replace(" ", "_") for c in df.columns]
    return df.toDF(*new_cols)

normalized = normalize_cols(df)
```

**9. Explain the role of `SparkSession` and how you would configure it for local and cluster modes.**

`SparkSession` is the entry point for DataFrame and SQL operations. For local testing use `SparkSession.builder.master("local[*]")...`, and for cluster set `master` to `yarn`/`mesos`/`spark://host:port` along with relevant config options (e.g., `config("spark.executor.memory","4g")`).

**10. Write a function in PySpark that validates schema against a provided `StructType` and logs mismatches.**

```python
from pyspark.sql.types import StructType


def validate_schema(df, expected: StructType):
    actual = df.schema
    if actual != expected:
        missing = set(expected.fieldNames()) - set(actual.fieldNames())
        extra = set(actual.fieldNames()) - set(expected.fieldNames())
        print(f"Schema mismatch. Missing: {missing}, Extra: {extra}")
        return False
    return True
```
**11. What are the key differences between an RDD and a DataFrame in PySpark, and when might you choose one over the other?**

RDDs are the low-level abstraction representing an immutable distributed collection of objects. They provide fine-grained control, support arbitrary Python objects, and require manual optimization (e.g. using `map`, `filter`). DataFrames are a higher-level abstraction built on top of RDDs with a schema and optimized by Catalyst; they support SQL queries and are generally faster due to automatic optimizations. Use DataFrames for most analytics and ETL tasks; use RDDs when you need custom serialization or untyped transformations not supported by DataFrames.

**12. How do you handle corrupt or malformed records when reading JSON/CSV files?**

Use options like `mode` and `columnNameOfCorruptRecord` with `spark.read` to capture or skip bad rows. Example:

```python
 df = (spark.read.option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .json("/path/to/file.json"))
```

You can then filter or log rows where `_corrupt_record` is not null.

**13. Describe how to tune a Spark job's parallelism and resource configuration for production.**

Adjust `spark.sql.shuffle.partitions`, `spark.default.parallelism`, executor memory (`spark.executor.memory`), and cores (`spark.executor.cores`) based on data size and cluster capacity. Monitor with the UI and use dynamic allocation if available. Set `spark.serializer` to `KryoSerializer` and register custom classes to reduce serialization overhead.

**14. Provide an example of using a PySpark UDF and explain the performance implications.**

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

@udf(IntegerType())
def add_one(x):
    return x + 1

df.withColumn("val1", add_one("val")).show()
```

UDFs break Catalyst optimizations and run row-by-row in Python, causing serialization overhead. Prefer built-in functions or pandas UDFs (vectorized) when possible.

**15. What is checkpointing in Spark and when should it be used?**

Checkpointing writes RDD/DataFrame to durable storage (e.g. HDFS) to truncate the lineage graph. Use it when lineage becomes very long (e.g. after many transformations or while using iterative algorithms) to avoid stack overflow and recomputation. There are two types: RDD checkpoint (`rdd.checkpoint()`) and streaming checkpoint for structured streaming.

**16. How would you write unit tests for a PySpark transformation?**

Use `pyspark.sql.SparkSession.builder.master("local[1]")` in a `pytest` fixture, create small sample data, apply the transformation, and assert results with `collect()` or `toPandas()`. Example:

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()

def test_normalize_cols(spark):
    df = spark.createDataFrame([(1, "A")], ["ID", "Name"])
    result = normalize_cols(df).columns
    assert result == ["id", "name"]
```

**17. Explain how to handle late-arriving data in structured streaming using watermarks.**

Use `withWatermark` on the event-time column and specify a delay threshold. Spark will maintain state only for the watermark duration and drop older events.

```python
stream = (spark.readStream.format("kafka") ...
          .withWatermark("event_time", "1 hour")
          .groupBy("key", window("event_time", "10 minutes"))
          .count())
```

**18. Demonstrate joining multiple DataFrames and resolving column name conflicts.**

```python
df1 = spark.read.parquet(".../orders")
df2 = spark.read.parquet(".../customers")

joined = (df1.alias("o")
          .join(df2.alias("c"), on=F.col("o.cust_id") == F.col("c.id"))
          .select(F.col("o.*"), F.col("c.name").alias("cust_name")))
```

Use aliases and `alias()` to rename conflicting columns before selecting.

**19. What's the difference between `foreachPartition` and `mapPartitions`? When would you use each?**

`mapPartitions` applies a function to each partition and returns a new RDD/DataFrame; it's used when the transformation yields output. `foreachPartition` is for side effects only (e.g., writing to an external database) and returns no value. Use `foreachPartition` when you need to initialize a connection once per partition.

**20. How can you integrate Delta Lake or another transactional storage format in PySpark?**

Enable the Delta Lake package and write/read using the `delta` format:

```python
spark = (SparkSession.builder
         .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1")
         .getOrCreate())

(df.write.format("delta").mode("overwrite").save("/delta/table"))

df = spark.read.format("delta").load("/delta/table")
```

Delta provides ACID transactions, time travel, and schema enforcement.
