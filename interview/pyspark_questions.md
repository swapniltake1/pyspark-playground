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
