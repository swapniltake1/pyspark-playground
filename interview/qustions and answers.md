# Azure Data Engineer Interview Questions and Answers

## 1. INTRODUCTION

1. Tell me about yourself
- Answer: Data Engineer with 2+ years BFSI experience; ETL/ELT with ADF + Databricks + PySpark + SQL; CDC pipelines; performance tuning.

## 2. PROJECT / PIPELINE

2. Explain your end-to-end ETL pipeline
- Ingest from SQL/REST/SFTP/Blob → ADLS Gen2 bronze via ADF → Databricks transformations (joins, aggregations, schema handling) → silver/gold layers → reporting.

3. What data are you processing?
- BFSI transaction and customer data; 50+ GB daily; financial transactions, account details.

4. What are your upstream data sources?
- Azure SQL/SQL Server, REST APIs, CSV/JSON via SFTP, Azure Blob.

5. What is the largest data size you handled?
- ~250-300 GB files; use ADF parallel copy, high DIU, partitioning, Databricks distributed compute.

6. How do you load a 250 GB file?
- ADF Copy Activity with parallelism, increased DIU, optional staging, file chunking, Parquet.

7. How did you implement CDC?
- Use watermark/change columns, audit columns, delta extraction, Delta Lake MERGE for upsert.

Example PySpark Delta MERGE:
```python
from delta.tables import DeltaTable

source_df = spark.read.format('parquet').load('/mnt/raw/delta_incremental')

delta_table = DeltaTable.forPath(spark, '/mnt/delta/target')

(delta_table.alias('t')
 .merge(source_df.alias('s'), 't.id = s.id')
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())
```

8. What is incremental loading and how do you implement it?
- Only process new/updated rows using watermark (e.g. LastModifiedDate), save last load timestamp in control table, query source with `WHERE modified > @watermark`, and apply Delta MERGE.



9. Challenges you faced?
- Large volume performance; solved with partitioning, caching, incremental loads, dynamic schema evolution.

## 3. SPARK / PYSPARK

10. What is Spark SQL?
- Spark module for structured data using SQL and DataFrames; Catalyst optimizer.

11. Why is Spark SQL optimization important?
- Reduces execution time & resources.

12. What is the Catalyst Optimizer?
- Query optimizer that converts logical plans to physical plans with rule-based and cost-based transforms.

13. What is the Tungsten engine?
- Low-level memory and CPU efficiency via code generation.

14. Logical vs Physical plan
- Logical: what to do; Physical: how to do it.

15. How to see execution plan?
- `df.explain(True)`

16. What is predicate pushdown?
- Pushes filter to data source.

17. How does predicate pushdown help?
- Reduces I/O.

18. What is column pruning?
- Select only needed columns.

19. Why avoid SELECT *?
- Extra I/O, slowness.

20. What is partition pruning?
- Read only needed partitions.

21. How partition pruning works?
- Spark uses partition filters to skip partitions.

22. Partition pruning vs ZORDER
- Pruning reduces partitions; ZORDER sorts data inside partitions for locality.

23. What is shuffle?
- Data movement during joins/aggregations across executors.

24. Why shuffle is expensive?
- Disk I/O, network, sorting.

25. What is broadcast join?
- Small table broadcast to executors to avoid shuffle.

26. When to use broadcast join?
- When one table is small enough.

27. If table is too large to broadcast?
- Spark uses shuffle join (sort-merge).

28. How Spark decides broadcast?
- `spark.sql.autoBroadcastJoinThreshold`.

29. How to force broadcast?
- `from pyspark.sql.functions import broadcast` and `df1.join(broadcast(df2), 'key')`.

30. What is AQE?
- Adaptive Query Execution adjusts plan at runtime.

31. Problems AQE solves
- Data skew, join optimization, partition tuning.

32. How AQE optimizes joins?
- Converts sort-merge to broadcast if small etc; reduces skew.

33. What is skew join optimization?
- Salting or splitting skew keys.

34. How to enable AQE?
- `spark.conf.set('spark.sql.adaptive.enabled','true')`.

35. What is caching?
- Persist data in memory.

36. When to cache?
- Reused dataset across multiple actions.

37. Cache large dataset issue?
- Memory spills, OOM.

38. Cache vs Persist
- `cache()` uses MEMORY_ONLY; `persist(StorageLevel.MEMORY_AND_DISK)` more options.

39. OPTIMIZE and ZORDER
- Delta OPTIMIZE compacts files; ZORDER improves data clustering.

40. Transformation vs Action
- Transformation lazy; action triggers execution.

41. What happens when Spark job runs?
- DAG built, stages, tasks, executors process tasks.

## Spark architecture and core concepts

42. Spark Architecture
- Driver, cluster manager, workers, executors, DAG → stages → tasks.

43. What is DAG?
- Directed acyclic graph of operations.

44. What is data skew?
- Uneven key distribution causing slow tasks.

45. How to handle data skew?
- Repartition, salt keys, filter skew keys separately.

46. How to optimize Spark jobs?
- Partitioning, caching, broadcast joins, avoid shuffle, efficient formats.

47. RDD vs DataFrame
- RDD low level; DataFrame optimized with Catalyst.

48. What is shuffling?
- Redistributing data across executors.

49. What is AQE? (repeat) 
- Adaptive Query Execution  runtime plan adjustments.

## Advanced Spark

50. How do you handle large joins in PySpark?
- Use broadcast for small side, shuffle for big joins, pre-partition by join key.

51. Broadcast and Accumulator
- Broadcast: read-only shared; Accumulator: counters/metrics.

52. How do you debug a failed Spark job?
- Spark UI logs, failed stage details, memory, data quality.

53. OutOfMemory error handling
- Reduce data per partition, increase executor memory, avoid wide shuffle.

54. Why job works in Dev but fails in Prod?
- Data volume, config/resource differences, data quality, permissions.

## SQL

55. Latest record per customer example (row_number):
```sql
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
  FROM orders
) t
WHERE rn = 1;
```

56. ROW_NUMBER vs RANK vs DENSE_RANK
- ROW_NUMBER: unique sequential.
- RANK: gaps for ties.
- DENSE_RANK: no gaps.

57. SQL optimization
- indexes, avoid SELECT *, filter early, use explain plan.

58. Cumulative distance (window):
```sql
SELECT customer_id, order_date, distance,
  SUM(distance) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_distance
FROM trips;
```

59. Average gap between purchases:
```sql
SELECT customer_id,
  AVG(DATEDIFF(day, prev_purchase, purchase_date)) AS avg_gap_days
FROM (
  SELECT *, LAG(purchase_date) OVER (PARTITION BY customer_id ORDER BY purchase_date) AS prev_purchase
  FROM purchases
) x
WHERE prev_purchase IS NOT NULL
GROUP BY customer_id;
```

60. First purchase handling
- LAG returns null for first row; filter nulls.

61. Latest order + total spend:
```sql
SELECT customer_id, order_id, total_spend
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
  FROM orders
) t
WHERE rn=1;
```

## 6. AZURE / ADF

61. Role of ADF
- Orchestration, ETL/ELT control.

62. Components of ADF
- Pipeline, activity, dataset, linked service, trigger.

63. Data Flow activity
- In-UI transformation (Spark engine).

64. Data Flow vs Databricks
- Data Flow for simpler ETL; Databricks for advanced code and scale.

65. Triggers in ADF
- schedule, tumbling window, event.

66. Set Variable activity
- Assign pipeline values dynamically.

67. Copy multiple files
- wildcard path or ForEach + dataset.

68. Databricks + ADLS integration
- mount ADLS or use service principal with ABFS.

## 7. CORE CONCEPTS

69. ETL vs ELT
- ETL: transform before load; ELT: load then transform.

70. Data Modeling
- Facts (metrics), dimensions (attributes).

71. What is Delta Lake?
- ACID, schema enforcement, time travel.

72. ACID properties
- Atomicity, consistency, isolation, durability.

73. Schema evolution
- Allow schema changes; use `mergeSchema` in Spark.

74. What is SCD?
- Type 1/2/3 change tracking.

75. Data lake vs warehouse
- Lake raw; warehouse structured high-performance.

## 8. SCENARIO

76. Pipeline slow
- check partitioning, shuffle, join strategy, data skew, resource configs.

77. Data mismatch
- row count checks, aggregates, record-level validation.

78. Corrupted data handling
- set badRecordsPath, store bad files.

## 9. PYTHON

79. Palindrome
```python
def is_palindrome(s):
    s = ''.join(ch.lower() for ch in s if ch.isalnum())
    return s == s[::-1]
``` 

80. Remove duplicates
```python
seen = set()
result = []
for num in numbers:
    if num not in seen:
        result.append(num)
        seen.add(num)
``` 

81. Read CSV in Spark
```python
df = spark.read.format('csv').option('header', 'true').load('path')
``` 

82. Pass parameters in Databricks
```python
filename = dbutils.widgets.get('filename')
``` 

## 10. AI

83. What is RAG?
- Retrieval-Augmented Generation: fetch context then generate with LLM.

84. Vector DB
- Stores embeddings; similarity search for semantic retrieval.
