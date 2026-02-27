# Data Engineering Interview Questions and Answers

**1. Describe the ETL process and outline how you would design an ETL pipeline for ingesting JSON logs into a data lake.**

ETL stands for Extract, Transform, Load. You extract data from sources, apply transformations (cleansing, normalization, enrichment), and load into a target system. For JSON logs:

1. **Extract**: use a scheduler or stream consumer to pull files from S3/FTP or subscribe to Kafka topics.
2. **Transform**: parse JSON, filter invalid records, standardize timestamps, add metadata.
3. **Load**: write to a partitioned Parquet store in the data lake (e.g. `s3://data/lake/logs/year=2026/month=02/`).

A PySpark job example:

```python
logs = spark.read.json("s3://incoming/logs/*.json")
clean = (logs.filter("status IS NOT NULL")
            .withColumn("event_time", F.to_timestamp("ts"))
            .withColumn("year", F.year("event_time"))
            .withColumn("month", F.month("event_time")))

clean.write.partitionBy("year","month").mode("append").parquet("s3://data/lake/logs/")
```

**2. What are the key differences between batch processing and stream processing? Give examples of tools used for each.**

- **Batch**: processes finite datasets at intervals. Examples: Apache Spark, Hadoop MapReduce.
- **Stream**: processes data continuously as it arrives, often with low latency. Examples: Apache Kafka Streams, Apache Flink, Structured Streaming in Spark.

Batch jobs can tolerate higher latency but are simpler; streams require state management and event-time handling.

**3. Explain the concept of data partitioning and bucketing in Hive or Spark and why they are useful.**

- **Partitioning**: physically divides a table/dataset by column value (e.g., date). It enables predicate pushdown and reduces I/O by scanning only relevant partitions.
- **Bucketing**: hashes a column into a fixed number of buckets. Useful for efficient joins and sampling because matching bucket numbers avoids full shuffle.

Together they improve query performance and manageability of large datasets.

**4. How would you ensure data quality and schema evolution when working with Parquet files in a data lake?**

- Use schema validation step in ETL, e.g., compare incoming schema against expected `StructType` and reject/fix mismatches.
- Employ tools like Apache Avro or Delta Lake/Apache Iceberg which support schema evolution and versioning.
- Use a metadata catalog (Hive Metastore, Glue) to track current schemas and update partitions.
- Implement checksums, row counts, null ratio metrics, and log anomalies.

**5. Write a sample SQL query to deduplicate records in a table keeping the latest record based on a timestamp column.**

```sql
WITH ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
  FROM raw_table
)
SELECT *
FROM ranked
WHERE rn = 1;
```

**6. What is a slowly changing dimension (SCD)? Describe the types and how you would implement SCD Type 2 in a data warehouse.**

A Slowly Changing Dimension is a dimension that changes over time. Types:
- **Type 1**: overwrite old value.
- **Type 2**: keep history by inserting new rows with effective date/expiry date.
- **Type 3**: add new column for previous value.

For Type 2, maintain `start_date`, `end_date`, and a current flag. On update, set `end_date` of existing row and insert new row with updated attributes and `start_date=now`.

**7. Discuss the advantages of using Apache Kafka in a data pipeline. Provide a small producer/consumer example in Python.**

Kafka provides durable, scalable, and ordered message streams; decouples producers/consumers; supports replayability. It's ideal for real‑time ingestion and buffering.

```python
from kafka import KafkaProducer, KafkaConsumer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('logs', b'first message')
producer.flush()

consumer = KafkaConsumer('logs', bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest',
                         group_id='my-group')
for msg in consumer:
    print(msg.value)
    break
```

**8. How can you monitor and alert on failures in a data pipeline? Mention tools or frameworks that assist with this.**

- Use workflow schedulers like Apache Airflow or AWS Step Functions with built‑in retry/alerting.
- Integrate logging systems (ELK, Splunk) and metrics (Prometheus, Grafana) to track job success, runtime, error rates.
- Set up notifications (Slack, email) on DAG/task failures.
- Use data observability platforms (Monte Carlo, Great Expectations) for data quality alerts.

**9. Explain the role of a metadata catalog (e.g., AWS Glue or Hive Metastore) and why it is important.**

A metadata catalog stores table definitions, schemas, partition information, and location pointers. It allows query engines to discover data structure without scanning files, enables schema enforcement, and supports interoperability between tools (Spark, Presto, Athena).

**10. What strategies can you employ to optimize query performance on big datasets in a distributed system?**

- Partition and bucket data properly.
- Use columnar formats (Parquet, ORC) with compression.
- Avoid wide transformations; filter early (predicate pushdown).
- Broadcast small tables in joins.
- Cache frequently accessed data.
- Use cost-based optimizer hints, tune shuffle partitions (`spark.sql.shuffle.partitions`).
- Maintain statistics and use indexes where supported.

