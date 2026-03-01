# Data Engineering Interview Questions & Answers - 2 Years Experience Level

## Scenario-Based Technical Interview Questions

---

## Scenario 1: Data Pipeline Bottleneck Analysis

### Question
You've built a PySpark data pipeline that processes 500GB of daily transaction data. The pipeline was running fine for 3 months, but now it's failing intermittently with out-of-memory errors. The job was not modified recently, but data volume increased. What are the potential causes and how would you debug this?

### Answer

**Root Cause Analysis Approach:**

1. **Monitor Resource Utilization**
   - Check executor memory distribution using Spark UI
   - Verify if data skew is present (some partitions much larger than others)
   - Check shuffle spike and GC (garbage collection) logs

2. **Data Skew Investigation**
   ```python
   # Check partition sizes
   df.rdd.mapPartitions(lambda x: [sum(1 for _ in x)]).collect()
   
   # Identify skewed keys
   df.groupBy('key').count().orderBy(desc('count')).show(10)
   ```

3. **Potential Causes:**
   - **Data Skew**: Some keys have dramatically more records → use salt technique
   - **Increased Data Volume**: More data in same partitions → increase partition count
   - **Memory Leaks**: Accumulator or broadcast variables growing
   - **Unoptimized Joins**: Creating large intermediate datasets
   - **Cache Inefficiency**: Previously cached data not being cleared

4. **Solutions:**
   ```python
   # Solution 1: Repartition data
   df_repartitioned = df.repartition(500)  # or more partitions
   
   # Solution 2: Handle data skew with salting
   from pyspark.sql.functions import rand, concat, lit
   df_salted = df.withColumn('salt', (rand() * 10).cast('int')) \
       .withColumn('skewed_key', concat('key', lit('_'), col('salt')))
   
   # Solution 3: Adjust Spark configuration
   spark.conf.set("spark.sql.adaptive.enabled", "true")
   spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
   ```

5. **Prevention Strategies:**
   - Implement Spark UI monitoring alerts
   - Use explain() to review execution plans
   - Profile with smaller data samples before full scale

---

## Scenario 2: Handling Late-Arriving Data

### Question
Your company ingests streaming data from 50+ microservices. You discovered that some services are sending data 2-3 hours late due to retry logic. This is breaking your real-time reporting dashboard which depends on hourly rollups. How would you design a solution?

### Answer

**Design Approach:**

1. **Watermarking Strategy**
   ```python
   from pyspark.sql.functions import window, col, max, current_timestamp
   
   # Define watermark to handle late data (2-3 hours allowance)
   df_with_watermark = df_events \
       .withWatermark("event_time", "3 hours") \
       .groupBy(window(col("event_time"), "1 hour")) \
       .agg(
           sum("amount").alias("total_amount"),
           count("*").alias("event_count")
       )
   ```

2. **Event Time vs Processing Time**
   - Use `event_time` (when event actually occurred) for business logic
   - Use `processing_time` (when system received it) for SLA tracking
   ```python
   df_with_times = df.withColumn("processing_time", current_timestamp()) \
       .withColumn("lateness_minutes", 
                   (col("processing_time").cast("long") - 
                    col("event_time").cast("long")) / 60)
   ```

3. **Storage Strategy**
   - Store raw incoming data separately (immutable)
   - Maintain "versioned" aggregates
   ```python
   # Append-only approach
   df_aggregates.write \
       .format("parquet") \
       .partitionBy("hour", "version") \
       .mode("append") \
       .save("s3://bucket/hourly_agg")
   ```

4. **Dashboard Handling**
   - Show "As of" timestamp on dashboard
   - Display two metrics: preliminary and final counts
   - Implement automated re-aggregation process
   ```python
   # Re-aggregation job (run every hour)
   late_arriving = df.filter(col("lateness_minutes") > 60)
   if late_arriving.count() > 0:
       trigger_re_aggregation()
   ```

5. **Monitoring**
   - Track percentage of late data
   - Alert if late data exceeds threshold
   - Log service-wise latency metrics

---

## Scenario 3: Schema Evolution in Production

### Question
You have a production data pipeline consuming Kafka messages with a strict schema. A backend team deployed changes that added 8 new optional fields to the data they send. Your pipeline is crashing because of schema mismatch. What's your approach to handle this gracefully?

### Answer

**Schema Evolution Strategy:**

1. **Immediate Fix - Make Schema Flexible**
   ```python
   from pyspark.sql.types import StructType, StructField, StringType
   
   # Use inferSchema=false with permissive mode
   schema = StructType([
       StructField("id", StringType(), True),
       StructField("name", StringType(), True),
       # Old fields...
   ])
   
   df = spark.read \
       .option("mode", "PROTOBUF") \
       .option("allowMissingColumns", "true") \
       .schema(schema) \
       .load("kafka_topic")
   ```

2. **Forward Compatibility - Accept Unknown Fields**
   ```python
   # Read full JSON and select only known columns
   df_raw = spark.readStream.format("kafka").load()
   df_parsed = df_raw.select(
       from_json(col("value"), schema).alias("data")
   ).select("data.*")
   
   # Add new fields with defaults if missing
   try:
       actual_schema = df_parsed.schema
       for new_field in ["field1", "field2", ...]:
           if new_field not in actual_schema.names:
               df_parsed = df_parsed.withColumn(new_field, lit(None))
   except:
       pass  # Field already exists
   ```

3. **Schema Registry Approach**
   ```python
   # Use Confluent Schema Registry or AWS Glue Schema Registry
   df = spark.readStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", servers) \
       .load() \
       .select(from_avro(col("value"), schemaId).alias("data")) \
       .select("data.*")
   ```

4. **Implement Schema Validation**
   ```python
   def validate_schema(df, required_fields):
       missing = [f for f in required_fields if f not in df.columns]
       if missing:
           raise ValueError(f"Missing required fields: {missing}")
       return df
   
   df = validate_schema(df, ["id", "name"])
   ```

5. **Documentation & Versioning**
   - Maintain schema changelog
   - Version endpoints
   - Coordinate with data producers
   - Add backward compatibility tests

---

## Scenario 4: Cost Optimization in Cloud Data Pipeline

### Question
Your company runs daily PySpark jobs on AWS EMR that cost $2,000/day. You need to reduce costs by 40% without significantly impacting SLA (98% of jobs complete within 6 hours). What optimization strategies would you implement?

### Answer

**Cost Optimization Strategy:**

1. **Cluster Right-Sizing**
   ```python
   # Analyze current usage
   # Before: m5.4xlarge (16 core) x 20 nodes = $2000/day
   # Approach: Use spot instances + reserved instances
   
   # Mix: 5 reserved + 10 spot instances
   # Estimated: $1200/day (40% reduction)
   ```

2. **Job Performance Optimization**
   ```python
   # Enable AQE (Adaptive Query Execution)
   spark.conf.set("spark.sql.adaptive.enabled", "true")
   spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
   spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
   
   # Optimize partitions
   df.repartition(optimal_partitions).write.parquet("path")
   ```

3. **Storage Optimization**
   ```python
   # Use columnar format with compression
   df.write \
       .format("parquet") \
       .option("compression", "snappy") \
       .mode("overwrite") \
       .save(output_path)
   
   # Partition strategically
   .partitionBy("year", "month", "day")
   ```

4. **Selective Processing**
   ```python
   # Process only changed data (incremental)
   last_run_date = get_last_run_date()
   df = spark.read.parquet(input_path) \
       .filter(col("created_date") > last_run_date)
   ```

5. **Autoscaling Configuration**
   ```python
   # EMR Autoscaling Policy
   - Scale up if: Yarn memory utilization > 80% for 2 min
   - Scale down if: Yarn memory utilization < 30% for 5 min
   - Max nodes: 20, Min nodes: 5
   ```

6. **Job Scheduling**
   ```python
   # Run heavy jobs during off-peak hours
   # Batch compatible jobs together
   # Use step executor mode for sequential jobs
   ```

**Expected Savings Breakdown:**
- Spot instances: 20% savings
- Performance optimization: 10% (less runtime)
- Better partitioning: 8% (reduced scanning)
- Incremental processing: 2% (less data processed)

---

## Scenario 5: Data Quality Framework Implementation

### Question
You inherited a legacy data pipeline that frequently produces incorrect aggregates due to duplicate records, missing values, and incorrect data types. There's no quality checks currently. Design a data quality framework that catches 95% of issues before data reaches downstream consumers.

### Answer

**Data Quality Framework Design:**

1. **Multi-Layer Validation**
   ```python
   from pyspark.sql.functions import col, isnan, isnull, when, count, lit
   
   class DataQualityValidator:
       def __init__(self, df, stage_name):
           self.df = df
           self.stage = stage_name
           self.issues = []
       
       def check_schema(self, expected_schema):
           """Validate schema matches expected"""
           actual_cols = set(self.df.columns)
           expected_cols = set(expected_schema.keys())
           
           if actual_cols != expected_cols:
               self.issues.append(f"Schema mismatch: {actual_cols ^ expected_cols}")
           return self
       
       def check_nulls(self, required_fields):
           """Check for nulls in critical columns"""
           for field in required_fields:
               null_count = self.df.filter(col(field).isNull()).count()
               if null_count > 0:
                   self.issues.append(f"{field}: {null_count} nulls found")
           return self
       
       def check_duplicates(self, key_columns):
           """Detect duplicate records"""
           dups = self.df.groupBy(key_columns).count() \
               .filter(col("count") > 1).count()
           if dups > 0:
               self.issues.append(f"Found {dups} duplicate key combinations")
           return self
       
       def check_value_ranges(self, column, min_val, max_val):
           """Validate numeric ranges"""
           out_of_range = self.df.filter(
               (col(column) < min_val) | (col(column) > max_val)
           ).count()
           if out_of_range > 0:
               self.issues.append(
                   f"{column}: {out_of_range} values outside [{min_val}, {max_val}]"
               )
           return self
       
       def check_format(self, column, pattern):
           """Validate string patterns (regex)"""
           from pyspark.sql.functions import regexp_extract
           invalid = self.df.filter(
               regexp_extract(col(column), pattern, 0) == ""
           ).count()
           if invalid > 0:
               self.issues.append(f"{column}: {invalid} values don't match pattern")
           return self
   ```

2. **Quality Metrics Collection**
   ```python
   def calculate_quality_metrics(df, stage_name):
       metrics = {
           "stage": stage_name,
           "record_count": df.count(),
           "null_percentages": {},
           "duplicate_rows": df.distinct().count(),
           "processed_at": current_timestamp()
       }
       
       for col_name in df.columns:
           null_pct = df.filter(col(col_name).isNull()).count() / metrics["record_count"]
           metrics["null_percentages"][col_name] = null_pct
       
       return metrics
   ```

3. **Automated Remediation**
   ```python
   def apply_quality_rules(df):
       # Remove duplicates keeping first occurrence
       df = df.dropDuplicates(["id"])
       
       # Fill missing values intelligently
       df = df.fillna({
           "amount": 0,
           "description": "Unknown",
           "status": "pending"
       })
       
       # Remove rows with critical nulls
       df = df.filter(col("user_id").isNotNull())
       
       # Fix data types
       df = df.withColumn("created_date", col("created_date").cast("timestamp"))
       
       return df
   ```

4. **Quarantine Pattern - Failed Records**
   ```python
   def process_with_quarantine(df, validator_fn):
       good_records = df
       bad_records = spark.createDataFrame([], df.schema)
       
       for record in df.collect():
           try:
               validator_fn(record)
           except:
               bad_records = bad_records.union(spark.createDataFrame([record]))
       
       # Write bad records for investigation
       bad_records.write.parquet("s3://bucket/quarantine/")
       
       return good_records
   ```

5. **Quality Gate in Pipeline**
   ```python
   # Sample validation
   validator = DataQualityValidator(df, "staging")
   (validator
       .check_schema(expected_schema)
       .check_nulls(["id", "amount"])
       .check_duplicates(["id"])
       .check_value_ranges("amount", 0, 1000000)
       .check_format("email", r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
   )
   
   if validator.issues:
       log_quality_issues(validator.issues)
       send_alert_to_data_team()
       # Option: Stop pipeline or quarantine
       raise DataQualityException(validator.issues)
   ```

6. **Monitoring Dashboard Metrics**
   - Records processed vs expected
   - Null/duplicate percentages by field
   - Failed validation count
   - Quarantine records count
   - Data freshness (hours since last update)

---

## Scenario 6: Designing a Data Lake Partition Strategy

### Question
Your data lake stores 20 different datasets ranging from 100GB to 50TB. Different teams query with different patterns: some filter by date, others by geography, and some by product category. Design an optimal partitioning strategy that balances query performance and storage efficiency.

### Answer

**Partition Strategy Design:**

1. **Analyze Query Patterns**
   ```python
   # Interview approach:
   - Identify top 5 filter conditions per dataset
   - Understand query frequency
   - Check SLA requirements
   
   Example:
   Dataset A (events): filtered by date (95%), geography (80%), user_id (40%)
   Dataset B (products): filtered by category (90%), region (70%)
   ```

2. **Partitioning Scheme** (Right to Left: Most frequent -> Least frequent)
   ```python
   # Poor partitioning (deep nesting, leads to small files)
   s3://datalake/events/year=2025/month=03/day=01/hour=12/
   
   # Better - matches query patterns
   s3://datalake/dataset=events/region=NA/event_date=2025-03-01/
   
   # Or by frequency
   s3://datalake/dataset=events/
       /date=2025-03-01/
       /region=NA/
       /category=purchase/
   ```

3. **Implementation**
   ```python
   def write_with_optimal_partitioning(df, dataset_name):
       if dataset_name == "events":
           df.repartition(col("date"), col("region")) \
               .write \
               .partitionBy("date", "region") \
               .mode("append") \
               .parquet("s3://lake/events/")
       
       elif dataset_name == "products":
           df.repartition(col("category"), col("region")) \
               .write \
               .partitionBy("category", "region") \
               .mode("append") \
               .parquet("s3://lake/products/")
   ```

4. **Handle Small File Problem**
   ```python
   # Issue: Too many partitions = too many small files
   # Solution: Coalesce or repartition before write
   
   def smart_write(df, partition_cols, output_path):
       # Calculate optimal partitions
       record_count = df.count()
       target_file_size_mb = 128
       cols_count = len(partition_cols)
       
       optimal_partitions = max(
           cols_count * 2,  # At least 2 files per partition combo
           int(record_count * 100 / (target_file_size_mb * 1024 * 1024))
       )
       
       df.repartition(optimal_partitions, *partition_cols) \
           .write \
           .partitionBy(*partition_cols) \
           .parquet(output_path)
   ```

5. **Partition Pruning Verification**
   ```python
   # Show which partitions are scanned
   df = spark.read.parquet("s3://lake/events/")
   df_filtered = df.filter((col("date") == "2025-03-01") & 
                           (col("region") == "NA"))
   
   df_filtered.explain(extended=True)
   # Should show: PushedFilters: [EqualTo(date, ...), EqualTo(region, ...)]
   ```

6. **Storage Efficiency Tips**
   - Avoid partitioning low-cardinality columns (boolean, status)
   - Use bucketing for high-cardinality (user_id)
   - Compact small partitions periodically
   - Archive old partitions to cheaper storage

---

## Scenario 7: Handling Data Consistency in Distributed Processing

### Question
Your team is building an ETL pipeline that updates a master customer dimension table daily. The pipeline runs in parallel with real-time queries happening on the same table. You need to ensure that queries either see old data or new data - never partial/incomplete data. How do you guarantee this?

### Answer

**Data Consistency Approach:**

1. **Immutable Table Pattern**
   ```python
   # Instead of UPDATE, create new version
   # Step 1: Process fresh data to new day's partition
   df_fresh = process_data()
   df_fresh.write \
       .partitionBy("load_date") \
       .mode("append") \
       .parquet("s3://lake/customer_master/")
   
   # Step 2: Update metadata pointer atomically
   update_latest_version_metadata("2025-03-01")
   
   # Step 3: Queries read from latest pointer
   latest_date = get_latest_metadata()
   df = spark.read.parquet(f"s3://lake/customer_master/load_date={latest_date}/")
   ```

2. **ACID Transactions with ACID Table Format**
   ```python
   # Using Delta Lake (ACID guarantees)
   df_fresh = process_data()
   
   df_fresh.write \
       .format("delta") \
       .mode("overwrite") \
       .option("mergeSchema", "true") \
       .save("s3://lake/customer_master")
   
   # Delta ensures ACID properties:
   # - Atomicity: All-or-nothing write
   # - Consistency: No partial reads
   # - Isolation: Readers see complete snapshot
   # - Durability: Data persisted
   ```

3. **Two-Phase Commit Pattern**
   ```python
   def atomic_update(df, table_path):
       import uuid
       
       # Phase 1: Write to temp location
       temp_path = f"{table_path}/.temp/{uuid.uuid4()}"
       df.write.parquet(temp_path)
       
       # Phase 2: Atomic rename
       # This is atomic at filesystem level
       os.rename(temp_path, f"{table_path}/data")
       
       # Update metadata to point to new data
       metadata = {"path": f"{table_path}/data", "timestamp": now()}
       write_atomic_metadata(metadata)
   ```

4. **Snapshot Isolation with View**
   ```python
   # Create versioned snapshots
   df_processed = process_data()
   df_processed.write \
       .mode("append") \
       .parquet("s3://lake/customer_master_v2/")
   
   # Create view that always reads latest
   spark.sql("""
   CREATE VIEW customer_master_latest AS
   SELECT * FROM parquet.`s3://lake/customer_master_v2/`
   WHERE load_date = (SELECT MAX(load_date) 
                      FROM parquet.`s3://lake/customer_master_v2/`)
   """)
   
   # Queries now read from view
   df = spark.sql("SELECT * FROM customer_master_latest")
   ```

5. **Transactional Consistency Check**
   ```python
   def validate_consistency(old_df, new_df):
       # Check not null constraint
       assert new_df.filter(col("customer_id").isNull()).count() == 0
       
       # Check no data loss for existing customers
       old_ids = set(old_df.select("customer_id").rdd.flatMap(lambda x: x).collect())
       new_ids = set(new_df.select("customer_id").rdd.flatMap(lambda x: x).collect())
       assert old_ids.issubset(new_ids), "Data loss detected"
       
       return True
   ```

---

## Scenario 8: Debugging and Monitoring Data Pipeline

### Question
A data pipeline you deployed is running successfully but producing incorrect results that users only discover hours later. The aggregates are off by ~5%. The data looks correct at intermediate stages. How would you systematically debug and monitor this?

### Answer

**Debugging Strategy:**

1. **Data Lineage Tracking**
   ```python
   # Add timestamp and source tracking at each stage
   from pyspark.sql.functions import current_timestamp, lit
   
   df = spark.read.parquet("input/")
   df = df.withColumn("_source_table", lit("raw_input")) \
           .withColumn("_processed_at_stage1", current_timestamp())
   
   # After each transformation
   df = df.filter(col("amount") > 0)
   df = df.withColumn("_processed_at_stage2", current_timestamp()) \
           .withColumn("_row_count_stage2", lit(df.count()))
   ```

2. **Intermediate Data Sampling**
   ```python
   # Save sample data at each stage for inspection
   def save_sample(df, stage_name):
       df.sample(0.001) \  # 0.1% sample
           .coalesce(1) \
           .write.mode("overwrite") \
           .parquet(f"debug_samples/{stage_name}/")
       
       logging.info(f"Saved {df.count()} records to debug")
   
   df_input = spark.read.parquet("input/")
   save_sample(df_input, "stage_1_input")
   
   df_filtered = df_input.filter(col("amount") > 0)
   save_sample(df_filtered, "stage_2_filter")
   ```

3. **Reconciliation Queries**
   ```python
   # Compare expected vs actual counts
   def reconcile(expected_df, actual_df, key_columns):
       expected_counts = expected_df.groupBy(key_columns) \
           .count().withColumnRenamed("count", "expected_count")
       
       actual_counts = actual_df.groupBy(key_columns) \
           .count().withColumnRenamed("count", "actual_count")
       
       reconciliation = expected_counts.join(actual_counts, 
                                              on=key_columns, 
                                              how="outer").fillna(0)
       
       mismatches = reconciliation.filter(
           col("expected_count") != col("actual_count")
       )
       
       return mismatches.show()
   ```

4. **Aggregate Sanity Checks**
   ```python
   # Known good baseline
   BASELINE_DAILY_REVENUE = 1000000  # Expected daily revenue
   VARIANCE_THRESHOLD = 0.05  # 5% allowable variance
   
   def validate_aggregates(df):
       actual_revenue = df.agg(sum("amount")).collect()[0][0]
       variance = abs(actual_revenue - BASELINE_DAILY_REVENUE) / BASELINE_DAILY_REVENUE
       
       if variance > VARIANCE_THRESHOLD:
           raise AnomalyDetectedException(
               f"Revenue anomaly: {actual_revenue} vs baseline {BASELINE_DAILY_REVENUE}"
           )
       return df
   ```

5. **Execution Plan Analysis**
   ```python
   # Check if optimization is causing issues
   df = spark.read.parquet("input/")
   df_agg = df.groupBy("category").agg(sum("amount")).collect()
   
   # See if query is optimized correctly
   df_agg_df.explain(extended=True)
   
   # Check for unexpected shuffles
   df_agg_df.explain(mode="cost")
   ```

6. **Logging Strategy**
   ```python
   import logging
   
   logger = logging.getLogger(__name__)
   
   # Log at critical points
   logger.info(f"Input records: {df_input.count()}")
   logger.info(f"After filtering: {df_filtered.count()}")
   logger.info(f"Null values in amount: {df.filter(col('amount').isNull()).count()}")
   logger.debug(f"Sample data: {df.limit(10).collect()}")
   
   # Metrics to log
   def log_metrics(df, stage):
       logger.info({
           "stage": stage,
           "record_count": df.count(),
           "null_counts": {col: df.filter(col(col).isNull()).count() 
                          for col in df.columns},
           "min_max": {col: (df.agg(min(col)).collect(), 
                            df.agg(max(col)).collect()) 
                       for col in df.columns}
       })
   ```

7. **Unit Testing Data Transformations**
   ```python
   import unittest
   
   class TestAggregations(unittest.TestCase):
       def setUp(self):
           self.spark = SparkSession.builder.appName("test").getOrCreate()
       
       def test_revenue_aggregation(self):
           # Create test data
           test_data = [
               ("cat_A", 100),
               ("cat_A", 200),
               ("cat_B", 150),
           ]
           df_test = self.spark.createDataFrame(test_data, ["category", "amount"])
           
           # Apply transformation
           result = df_test.groupBy("category").agg(sum("amount"))
           
           # Assert expected results
           collected = result.collect()
           self.assertEqual(collected[0]["sum(amount)"], 300)  # cat_A
           self.assertEqual(collected[1]["sum(amount)"], 150)  # cat_B
   ```

---

## Key Takeaways for 2-Year Data Engineers

### Best Practices Checklist:
- ✅ Always validate schema before processing
- ✅ Test with sample data before full scale
- ✅ Implement comprehensive logging and monitoring
- ✅ Use version control for all pipeline code
- ✅ Document data lineage and transformations
- ✅ Plan for incremental processing (not daily full refreshes)
- ✅ Implement data quality gates in pipelines
- ✅ Monitor and optimize for cost and performance
- ✅ Have rollback/recovery procedures
- ✅ Maintain documentation of schema, SLA, and known issues

### Technologies to Master at 2-Year Level:
- PySpark (core operations, optimization)
- SQL (complex joins, window functions, CTEs)
- Data formats (Parquet, ORC, Avro)
- Cloud platforms (AWS S3/EMR, Azure, GCP)
- Workflow orchestration (Airflow, Databricks)
- Data quality (Great Expectations, custom validations)
- Version control and CI/CD
- Basic monitoring and logging
