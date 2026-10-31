# Pyspark-PlayGround

## Overview
This repository contains a **comprehensive, interview-focused PySpark project** built on the BigMart Sales dataset. The project is designed to demonstrate **real-world Spark data engineering skills**, covering both **core transformations** and **advanced performance optimization techniques** commonly discussed in data engineering interviews.

All examples are written in **pure PySpark (Spark 3.x)** and are **fully compatible with Databricks Runtime**, requiring only minor environment-specific adjustments.

---

## Objectives

- Demonstrate hands-on PySpark expertise using a realistic retail dataset
- Cover commonly asked **interview scenarios and edge cases**
- Showcase **performance tuning, scalability, and fault tolerance** concepts
- Provide production-style, readable, and reusable code

---

## Dataset

**BigMart Sales Dataset (CSV)**

**Key Columns:**
- Item_Identifier
- Item_Weight
- Item_Fat_Content
- Item_Visibility
- Item_Type
- Item_MRP
- Outlet_Identifier
- Outlet_Establishment_Year
- Outlet_Size
- Outlet_Location_Type
- Outlet_Type
- Item_Outlet_Sales

---

## Project Structure

```
bigmart-pyspark-interview/
│
├── Data/
│   ├── BigMart Sales.csv          # Main retail dataset used in PySpark scenarios
│   └── drivers.json               # Additional JSON sample data
│
├── common/
│   └── spark_session.py        # Centralized SparkSession (local use)
│
├── scenarios/                  # Independent, interview-focused scenarios
│   ├── 01_read_with_schema.py
│   ├── 02_schema_validation.py
│   ├── 03_null_analysis.py
│   ├── 04_fill_missing_values.py
│   ├── 05_case_when_column.py
│   ├── 06_data_standardization.py
│   ├── 07_basic_aggregations.py
│   ├── 08_window_ranking.py
│   ├── 09_deduplication.py
│   ├── 10_top_n_per_group.py
│   ├── 11_percentage_contribution.py
│   ├── 12_complex_filtering.py
│   ├── 13_feature_engineering_outlet_age.py
│   ├── 14_pivot_sales.py
│   ├── 15_broadcast_join.py
│   ├── 16_cache_and_persist.py
│   ├── 17_explain_plan.py
│   ├── 18_repartition_vs_coalesce.py
│   ├── 19_skew_handling_salt.py
│   ├── 20_checkpoint_example.py
│   └── advanced_pyspark_interview_scenarios.py  # Consolidated end-to-end pipeline
│
└── interview/                  # Question bank for interview preparation
    ├── data_engineer_2yr_scenarios.md         # Scenario-based DE Q&A (2 years experience)
    ├── data_engineering_questions.md           # Core DE interview Q&A
    ├── cloud_data_engineering_questions.md     # Cloud DE interview Q&A
    ├── pyspark_questions.md                    # PySpark interview Q&A
    ├── spark_sql_questions.md                  # Spark SQL interview Q&A
    ├── Data creation.dbquery.ipynb
    └── Find 2nd highest salary.dbquery.ipynb
```

---

## Key Concepts Covered

### Core PySpark
- Explicit schema definition
- Null handling strategies
- Column transformations (CASE WHEN, string functions)
- Aggregations and groupBy operations
- Window functions (ranking, Top-N problems)

### Performance Optimization
- Broadcast joins
- Repartition vs coalesce
- Cache vs persist
- Explain plans (logical & physical)
- Small files problem mitigation

### Advanced & Real-World Scenarios
- Adaptive Query Execution (AQE)
- Data skew handling (manual salting + AQE)
- Checkpointing and DAG truncation
- Partition pruning
- Schema evolution with Parquet
- Idempotent and partitioned writes
- Data quality checks
- Accumulators and broadcast variables

---

## Databricks Compatibility

This project is **Databricks-ready**.

### Required Adjustments on Databricks

1. **File paths**
   ```python
   # Local
   "/Volumes/raw-data/git/allfiles/BigMart Sales.csv"

   # Databricks
   "dbfs:/FileStore/bigmart/BigMart_Sales.csv"
   ```

2. **SparkSession**
   - Databricks provides `spark` automatically
   - Remove manual `SparkSession.builder` calls

3. **Checkpoint directory**
   ```python
   spark.sparkContext.setCheckpointDir("dbfs:/tmp/spark_checkpoints")
   ```

---

## How to Run

### Local
```bash
spark-submit scenarios/01_read_with_schema.py
```

### Databricks
- Upload files to Workspace or Repo
- Attach to a cluster
- Run as notebooks or Python scripts

---

## Interview Positioning

This project supports interview discussions around:

- Spark performance tuning and scalability
- Handling large datasets with skew and joins
- Fault tolerance and pipeline reliability
- Writing production-quality PySpark code

**Suggested interview summary:**
> "I built an end-to-end PySpark project covering AQE, broadcast joins, skew handling, checkpointing, and performance optimization on Databricks-compatible Spark 3.x."

---

## Interview Preparation Content

The `interview/` folder now includes both **scenario-based** and **topic-based** question sets tailored for data engineers.

### 1) Scenario-Based Questions (2 Years Experience)

Use `interview/data_engineer_2yr_scenarios.md` to practice practical, real-world debugging and design discussions commonly asked for data engineers with around 2 years of experience.

Covered themes include:
- Pipeline bottlenecks and OOM debugging
- Late-arriving data handling in streaming systems
- Schema evolution and backward compatibility
- Cloud cost optimization without breaking SLA
- Failure recovery and idempotency design

### 2) Topic-Based Interview Question Banks

- `interview/data_engineering_questions.md`: ETL, partitioning, schema evolution, Kafka, monitoring, optimization
- `interview/cloud_data_engineering_questions.md`: Cloud storage, IAM, serverless ETL, Iceberg, cloud monitoring
- `interview/pyspark_questions.md`: Core PySpark APIs, partitioning, cache vs persist, joins and transformations
- `interview/spark_sql_questions.md`: Spark SQL fundamentals, temp views, joins, Hive support, UDFs

### Recommended Practice Plan

1. Start with one scenario question and explain your solution out loud.
2. Map your explanation to a matching script under `scenarios/`.
3. Add trade-offs (performance, cost, reliability) as if speaking in a live interview.
4. Finish with a concise production-ready approach.

---

## Daily PySpark Interview Practice

New date-based practice scripts are added under `daily/`. Each script includes a practical interview problem, sample DataFrames, a runnable PySpark solution, optimization guidance, expected results/assertions, and follow-up interview questions.

### Latest Practice

- [2026-10-30 23:00 — Skew-Aware Joins with Deterministic Salting](daily/2026-10-30-2300-skew-aware-joins.py): detect hot keys, distribute fact rows across deterministic salt buckets, selectively replicate hot dimension rows, and assert that salted and baseline aggregates match.
- [2026-10-30 — Event-Time Sessionization and Late-Arriving Events](daily/2026-10-30-event-time-sessionization.py): build deterministic inactivity-gap sessions with `lag` and cumulative windows; handle out-of-order arrivals, exact-threshold gaps, invalid records, and session boundary changes caused by late data.
- [2026-10-28 — Nested Arrays, Explode, and Order Analytics](daily/2026-10-28-nested-arrays-order-analytics.py): compare item-level SKU aggregation using `explode` with an in-array higher-order `aggregate` for order totals, including empty/null arrays and result assertions.
- [2026-10-27 — Incremental Loads, Late-Arriving Records, and Deduplication](daily/2026-10-27-incremental-load-late-arrivals.py): compare a straightforward snapshot merge approach with an incremental candidate approach using an ingestion-time lookback, deterministic latest-record selection, and assertions for idempotent final-state results.

Run the latest example locally with:

```bash
spark-submit daily/2026-10-30-2300-skew-aware-joins.py
```

Previous practice: [2026-10-30 — Event-Time Sessionization and Late-Arriving Events](daily/2026-10-30-event-time-sessionization.py).

The daily scripts use small in-memory sample datasets unless a practice explicitly requires an external source or service.

---

## Future Enhancements

- Delta Lake implementation (MERGE, time travel)
- Unity Catalog integration
- Databricks SQL dashboards
- Streaming version of the pipeline

---

## Author

Swapnil  
Application Developer | Data Engineering & PySpark

---

## Tags

**Technologies:**  
PySpark, Apache Spark, Spark SQL, Databricks, Azure Databricks

**Concepts:**  
Big Data, Data Engineering, ETL Pipelines, Performance Optimization, Distributed Computing

**Spark Features:**  
Adaptive Query Execution (AQE), Broadcast Join, Window Functions, Checkpointing, Partition Pruning, Schema Evolution, Data Skew Handling

**Use Cases:**  
Interview Preparation, Production-Ready Pipelines, Retail Analytics, Batch Processing

---

## License

This project is intended for **learning, interview preparation, and demonstration purposes**. Thanks for visiting

### Hourly Data Engineering Practice — 2026-10-31 00:03

- [Schema Drift Normalization and Row-Level Quarantine](daily/2026-10-31-0003-schema-drift-normalization.py) — a runnable PySpark scenario that normalizes three customer source schema versions into a stable contract, safely casts changing identifiers, routes invalid rows to quarantine with reason codes, and selects the latest valid customer state deterministically.
- Includes explicit source schemas, in-memory sample DataFrames, expected row-count/customer assertions, optimization guidance, and production/Delta Lake interview follow-ups.

Run with `spark-submit daily/2026-10-31-0003-schema-drift-normalization.py` on Spark 3.2+.


### Hourly Data Engineering Practice — 2026-10-31 01:04

- [Partition-Aware Writes & Small-File Mitigation](daily/2026-10-31-0104-partitioned-writes-small-files.py) — a runnable PySpark scenario comparing an unkeyed repartition with a repartition aligned to the Parquet output partition key, then validating read-back row counts, unique transaction IDs, and output file counts.
- Includes 1,200 deterministic sample transactions, physical-plan inspection, a cautious small-extract coalesce example, file-layout trade-offs, optimization guidance, and interview follow-ups.

Run with `spark-submit daily/2026-10-31-0104-partitioned-writes-small-files.py` on PySpark 3.3+.


### Hourly Data Engineering Practice — 2026-10-31 02:00

- [Order Lifecycle State-Machine Validation](daily/2026-10-31-0200-order-state-machine-validation.py) — a runnable PySpark scenario that deduplicates replayed event IDs, orders each order's event timeline deterministically, validates allowed state transitions, identifies invalid jumps, and calculates delivery lifecycle durations and current observed states.
- Includes explicit in-memory event schemas, a replay with a later ingestion timestamp, successful/cancelled/in-progress orders, expected-result assertions, formatted physical-plan inspection, optimization guidance, and interview follow-ups.

Run with spark-submit daily/2026-10-31-0200-order-state-machine-validation.py on PySpark 3.3+.


### Hourly Data Engineering Practice — 2026-10-31 03:01

- [Reusable Data-Quality Rule Engine & Row-Level Quarantine](daily/2026-10-31-0301-data-quality-rule-engine.py) — a runnable PySpark scenario that applies declarative customer checks for required fields, email format, country-domain membership, credit-limit range, timestamp chronology, and duplicate business keys; every invalid row is retained with all matching rule codes.
- Includes explicit schemas, deterministic in-memory sample data, a broadcast reference lookup, rule-level failure metrics, expected row-count/customer-ID assertions, persistence guidance for repeated actions, and production interview follow-ups.

Run with `spark-submit daily/2026-10-31-0301-data-quality-rule-engine.py` on PySpark 3.3+.

### Daily PySpark Practice — 2026-10-31 (03:58)

- [Structured Streaming Watermarks & Event-Time Windows](daily/2026-10-31-0358-structured-streaming-watermarks.py) — deterministic batch assertions plus an optional file-source streaming pipeline.
- Topics: replay deduplication, event-time tumbling windows, watermark-based state retention, append-mode output, checkpointing, and streaming operations.

### Hourly Data Engineering Practice — 2026-10-31 04:58

- [Conservative Customer Entity Resolution & Survivorship](daily/2026-10-31-0458-customer-entity-resolution.py) — a runnable PySpark scenario that normalizes CRM, billing, and support records, creates conservative exact-match blocking keys, assigns deterministic exercise entity IDs, applies field-level non-null survivorship by source priority, and flags conflicting/shared phone numbers for stewardship.
- Includes explicit schemas, seven realistic source records, expected entity/survivorship assertions, formatted physical-plan inspection, optimization and privacy guidance, and interview follow-ups. Run with `spark-submit daily/2026-10-31-0458-customer-entity-resolution.py` on PySpark 3.3+.


### Hourly Data Engineering Practice — 2026-10-31 06:00

- [Pipeline Lineage Graph Traversal & Downstream Impact Analysis](daily/2026-10-31-0600-pipeline-lineage-impact-analysis.py) — a runnable PySpark scenario that builds a medallion-style dependency graph, performs breadth-first downstream impact traversal with shortest dependency depth, validates edge references, and derives topological execution layers using Kahn's algorithm.
- Includes deterministic sample nodes/edges, assertions for expected impacted jobs and DAG levels, deliberate orphan-edge and cycle tests, formatted physical-plan inspection, optimization guidance, and interview follow-ups. Run with `spark-submit daily/2026-10-31-0600-pipeline-lineage-impact-analysis.py` on PySpark 3.3+.


### Hourly Data Engineering Practice — 2026-10-31 07:02

- [Telemetry Wide-to-Long Normalization & Quality Validation](daily/2026-10-31-0702-telemetry-wide-to-long-quality.py) — a runnable PySpark scenario that unpivots wide device metrics into a metric-grain fact, validates values against a broadcast threshold dimension, preserves missing measurements, computes event completeness and device-hour quality rollups, and pivots the data back to its source contract.
- Includes explicit schemas, deterministic in-memory telemetry, expected status-count and reconstruction assertions, formatted physical-plan inspection, optimization guidance, and interview follow-ups. Run with `spark-submit daily/2026-10-31-0702-telemetry-wide-to-long-quality.py` on PySpark 3.3+.


### Hourly Data Engineering Practice — 2026-10-31 07:55

- [Idempotent File Ingestion, Durable Manifest & Quarantine](daily/2026-10-31-0755-idempotent-file-ingestion-manifest.py) — a runnable PySpark scenario that validates landing-file metadata, removes repeated discoveries, quarantines conflicting checksums and mutated processed files, anti-joins against a durable processed manifest, deduplicates rows within new files, and separates accepted records from row-level quarantine.
- Includes explicit in-memory schemas and sample manifests, expected file/row assertions, per-file accounting, physical-plan inspection, Delta-style transactional write guidance, small-file and broadcast considerations, and interview follow-ups. Run with spark-submit daily/2026-10-31-0755-idempotent-file-ingestion-manifest.py on PySpark 3.3+.


### Daily PySpark Interview Practice — 2026-10-31

- [Multi-Dimensional Aggregations with rollup(), cube(), and grouping_id()](daily/2026-10-31-multidimensional-rollups.py) — a runnable retail analytics exercise that builds hierarchical regional subtotals with `rollup()` and all dimensional combinations with `cube()`, using `grouping_id()` to distinguish genuine NULL dimension values from subtotal rows.
- Includes explicit DataFrame schemas, realistic completed/cancelled orders, expected revenue and order-count assertions, formatted physical-plan inspection, aggregation/shuffle optimization guidance, and interview follow-up questions.

Run with `spark-submit daily/2026-10-31-multidimensional-rollups.py` on PySpark 3.x.
