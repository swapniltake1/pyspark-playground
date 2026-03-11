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

This project is intended for **learning, interview preparation, and demonstration purposes**.
