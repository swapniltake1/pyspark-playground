# Cloud Data Engineering Interview Questions and Answers

1. **What are common cloud storage options for big data, and how do they differ?**

   - **Amazon S3 / Azure Blob / Google Cloud Storage**: object stores with high durability and scalability; data is stored in files and accessed via REST APIs.
   - **HDFS on cloud VMs**: provides HDFS semantics but requires cluster management.
   - **Cloud data warehouses (Redshift, BigQuery, Synapse)**: optimized for analytics with columnar storage and SQL interfaces.

2. **Describe how you would set up a serverless ETL pipeline using AWS services.**

   Use AWS Glue for serverless Spark jobs to transform data, trigger via EventBridge when files land in S3, store metadata in Glue Data Catalog, and orchestrate using Step Functions or Glue Workflows.

3. **How does IAM (Identity and Access Management) affect data engineering on cloud platforms?**

   IAM policies control who can read/write data, launch clusters, and access metadata catalogs. Proper least-privilege roles are essential to secure pipelines and prevent data leaks.

4. **What is Apache Iceberg and why would you use it on cloud storage?**

   Iceberg is a table format that brings ACID transactions, schema evolution, and time-travel to object stores. It avoids small-file problems and simplifies metadata management.

5. **Explain how to optimize costs when running Spark jobs on cloud infrastructure.**

   - Use spot/preemptible instances for workers.
   - Resize clusters dynamically (auto-scaling).
   - Use serverless offerings (Databricks serverless, AWS Glue).
   - Delete idle clusters and clean up intermediate data.

6. **How can you ingest streaming data into a cloud data lake?**

   Use Kafka or cloud-native services (Kinesis, Pub/Sub) to capture events, then run streaming jobs (Spark Structured Streaming, Flink, Dataflow) writing to partitioned Parquet/Delta tables in cloud storage.

7. **What monitoring tools are available in cloud platforms for data pipelines?**

   - **AWS**: CloudWatch logs/metrics, Glue job metrics, EMR monitoring, AWS Managed Service for Prometheus.
   - **Azure**: Monitor, Log Analytics, Synapse workspace monitoring.
   - **GCP**: Stackdriver, Dataflow/Flink monitoring dashboards.

8. **Describe how data versioning works in Delta Lake or Iceberg.**

   Both maintain a transaction log (JSON or Parquet) recording file additions/deletions. You can query historical snapshots by timestamp or version, and roll back to previous states.

9. **What is a data lakehouse and how does it differ from a traditional data lake?**

   A lakehouse combines the openness of a data lake (object storage, schema-on-read) with data warehouse features like ACID transactions, indexing, and performance optimizations via formats like Delta/Iceberg.

10. **How would you secure sensitive data in transit and at rest in a cloud data pipeline?**

    - Encrypt data at rest using server-side or client-side encryption (SSE-KMS, CMEK).
    - Use TLS/SSL for data in motion.
    - Apply network security (VPC endpoints, private links) and token-based authentication for services.
