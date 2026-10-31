"""
Topic: Idempotent file ingestion with a durable manifest and quarantine
Runtime: PySpark 3.3+
Run: spark-submit daily/2026-10-31-0755-idempotent-file-ingestion-manifest.py

The exercise models a landing zone where files can be rediscovered, replayed,
mutated after processing, or arrive with invalid metadata. It uses in-memory
DataFrames so the transformation logic is reproducible without cloud credentials.
"""

from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType
)

spark = SparkSession.builder.appName(
    "IdempotentFileIngestionManifest"
).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 1. REALISTIC INPUTS: incoming file inventory and durable processed manifest
# ---------------------------------------------------------------------------

manifest_schema = StructType([
    StructField("file_id", StringType(), False),
    StructField("path", StringType(), True),
    StructField("checksum", StringType(), True),
    StructField("discovered_at", TimestampType(), True),
])

incoming_manifest = spark.createDataFrame([
    ("f-001", "/landing/orders/part-001.json", "sha-aaa", datetime(2026, 10, 31, 7, 30, 0)),
    ("f-002", "/landing/orders/part-002.json", "sha-bbb", datetime(2026, 10, 31, 7, 31, 0)),
    # Same immutable object discovered twice: one logical file, not two loads.
    ("f-002", "/landing/orders/part-002.json", "sha-bbb", datetime(2026, 10, 31, 7, 32, 0)),
    # Same file identity but a different checksum from the committed manifest.
    ("f-003", "/landing/orders/part-003.json", "sha-new", datetime(2026, 10, 31, 7, 33, 0)),
    ("f-004", "/landing/orders/part-004.json", "sha-ddd", datetime(2026, 10, 31, 7, 34, 0)),
    # Invalid metadata: checksum is required for immutable-file idempotency.
    ("f-005", "/landing/orders/part-005.json", None, datetime(2026, 10, 31, 7, 35, 0)),
    # Conflicting observations for one file identity must not be chosen silently.
    ("f-006", "/landing/orders/part-006.json", "sha-x", datetime(2026, 10, 31, 7, 36, 0)),
    ("f-006", "/landing/orders/part-006.json", "sha-y", datetime(2026, 10, 31, 7, 37, 0)),
], manifest_schema)

processed_schema = StructType([
    StructField("file_id", StringType(), False),
    StructField("checksum", StringType(), False),
    StructField("processed_at", TimestampType(), False),
])

processed_manifest = spark.createDataFrame([
    ("f-001", "sha-aaa", datetime(2026, 10, 31, 7, 10, 0)),
    ("f-003", "sha-old", datetime(2026, 10, 31, 7, 20, 0)),
], processed_schema)

# ---------------------------------------------------------------------------
# 2. VALIDATE MANIFEST METADATA AND ISOLATE AMBIGUOUS FILE IDENTITIES
# ---------------------------------------------------------------------------

required_metadata_ok = (
    F.col("file_id").isNotNull()
    & (F.length(F.trim(F.col("file_id"))) > 0)
    & F.col("path").isNotNull()
    & (F.length(F.trim(F.col("path"))) > 0)
    & F.col("checksum").isNotNull()
    & (F.length(F.trim(F.col("checksum"))) > 0)
    & F.col("discovered_at").isNotNull()
)

invalid_manifest = (
    incoming_manifest
    .filter(~F.coalesce(required_metadata_ok, F.lit(False)))
    .withColumn("reason_code", F.lit("MISSING_REQUIRED_METADATA"))
)

valid_manifest = incoming_manifest.filter(
    F.coalesce(required_metadata_ok, F.lit(False))
)

conflicting_ids = (
    valid_manifest
    .groupBy("file_id")
    .agg(F.countDistinct("checksum").alias("checksum_versions"))
    .filter(F.col("checksum_versions") > 1)
    .select("file_id")
)

conflicting_manifest_rows = (
    valid_manifest
    .join(conflicting_ids, on="file_id", how="inner")
    .withColumn("reason_code", F.lit("CONFLICTING_CHECKSUMS"))
)

# Remove all observations for ambiguous file IDs. Never pick an arbitrary hash.
unambiguous_manifest = valid_manifest.join(
    conflicting_ids, on="file_id", how="left_anti"
)

# If the same file/checksum is discovered repeatedly, retain its latest
# observation deterministically. The checksum is the content identity.
latest_observation = Window.partitionBy("file_id").orderBy(
    F.col("discovered_at").desc(),
    F.col("path").asc(),
    F.col("checksum").asc(),
)

canonical_manifest = (
    unambiguous_manifest
    .withColumn("_rn", F.row_number().over(latest_observation))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# ---------------------------------------------------------------------------
# 3. COMPARE WITH THE DURABLE MANIFEST: unchanged, new, or mutated files
# ---------------------------------------------------------------------------

processed_keys = processed_manifest.select(
    "file_id", F.col("checksum").alias("processed_checksum")
)

joined_to_processed = canonical_manifest.join(
    processed_keys, on="file_id", how="inner"
)

already_processed = joined_to_processed.filter(
    F.col("checksum") == F.col("processed_checksum")
).select("file_id", "path", "checksum")

changed_files = (
    joined_to_processed
    .filter(F.col("checksum") != F.col("processed_checksum"))
    .select(
        "file_id", "path", "checksum",
        "processed_checksum",
    )
)

# A changed object with an already-committed file_id is quarantined. Reprocess
# it only through an explicit correction/backfill workflow with a new version.
changed_manifest_quarantine = (
    changed_files
    .select(
        "file_id", "path", "checksum",
        F.lit(None).cast(TimestampType()).alias("discovered_at"),
    )
    .withColumn("reason_code", F.lit("CHECKSUM_CHANGED_FOR_PROCESSED_FILE"))
)

new_files = canonical_manifest.join(
    processed_manifest.select("file_id"),
    on="file_id",
    how="left_anti",
)

# One manifest quarantine table keeps the operational reason beside the file.
manifest_quarantine = (
    invalid_manifest
    .unionByName(conflicting_manifest_rows)
    .unionByName(changed_manifest_quarantine)
)

# ---------------------------------------------------------------------------
# 4. REALISTIC FILE ROWS: only new files are eligible for this micro-batch
# ---------------------------------------------------------------------------

raw_schema = StructType([
    StructField("file_id", StringType(), False),
    StructField("record_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("ingested_at", TimestampType(), False),
])

raw_rows = spark.createDataFrame([
    ("f-002", "r-001", "C-101", 120.0, datetime(2026, 10, 31, 7, 25), datetime(2026, 10, 31, 7, 32)),
    ("f-002", "r-002", "C-102",  40.0, datetime(2026, 10, 31, 7, 26), datetime(2026, 10, 31, 7, 32)),
    # Duplicate record in one file: latest ingestion wins deterministically.
    ("f-004", "r-001", "C-103",  70.0, datetime(2026, 10, 31, 7, 27), datetime(2026, 10, 31, 7, 34)),
    ("f-004", "r-001", "C-103",  75.0, datetime(2026, 10, 31, 7, 27), datetime(2026, 10, 31, 7, 35)),
    # Invalid business row is quarantined without discarding valid rows in file.
    ("f-004", "r-002", "C-104",  -5.0, datetime(2026, 10, 31, 7, 28), datetime(2026, 10, 31, 7, 35)),
], raw_schema)

selected_rows = raw_rows.join(
    new_files.select("file_id"), on="file_id", how="inner"
)

record_order = Window.partitionBy("file_id", "record_id").orderBy(
    F.col("ingested_at").desc(),
    F.col("amount").desc_nulls_last(),
)

deduplicated_rows = (
    selected_rows
    .withColumn("_rn", F.row_number().over(record_order))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

row_is_valid = (
    F.col("record_id").isNotNull()
    & F.col("customer_id").isNotNull()
    & F.col("amount").isNotNull()
    & (F.col("amount") >= 0)
    & F.col("event_ts").isNotNull()
)

accepted_rows = deduplicated_rows.filter(
    F.coalesce(row_is_valid, F.lit(False))
)

quarantined_rows = (
    deduplicated_rows
    .filter(~F.coalesce(row_is_valid, F.lit(False)))
    .withColumn(
        "reason_code",
        F.when(F.col("record_id").isNull(), "MISSING_RECORD_ID")
         .when(F.col("customer_id").isNull(), "MISSING_CUSTOMER_ID")
         .when(F.col("amount").isNull(), "MISSING_AMOUNT")
         .when(F.col("amount") < 0, "NEGATIVE_AMOUNT")
         .when(F.col("event_ts").isNull(), "MISSING_EVENT_TIMESTAMP")
         .otherwise("INVALID_ROW"),
    )
)

# ---------------------------------------------------------------------------
# 5. EXPECTED OUTPUT / ASSERTIONS
# ---------------------------------------------------------------------------

def sorted_values(df, column_name):
    return sorted(row[column_name] for row in df.select(column_name).distinct().collect())


assert sorted_values(new_files, "file_id") == ["f-002", "f-004"]
assert sorted_values(already_processed, "file_id") == ["f-001"]
assert sorted_values(changed_files, "file_id") == ["f-003"]
assert sorted_values(conflicting_ids, "file_id") == ["f-006"]
assert invalid_manifest.count() == 1

assert accepted_rows.count() == 3
assert quarantined_rows.count() == 1
assert abs(accepted_rows.agg(F.sum("amount").alias("total")).first()["total"] - 235.0) < 0.0001

accepted_ids = {
    (r["file_id"], r["record_id"], r["amount"])
    for r in accepted_rows.select("file_id", "record_id", "amount").collect()
}
assert accepted_ids == {
    ("f-002", "r-001", 120.0),
    ("f-002", "r-002", 40.0),
    ("f-004", "r-001", 75.0),
}

assert quarantined_rows.select("reason_code").first()["reason_code"] == "NEGATIVE_AMOUNT"

# Per-file accounting: every deduplicated row is either accepted or quarantined.
accepted_counts = accepted_rows.groupBy("file_id").agg(
    F.count("*").alias("accepted_rows")
)
quarantine_counts = quarantined_rows.groupBy("file_id").agg(
    F.count("*").alias("quarantined_rows")
)

file_summary = (
    new_files.select("file_id", "path", "checksum")
    .join(accepted_counts, on="file_id", how="left")
    .join(quarantine_counts, on="file_id", how="left")
    .fillna(0, subset=["accepted_rows", "quarantined_rows"])
    .withColumn(
        "accounted_rows",
        F.col("accepted_rows") + F.col("quarantined_rows"),
    )
    .orderBy("file_id")
)

assert file_summary.agg(
    F.sum("accounted_rows").alias("rows")
).first()["rows"] == deduplicated_rows.count()

print("New files eligible for processing:")
new_files.orderBy("file_id").show(truncate=False)

print("Accepted records:")
accepted_rows.orderBy("file_id", "record_id").show(truncate=False)

print("Row-level quarantine:")
quarantined_rows.orderBy("file_id", "record_id").show(truncate=False)

print("Manifest-level quarantine:")
manifest_quarantine.orderBy("file_id").show(truncate=False)

print("Per-file accounting:")
file_summary.show(truncate=False)

# ---------------------------------------------------------------------------
# 6. PRODUCTION WRITE PATTERN (DESIGN NOTE)
# ---------------------------------------------------------------------------
# In production, write accepted rows, row quarantine, manifest quarantine, and
# the completed-file manifest with a transaction strategy that prevents a file
# from being marked complete before its data/quarantine outputs are durable.
#
# A practical Delta design:
#   - Bronze: append source rows with file_id, checksum, batch_id and lineage.
#   - Quarantine: append invalid rows and stable reason codes.
#   - Manifest: MERGE on (source_system, file_id, checksum).
#   - Reprocess: require a new file version or an explicit backfill run_id.
#
# Do not claim end-to-end exactly-once merely because a checkpoint exists.
# Object listing, data writes, and manifest commits are separate failure domains.
# Use atomic table transactions where possible and make every write idempotent.

# ---------------------------------------------------------------------------
# 7. OPTIMIZATION GUIDANCE
# ---------------------------------------------------------------------------
# - Filter the manifest before joining file rows; avoid scanning old landing data.
# - Broadcast processed_manifest only when it is genuinely small:
#       new_files = canonical_manifest.join(
#           F.broadcast(processed_manifest.select("file_id")), "file_id", "left_anti"
#       )
# - Do not collect production manifests to the driver. collect() above is limited
#   to tiny deterministic assertions in this interview exercise.
# - Partition bronze by a low/medium-cardinality ingestion_date or source_system,
#   not by file_id; one directory per file creates a small-files problem.
# - Persist canonical_manifest only if multiple downstream actions reuse it and
#   its recomputation is more expensive than storage/memory overhead.
# - Use a durable checksum supplied by object storage or computed during ingestion;
#   avoid reading entire large files again just to decide whether they changed.
# - Inspect exchanges and join strategies with explain("formatted").

print("Physical plan for selecting unprocessed files:")
new_files.explain("formatted")

# ---------------------------------------------------------------------------
# 8. INTERVIEW FOLLOW-UPS
# ---------------------------------------------------------------------------
# 1. What is the difference between file path identity and content identity?
# 2. How would you handle an object overwritten at the same path?
# 3. What happens if the job crashes after writing bronze but before manifest commit?
# 4. How would you design a replay/backfill without corrupting the current batch?
# 5. When is a broadcast anti-join appropriate, and when can it cause driver pressure?
# 6. How would you handle millions of tiny files and object-store listing latency?
# 7. What metadata would you retain for auditability and data lineage?
# 8. How would you evolve this design for Auto Loader or a Kafka-based source?

spark.stop()
