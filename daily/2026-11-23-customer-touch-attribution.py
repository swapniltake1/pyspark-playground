"""
Daily PySpark Interview Practice — 2026-11-23
Topic: Deterministic Customer-Level Attribution from Multi-Touch Events

Run:
    spark-submit daily/2026-11-23-customer-touch-attribution.py

Covers event deduplication, direct-traffic policy, deterministic ordering,
last-non-direct attribution, conversion windows, broadcast joins, assertions,
and performance trade-offs. Compatible with PySpark 3.x.
"""
from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, TimestampType, DoubleType
)

spark = SparkSession.builder.appName("CustomerTouchAttribution").getOrCreate()

touch_schema = StructType([
    StructField("event_id", LongType(), False),
    StructField("customer_id", LongType(), False),
    StructField("session_id", StringType(), False),
    StructField("channel", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_ts", TimestampType(), False),
    StructField("ingested_at", TimestampType(), False),
    StructField("campaign_id", StringType(), True),
    StructField("event_value", DoubleType(), True),
])

touch_rows = [
    (1, 101, "s1", "paid_search", "touch", datetime(2026, 11, 1, 9, 0), datetime(2026, 11, 1, 9, 1), "brand", 0.0),
    (2, 101, "s2", "email", "touch", datetime(2026, 11, 2, 10, 0), datetime(2026, 11, 2, 10, 1), "nov", 0.0),
    (3, 101, "s3", "direct", "touch", datetime(2026, 11, 3, 8, 0), datetime(2026, 11, 3, 8, 1), None, 0.0),
    (4, 101, "s3", "direct", "purchase", datetime(2026, 11, 3, 8, 5), datetime(2026, 11, 3, 8, 6), None, 120.0),
    (5, 102, "s4", "social", "touch", datetime(2026, 11, 2, 11, 0), datetime(2026, 11, 2, 11, 2), "launch", 0.0),
    (6, 102, "s5", "direct", "purchase", datetime(2026, 11, 4, 12, 0), datetime(2026, 11, 4, 12, 1), None, 80.0),
    (7, 103, "s6", "affiliate", "touch", datetime(2026, 11, 1, 12, 0), datetime(2026, 11, 1, 12, 1), "partner", 0.0),
    (8, 103, "s7", "paid_search", "touch", datetime(2026, 11, 5, 10, 0), datetime(2026, 11, 5, 10, 1), "generic", 0.0),
    (9, 103, "s8", "direct", "purchase", datetime(2026, 11, 5, 10, 2), datetime(2026, 11, 5, 10, 3), None, 200.0),
    # A replay of event 8 with a later ingestion timestamp must collapse to one event.
    (8, 103, "s7", "paid_search", "touch", datetime(2026, 11, 5, 10, 0), datetime(2026, 11, 5, 10, 5), "generic", 0.0),
    (10, 104, "s9", "direct", "purchase", datetime(2026, 11, 6, 9, 0), datetime(2026, 11, 6, 9, 1), None, 50.0),
    (11, 105, "s10", "email", "touch", datetime(2026, 10, 1, 9, 0), datetime(2026, 10, 1, 9, 1), "old", 0.0),
    (12, 105, "s11", "direct", "purchase", datetime(2026, 11, 20, 9, 0), datetime(2026, 11, 20, 9, 1), None, 60.0),
]

touches = spark.createDataFrame(touch_rows, touch_schema)

# Campaign dimension is deliberately small and suitable for a broadcast join.
campaign_schema = StructType([
    StructField("campaign_id", StringType(), False),
    StructField("campaign_name", StringType(), False),
    StructField("channel_group", StringType(), False),
])
campaign_rows = [
    ("brand", "Brand Search", "Search"),
    ("nov", "November Newsletter", "Email"),
    ("launch", "Product Launch", "Social"),
    ("partner", "Partner Referral", "Affiliate"),
    ("generic", "Generic Search", "Search"),
    ("old", "Expired Campaign", "Email"),
]
campaigns = spark.createDataFrame(campaign_rows, campaign_schema)

# Q1. Canonicalize event replays deterministically.
dedupe_window = Window.partitionBy("event_id").orderBy(
    F.col("ingested_at").desc(),
    F.col("session_id").asc()
)
canonical = (
    touches
    .withColumn("_rn", F.row_number().over(dedupe_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# Q2. Extract purchases and find the most recent eligible non-direct touch
# within a 30-day lookback. Direct purchases remain conversions, but direct
# touches do not overwrite a prior attributable marketing touch.
purchases = (
    canonical.filter(F.col("event_type") == "purchase")
    .select(
        F.col("event_id").alias("purchase_event_id"),
        "customer_id",
        F.col("event_ts").alias("purchase_ts"),
        F.col("event_value").alias("revenue")
    )
)

eligible_touches = (
    canonical.filter(
        (F.col("event_type") == "touch") &
        (F.lower(F.col("channel")) != "direct")
    )
    .select(
        "customer_id",
        F.col("event_id").alias("touch_event_id"),
        F.col("event_ts").alias("touch_ts"),
        "channel",
        "campaign_id"
    )
)

p = purchases.alias("p")
t = eligible_touches.alias("t")
matches = (
    p.join(
        t,
        (F.col("p.customer_id") == F.col("t.customer_id")) &
        (F.col("t.touch_ts") <= F.col("p.purchase_ts")) &
        (F.col("t.touch_ts") >= F.col("p.purchase_ts") - F.expr("INTERVAL 30 DAYS")),
        "left"
    )
    .select(
        "p.purchase_event_id", "p.customer_id", "p.purchase_ts", "p.revenue",
        "t.touch_event_id", "t.touch_ts", "t.channel", "t.campaign_id"
    )
)

last_touch_window = Window.partitionBy("purchase_event_id").orderBy(
    F.col("touch_ts").desc_nulls_last(),
    F.col("touch_event_id").desc_nulls_last()
)

attributed = (
    matches.withColumn("_rn", F.row_number().over(last_touch_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .withColumn(
        "attributed_channel",
        F.coalesce(F.col("channel"), F.lit("unattributed_direct_or_no_touch"))
    )
)

# Q3. Enrich with campaign metadata using a broadcast join.
result = (
    attributed.join(F.broadcast(campaigns), on="campaign_id", how="left")
    .select(
        "purchase_event_id", "customer_id", "purchase_ts", "revenue",
        "attributed_channel", "campaign_id", "campaign_name", "channel_group"
    )
    .orderBy("customer_id")
)

# Q4. Build a channel-level conversion and revenue report.
channel_report = (
    result.groupBy("attributed_channel")
    .agg(
        F.count("*").alias("conversions"),
        F.sum("revenue").alias("attributed_revenue"),
        F.countDistinct("customer_id").alias("unique_customers")
    )
    .orderBy(F.col("attributed_revenue").desc())
)

# Assertions: replay removed; each purchase has exactly one attribution row.
assert canonical.filter(F.col("event_id") == 8).count() == 1
assert purchases.count() == 5
assert result.count() == 5
assert result.select("purchase_event_id").distinct().count() == 5

by_customer = {
    row["customer_id"]: row.asDict()
    for row in result.select(
        "customer_id", "attributed_channel", "campaign_id", "revenue"
    ).collect()
}
assert by_customer[101]["attributed_channel"] == "email"
assert by_customer[101]["campaign_id"] == "nov"
assert by_customer[102]["attributed_channel"] == "social"
assert by_customer[103]["attributed_channel"] == "paid_search"
assert by_customer[104]["attributed_channel"] == "unattributed_direct_or_no_touch"
# Customer 105's email touch is outside the 30-day lookback.
assert by_customer[105]["attributed_channel"] == "unattributed_direct_or_no_touch"

print("Canonical event count:", canonical.count())
print("Purchase attribution:")
result.show(truncate=False)
print("Channel report:")
channel_report.show(truncate=False)

# Interview follow-ups:
# 1. How would attribution change for first-touch, linear, or position-based models?
# 2. How would you handle late-arriving touch events that arrive after a purchase?
# 3. What business timezone defines the 30-day lookback boundary?
# 4. How would you avoid the potentially expensive range join at very high volume?
# 5. How would you make the output idempotent when writing daily partitions?
#
# Optimization notes:
# - Deduplicate once before downstream branches; cache only if multiple actions
#   justify the storage cost, and unpersist after use.
# - Broadcast only a genuinely small campaign dimension.
# - The customer/time range join can shuffle heavily. For scale, pre-filter
#   touches by the reporting window, partition by customer where appropriate,
#   and evaluate a bounded sessionization/interval strategy.
# - Keep a deterministic tie-breaker for equal timestamps.
# - Validate the physical plan with result.explain("formatted").
result.explain("formatted")

spark.stop()
