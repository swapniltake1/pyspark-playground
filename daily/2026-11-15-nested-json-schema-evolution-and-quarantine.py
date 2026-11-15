"""
Daily PySpark Interview Practice — 2026-11-15
Topic: Nested JSON parsing, producer schema evolution, and malformed-record quarantine

Problem
-------
An orders pipeline receives JSON events from two producer versions. Parse both
contracts, preserve compatible records, quarantine malformed or semantically
invalid events with reason codes, and calculate order-level item metrics.

Run:
    spark-submit daily/2026-11-15-nested-json-schema-evolution-and-quarantine.py

Requirements: PySpark 3.1+ (for higher-order array functions and unionByName with
allowMissingColumns=True). No external files or services are required.
"""
import json
from decimal import Decimal

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.storagelevel import StorageLevel


AMOUNT_TYPE = DecimalType(18, 2)

ITEM_SCHEMA = StructType([
    StructField("sku", StringType(), True),
    StructField("qty", IntegerType(), True),
    StructField("unit_price", DecimalType(12, 2), True),
])

DEVICE_SCHEMA = StructType([
    StructField("os", StringType(), True),
    StructField("browser", StringType(), True),
])

# Version 1 did not send coupon_code.
EVENT_SCHEMA_V1 = StructType([
    StructField("event_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("currency", StringType(), True),
    StructField("items", ArrayType(ITEM_SCHEMA, containsNull=True), True),
    StructField("device", DEVICE_SCHEMA, True),
    StructField("_corrupt_record", StringType(), True),
])

# Version 2 adds an optional coupon_code. Both versions retain the common fields.
EVENT_SCHEMA_V2 = StructType([
    StructField("event_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("currency", StringType(), True),
    StructField("items", ArrayType(ITEM_SCHEMA, containsNull=True), True),
    StructField("device", DEVICE_SCHEMA, True),
    StructField("coupon_code", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])


def make_payload(event):
    return json.dumps(event, separators=(",", ":"))


def build_raw_events(spark):
    valid_v1 = {
        "event_id": "EV-100",
        "order_id": "ORD-100",
        "customer_id": "CUS-10",
        "event_ts": "2026-11-15T09:58:00",
        "currency": "INR",
        "items": [
            {"sku": "SKU-A", "qty": 2, "unit_price": 10.50},
            {"sku": "SKU-B", "qty": 1, "unit_price": 4.00},
        ],
        "device": {"os": "Android", "browser": "Chrome"},
    }
    valid_v2 = {
        "event_id": "EV-101",
        "order_id": "ORD-101",
        "customer_id": "CUS-11",
        "event_ts": "2026-11-15T09:59:00",
        "currency": "INR",
        "items": [
            {"sku": "SKU-C", "qty": 3, "unit_price": 7.25},
        ],
        "device": {"os": "iOS", "browser": "Safari"},
        "coupon_code": "SAVE5",
        # This producer field is intentionally not in the declared contract.
        "app_build": "8.4.1",
    }
    invalid_item_v2 = {
        "event_id": "EV-103",
        "order_id": "ORD-103",
        "customer_id": "CUS-13",
        "event_ts": "2026-11-15T10:01:00",
        "currency": "INR",
        "items": [{"sku": "SKU-X", "qty": -2, "unit_price": 3.50}],
        "coupon_code": None,
    }
    missing_id_v2 = {
        "order_id": "ORD-105",
        "customer_id": "CUS-15",
        "event_ts": "2026-11-15T10:03:00",
        "currency": "INR",
        "items": [{"sku": "SKU-D", "qty": 1, "unit_price": 9.00}],
    }
    missing_items_v2 = {
        "event_id": "EV-106",
        "order_id": "ORD-106",
        "customer_id": "CUS-16",
        "event_ts": "2026-11-15T10:04:00",
        "currency": "INR",
        "items": None,
    }
    empty_items_v1 = {
        "event_id": "EV-104",
        "order_id": "ORD-104",
        "customer_id": "CUS-14",
        "event_ts": "2026-11-15T10:02:00",
        "currency": "INR",
        "items": [],
    }

    rows = [
        ("RAW-100", "v1", "2026-11-15 10:00:01", make_payload(valid_v1)),
        ("RAW-101", "v2", "2026-11-15 10:00:02", make_payload(valid_v2)),
        (
            "RAW-102", "v1", "2026-11-15 10:00:03",
            '{"event_id":"EV-102","order_id":"ORD-102","items":[{"sku":"SKU-Z","qty":2}',
        ),
        ("RAW-103", "v2", "2026-11-15 10:00:04", make_payload(invalid_item_v2)),
        ("RAW-104", "v1", "2026-11-15 10:00:05", make_payload(empty_items_v1)),
        ("RAW-105", "v2", "2026-11-15 10:00:06", make_payload(missing_id_v2)),
        ("RAW-106", "v2", "2026-11-15 10:00:07", make_payload(missing_items_v2)),
    ]
    return (
        spark.createDataFrame(
            rows,
            ["raw_record_id", "source_version", "ingested_at_text", "payload"],
        )
        .withColumn("ingested_at", F.to_timestamp("ingested_at_text"))
        .drop("ingested_at_text")
    )


def parse_one_version(raw, version, schema):
    # Filter by producer version before invoking from_json with that version's schema.
    return (
        raw.where(F.col("source_version") == version)
        .withColumn(
            "parsed",
            F.from_json(
                F.col("payload"),
                schema,
                {
                    "mode": "PERMISSIVE",
                    "columnNameOfCorruptRecord": "_corrupt_record",
                },
            ),
        )
        .select("raw_record_id", "source_version", "ingested_at", "parsed.*")
    )


def classify_events(parsed):
    malformed = F.col("_corrupt_record").isNotNull()
    invalid_item = F.coalesce(
        F.exists(
            F.col("items"),
            lambda item: (
                item.isNull()
                | item["sku"].isNull()
                | item["qty"].isNull()
                | (item["qty"] <= 0)
                | item["unit_price"].isNull()
                | (item["unit_price"] < F.lit(0))
            ),
        ),
        F.lit(False),
    )

    # Retain every applicable reason, rather than hiding secondary failures
    # behind a first-match CASE expression.
    reasons = F.filter(
        F.array(
            F.when(malformed, F.lit("MALFORMED_JSON")),
            F.when(F.col("event_id").isNull(), F.lit("MISSING_EVENT_ID")),
            F.when(F.col("order_id").isNull(), F.lit("MISSING_ORDER_ID")),
            F.when(F.col("customer_id").isNull(), F.lit("MISSING_CUSTOMER_ID")),
            F.when(F.col("event_ts").isNull(), F.lit("MISSING_OR_INVALID_EVENT_TS")),
            F.when(F.col("items").isNull(), F.lit("MISSING_ITEMS_ARRAY")),
            F.when(invalid_item, F.lit("INVALID_ITEM")),
            F.when(
                F.col("currency").isNull()
                | (~F.col("currency").isin("INR", "USD")),
                F.lit("UNSUPPORTED_CURRENCY"),
            ),
        ),
        lambda reason: reason.isNotNull(),
    )

    return parsed.withColumn("reason_codes", F.concat_ws("|", reasons))


def item_metrics_exploded(accepted):
    """Readable baseline: explode to item grain, aggregate, then restore event grain."""
    zero_amount = F.lit(Decimal("0.00")).cast(AMOUNT_TYPE)
    return (
        accepted
        .select(
            "raw_record_id",
            "event_id",
            F.explode_outer("items").alias("item"),
        )
        .groupBy("raw_record_id", "event_id")
        .agg(
            F.coalesce(
                F.sum(
                    (F.col("item.qty") * F.col("item.unit_price")).cast(AMOUNT_TYPE)
                ).cast(AMOUNT_TYPE),
                zero_amount,
            ).alias("order_total"),
            F.coalesce(F.sum(F.col("item.qty")), F.lit(0))
            .cast("long")
            .alias("total_units"),
            F.count(F.col("item.sku")).alias("line_count"),
        )
    )


def item_metrics_in_array(accepted):
    """
    Optimized order-grain path: higher-order aggregate computes totals inside
    each items array, avoiding explode + groupBy + shuffle.
    """
    zero_amount = F.lit(Decimal("0.00")).cast(AMOUNT_TYPE)
    zero_units = F.lit(0).cast("long")

    order_total = F.aggregate(
        F.col("items"),
        zero_amount,
        lambda acc, item: (
            acc + (item["unit_price"] * item["qty"]).cast(AMOUNT_TYPE)
        ).cast(AMOUNT_TYPE),
    )
    total_units = F.aggregate(
        F.col("items"),
        zero_units,
        lambda acc, item: (acc + item["qty"].cast("long")).cast("long"),
    )

    return accepted.select(
        "raw_record_id",
        "event_id",
        order_total.alias("order_total"),
        total_units.alias("total_units"),
        F.size("items").cast("long").alias("line_count"),
    )


def metric_rows(df):
    return [
        (
            row["raw_record_id"],
            row["order_total"],
            int(row["total_units"]),
            int(row["line_count"]),
        )
        for row in df.orderBy("raw_record_id").collect()
    ]


def main():
    spark = (
        SparkSession.builder
        .appName("NestedJsonSchemaEvolutionAndQuarantine")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    raw = build_raw_events(spark)

    v1 = parse_one_version(raw, "v1", EVENT_SCHEMA_V1)
    v2 = parse_one_version(raw, "v2", EVENT_SCHEMA_V2)

    # Union by column name makes the V1-only missing coupon_code become NULL.
    parsed = v1.unionByName(v2, allowMissingColumns=True)
    classified = classify_events(parsed)

    accepted = classified.where(F.col("reason_codes") == "")
    quarantine = classified.where(F.col("reason_codes") != "")

    # Expected counts: 3 accepted (including one empty items array), 4 quarantined.
    assert accepted.count() == 3
    assert quarantine.count() == 4

    # Version 1 has no coupon_code; the evolved union supplies a NULL value.
    v1_coupon = (
        parsed.where(F.col("raw_record_id") == "RAW-100")
        .select("coupon_code")
        .first()["coupon_code"]
    )
    assert v1_coupon is None

    v2_coupon = (
        parsed.where(F.col("raw_record_id") == "RAW-101")
        .select("coupon_code")
        .first()["coupon_code"]
    )
    assert v2_coupon == "SAVE5"

    quarantine_reasons = {
        row["raw_record_id"]: row["reason_codes"].split("|")
        for row in quarantine.select("raw_record_id", "reason_codes").collect()
    }
    assert "MALFORMED_JSON" in quarantine_reasons["RAW-102"]
    assert "INVALID_ITEM" in quarantine_reasons["RAW-103"]
    assert "MISSING_EVENT_ID" in quarantine_reasons["RAW-105"]
    assert "MISSING_ITEMS_ARRAY" in quarantine_reasons["RAW-106"]

    # Baseline and optimized implementations must produce identical event-grain rows.
    baseline = item_metrics_exploded(accepted)
    optimized = item_metrics_in_array(accepted)
    assert metric_rows(baseline) == metric_rows(optimized)

    expected = {
        "RAW-100": (Decimal("25.00"), 3, 2),
        "RAW-101": (Decimal("21.75"), 3, 1),
        "RAW-104": (Decimal("0.00"), 0, 0),
    }
    actual = {
        row["raw_record_id"]: (
            row["order_total"],
            int(row["total_units"]),
            int(row["line_count"]),
        )
        for row in optimized.collect()
    }
    assert actual == expected, f"Unexpected item metrics: {actual}"

    # The unknown V2 field app_build is not part of the declared schema and is
    # therefore ignored. Preserve the raw payload or evolve the contract if needed.
    assert "app_build" not in parsed.columns

    print("Accepted events:")
    accepted.select(
        "raw_record_id", "event_id", "source_version", "coupon_code"
    ).orderBy("raw_record_id").show(truncate=False)

    print("Quarantined events and all applicable reason codes:")
    quarantine.select("raw_record_id", "reason_codes").orderBy(
        "raw_record_id"
    ).show(truncate=False)

    print("Order metrics from the optimized in-array aggregation:")
    optimized.orderBy("raw_record_id").show(truncate=False)

    print("Baseline physical plan (explode + aggregation):")
    baseline.explain("formatted")

    print("Optimized physical plan (higher-order array aggregation):")
    optimized.explain("formatted")

    print("All assertions passed.")

    # This example intentionally does not cache the parsed DataFrame: it is a
    # small, one-pass workload. In production, persist a narrow normalized
    # projection only when multiple downstream actions reuse it, then unpersist.
    spark.stop()


if __name__ == "__main__":
    main()


"""
Expected result summary
-----------------------
Accepted:
  RAW-100 / EV-100 / V1 / coupon_code NULL
  RAW-101 / EV-101 / V2 / coupon_code SAVE5
  RAW-104 / EV-104 / V1 / coupon_code NULL

Quarantine:
  RAW-102 -> MALFORMED_JSON (and any missing-field reasons detected)
  RAW-103 -> INVALID_ITEM
  RAW-105 -> MISSING_EVENT_ID
  RAW-106 -> MISSING_ITEMS_ARRAY

Order metrics:
  RAW-100 -> INR 25.00, 3 units, 2 lines
  RAW-101 -> INR 21.75, 3 units, 1 line
  RAW-104 -> INR 0.00, 0 units, 0 lines

Interview follow-ups
--------------------
1. What is the difference between PERMISSIVE, DROPMALFORMED, and FAILFAST parsing?
2. Why must the corrupt-record column be included in the supplied schema?
3. How would a schema registry and compatibility rules govern producer evolution?
4. What happens to unknown JSON fields when from_json uses an explicit schema?
5. When is explode necessary, and when can higher-order array functions avoid a shuffle?
6. How would you retain raw payloads and quarantine metadata for replay and audit?
7. How would you handle timestamp formats, time zones, and producer-specific contracts?

Optimization notes
------------------
- Filter by producer version before parsing so each row is parsed with its contract.
- Use unionByName(allowMissingColumns=True) for additive optional fields.
- Use native Spark expressions; avoid Python UDFs for parsing and validation.
- Keep the event grain for order-level metrics. explode + groupBy is useful when
  item-grain output is required, but it introduces row expansion and a shuffle.
- For repeated downstream consumers, consider persisting a narrow normalized
  projection; do not cache a one-pass DataFrame by default.
- Explicit schemas make parsing predictable. Unknown fields are not automatically
  rescued; preserve the raw payload or use a deliberate schema-rescue strategy.
"""
