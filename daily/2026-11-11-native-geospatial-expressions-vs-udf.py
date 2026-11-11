"""
Daily PySpark Interview Practice — 2026-11-11
Topic: Geospatial distance calculation — native Spark expressions vs Python UDF

Problem
--------
For each delivery, enrich its hub coordinates and service radius, calculate
great-circle distance to the destination, and classify whether it is within
the hub's service radius. Handle missing hubs and invalid/null coordinates.

Run:
    spark-submit daily/2026-11-11-native-geospatial-expressions-vs-udf.py

Compatible with PySpark 3.x. No external files or third-party Python packages
are required.
"""
from math import asin, cos, radians, sin, sqrt
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)


EARTH_RADIUS_KM = 6371.0088


def haversine_python(hub_lat, hub_lon, dest_lat, dest_lon):
    """Reference implementation used to benchmark correctness, not production."""
    values = (hub_lat, hub_lon, dest_lat, dest_lon)
    if any(value is None for value in values):
        return None
    if not (-90 <= hub_lat <= 90 and -90 <= dest_lat <= 90):
        return None
    if not (-180 <= hub_lon <= 180 and -180 <= dest_lon <= 180):
        return None

    lat1, lat2 = radians(hub_lat), radians(dest_lat)
    delta_lat = lat2 - lat1
    delta_lon = radians(dest_lon - hub_lon)
    a = (
        sin(delta_lat / 2) ** 2
        + cos(lat1) * cos(lat2) * sin(delta_lon / 2) ** 2
    )
    # Clamp against floating-point drift before asin.
    a = min(1.0, max(0.0, a))
    return 2 * EARTH_RADIUS_KM * asin(sqrt(a))


def native_haversine_expression():
    """Equivalent calculation using Spark SQL expressions only."""
    lat1 = F.radians(F.col("hub_lat"))
    lat2 = F.radians(F.col("destination_lat"))
    delta_lat = lat2 - lat1
    delta_lon = F.radians(F.col("destination_lon") - F.col("hub_lon"))
    a = (
        F.pow(F.sin(delta_lat / F.lit(2.0)), 2)
        + F.cos(lat1) * F.cos(lat2)
        * F.pow(F.sin(delta_lon / F.lit(2.0)), 2)
    )
    # Numerical clamp protects sqrt/asin from tiny floating-point overshoots.
    a = F.least(F.lit(1.0), F.greatest(F.lit(0.0), a))
    return F.lit(2.0 * EARTH_RADIUS_KM) * F.asin(F.sqrt(a))


def build_delivery_features(shipments, hubs, optimized):
    """
    optimized=False: reference path without explicit broadcast/repartition hints.
    optimized=True: narrow projection + broadcast small hub dimension + native
                    Catalyst expressions (no Python worker serialization).
    """
    shipment_columns = [
        "shipment_id", "hub_id", "destination_lat", "destination_lon"
    ]
    hub_columns = ["hub_id", "hub_lat", "hub_lon", "service_radius_km"]

    if optimized:
        shipment_input = shipments.select(*shipment_columns)
        hub_input = hubs.select(*hub_columns)
        joined = shipment_input.join(F.broadcast(hub_input), "hub_id", "left")
    else:
        joined = shipments.join(hubs, "hub_id", "left")

    coordinates_valid = (
        F.col("hub_lat").between(-90.0, 90.0)
        & F.col("hub_lon").between(-180.0, 180.0)
        & F.col("destination_lat").between(-90.0, 90.0)
        & F.col("destination_lon").between(-180.0, 180.0)
    )

    if optimized:
        distance = native_haversine_expression()
        result = joined.withColumn(
            "distance_km",
            F.when(coordinates_valid, F.round(distance, 3))
        )
    else:
        distance_udf = F.udf(haversine_python, DoubleType())
        result = joined.withColumn(
            "distance_km",
            F.round(
                distance_udf(
                    "hub_lat", "hub_lon", "destination_lat", "destination_lon"
                ),
                3
            )
        )

    return (
        result
        .withColumn(
            "serviceability_status",
            F.when(F.col("hub_lat").isNull(), F.lit("UNKNOWN_HUB"))
             .when(~coordinates_valid, F.lit("INVALID_COORDINATES"))
             .when(F.col("distance_km") <= F.col("service_radius_km"),
                   F.lit("WITHIN_RADIUS"))
             .otherwise(F.lit("OUTSIDE_RADIUS"))
        )
        .select(
            "shipment_id", "hub_id", "distance_km",
            "service_radius_km", "serviceability_status"
        )
    )


def main():
    spark = (
        SparkSession.builder
        .appName("NativeGeospatialExpressionsVsUDF")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    shipment_schema = StructType([
        StructField("shipment_id", StringType(), False),
        StructField("hub_id", StringType(), False),
        StructField("destination_lat", DoubleType(), True),
        StructField("destination_lon", DoubleType(), True),
    ])
    shipment_rows = [
        ("SHP-101", "H-PUN", 18.5204, 73.8567),  # Same as Pune hub
        ("SHP-102", "H-PUN", 19.0760, 72.8777),  # Mumbai, outside 25 km
        ("SHP-103", "H-NSK", 19.9975, 73.7898),  # Same as Nashik hub
        ("SHP-104", "H-NSK", 20.0110, 73.7900),  # Nearby Nashik destination
        ("SHP-105", "H-NSK", None, None),        # Missing destination
        ("SHP-106", "H-PUN", 91.0, 73.0),        # Invalid latitude
        ("SHP-107", "H-UNKNOWN", 18.5204, 73.8567),  # Missing hub dimension
    ]
    shipments = spark.createDataFrame(shipment_rows, shipment_schema)

    hub_schema = StructType([
        StructField("hub_id", StringType(), False),
        StructField("hub_lat", DoubleType(), False),
        StructField("hub_lon", DoubleType(), False),
        StructField("service_radius_km", DoubleType(), False),
    ])
    hub_rows = [
        ("H-PUN", 18.5204, 73.8567, 25.0),
        ("H-NSK", 19.9975, 73.7898, 15.0),
    ]
    hubs = spark.createDataFrame(hub_rows, hub_schema)

    # Data contract checks: duplicate hub keys would multiply shipment rows.
    assert hubs.groupBy("hub_id").count().where(F.col("count") > 1).count() == 0
    assert shipments.groupBy("shipment_id").count().where(F.col("count") > 1).count() == 0

    # Q1. Build a correct reference result with a regular Python UDF.
    reference = build_delivery_features(shipments, hubs, optimized=False)

    # Q2. Build the production-oriented result with native expressions and a
    #     broadcast join. Spark can optimize native expressions in Catalyst.
    optimized = build_delivery_features(shipments, hubs, optimized=True)

    # Q3. Prove both implementations agree, including NULL behavior.
    comparison = (
        reference.select("shipment_id", F.col("distance_km").alias("udf_km"))
        .join(
            optimized.select("shipment_id", F.col("distance_km").alias("native_km")),
            "shipment_id"
        )
    )
    mismatches = comparison.where(
        F.col("udf_km").isNull() != F.col("native_km").isNull()
    ).unionByName(
        comparison.where(
            F.col("udf_km").isNotNull()
            & F.col("native_km").isNotNull()
            & (F.abs(F.col("udf_km") - F.col("native_km")) > F.lit(0.001))
        )
    )
    assert mismatches.count() == 0

    # Q4. Validate the business classifications and important edge cases.
    actual = {
        row["shipment_id"]: row
        for row in optimized.collect()
    }
    expected_status = {
        "SHP-101": "WITHIN_RADIUS",
        "SHP-102": "OUTSIDE_RADIUS",
        "SHP-103": "WITHIN_RADIUS",
        "SHP-104": "WITHIN_RADIUS",
        "SHP-105": "INVALID_COORDINATES",
        "SHP-106": "INVALID_COORDINATES",
        "SHP-107": "UNKNOWN_HUB",
    }
    assert {key: value["serviceability_status"] for key, value in actual.items()} == expected_status
    assert actual["SHP-101"]["distance_km"] == 0.0
    assert 120.0 < actual["SHP-102"]["distance_km"] < 122.0
    assert actual["SHP-104"]["distance_km"] < 2.0
    assert actual["SHP-105"]["distance_km"] is None
    assert actual["SHP-107"]["distance_km"] is None

    # Q5. Aggregate serviceability outcomes without collecting detail rows.
    status_counts = {
        row["serviceability_status"]: row["count"]
        for row in optimized.groupBy("serviceability_status").count().collect()
    }
    assert status_counts == {
        "WITHIN_RADIUS": 3,
        "OUTSIDE_RADIUS": 1,
        "INVALID_COORDINATES": 2,
        "UNKNOWN_HUB": 1,
    }

    print("Optimized delivery features:")
    optimized.orderBy("shipment_id").show(truncate=False)
    print("Serviceability counts:")
    optimized.groupBy("serviceability_status").count().orderBy(
        "serviceability_status"
    ).show(truncate=False)

    print("Optimized physical plan (look for BroadcastHashJoin and native expressions):")
    optimized.explain("formatted")

    print("\nInterview follow-ups:")
    print("1. Why are native Spark expressions usually faster than a regular Python UDF?")
    print("2. When would a pandas UDF be appropriate, and what Arrow constraints apply?")
    print("3. When is broadcasting a dimension unsafe or too memory-intensive?")
    print("4. How would you handle skew when one hub owns most shipments?")
    print("5. Why is great-circle distance different from road-route distance?")
    print("6. How would you test coordinate order, antimeridian crossings, and polar locations?")
    print("7. How would you monitor UDF time, shuffle read, broadcast size, and executor memory?")

    spark.stop()


if __name__ == "__main__":
    main()
