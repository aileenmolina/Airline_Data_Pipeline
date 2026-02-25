import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType

# ============================================================
# SILVER BOOKINGS (FACT STAGING)
# ============================================================

# 1) Stage table (DLT-managed, no transformations)
@dlt.table(
    name="stage_bookings",
    comment="Raw bookings loaded from Bronze into a DLT-managed Silver staging table."
)
def stage_bookings():
    return (
        spark.readStream.format("delta")
            .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
    )


# 2) Transform view (cleaning + typing)
@dlt.view(
    name="transformed_bookings",
    comment="Cleaned and typed booking records with modifiedDate added."
)
def transformed_bookings():
    df = dlt.read("stage_bookings")

    df = (
        df.withColumn("amount", col("amount").cast(DoubleType()))
          .withColumn("booking_date", to_date(col("booking_date")))
          .withColumn("modifiedDate", current_timestamp())
          .drop("_rescued_data")
    )

    return df


# 3) Silver fact staging table (cleaned fact, NO CDC)
@dlt.table(
    name="silver_bookings",
    comment="Silver bookings fact staging table with data quality rules applied.",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop({
    "valid_booking_id": "booking_id IS NOT NULL",
    "valid_passenger_id": "passenger_id IS NOT NULL",
    "valid_modified_date": "modifiedDate IS NOT NULL"
})
def silver_bookings():
    return dlt.read("transformed_bookings")



# ============================================================
# SILVER FLIGHTS (DIMENSION WITH CDC)
# ============================================================

@dlt.view(
    name="transformed_flights",
    comment="Cleaned flight records with modifiedDate added."
)
def transformed_flights():
    df = (
        spark.readStream.format("delta")
            .load("/Volumes/workspace/bronze/bronzevolume/flights/data/")
    )

    df = (
        df.withColumn("flight_date", to_date(col("flight_date")))
          .withColumn("modifiedDate", current_timestamp())
          .drop("_rescued_data")
    )

    return df


dlt.create_streaming_table(
    name="silver_flights",
    comment="Silver flights dimension with CDC applied.",
    table_properties={"quality": "silver"},
    expect_all={
        "valid_flight_id": "flight_id IS NOT NULL",
        "valid_modified_date": "modifiedDate IS NOT NULL"
    }
)

dlt.create_auto_cdc_flow(
    target="silver_flights",
    source="transformed_flights",
    keys=["flight_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type=1
)



# ============================================================
# SILVER AIRPORTS (DIMENSION WITH CDC)
# ============================================================

@dlt.view(
    name="transformed_airports",
    comment="Cleaned airport records with modifiedDate added."
)
def transformed_airports():
    df = (
        spark.readStream.format("delta")
            .load("/Volumes/workspace/bronze/bronzevolume/airports/data/")
    )

    df = (
        df.withColumn("modifiedDate", current_timestamp())
          .drop("_rescued_data")
    )

    return df


dlt.create_streaming_table(
    name="silver_airports",
    comment="Silver airports dimension with CDC applied.",
    table_properties={"quality": "silver"},
    expect_all={
        "valid_airport_id": "airport_id IS NOT NULL",
        "valid_modified_date": "modifiedDate IS NOT NULL"
    }
)

dlt.create_auto_cdc_flow(
    target="silver_airports",
    source="transformed_airports",
    keys=["airport_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type=1
)



# ============================================================
# SILVER CUSTOMERS (DIMENSION WITH CDC)
# ============================================================

@dlt.view(
    name="transformed_customers",
    comment="Cleaned customer records with modifiedDate added."
)
def transformed_customers():
    df = (
        spark.readStream.format("delta")
            .load("/Volumes/workspace/bronze/bronzevolume/customers/data/")
    )

    df = (
        df.withColumn("modifiedDate", current_timestamp())
          .drop("_rescued_data")
    )

    return df


dlt.create_streaming_table(
    name="silver_customers",
    comment="Silver customers dimension with CDC applied.",
    table_properties={"quality": "silver"},
    expect_all={
        "valid_passenger_id": "passenger_id IS NOT NULL",
        "valid_modified_date": "modifiedDate IS NOT NULL"
    }
)

dlt.create_auto_cdc_flow(
    target="silver_customers",
    source="transformed_customers",
    keys=["passenger_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type=1
)
