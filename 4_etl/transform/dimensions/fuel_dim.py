from pyspark.sql.functions import col, lit, trim, initcap, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def transform_fuel_dim(fuel_type_df, csv_cars_df=None):
    """
    Transform fuel dimension with SCD Type 2 implementation
    Track evolution of fuel technologies through time
    """
    
    # Process database fuel types
    merged_df = (
        fuel_type_df
        .select(
            col("fuel_id"),
            trim(col("type")).alias("type")
        )
        .withColumn("type", initcap(trim(col("type"))))
        .dropDuplicates()
    )

    # If CSV data exists, extract fuel types from it
    if csv_cars_df:
        # Extract unique fuel types from CSV
        csv_fuel_df = (
            csv_cars_df
            .selectExpr("fuelType as type")
            .withColumn("type", initcap(trim(col("type"))))
            .dropDuplicates()
            .withColumn("fuel_id", lit(None).cast("int"))
        )

        # Union with database data
        merged_df = merged_df.select("fuel_id", "type") \
                             .unionByName(csv_fuel_df) \
                             .dropDuplicates(["type"])

    # Add SCD Type 2 columns
    merged_df = merged_df.withColumn("version", lit(1)) \
                         .withColumn("date_from", current_timestamp()) \
                         .withColumn("date_to", lit(None).cast("timestamp")) \
                         .withColumn("is_current", lit(True))

    # Add surrogate key using row_number
    window = Window.orderBy("type")
    merged_df = merged_df.withColumn("fuel_tk", row_number().over(window))

    # Reorder columns according to star schema design
    final_df = merged_df.select(
        "fuel_tk",
        "version",
        "date_from",
        "date_to",
        "fuel_id",
        "type",
        "is_current"
    )

    return final_df
