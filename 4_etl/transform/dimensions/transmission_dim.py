from pyspark.sql.functions import col, lit, trim, initcap, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def transform_transmission_dim(transmission_type_df, csv_cars_df=None):
    """
    Transform transmission dimension with SCD Type 1 implementation
    Simple transmission types that rarely change
    """
    
    # Process database transmission types
    merged_df = (
        transmission_type_df
        .select(
            col("transmission_id"),
            trim(col("type")).alias("type")
        )
        .withColumn("type", initcap(trim(col("type"))))
        .dropDuplicates()
    )

    # If CSV data exists, extract transmission types from it
    if csv_cars_df:
        # Extract unique transmission types from CSV
        csv_transmission_df = (
            csv_cars_df
            .selectExpr("transmission as type")
            .withColumn("type", initcap(trim(col("type"))))
            .dropDuplicates()
            .withColumn("transmission_id", lit(None).cast("int"))
        )

        # Union with database data
        merged_df = merged_df.select("transmission_id", "type") \
                             .unionByName(csv_transmission_df) \
                             .dropDuplicates(["type"])

    # Add SCD Type 1 columns (last_updated for audit trail)
    merged_df = merged_df.withColumn("last_updated", current_timestamp())

    # Add surrogate key using row_number
    window = Window.orderBy("type")
    merged_df = merged_df.withColumn("transmission_tk", row_number().over(window))

    # Reorder columns according to star schema design
    final_df = merged_df.select(
        "transmission_tk",
        "transmission_id",
        "type",
        "last_updated"
    )

    return final_df
