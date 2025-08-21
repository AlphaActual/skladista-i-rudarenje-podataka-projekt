from pyspark.sql.functions import col, lit, trim, initcap, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def transform_location_dim(country_df, region_df):
    """
    Transform location dimension with SCD Type 1 implementation
    Geographic information that changes rarely
    """
    
    # Aliases
    c = country_df.alias("c")
    r = region_df.alias("r")

    # Join country and region information
    merged_df = (
        c.join(r, col("c.region_id") == col("r.region_id"), "left")
         .select(
             col("c.country_id").alias("location_id"),
             trim(col("c.name")).alias("country"),
             trim(col("r.name")).alias("region")
         )
         .withColumn("country", initcap(trim(col("country"))))
         .withColumn("region", initcap(trim(col("region"))))
         .fillna({"region": "Unknown"})
         .dropDuplicates()
    )

    # Add SCD Type 1 columns (last_updated for audit trail)
    merged_df = merged_df.withColumn("last_updated", current_timestamp())

    # Add surrogate key using row_number
    window = Window.orderBy("region", "country")
    merged_df = merged_df.withColumn("location_tk", row_number().over(window))

    # Reorder columns according to star schema design
    final_df = merged_df.select(
        "location_tk",
        "location_id",
        "country",
        "region",
        "last_updated"
    )

    return final_df
