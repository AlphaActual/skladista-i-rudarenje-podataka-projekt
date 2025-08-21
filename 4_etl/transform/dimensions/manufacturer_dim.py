from pyspark.sql.functions import col, lit, trim, initcap, current_timestamp, when, isnotnull
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def transform_manufacturer_dim(manufacturer_df, country_df, region_df, csv_cars_df=None):
    """
    Transform manufacturer dimension with SCD Type 2 implementation
    Includes manufacturer, country and region information
    """
    
    # Aliases
    m = manufacturer_df.alias("m")
    c = country_df.alias("c")
    r = region_df.alias("r")

    # Join database tables to get complete manufacturer information
    merged_df = (
        m.join(c, col("m.country_id") == col("c.country_id"), "left")
         .join(r, col("c.region_id") == col("r.region_id"), "left")
         .select(
             col("m.manufacturer_id").alias("manufacturer_id"),
             trim(col("m.name")).alias("name"),
             trim(col("c.name")).alias("country"),
             trim(col("r.name")).alias("region")
         )
         .withColumn("name", initcap(trim(col("name"))))
         .withColumn("country", initcap(trim(col("country"))))
         .withColumn("region", initcap(trim(col("region"))))
         .fillna({"country": "Unknown", "region": "Unknown"})
    )

    # If CSV data exists, extract manufacturer information from it
    if csv_cars_df:
        # Extract unique manufacturers from CSV
        csv_manufacturer_df = (
            csv_cars_df
            .selectExpr("manufacturer as name")
            .withColumn("name", initcap(trim(col("name"))))
            .dropDuplicates()
            .withColumn("manufacturer_id", lit(None).cast("int"))
            .withColumn("country", lit("Unknown"))
            .withColumn("region", lit("Unknown"))
        )

        # Union with database data
        merged_df = merged_df.select("manufacturer_id", "name", "country", "region") \
                             .unionByName(csv_manufacturer_df) \
                             .dropDuplicates(["name"])

    # Add SCD Type 2 columns
    merged_df = merged_df.withColumn("version", lit(1)) \
                         .withColumn("date_from", current_timestamp()) \
                         .withColumn("date_to", lit(None).cast("timestamp")) \
                         .withColumn("is_current", lit(True))

    # Add surrogate key using row_number
    window = Window.orderBy("region", "country", "name")
    merged_df = merged_df.withColumn("manufacturer_tk", row_number().over(window))

    # Reorder columns according to star schema design
    final_df = merged_df.select(
        "manufacturer_tk",
        "version",
        "date_from",
        "date_to",
        "manufacturer_id",
        "name",
        "country",
        "region",
        "is_current"
    )

    return final_df
