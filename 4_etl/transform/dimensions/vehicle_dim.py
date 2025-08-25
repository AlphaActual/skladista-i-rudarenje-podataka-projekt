from pyspark.sql.functions import col, lit, trim, initcap, current_timestamp, when
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def transform_vehicle_dim(model_df, manufacturer_df, csv_cars_df=None):
    """
    Transform vehicle dimension with SCD Type 2 implementation
    Simplified to avoid Cartesian product - only includes model information
    Categories will be determined dynamically in fact table based on actual values
    """
    
    # Aliases
    mod = model_df.alias("mod")
    man = manufacturer_df.alias("man")

    # Join database tables to get basic vehicle information (NO cross joins)
    merged_df = (
        mod.join(man, col("mod.manufacturer_id") == col("man.manufacturer_id"), "left")
           .select(
               col("mod.model_id").alias("vehicle_id"),
               trim(col("mod.name")).alias("model_name"),
               trim(col("man.name")).alias("manufacturer_name")
           )
           .withColumn("model_name", initcap(trim(col("model_name"))))
           .withColumn("manufacturer_name", initcap(trim(col("manufacturer_name"))))
           .dropDuplicates()
    )

    # If CSV data exists, extract vehicle information from it
    if csv_cars_df:
        # Extract unique models from CSV
        csv_vehicle_df = (
            csv_cars_df
            .select(
                col("manufacturer").alias("manufacturer_name"),
                col("model").alias("model_name")
            )
            .withColumn("model_name", initcap(trim(col("model_name"))))
            .withColumn("manufacturer_name", initcap(trim(col("manufacturer_name"))))
            .dropDuplicates()
            .withColumn("vehicle_id", lit(None).cast("int"))
        )

        # Union with database data
        merged_df = merged_df.select("vehicle_id", "model_name", "manufacturer_name") \
                             .unionByName(csv_vehicle_df) \
                             .dropDuplicates(["model_name", "manufacturer_name"])

    # Add SCD Type 2 columns
    merged_df = merged_df.withColumn("version", lit(1)) \
                         .withColumn("date_from", current_timestamp()) \
                         .withColumn("date_to", lit(None).cast("timestamp")) \
                         .withColumn("is_current", lit(True))

    # Add surrogate key using row_number
    window = Window.orderBy("model_name", "manufacturer_name")
    merged_df = merged_df.withColumn("vehicle_tk", row_number().over(window))

    # Reorder columns according to simplified star schema design
    final_df = merged_df.select(
        "vehicle_tk",
        "version",
        "date_from",
        "date_to",
        "vehicle_id",
        "model_name",
        "manufacturer_name",
        "is_current"
    )

    return final_df
