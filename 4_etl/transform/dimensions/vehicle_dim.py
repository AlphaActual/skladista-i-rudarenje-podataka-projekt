from pyspark.sql.functions import col, lit, trim, initcap, current_timestamp, when
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def transform_vehicle_dim(model_df, manufacturer_df, mileage_category_df, engine_size_class_df, age_category_df, csv_cars_df=None):
    """
    Transform vehicle dimension with SCD Type 2 implementation
    Includes model information with categories
    """
    
    # Aliases
    mod = model_df.alias("mod")
    man = manufacturer_df.alias("man")
    mil = mileage_category_df.alias("mil")
    eng = engine_size_class_df.alias("eng")
    age = age_category_df.alias("age")

    # Join database tables to get complete vehicle information
    merged_df = (
        mod.join(man, col("mod.manufacturer_id") == col("man.manufacturer_id"), "left")
           .join(mil, lit(1) == lit(1), "cross")  # Cross join for now - will be matched in fact table
           .join(eng, lit(1) == lit(1), "cross")  # Cross join for now - will be matched in fact table  
           .join(age, lit(1) == lit(1), "cross")  # Cross join for now - will be matched in fact table
           .select(
               col("mod.model_id").alias("vehicle_id"),
               trim(col("mod.name")).alias("model_name"),
               trim(col("mil.category")).alias("mileage_category"),
               trim(col("eng.size_class")).alias("engine_size_class"),
               trim(col("age.category")).alias("age_category")
           )
           .withColumn("model_name", initcap(trim(col("model_name"))))
           .withColumn("mileage_category", initcap(trim(col("mileage_category"))))
           .withColumn("engine_size_class", initcap(trim(col("engine_size_class"))))
           .withColumn("age_category", initcap(trim(col("age_category"))))
           .dropDuplicates()
    )

    # If CSV data exists, extract vehicle information from it
    if csv_cars_df:
        # Extract unique models from CSV with categories
        csv_vehicle_df = (
            csv_cars_df
            .selectExpr("model as model_name")
            .withColumn("model_name", initcap(trim(col("model_name"))))
            .dropDuplicates()
            .withColumn("vehicle_id", lit(None).cast("int"))
            .withColumn("mileage_category", lit("Unknown"))
            .withColumn("engine_size_class", lit("Unknown"))
            .withColumn("age_category", lit("Unknown"))
        )

        # Union with database data
        merged_df = merged_df.select("vehicle_id", "model_name", "mileage_category", "engine_size_class", "age_category") \
                             .unionByName(csv_vehicle_df) \
                             .dropDuplicates(["model_name", "mileage_category", "engine_size_class", "age_category"])

    # Add SCD Type 2 columns
    merged_df = merged_df.withColumn("version", lit(1)) \
                         .withColumn("date_from", current_timestamp()) \
                         .withColumn("date_to", lit(None).cast("timestamp")) \
                         .withColumn("is_current", lit(True))

    # Add surrogate key using row_number
    window = Window.orderBy("model_name", "mileage_category", "engine_size_class", "age_category")
    merged_df = merged_df.withColumn("vehicle_tk", row_number().over(window))

    # Reorder columns according to star schema design
    final_df = merged_df.select(
        "vehicle_tk",
        "version",
        "date_from",
        "date_to",
        "vehicle_id",
        "model_name",
        "mileage_category",
        "engine_size_class",
        "age_category",
        "is_current"
    )

    return final_df
