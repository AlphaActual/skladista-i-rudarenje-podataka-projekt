from pyspark.sql.functions import col, lit, trim, initcap, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def transform_mileage_category_dim(mileage_category_df):
    """
    Transform mileage category dimension
    """
    
    # Clean and prepare the dimension
    mileage_dim = (
        mileage_category_df
        .select(
            col("mileage_category_id"),
            trim(col("category")).alias("mileage_category")
        )
        .withColumn("mileage_category", initcap(trim(col("mileage_category"))))
        .dropDuplicates()
    )
    
    # Add SCD Type 2 columns
    mileage_dim = mileage_dim.withColumn("version", lit(1)) \
                             .withColumn("date_from", current_timestamp()) \
                             .withColumn("date_to", lit(None).cast("timestamp")) \
                             .withColumn("is_current", lit(True))
    
    # Add surrogate key
    window = Window.orderBy("mileage_category")
    mileage_dim = mileage_dim.withColumn("mileage_category_tk", row_number().over(window))
    
    # Final structure
    final_df = mileage_dim.select(
        "mileage_category_tk",
        "version",
        "date_from", 
        "date_to",
        "mileage_category_id",
        "mileage_category",
        "is_current"
    )
    
    return final_df

def transform_engine_size_class_dim(engine_size_class_df):
    """
    Transform engine size class dimension
    """
    
    # Clean and prepare the dimension
    engine_dim = (
        engine_size_class_df
        .select(
            col("engine_size_class_id"),
            trim(col("size_class")).alias("engine_size_class")
        )
        .withColumn("engine_size_class", initcap(trim(col("engine_size_class"))))
        .dropDuplicates()
    )
    
    # Add SCD Type 2 columns
    engine_dim = engine_dim.withColumn("version", lit(1)) \
                           .withColumn("date_from", current_timestamp()) \
                           .withColumn("date_to", lit(None).cast("timestamp")) \
                           .withColumn("is_current", lit(True))
    
    # Add surrogate key
    window = Window.orderBy("engine_size_class")
    engine_dim = engine_dim.withColumn("engine_size_class_tk", row_number().over(window))
    
    # Final structure
    final_df = engine_dim.select(
        "engine_size_class_tk",
        "version",
        "date_from",
        "date_to", 
        "engine_size_class_id",
        "engine_size_class",
        "is_current"
    )
    
    return final_df

def transform_age_category_dim(age_category_df):
    """
    Transform age category dimension
    """
    
    # Clean and prepare the dimension
    age_dim = (
        age_category_df
        .select(
            col("age_category_id"),
            trim(col("category")).alias("age_category")
        )
        .withColumn("age_category", initcap(trim(col("age_category"))))
        .dropDuplicates()
    )
    
    # Add SCD Type 2 columns
    age_dim = age_dim.withColumn("version", lit(1)) \
                     .withColumn("date_from", current_timestamp()) \
                     .withColumn("date_to", lit(None).cast("timestamp")) \
                     .withColumn("is_current", lit(True))
    
    # Add surrogate key
    window = Window.orderBy("age_category") 
    age_dim = age_dim.withColumn("age_category_tk", row_number().over(window))
    
    # Final structure
    final_df = age_dim.select(
        "age_category_tk",
        "version",
        "date_from",
        "date_to",
        "age_category_id", 
        "age_category",
        "is_current"
    )
    
    return final_df
