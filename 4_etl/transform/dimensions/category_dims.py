from pyspark.sql.functions import col, lit, trim, initcap, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

from pyspark.sql.functions import col, lit, trim, initcap, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def transform_mileage_category_dim(spark_session):
    """
    Transform mileage category dimension - creates static categories
    """
    
    # Create static mileage categories
    mileage_categories = [
        (1, "Low"),
        (2, "Medium"), 
        (3, "High"),
        (4, "Very High")
    ]
    
    mileage_dim = spark_session.createDataFrame(mileage_categories, ["mileage_category_id", "mileage_category"])
    
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

def transform_engine_size_class_dim(spark_session):
    """
    Transform engine size class dimension - creates static categories
    """
    
    # Create static engine size categories
    engine_size_categories = [
        (1, "Small"),
        (2, "Medium"),
        (3, "Large"),
        (4, "Very Large")
    ]
    
    engine_dim = spark_session.createDataFrame(engine_size_categories, ["engine_size_class_id", "engine_size_class"])
    
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

def transform_age_category_dim(spark_session):
    """
    Transform age category dimension - creates static categories
    """
    
    # Create static age categories
    age_categories = [
        (1, "New"),
        (2, "Recent"),
        (3, "Used"),
        (4, "Old")
    ]
    
    age_dim = spark_session.createDataFrame(age_categories, ["age_category_id", "age_category"])
    
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
