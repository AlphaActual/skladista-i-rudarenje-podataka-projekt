from pyspark.sql.functions import col, lit, trim, when, regexp_extract
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def transform_date_dim(decade_df, csv_cars_df=None):
    """
    Transform date dimension (SCD Type 0 - Static)
    Years and decades don't change after being recorded
    """
    
    # Process database decades
    merged_df = (
        decade_df
        .select(
            trim(col("decade")).alias("decade")
        )
        .dropDuplicates()
    )

    # Extract year information if CSV data exists
    if csv_cars_df:
        # Extract unique years from CSV and create decades
        csv_date_df = (
            csv_cars_df
            .select(col("year"))
            .dropDuplicates()
            .withColumn("decade", 
                ((col("year") / 10).cast("int") * 10).cast("string") + lit("s")
            )
            .select("year", "decade")
        )
        
        # Get unique decades from CSV
        csv_decades_df = csv_date_df.select("decade").dropDuplicates()
        
        # Union with database data
        merged_df = merged_df.unionByName(csv_decades_df).dropDuplicates()
        
        # Now create full date dimension with all years
        years_df = csv_date_df.select("year", "decade").dropDuplicates()
        
        # Create comprehensive date dimension
        final_df = years_df.withColumn("date_tk", col("year"))
        
    else:
        # If no CSV data, create basic structure from decades
        final_df = (
            merged_df
            .withColumn("year", 
                # Extract decade start year and add 5 as representative year
                regexp_extract(col("decade"), r"(\d{4})s", 1).cast("int") + lit(5)
            )
            .withColumn("date_tk", col("year"))
        )

    # Reorder columns according to star schema design
    final_df = final_df.select(
        "date_tk",
        "year", 
        "decade"
    )

    return final_df
