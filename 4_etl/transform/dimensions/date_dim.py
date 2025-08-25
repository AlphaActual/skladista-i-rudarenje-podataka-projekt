from pyspark.sql.functions import col, lit, concat

def transform_date_dim(car_df, csv_cars_df=None):
    """
    Transform date dimension (SCD Type 0 - Static)
    Years and decades don't change after being recorded
    """

    # Extract unique years and decades from database
    mysql_df = (
        car_df
        .select(col("year"))
        .dropDuplicates()
        .withColumn("decade", concat(((col("year").cast("int") / 10).cast("int") * 10).cast("string"), lit("s")))
        .select("year", "decade")
    )

    if csv_cars_df:
        # Extract unique years and decades from CSV
        csv_date_df = (
            csv_cars_df
            .select(col("year"))
            .dropDuplicates()
            .withColumn("decade", concat(((col("year").cast("int") / 10).cast("int") * 10).cast("string"), lit("s")))
            .select("year", "decade")
        )
        # Union database and CSV data
        combined_df = mysql_df.unionByName(csv_date_df).dropDuplicates()
        final_df = combined_df.withColumn("date_tk", col("year"))
    else:
        final_df = mysql_df.withColumn("date_tk", col("year"))

    # Reorder columns according to star schema design
    final_df = final_df.select(
        "date_tk",
        "year", 
        "decade"
    )

    return final_df