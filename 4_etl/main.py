# main.py
from extract.extract_mysql import extract_all_tables
from extract.extract_csv import extract_from_csv
from transform.pipeline import run_transformations
from spark_session import get_spark_session
from load.run_loading import write_all_tables_to_mysql
import os

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    spark.catalog.clearCache()

    # Load data
    print("🚀 Starting data extraction")
    mysql_df = extract_all_tables()
    csv_df = {"csv_cars": extract_from_csv("2_relational_model/processed/cars_data_20.csv")}
    merged_df = {**mysql_df, **csv_df}
    print("✅ Data extraction completed")

    # Transform data
    print("🚀 Starting data transformation")
    load_ready_dict = run_transformations(merged_df)
    print("✅ Data transformation completed")

    # Load data
    print("🚀 Starting data loading")
    write_all_tables_to_mysql(load_ready_dict)
    print("👏 Data loading completed")

if __name__ == "__main__":
    main()