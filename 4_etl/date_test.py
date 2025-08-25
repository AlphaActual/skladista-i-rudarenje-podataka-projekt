# main.py
from extract.extract_mysql import extract_all_tables
from extract.extract_csv import extract_from_csv
from spark_session import get_spark_session
from transform.dimensions.date_dim import transform_date_dim
from pyspark.sql import DataFrame
import pymysql
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
    print("🚀 Starting date transformation")

    def run_transformations(raw_data):
        """Transform raw car data into dimensional model"""
        
        # Get Spark session for category dimensions
        spark = get_spark_session("Transform_Categories")
        

        date_dim = transform_date_dim(
            raw_data["car"],
            csv_cars_df=raw_data.get("csv_cars")
        )

        # print("Preview of date_dim:")
        # date_dim.show(100, truncate=False)
        print("6️⃣ Date dimension complete")
        
        return {
            "dim_date": date_dim, 
        }


    load_ready_dict = run_transformations(merged_df)



    print("✅ Data transformation completed")

    # Load data
    print("🚀 Starting data loading")

    

    def write_spark_df_to_mysql(spark_df: DataFrame, table_name: str, mode: str = "append"):
        """Write Spark DataFrame to MySQL data warehouse using JDBC"""
        
        
        jdbc_url = "jdbc:mysql://127.0.0.1:3306/cars_dw?useSSL=false&allowPublicKeyRetrieval=true"
        connection_properties = {
            "user": "root",
            "password": "root",
            "driver": "com.mysql.cj.jdbc.Driver"
        }

        try:
            print(f"Writing {spark_df.count()} rows to table `{table_name}` with mode `{mode}`...")
            
            spark_df.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode=mode,
                properties=connection_properties
            )
            print(f"✅ Done writing to `{table_name}`.")
            
        except Exception as e:
            print(f"Error writing to table {table_name}: {e}")
            

    def write_all_tables_to_mysql(load_ready_dict):
        
        dimension_tables = ['dim_date']
       
        
        # Load dimension tables first
        for table_name in dimension_tables:
            if table_name in load_ready_dict:
                write_spark_df_to_mysql(load_ready_dict[table_name], table_name)
        


    # write_all_tables_to_mysql(load_ready_dict)
    # print("👏 Data loading completed")

if __name__ == "__main__":
    main()