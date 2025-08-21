from pyspark.sql import DataFrame

def write_spark_df_to_mysql(spark_df: DataFrame, table_name: str, mode: str = "overwrite"):
    """Write Spark DataFrame to MySQL data warehouse using JDBC"""
    
    # First ensure the data warehouse database exists
    create_dw_database()
    
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
        print("Falling back to CSV export...")
        
        # Fallback: export to CSV
        output_path = f"4_etl/output/{table_name}.csv"
        spark_df.coalesce(1).write.mode(mode).option("header", "true").csv(output_path)
        print(f"✅ Data exported to {output_path}")

def create_dw_database():
    """Create data warehouse database if it doesn't exist"""
    from spark_session import get_spark_session
    
    try:
        spark = get_spark_session("Create_DW_Database")
        
        # Connect to MySQL server (without database) to create the database
        jdbc_url = "jdbc:mysql://127.0.0.1:3306/?useSSL=false&allowPublicKeyRetrieval=true"
        connection_properties = {
            "user": "root",
            "password": "root",
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        
        # Create a dummy DataFrame to establish connection and execute SQL
        dummy_df = spark.createDataFrame([(1,)], ["dummy"])
        
        # Create database using Spark SQL
        spark.sql("CREATE DATABASE IF NOT EXISTS cars_dw")
        
        print("✅ Data warehouse database created/verified")
        
    except Exception as e:
        print(f"Note: Could not create database automatically: {e}")
        print("Please ensure 'cars_dw' database exists in MySQL")