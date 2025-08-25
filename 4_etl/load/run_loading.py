from pyspark.sql import DataFrame
import pymysql

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

def drop_fact_tables_first():
    """Drop fact tables first to avoid foreign key constraint issues"""
    try:
        connection = pymysql.connect(
            host='127.0.0.1',
            user='root',
            password='root',
            database='cars_dw'
        )
        cursor = connection.cursor()
        
        # Drop fact tables first to remove foreign key constraints
        fact_tables = ['fact_car_sales']
        for table in fact_tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
            print(f"🗑️ Dropped fact table {table} (if existed)")
            
        connection.commit()
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"Note: Could not drop fact tables: {e}")

def write_all_tables_to_mysql(load_ready_dict):
    """Write all tables to MySQL in correct order to avoid foreign key issues"""
    
    # First drop fact tables to remove foreign key constraints
    drop_fact_tables_first()
    
    # Define loading order: dimensions first, then facts
    dimension_tables = ['dim_manufacturer', 'dim_vehicle', 'dim_transmission', 
                       'dim_fuel', 'dim_location', 'dim_date',
                       'dim_mileage_category', 'dim_engine_size_class', 'dim_age_category']
    fact_tables = ['fact_car_sales']
    
    # Load dimension tables first
    for table_name in dimension_tables:
        if table_name in load_ready_dict:
            write_spark_df_to_mysql(load_ready_dict[table_name], table_name)
    
    # Then load fact tables
    for table_name in fact_tables:
        if table_name in load_ready_dict:
            write_spark_df_to_mysql(load_ready_dict[table_name], table_name)

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