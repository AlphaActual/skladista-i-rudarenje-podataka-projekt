# extract/extract_mysql.py
from spark_session import get_spark_session

def extract_table(table_name, database="cars"):
    """Extract table using Spark JDBC"""
    spark = get_spark_session("ETL_Extract_MySQL")

    jdbc_url = f"jdbc:mysql://127.0.0.1:3306/{database}?useSSL=false&allowPublicKeyRetrieval=true"
    connection_properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    try:
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
        return df
    except Exception as e:
        print(f"Error extracting table {table_name}: {e}")
        # Return empty DataFrame if table doesn't exist
        return spark.createDataFrame([], "struct<id:int>")

def extract_all_tables():
    """Extract all tables from the cars relational database"""
    return {
        "region": extract_table("region"),
        "country": extract_table("country"),
        "manufacturer": extract_table("manufacturer"),
        "model": extract_table("model"),
        "transmission_type": extract_table("transmission_type"),
        "fuel_type": extract_table("fuel_type"),
        "decade": extract_table("decade"),
        "mileage_category": extract_table("mileage_category"),
        "engine_size_class": extract_table("engine_size_class"),
        "age_category": extract_table("age_category"),
        "car": extract_table("car"),
    }