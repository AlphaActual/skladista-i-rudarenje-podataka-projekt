# spark_session.py
from pyspark.sql import SparkSession

# Lazy-load pattern: Spark is only created when this function is called
def get_spark_session(app_name="ETL_Cars_App"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "Connectors/mysql-connector-j-9.2.0.jar") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()