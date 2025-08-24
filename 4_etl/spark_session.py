# spark_session.py
import os
import sys
from pyspark.sql import SparkSession

# Lazy-load pattern: Spark is only created when this function is called
def get_spark_session(app_name="ETL_App"):
    # Get the project root directory (one level up from 4_etl)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    mysql_jar_path = os.path.join(project_root, "Connectors", "mysql-connector-j-9.2.0.jar")
    
    # Set up environment variables if needed (similar to test_spark_simple.py)
    if 'PYSPARK_PYTHON' not in os.environ:
        os.environ['PYSPARK_PYTHON'] = sys.executable
    
    # Set HADOOP_HOME to fix winutils.exe warning on Windows
    if 'HADOOP_HOME' not in os.environ:
        os.environ['HADOOP_HOME'] = r'C:\hadoop'
    
    # Add hadoop bin to PATH if not already there
    hadoop_bin = r'C:\hadoop\bin'
    if hadoop_bin not in os.environ.get('PATH', ''):
        os.environ['PATH'] = hadoop_bin + ';' + os.environ.get('PATH', '')
    
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", mysql_jar_path) \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()