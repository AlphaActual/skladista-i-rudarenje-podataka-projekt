#!/usr/bin/env python3
"""
Simple Spark test script to check if PySpark is working
"""

import os
import sys

def test_spark_basic():
    """Test basic Spark functionality step by step"""
    
    print("=" * 60)
    print("🔍 SPARK ENVIRONMENT TEST")
    print("=" * 60)
    
    # Step 1: Check Python version
    print(f"1. Python version: {sys.version}")
    
    # Step 2: Check environment variables
    print(f"2. JAVA_HOME: {os.environ.get('JAVA_HOME', 'NOT SET')}")
    print(f"3. SPARK_HOME: {os.environ.get('SPARK_HOME', 'NOT SET')}")
    print(f"4. PYSPARK_PYTHON: {os.environ.get('PYSPARK_PYTHON', 'NOT SET')}")
    
    # Step 3: Try to import PySpark
    print("\n5. Testing PySpark import...")
    try:
        import pyspark
        print(f"   ✅ PySpark imported successfully")
        print(f"   📍 PySpark location: {pyspark.__file__}")
        print(f"   📦 PySpark version: {pyspark.__version__}")
    except ImportError as e:
        print(f"   ❌ Failed to import PySpark: {e}")
        return False
    
    # Step 4: Check PySpark installation
    print("\n6. Checking PySpark installation...")
    pyspark_dir = os.path.dirname(pyspark.__file__)
    jars_dir = os.path.join(pyspark_dir, 'jars')
    bin_dir = os.path.join(pyspark_dir, 'bin')
    
    print(f"   📁 PySpark directory: {pyspark_dir}")
    print(f"   📁 Jars directory exists: {os.path.exists(jars_dir)}")
    print(f"   📁 Bin directory exists: {os.path.exists(bin_dir)}")
    
    if os.path.exists(jars_dir):
        jar_files = [f for f in os.listdir(jars_dir) if f.endswith('.jar')]
        print(f"   📦 Number of jar files: {len(jar_files)}")
        
        # Look for key jars
        spark_core = [f for f in jar_files if 'spark-core' in f]
        if spark_core:
            print(f"   ✅ Spark core jar found: {spark_core[0]}")
        else:
            print(f"   ❌ Spark core jar NOT found")
    
    # Step 5: Set environment variables if needed
    print("\n7. Setting up environment...")
    if 'SPARK_HOME' not in os.environ:
        os.environ['SPARK_HOME'] = pyspark_dir
        print(f"   🔧 SPARK_HOME set to: {pyspark_dir}")
    
    if 'PYSPARK_PYTHON' not in os.environ:
        os.environ['PYSPARK_PYTHON'] = sys.executable
        print(f"   🔧 PYSPARK_PYTHON set to: {sys.executable}")
    
    # Set HADOOP_HOME to fix winutils.exe warning
    if 'HADOOP_HOME' not in os.environ:
        os.environ['HADOOP_HOME'] = r'C:\hadoop'
        print(f"   🔧 HADOOP_HOME set to: C:\\hadoop")
    
    # Add hadoop bin to PATH
    hadoop_bin = r'C:\hadoop\bin'
    if hadoop_bin not in os.environ.get('PATH', ''):
        os.environ['PATH'] = hadoop_bin + ';' + os.environ.get('PATH', '')
        print(f"   🔧 Added {hadoop_bin} to PATH")
    
    # Step 6: Try to create minimal Spark session
    print("\n8. Testing minimal Spark session...")
    try:
        from pyspark.sql import SparkSession
        
        # Create the most basic Spark session possible with additional Windows configs
        spark = SparkSession.builder \
            .appName("BasicTest") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        print(f"   ✅ Spark session created successfully!")
        print(f"   📊 Spark version: {spark.version}")
        print(f"   🎯 Spark master: {spark.sparkContext.master}")
        
        # Test basic operation
        print("\n9. Testing basic Spark operations...")
        test_data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
        df = spark.createDataFrame(test_data, ["id", "name"])
        count = df.count()
        print(f"   ✅ Created DataFrame with {count} rows")
        
        # Show first row
        first_row = df.first()
        print(f"   ✅ First row: {first_row}")
        
        spark.stop()
        print(f"   ✅ Spark session stopped successfully")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Failed to create Spark session: {e}")
        print(f"   🔧 Error type: {type(e).__name__}")
        
        # Try to get more details
        if hasattr(e, '__cause__') and e.__cause__:
            print(f"   🔧 Caused by: {e.__cause__}")
        
        return False

def test_java_connection():
    """Test Java connectivity separately"""
    print("\n" + "=" * 60)
    print("☕ JAVA CONNECTION TEST")
    print("=" * 60)
    
    java_home = os.environ.get('JAVA_HOME')
    if not java_home:
        print("❌ JAVA_HOME is not set")
        return False
    
    java_exe = os.path.join(java_home, 'bin', 'java.exe')
    if not os.path.exists(java_exe):
        java_exe = os.path.join(java_home, 'bin', 'java')
    
    print(f"☕ Java executable: {java_exe}")
    print(f"☕ Java exists: {os.path.exists(java_exe)}")
    
    if os.path.exists(java_exe):
        try:
            import subprocess
            result = subprocess.run([java_exe, '-version'], 
                                  capture_output=True, text=True, timeout=10)
            print(f"☕ Java version check exit code: {result.returncode}")
            if result.stderr:
                print(f"☕ Java version: {result.stderr.split()[0:3]}")
            return result.returncode == 0
        except Exception as e:
            print(f"❌ Failed to run Java: {e}")
            return False
    
    return False

if __name__ == "__main__":
    print("🚀 Starting Spark compatibility test...")
    
    # Test Java first
    java_ok = test_java_connection()
    
    # Test Spark
    spark_ok = test_spark_basic()
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 TEST SUMMARY")
    print("=" * 60)
    print(f"☕ Java connectivity: {'✅ PASS' if java_ok else '❌ FAIL'}")
    print(f"⚡ Spark functionality: {'✅ PASS' if spark_ok else '❌ FAIL'}")
    
    if java_ok and spark_ok:
        print("\n🎉 SUCCESS: Spark is working correctly with your Python!")
        print("You can now use Spark in your ETL pipeline.")
    else:
        print("\n⚠️  ISSUES DETECTED:")
        if not java_ok:
            print("   - Java connectivity problems")
        if not spark_ok:
            print("   - Spark session creation problems")
        print("\nPlease check the error messages above for troubleshooting.")
    
    sys.exit(0 if (java_ok and spark_ok) else 1)
