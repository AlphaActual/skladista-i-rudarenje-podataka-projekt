#!/usr/bin/env python3
"""
Test script for the ETL Spark session
"""

from spark_session import get_spark_session

def test_spark_session():
    print("Testing Spark session creation...")
    try:
        spark = get_spark_session()
        print(f"✅ Spark session created successfully!")
        print(f"📊 Spark version: {spark.version}")
        print(f"🎯 Spark master: {spark.sparkContext.master}")
        
        # Test basic operation
        test_data = [(1, "Test"), (2, "Data")]
        df = spark.createDataFrame(test_data, ["id", "name"])
        count = df.count()
        print(f"✅ Created test DataFrame with {count} rows")
        
        spark.stop()
        print("✅ Spark session stopped successfully")
        return True
    except Exception as e:
        print(f"❌ Failed: {e}")
        return False

if __name__ == "__main__":
    success = test_spark_session()
    if success:
        print("🎉 ETL Spark session is working correctly!")
    else:
        print("⚠️  ETL Spark session has issues")
