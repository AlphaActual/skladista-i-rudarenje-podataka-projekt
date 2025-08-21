#!/usr/bin/env python3
"""
Test script to verify ETL pipeline imports and basic functionality
"""

import sys
import os

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """Test that all ETL modules can be imported"""
    print("🔍 Testing ETL module imports...")
    
    try:
        from spark_session import get_spark_session
        print("✅ spark_session - OK")
    except ImportError as e:
        print(f"❌ spark_session - FAILED: {e}")
        return False
    
    try:
        from extract.extract_mysql import extract_all_tables
        from extract.extract_csv import extract_from_csv
        print("✅ extract modules - OK")
    except ImportError as e:
        print(f"❌ extract modules - FAILED: {e}")
        return False
    
    try:
        from transform.pipeline import run_transformations
        print("✅ transform pipeline - OK")
    except ImportError as e:
        print(f"❌ transform pipeline - FAILED: {e}")
        return False
    
    try:
        from transform.dimensions.manufacturer_dim import transform_manufacturer_dim
        from transform.dimensions.vehicle_dim import transform_vehicle_dim
        from transform.dimensions.transmission_dim import transform_transmission_dim
        from transform.dimensions.fuel_dim import transform_fuel_dim
        from transform.dimensions.location_dim import transform_location_dim
        from transform.dimensions.date_dim import transform_date_dim
        print("✅ dimension transforms - OK")
    except ImportError as e:
        print(f"❌ dimension transforms - FAILED: {e}")
        return False
    
    try:
        from transform.facts.car_sales_fact import transform_car_sales_fact
        print("✅ fact transforms - OK")
    except ImportError as e:
        print(f"❌ fact transforms - FAILED: {e}")
        return False
    
    try:
        from load.run_loading import write_spark_df_to_mysql
        print("✅ load module - OK")
    except ImportError as e:
        print(f"❌ load module - FAILED: {e}")
        return False
    
    return True

def test_spark_session():
    """Test Spark session creation"""
    print("\n🔍 Testing Spark session creation...")
    
    try:
        from spark_session import get_spark_session
        spark = get_spark_session("ETL_Test")
        print(f"✅ Spark session created: {spark.sparkContext.appName}")
        
        # Test basic Spark functionality
        test_df = spark.createDataFrame([(1, "test")], ["id", "name"])
        count = test_df.count()
        print(f"✅ Basic Spark operations work: {count} rows")
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"❌ Spark session test - FAILED: {e}")
        return False

def test_csv_reading():
    """Test CSV reading functionality"""
    print("\n🔍 Testing CSV reading...")
    
    try:
        from extract.extract_csv import extract_from_csv
        
        # Check if CSV file exists
        csv_path = "2_relational_model/processed/cars_data_20.csv"
        if not os.path.exists(csv_path):
            print(f"⚠️  CSV file not found: {csv_path}")
            return False
        
        df = extract_from_csv(csv_path)
        count = df.count()
        columns = df.columns
        
        print(f"✅ CSV reading works: {count} rows, {len(columns)} columns")
        print(f"   Columns: {columns[:5]}...")  # Show first 5 columns
        
        return True
        
    except Exception as e:
        print(f"❌ CSV reading test - FAILED: {e}")
        return False

def main():
    """Run all tests"""
    print("🚀 ETL Pipeline Test Suite")
    print("=" * 50)
    
    tests = [
        ("Module Imports", test_imports),
        ("Spark Session", test_spark_session),
        ("CSV Reading", test_csv_reading)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n📋 Running: {test_name}")
        result = test_func()
        results.append((test_name, result))
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 Test Results Summary:")
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"   {test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! ETL pipeline is ready.")
        return 0
    else:
        print("⚠️  Some tests failed. Check the errors above.")
        return 1

if __name__ == "__main__":
    exit(main())
