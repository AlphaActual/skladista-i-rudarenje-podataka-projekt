#!/usr/bin/env python3
"""
Quick Data Warehouse Validation Script
======================================

A simplified validation script to quickly check if the data warehouse is working.
Run this first before the comprehensive validation.
"""

import pymysql
from spark_session import get_spark_session
from pyspark.sql.functions import col, count, avg

def quick_validation():
    """Perform quick validation of the data warehouse"""
    
    print("🚀 QUICK DATA WAREHOUSE VALIDATION")
    print("=" * 50)
    
    # Test database connection
    print("\n1. Testing database connection...")
    try:
        connection = pymysql.connect(
            host='127.0.0.1',
            user='root',
            password='root',
            database='cars_dw'
        )
        cursor = connection.cursor()
        cursor.execute("SELECT DATABASE()")
        db_name = cursor.fetchone()[0]
        print(f"   ✅ Connected to: {db_name}")
        
        # Check tables
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        print(f"   📊 Found {len(tables)} tables: {tables}")
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"   ❌ Database connection failed: {e}")
        print("   💡 Make sure MySQL is running and cars_dw database exists")
        return False
    
    # Test Spark connection and data loading
    print("\n2. Testing Spark and data loading...")
    try:
        spark = get_spark_session("Quick_Validation")
        spark.sparkContext.setLogLevel("ERROR")
        
        jdbc_url = "jdbc:mysql://127.0.0.1:3306/cars_dw?useSSL=false&allowPublicKeyRetrieval=true"
        connection_properties = {
            "user": "root",
            "password": "root",
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        
        # Try to load fact table
        fact_df = spark.read.jdbc(
            url=jdbc_url,
            table="fact_car_sales",
            properties=connection_properties
        )
        
        fact_count = fact_df.count()
        print(f"   ✅ Loaded fact table: {fact_count:,} records")
        
        if fact_count == 0:
            print("   ⚠️  Fact table is empty - ETL may not have run yet")
            return False
        
        # Show basic statistics
        print(f"   📊 Fact table columns: {len(fact_df.columns)}")
        
        # Quick stats
        stats = fact_df.agg(
            count("*").alias("total_records"),
            avg("price").alias("avg_price")
        ).collect()[0]
        
        print(f"   💰 Average car price: £{stats['avg_price']:,.0f}")
        
        print("   ✅ Spark connectivity successful")
        
    except Exception as e:
        print(f"   ❌ Spark/data loading failed: {e}")
        return False
    
    # Test dimension tables
    print("\n3. Testing dimension tables...")
    dim_tables = ['dim_manufacturer', 'dim_vehicle', 'dim_fuel', 'dim_transmission', 'dim_location', 'dim_date']
    
    for table in dim_tables:
        try:
            df = spark.read.jdbc(
                url=jdbc_url,
                table=table,
                properties=connection_properties
            )
            count = df.count()
            print(f"   ✅ {table}: {count} records")
            
        except Exception as e:
            print(f"   ❌ {table}: Error - {e}")
    
    # Quick analytical query
    print("\n4. Testing analytical query...")
    try:
        manufacturer_df = spark.read.jdbc(
            url=jdbc_url,
            table="dim_manufacturer",
            properties=connection_properties
        )
        
        result = (
            fact_df.alias('f')
            .join(manufacturer_df.alias('m'), col('f.manufacturer_tk') == col('m.manufacturer_tk'))
            .groupBy('m.name')
            .agg(count('*').alias('car_count'), avg('f.price').alias('avg_price'))
            .orderBy(col('car_count').desc())
        )
        
        print("   📊 Top 5 manufacturers by car count:")
        result.show(5, truncate=False)
        
        print("   ✅ Analytical queries working")
        
    except Exception as e:
        print(f"   ❌ Analytical query failed: {e}")
        return False
    
    print("\n" + "=" * 50)
    print("🎉 QUICK VALIDATION COMPLETED SUCCESSFULLY!")
    print("✅ Your data warehouse appears to be working correctly.")
    print("🔍 Run 'python test_data_warehouse.py' for comprehensive validation.")
    
    return True

if __name__ == "__main__":
    quick_validation()
