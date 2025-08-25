#!/usr/bin/env python3
"""
NULL Values Investigation Script
===============================
Investigate NULL and -1 values in the data warehouse
"""

import pymysql

def investigate_null_values():
    """Investigate NULL and -1 values in detail"""
    
    print("🔍 INVESTIGATING NULL AND -1 VALUES")
    print("=" * 60)
    
    try:
        connection = pymysql.connect(
            host='127.0.0.1',
            user='root',
            password='root',
            database='cars_dw'
        )
        cursor = connection.cursor()
        
        # 1. Check dim_date table specifically
        print("\n📅 DIMENSION DATE TABLE ANALYSIS")
        print("-" * 40)
        
        cursor.execute("SELECT * FROM dim_date LIMIT 10")
        results = cursor.fetchall()
        
        cursor.execute("DESCRIBE dim_date")
        columns = [col[0] for col in cursor.fetchall()]
        
        print(f"Columns: {columns}")
        print("Sample data:")
        for row in results:
            print(f"  {dict(zip(columns, row))}")
        
        # Check for NULL decades
        cursor.execute("SELECT COUNT(*) FROM dim_date WHERE decade IS NULL")
        null_decades = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM dim_date")
        total_dates = cursor.fetchone()[0]
        
        print(f"\nNULL decades: {null_decades}/{total_dates}")
        
        if null_decades > 0:
            cursor.execute("SELECT date_tk, year, decade FROM dim_date WHERE decade IS NULL")
            null_results = cursor.fetchall()
            print("Records with NULL decades:")
            for row in null_results[:5]:
                print(f"  date_tk: {row[0]}, year: {row[1]}, decade: {row[2]}")
        
        # 2. Check dim_manufacturer for date_to NULLs
        print("\n🏭 DIMENSION MANUFACTURER TABLE ANALYSIS")
        print("-" * 40)
        
        cursor.execute("DESCRIBE dim_manufacturer")
        columns = [col[0] for col in cursor.fetchall()]
        print(f"Columns: {columns}")
        
        if 'date_to' in columns:
            cursor.execute("SELECT COUNT(*) FROM dim_manufacturer WHERE date_to IS NULL")
            null_date_to = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM dim_manufacturer")
            total_manufacturers = cursor.fetchone()[0]
            
            print(f"NULL date_to values: {null_date_to}/{total_manufacturers}")
            
            cursor.execute("SELECT manufacturer_tk, name, date_from, date_to, is_current FROM dim_manufacturer LIMIT 5")
            results = cursor.fetchall()
            print("Sample manufacturer records:")
            for row in results:
                print(f"  TK: {row[0]}, Name: {row[1]}, From: {row[2]}, To: {row[3]}, Current: {row[4]}")
        
        # 3. Check fact table for -1 values
        print("\n📊 FACT TABLE -1 VALUES ANALYSIS")
        print("-" * 40)
        
        # Count -1 values in each foreign key column
        fk_columns = ['date_tk', 'manufacturer_tk', 'vehicle_tk', 'transmission_tk', 'fuel_tk', 'location_tk']
        
        cursor.execute("SELECT COUNT(*) FROM fact_car_sales")
        total_facts = cursor.fetchone()[0]
        
        for fk_col in fk_columns:
            cursor.execute(f"SELECT COUNT(*) FROM fact_car_sales WHERE {fk_col} = -1")
            minus_one_count = cursor.fetchone()[0]
            percentage = (minus_one_count / total_facts) * 100
            print(f"{fk_col}: {minus_one_count} (-1 values) = {percentage:.2f}%")
        
        # 4. Sample records with -1 values
        print("\n🔍 SAMPLE RECORDS WITH -1 VALUES")
        print("-" * 40)
        
        cursor.execute("""
            SELECT car_sales_tk, date_tk, manufacturer_tk, vehicle_tk, transmission_tk, fuel_tk, location_tk, price
            FROM fact_car_sales 
            WHERE date_tk = -1 OR manufacturer_tk = -1 OR vehicle_tk = -1 OR transmission_tk = -1 OR fuel_tk = -1 OR location_tk = -1
            LIMIT 10
        """)
        
        results = cursor.fetchall()
        if results:
            print("Sample fact records with -1 values:")
            print("ID       Date  Manuf Vehic Trans Fuel  Loc   Price")
            print("-" * 55)
            for row in results:
                print(f"{row[0]:<8} {row[1]:<5} {row[2]:<5} {row[3]:<5} {row[4]:<5} {row[5]:<5} {row[6]:<5} £{row[7]}")
        else:
            print("✅ No records with -1 values found in fact table!")
        
        # 5. Check if -1 records exist in dimension tables
        print("\n🔍 CHECKING FOR -1 RECORDS IN DIMENSION TABLES")
        print("-" * 40)
        
        dim_tables = [
            ('dim_date', 'date_tk'),
            ('dim_manufacturer', 'manufacturer_tk'),
            ('dim_vehicle', 'vehicle_tk'),
            ('dim_transmission', 'transmission_tk'),
            ('dim_fuel', 'fuel_tk'),
            ('dim_location', 'location_tk')
        ]
        
        for table, key_col in dim_tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {key_col} = -1")
            minus_one_count = cursor.fetchone()[0]
            
            if minus_one_count > 0:
                cursor.execute(f"SELECT * FROM {table} WHERE {key_col} = -1")
                result = cursor.fetchone()
                print(f"✅ {table}: Has -1 record: {result}")
            else:
                print(f"❌ {table}: Missing -1 record (needed for unknown values)")
        
        # 6. Check data transformation logic
        print("\n🔧 CHECKING TRANSFORMATION LOGIC ISSUES")
        print("-" * 40)
        
        # Check if decades are being calculated correctly
        cursor.execute("""
            SELECT year, 
                   CASE 
                       WHEN year BETWEEN 1990 AND 1999 THEN '1990s'
                       WHEN year BETWEEN 2000 AND 2009 THEN '2000s'
                       WHEN year BETWEEN 2010 AND 2019 THEN '2010s'
                       WHEN year BETWEEN 2020 AND 2029 THEN '2020s'
                       ELSE 'Other'
                   END as calculated_decade,
                   decade
            FROM dim_date 
            WHERE decade IS NULL OR decade != CASE 
                       WHEN year BETWEEN 1990 AND 1999 THEN '1990s'
                       WHEN year BETWEEN 2000 AND 2009 THEN '2000s'
                       WHEN year BETWEEN 2010 AND 2019 THEN '2010s'
                       WHEN year BETWEEN 2020 AND 2029 THEN '2020s'
                       ELSE 'Other'
                   END
            LIMIT 10
        """)
        
        decade_issues = cursor.fetchall()
        if decade_issues:
            print("Decade calculation issues found:")
            for row in decade_issues:
                print(f"  Year: {row[0]}, Should be: {row[1]}, Currently: {row[2]}")
        else:
            print("✅ No decade calculation issues found")
        
        cursor.close()
        connection.close()
        
        print("\n" + "=" * 60)
        print("🎯 INVESTIGATION COMPLETED!")
        
    except Exception as e:
        print(f"❌ Investigation failed: {e}")

if __name__ == "__main__":
    investigate_null_values()
