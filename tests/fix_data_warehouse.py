#!/usr/bin/env python3
"""
Data Warehouse Fix Script
========================
Fix the NULL and -1 value issues in the data warehouse
"""

import pymysql

def fix_data_warehouse_issues():
    """Fix identified data quality issues"""
    
    print("🔧 FIXING DATA WAREHOUSE ISSUES")
    print("=" * 50)
    
    try:
        connection = pymysql.connect(
            host='127.0.0.1',
            user='root',
            password='root',
            database='cars_dw'
        )
        cursor = connection.cursor()
        
        # Fix 1: Add missing -1 records to dimension tables
        print("\n1️⃣ Adding missing -1 (Unknown) records to dimension tables...")
        
        # Add -1 record to dim_date
        cursor.execute("""
            INSERT IGNORE INTO dim_date (date_tk, year, decade)
            VALUES (-1, -1, 'Unknown')
        """)
        print("   ✅ Added -1 record to dim_date")
        
        # Add -1 record to dim_manufacturer
        cursor.execute("""
            INSERT IGNORE INTO dim_manufacturer (manufacturer_tk, version, date_from, date_to, manufacturer_id, name, country, region, is_current)
            VALUES (-1, 1, '1900-01-01', NULL, -1, 'Unknown', 'Unknown', 'Unknown', 1)
        """)
        print("   ✅ Added -1 record to dim_manufacturer")
        
        # Add -1 record to dim_vehicle
        cursor.execute("""
            INSERT IGNORE INTO dim_vehicle (vehicle_tk, model_name)
            VALUES (-1, 'Unknown')
        """)
        print("   ✅ Added -1 record to dim_vehicle")
        
        # Add -1 record to dim_transmission
        cursor.execute("""
            INSERT IGNORE INTO dim_transmission (transmission_tk, type)
            VALUES (-1, 'Unknown')
        """)
        print("   ✅ Added -1 record to dim_transmission")
        
        # Add -1 record to dim_fuel
        cursor.execute("""
            INSERT IGNORE INTO dim_fuel (fuel_tk, type)
            VALUES (-1, 'Unknown')
        """)
        print("   ✅ Added -1 record to dim_fuel")
        
        # Add -1 record to dim_location
        cursor.execute("""
            INSERT IGNORE INTO dim_location (location_tk, country, region)
            VALUES (-1, 'Unknown', 'Unknown')
        """)
        print("   ✅ Added -1 record to dim_location")
        
        # Fix 2: Update NULL decades in dim_date
        print("\n2️⃣ Fixing NULL decades in dim_date...")
        
        cursor.execute("""
            UPDATE dim_date 
            SET decade = CASE 
                WHEN year BETWEEN 1990 AND 1999 THEN '1990s'
                WHEN year BETWEEN 2000 AND 2009 THEN '2000s'
                WHEN year BETWEEN 2010 AND 2019 THEN '2010s'
                WHEN year BETWEEN 2020 AND 2029 THEN '2020s'
                ELSE 'Other'
            END
            WHERE decade IS NULL AND year != -1
        """)
        
        affected_rows = cursor.rowcount
        print(f"   ✅ Updated {affected_rows} decade values")
        
        # Commit all changes
        connection.commit()
        
        # Verify fixes
        print("\n✅ VERIFICATION OF FIXES")
        print("-" * 30)
        
        # Check -1 records exist
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
            count = cursor.fetchone()[0]
            if count > 0:
                print(f"   ✅ {table}: -1 record exists")
            else:
                print(f"   ❌ {table}: -1 record still missing")
        
        # Check decades
        cursor.execute("SELECT COUNT(*) FROM dim_date WHERE decade IS NULL")
        null_decades = cursor.fetchone()[0]
        print(f"   📅 Remaining NULL decades: {null_decades}")
        
        # Show sample of fixed data
        print("\n📊 SAMPLE OF FIXED DATA")
        print("-" * 30)
        
        cursor.execute("SELECT date_tk, year, decade FROM dim_date WHERE decade IS NOT NULL LIMIT 5")
        results = cursor.fetchall()
        print("Fixed dim_date records:")
        for row in results:
            print(f"   TK: {row[0]}, Year: {row[1]}, Decade: {row[2]}")
        
        cursor.close()
        connection.close()
        
        print("\n" + "=" * 50)
        print("🎉 DATA WAREHOUSE FIXES COMPLETED!")
        print("✅ All -1 (Unknown) records have been added")
        print("✅ Decade calculations have been fixed")
        print("ℹ️  Note: date_to NULLs in manufacturer table are correct for SCD Type 2")
        
    except Exception as e:
        print(f"❌ Fix failed: {e}")

if __name__ == "__main__":
    fix_data_warehouse_issues()
