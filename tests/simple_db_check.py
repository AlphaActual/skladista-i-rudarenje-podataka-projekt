#!/usr/bin/env python3
"""
Simple Database Check
====================
Check if the data warehouse has data without using Spark
"""

import pymysql

def simple_db_check():
    """Simple database validation without Spark"""
    
    print("🔍 SIMPLE DATABASE CHECK")
    print("=" * 40)
    
    try:
        # Connect to database
        connection = pymysql.connect(
            host='127.0.0.1',
            user='root',
            password='root',
            database='cars_dw'
        )
        cursor = connection.cursor()
        
        print("✅ Connected to cars_dw database")
        
        # Check tables and their row counts
        print("\n📊 Table Information:")
        print("-" * 40)
        
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        
        total_records = 0
        for table in sorted(tables):
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            total_records += count
            print(f"{table:<25}: {count:>8,} rows")
        
        print("-" * 40)
        print(f"{'TOTAL RECORDS':<25}: {total_records:>8,}")
        
        # Check fact table sample data
        if 'fact_car_sales' in tables:
            print("\n💰 Sample from fact_car_sales:")
            cursor.execute("""
                SELECT car_sales_tk, price, mileage, year 
                FROM fact_car_sales f
                JOIN dim_date d ON f.date_tk = d.date_tk
                LIMIT 5
            """)
            
            results = cursor.fetchall()
            print("ID       Price     Mileage   Year")
            print("-" * 35)
            for row in results:
                print(f"{row[0]:<8} £{row[1]:<8} {row[2]:<8} {row[3]}")
        
        # Check price statistics
        if 'fact_car_sales' in tables:
            print("\n📈 Price Statistics:")
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_cars,
                    MIN(price) as min_price,
                    MAX(price) as max_price,
                    AVG(price) as avg_price
                FROM fact_car_sales
            """)
            
            stats = cursor.fetchone()
            print(f"Total cars: {stats[0]:,}")
            print(f"Price range: £{stats[1]:,.0f} - £{stats[2]:,.0f}")
            print(f"Average price: £{stats[3]:,.0f}")
        
        # Check dimensional data
        print("\n🏭 Top 5 Manufacturers:")
        cursor.execute("""
            SELECT m.name, COUNT(*) as car_count, AVG(f.price) as avg_price
            FROM fact_car_sales f
            JOIN dim_manufacturer m ON f.manufacturer_tk = m.manufacturer_tk
            GROUP BY m.name
            ORDER BY car_count DESC
            LIMIT 5
        """)
        
        results = cursor.fetchall()
        print("Manufacturer          Cars    Avg Price")
        print("-" * 40)
        for row in results:
            print(f"{row[0]:<20} {row[1]:>5} £{row[2]:>8,.0f}")
        
        cursor.close()
        connection.close()
        
        print("\n" + "=" * 40)
        print("🎉 DATABASE CHECK COMPLETED!")
        
        if total_records > 0:
            print("✅ Your data warehouse contains data and appears functional!")
        else:
            print("⚠️  Tables exist but contain no data. ETL may need to be run.")
            
        return True
        
    except Exception as e:
        print(f"❌ Database check failed: {e}")
        return False

if __name__ == "__main__":
    simple_db_check()
