#!/usr/bin/env python3
"""
Advanced Data Warehouse Analysis
=================================
Perform sophisticated analytical queries to demonstrate data warehouse capabilities
"""

import pymysql
from datetime import datetime

def advanced_analysis():
    """Perform advanced analytical queries"""
    
    print("🔬 ADVANCED DATA WAREHOUSE ANALYSIS")
    print("=" * 60)
    
    try:
        # Connect to database
        connection = pymysql.connect(
            host='127.0.0.1',
            user='root',
            password='root',
            database='cars_dw'
        )
        cursor = connection.cursor()
        
        # Analysis 1: Market Share Analysis
        print("\n📊 MARKET SHARE ANALYSIS")
        print("-" * 40)
        
        cursor.execute("""
            SELECT 
                m.name as manufacturer,
                COUNT(*) as car_count,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_car_sales), 2) as market_share_pct,
                AVG(f.price) as avg_price,
                MIN(f.price) as min_price,
                MAX(f.price) as max_price
            FROM fact_car_sales f
            JOIN dim_manufacturer m ON f.manufacturer_tk = m.manufacturer_tk
            GROUP BY m.name, m.manufacturer_tk
            ORDER BY car_count DESC
        """)
        
        results = cursor.fetchall()
        print(f"{'Manufacturer':<15} {'Count':<8} {'Share%':<8} {'Avg£':<10} {'Min£':<8} {'Max£':<10}")
        print("-" * 70)
        for row in results:
            print(f"{row[0]:<15} {row[1]:<8} {row[2]:<8} {row[3]:<10,.0f} {row[4]:<8,.0f} {row[5]:<10,.0f}")
        
        # Analysis 2: Price Trends by Year
        print("\n📈 PRICE TRENDS BY YEAR")
        print("-" * 40)
        
        cursor.execute("""
            SELECT 
                d.year,
                COUNT(*) as cars_sold,
                AVG(f.price) as avg_price,
                AVG(f.mileage) as avg_mileage,
                AVG(f.age) as avg_age
            FROM fact_car_sales f
            JOIN dim_date d ON f.date_tk = d.date_tk
            GROUP BY d.year
            ORDER BY d.year
        """)
        
        results = cursor.fetchall()
        print(f"{'Year':<6} {'Cars':<8} {'Avg Price':<12} {'Avg Mileage':<12} {'Avg Age':<8}")
        print("-" * 50)
        for row in results:
            print(f"{row[0]:<6} {row[1]:<8} £{row[2]:<11,.0f} {row[3]:<12,.0f} {row[4]:<8.1f}")
        
        # Analysis 3: Fuel Type Evolution
        print("\n⛽ FUEL TYPE ANALYSIS")
        print("-" * 40)
        
        cursor.execute("""
            SELECT 
                fuel.type as fuel_type,
                COUNT(*) as car_count,
                AVG(f.price) as avg_price,
                AVG(f.mpg) as avg_mpg,
                AVG(f.engine_size) as avg_engine_size
            FROM fact_car_sales f
            JOIN dim_fuel fuel ON f.fuel_tk = fuel.fuel_tk
            GROUP BY fuel.type
            ORDER BY car_count DESC
        """)
        
        results = cursor.fetchall()
        print(f"{'Fuel Type':<12} {'Count':<8} {'Avg Price':<12} {'Avg MPG':<10} {'Avg Engine':<12}")
        print("-" * 60)
        for row in results:
            print(f"{row[0]:<12} {row[1]:<8} £{row[2]:<11,.0f} {row[3]:<10.1f} {row[4]:<12.1f}L")
        
        # Analysis 4: Transmission Analysis
        print("\n⚙️  TRANSMISSION ANALYSIS")
        print("-" * 40)
        
        cursor.execute("""
            SELECT 
                t.type as transmission,
                COUNT(*) as car_count,
                AVG(f.price) as avg_price,
                AVG(f.mpg) as avg_mpg
            FROM fact_car_sales f
            JOIN dim_transmission t ON f.transmission_tk = t.transmission_tk
            GROUP BY t.type
            ORDER BY car_count DESC
        """)
        
        results = cursor.fetchall()
        print(f"{'Transmission':<15} {'Count':<8} {'Avg Price':<12} {'Avg MPG':<10}")
        print("-" * 50)
        for row in results:
            print(f"{row[0]:<15} {row[1]:<8} £{row[2]:<11,.0f} {row[3]:<10.1f}")
        
        # Analysis 5: Price vs Mileage Correlation
        print("\n🛣️  MILEAGE IMPACT ON PRICING")
        print("-" * 40)
        
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN f.mileage <= 20000 THEN 'Low (0-20k)'
                    WHEN f.mileage <= 50000 THEN 'Medium (20-50k)'
                    WHEN f.mileage <= 100000 THEN 'High (50-100k)'
                    ELSE 'Very High (100k+)'
                END as mileage_category,
                COUNT(*) as car_count,
                AVG(f.price) as avg_price,
                AVG(f.age) as avg_age
            FROM fact_car_sales f
            GROUP BY 
                CASE 
                    WHEN f.mileage <= 20000 THEN 'Low (0-20k)'
                    WHEN f.mileage <= 50000 THEN 'Medium (20-50k)'
                    WHEN f.mileage <= 100000 THEN 'High (50-100k)'
                    ELSE 'Very High (100k+)'
                END
            ORDER BY avg_price DESC
        """)
        
        results = cursor.fetchall()
        print(f"{'Mileage Category':<20} {'Count':<8} {'Avg Price':<12} {'Avg Age':<10}")
        print("-" * 55)
        for row in results:
            print(f"{row[0]:<20} {row[1]:<8} £{row[2]:<11,.0f} {row[3]:<10.1f}")
        
        # Analysis 6: Premium vs Budget Brands
        print("\n💎 PREMIUM VS BUDGET ANALYSIS")
        print("-" * 40)
        
        cursor.execute("""
            SELECT 
                m.name as manufacturer,
                COUNT(*) as total_cars,
                AVG(f.price) as avg_price,
                CASE 
                    WHEN AVG(f.price) > 25000 THEN 'Premium'
                    WHEN AVG(f.price) > 15000 THEN 'Mid-Range'
                    ELSE 'Budget'
                END as price_category
            FROM fact_car_sales f
            JOIN dim_manufacturer m ON f.manufacturer_tk = m.manufacturer_tk
            GROUP BY m.name, m.manufacturer_tk
            HAVING COUNT(*) > 1000  -- Only brands with significant volume
            ORDER BY avg_price DESC
        """)
        
        results = cursor.fetchall()
        print(f"{'Manufacturer':<15} {'Cars':<8} {'Avg Price':<12} {'Category':<12}")
        print("-" * 50)
        for row in results:
            print(f"{row[0]:<15} {row[1]:<8} £{row[2]:<11,.0f} {row[3]:<12}")
        
        # Analysis 7: Geographic Distribution
        print("\n🌍 GEOGRAPHIC ANALYSIS")
        print("-" * 40)
        
        cursor.execute("""
            SELECT 
                l.country,
                l.region,
                COUNT(*) as car_count,
                AVG(f.price) as avg_price
            FROM fact_car_sales f
            JOIN dim_location l ON f.location_tk = l.location_tk
            WHERE l.country != 'Unknown'
            GROUP BY l.country, l.region
            ORDER BY car_count DESC
        """)
        
        results = cursor.fetchall()
        print(f"{'Country':<15} {'Region':<15} {'Count':<8} {'Avg Price':<12}")
        print("-" * 55)
        for row in results:
            print(f"{row[0]:<15} {row[1]:<15} {row[2]:<8} £{row[3]:<11,.0f}")
        
        # Summary Statistics
        print("\n📊 SUMMARY STATISTICS")
        print("-" * 40)
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT manufacturer_tk) as unique_manufacturers,
                COUNT(DISTINCT vehicle_tk) as unique_models,
                MIN(price) as min_price,
                MAX(price) as max_price,
                AVG(price) as avg_price,
                STDDEV(price) as price_stddev,
                MIN(mileage) as min_mileage,
                MAX(mileage) as max_mileage,
                AVG(mileage) as avg_mileage
            FROM fact_car_sales
        """)
        
        stats = cursor.fetchone()
        print(f"Total Records: {stats[0]:,}")
        print(f"Unique Manufacturers: {stats[1]}")
        print(f"Unique Vehicle Models: {stats[2]}")
        print(f"Price Range: £{stats[3]:,.0f} - £{stats[4]:,.0f}")
        print(f"Average Price: £{stats[5]:,.0f} (±£{stats[6]:,.0f})")
        print(f"Mileage Range: {stats[7]:,} - {stats[8]:,} miles")
        print(f"Average Mileage: {stats[9]:,.0f} miles")
        
        cursor.close()
        connection.close()
        
        print("\n" + "=" * 60)
        print("🎉 ADVANCED ANALYSIS COMPLETED!")
        print("✅ Your data warehouse supports sophisticated analytical queries!")
        print("🚀 Ready for business intelligence and reporting applications!")
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")

if __name__ == "__main__":
    advanced_analysis()
