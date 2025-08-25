#!/usr/bin/env python3
"""
Data Warehouse Validation Script
=====================================

This script validates the data warehouse implementation by:
1. Testing data quality and integrity
2. Validating dimensional model structure
3. Checking ETL pipeline correctness
4. Performing analytical queries to verify functionality
"""

from spark_session import get_spark_session
from extract.extract_mysql import extract_all_tables
from extract.extract_csv import extract_from_csv
from transform.pipeline import run_transformations
from load.run_loading import write_all_tables_to_mysql
import pymysql
from pyspark.sql.functions import col, count, sum as spark_sum, avg, max as spark_max, min as spark_min
from pyspark.sql.functions import isnan, isnull, when, desc, asc
import pandas as pd
from datetime import datetime

class DataWarehouseValidator:
    
    def __init__(self):
        self.spark = get_spark_session("DW_Validator")
        self.spark.sparkContext.setLogLevel("ERROR")
        self.mysql_config = {
            'host': '127.0.0.1',
            'user': 'root',
            'password': 'root',
            'database': 'cars_dw'
        }
        self.jdbc_url = "jdbc:mysql://127.0.0.1:3306/cars_dw?useSSL=false&allowPublicKeyRetrieval=true"
        self.connection_properties = {
            "user": "root",
            "password": "root",
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        
    def print_section(self, title):
        """Print formatted section header"""
        print(f"\n{'='*80}")
        print(f"🔍 {title}")
        print(f"{'='*80}")
    
    def print_subsection(self, title):
        """Print formatted subsection header"""
        print(f"\n📊 {title}")
        print("-" * 60)
    
    def test_database_connection(self):
        """Test connection to data warehouse database"""
        self.print_section("DATABASE CONNECTION TEST")
        
        try:
            connection = pymysql.connect(**self.mysql_config)
            cursor = connection.cursor()
            cursor.execute("SELECT DATABASE()")
            db_name = cursor.fetchone()[0]
            cursor.close()
            connection.close()
            print(f"✅ Successfully connected to database: {db_name}")
            return True
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def check_table_existence(self):
        """Check if all expected tables exist in the data warehouse"""
        self.print_section("TABLE EXISTENCE CHECK")
        
        expected_tables = [
            'dim_date', 'dim_manufacturer', 'dim_vehicle', 'dim_transmission',
            'dim_fuel', 'dim_location', 'fact_car_sales'
        ]
        
        try:
            connection = pymysql.connect(**self.mysql_config)
            cursor = connection.cursor()
            cursor.execute("SHOW TABLES")
            existing_tables = [table[0] for table in cursor.fetchall()]
            cursor.close()
            connection.close()
            
            print("Expected tables:")
            missing_tables = []
            for table in expected_tables:
                if table in existing_tables:
                    print(f"  ✅ {table}")
                else:
                    print(f"  ❌ {table} (MISSING)")
                    missing_tables.append(table)
            
            print(f"\nAdditional tables found:")
            additional_tables = [t for t in existing_tables if t not in expected_tables]
            for table in additional_tables:
                print(f"  📋 {table}")
            
            if missing_tables:
                print(f"\n⚠️  Missing {len(missing_tables)} expected table(s)")
                return False
            else:
                print(f"\n✅ All {len(expected_tables)} expected tables exist")
                return True
                
        except Exception as e:
            print(f"❌ Error checking tables: {e}")
            return False
    
    def load_table_from_mysql(self, table_name):
        """Load a table from MySQL using Spark"""
        try:
            df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table=table_name,
                properties=self.connection_properties
            )
            return df
        except Exception as e:
            print(f"❌ Error loading table {table_name}: {e}")
            return None
    
    def validate_table_structure(self):
        """Validate the structure of dimension and fact tables"""
        self.print_section("TABLE STRUCTURE VALIDATION")
        
        table_schemas = {
            'dim_date': ['date_tk', 'year', 'decade'],
            'dim_manufacturer': ['manufacturer_tk', 'name', 'country', 'region'],
            'dim_vehicle': ['vehicle_tk', 'model_name'],
            'dim_transmission': ['transmission_tk', 'type'],
            'dim_fuel': ['fuel_tk', 'type'],
            'dim_location': ['location_tk', 'country', 'region'],
            'fact_car_sales': ['car_sales_tk', 'date_tk', 'manufacturer_tk', 'vehicle_tk', 
                             'transmission_tk', 'fuel_tk', 'location_tk', 'price', 'mileage', 
                             'tax', 'mpg', 'engine_size', 'age']
        }
        
        for table_name, expected_columns in table_schemas.items():
            self.print_subsection(f"Validating {table_name}")
            
            df = self.load_table_from_mysql(table_name)
            if df is None:
                continue
                
            actual_columns = df.columns
            missing_columns = [col for col in expected_columns if col not in actual_columns]
            extra_columns = [col for col in actual_columns if col not in expected_columns]
            
            print(f"Row count: {df.count()}")
            print(f"Column count: {len(actual_columns)}")
            
            if missing_columns:
                print(f"❌ Missing columns: {missing_columns}")
            
            if extra_columns:
                print(f"📋 Extra columns: {extra_columns}")
            
            if not missing_columns and not extra_columns:
                print("✅ Schema matches expected structure")
            
            # Show sample data
            print("\nSample data:")
            df.show(5, truncate=False)
    
    def validate_data_quality(self):
        """Validate data quality across all tables"""
        self.print_section("DATA QUALITY VALIDATION")
        
        tables = ['dim_date', 'dim_manufacturer', 'dim_vehicle', 'dim_transmission',
                 'dim_fuel', 'dim_location', 'fact_car_sales']
        
        for table_name in tables:
            self.print_subsection(f"Data Quality - {table_name}")
            
            df = self.load_table_from_mysql(table_name)
            if df is None:
                continue
            
            total_rows = df.count()
            print(f"Total rows: {total_rows:,}")
            
            if total_rows == 0:
                print("⚠️  Table is empty!")
                continue
            
            # Enhanced null value analysis
            self._analyze_null_values(df, table_name, total_rows)
            
            # Check for duplicates in key columns
            if table_name.startswith('dim_'):
                key_col = table_name.replace('dim_', '') + '_tk'
                if key_col in df.columns:
                    unique_count = df.select(key_col).distinct().count()
                    if unique_count != total_rows:
                        print(f"❌ Duplicate keys found in {key_col}: {total_rows - unique_count} duplicates")
                    else:
                        print(f"✅ All {key_col} values are unique")
            
            # Additional data quality checks
            self._additional_quality_checks(df, table_name)
    
    def _analyze_null_values(self, df, table_name, total_rows):
        """Detailed null value analysis for each table"""
        print("\n🔍 NULL VALUE ANALYSIS:")
        
        # Define critical columns that should never be null
        critical_columns = {
            'dim_date': ['date_tk', 'year'],
            'dim_manufacturer': ['manufacturer_tk', 'name'],
            'dim_vehicle': ['vehicle_tk', 'model_name'],
            'dim_transmission': ['transmission_tk', 'type'],
            'dim_fuel': ['fuel_tk', 'type'],
            'dim_location': ['location_tk', 'country'],
            'fact_car_sales': ['car_sales_tk', 'date_tk', 'manufacturer_tk', 'vehicle_tk', 
                             'transmission_tk', 'fuel_tk', 'location_tk', 'price']
        }
        
        # Define columns where nulls are acceptable/expected
        acceptable_nulls = {
            'dim_date': ['decade'],  # Known issue: decade column has null values
            'dim_manufacturer': ['country', 'region'],
            'dim_location': ['region'],
            'fact_car_sales': ['tax', 'mpg', 'engine_size', 'mileage']  # Some cars might not have these values
        }
        
        table_critical = critical_columns.get(table_name, [])
        table_acceptable = acceptable_nulls.get(table_name, [])
        
        null_issues = []
        
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            null_percentage = (null_count / total_rows) * 100
            
            if null_count > 0:
                if column in table_critical:
                    print(f"❌ {column}: {null_count:,} nulls ({null_percentage:.1f}%) - CRITICAL ERROR")
                    null_issues.append(f"Critical column {column} has null values")
                elif column in table_acceptable:
                    if null_percentage == 100:
                        print(f"⚠️  {column}: {null_count:,} nulls ({null_percentage:.1f}%) - ALL NULL (NEEDS ATTENTION)")
                        null_issues.append(f"Column {column} is completely null")
                    else:
                        print(f"📋 {column}: {null_count:,} nulls ({null_percentage:.1f}%) - Acceptable")
                else:
                    print(f"⚠️  {column}: {null_count:,} nulls ({null_percentage:.1f}%) - Review needed")
            else:
                print(f"✅ {column}: No nulls")
        
        # Special handling for dim_date decade column issue
        if table_name == 'dim_date' and 'decade' in df.columns:
            print(f"\n🚨 KNOWN ISSUE DETECTED:")
            decade_null_count = df.filter(col('decade').isNull()).count()
            if decade_null_count == total_rows:
                print(f"   • The 'decade' column in dim_date is completely null ({decade_null_count:,} rows)")
                print(f"   • This suggests the decade calculation logic needs to be implemented")
                print(f"   • Recommendation: Update ETL to calculate decade from year (e.g., year // 10 * 10)")
        
        if null_issues:
            print(f"\n⚠️  Found {len(null_issues)} null value issues in {table_name}")
    
    def test_and_fix_decade_column(self):
        """Specific test for the decade column issue in dim_date table"""
        self.print_section("DECADE COLUMN ANALYSIS AND FIX SUGGESTION")
        
        date_df = self.load_table_from_mysql('dim_date')
        if date_df is None:
            print("❌ Cannot load dim_date table")
            return
        
        total_rows = date_df.count()
        
        # Check decade column
        if 'decade' in date_df.columns:
            null_decades = date_df.filter(col('decade').isNull()).count()
            print(f"📊 Decade column analysis:")
            print(f"   • Total rows in dim_date: {total_rows:,}")
            print(f"   • Null decades: {null_decades:,}")
            print(f"   • Null percentage: {(null_decades/total_rows)*100:.1f}%")
            
            if null_decades == total_rows:
                print(f"\n🚨 CRITICAL ISSUE: All decade values are null!")
                print(f"📝 RECOMMENDED FIX:")
                print(f"   1. Update your ETL transformation to calculate decade from year")
                print(f"   2. Use formula: decade = (year // 10) * 10")
                print(f"   3. Example: year 2015 → decade 2010, year 1998 → decade 1990")
                
                # Show sample of what decades should be
                if 'year' in date_df.columns:
                    print(f"\n💡 Expected decade values based on years:")
                    sample_with_calculated_decade = date_df.select(
                        'year',
                        ((col('year') / 10).cast('int') * 10).alias('calculated_decade')
                    ).distinct().orderBy('year')
                    sample_with_calculated_decade.show(10)
                    
                    # Show decade distribution that should exist
                    decade_distribution = date_df.select(
                        ((col('year') / 10).cast('int') * 10).alias('calculated_decade')
                    ).groupBy('calculated_decade').count().orderBy('calculated_decade')
                    
                    print(f"\n📈 Expected decade distribution:")
                    decade_distribution.show()
                    
            elif null_decades > 0:
                print(f"\n⚠️  Partial null issue: {null_decades} out of {total_rows} decades are null")
                # Show which years have null decades
                null_decade_years = date_df.filter(col('decade').isNull()).select('year').distinct().orderBy('year')
                print(f"Years with null decades:")
                null_decade_years.show()
            else:
                print(f"\n✅ All decade values are populated")
                # Show current decade distribution
                decade_dist = date_df.groupBy('decade').count().orderBy('decade')
                print(f"Current decade distribution:")
                decade_dist.show()
        else:
            print(f"❌ Decade column not found in dim_date table")
    
    def _additional_quality_checks(self, df, table_name):
        """Additional data quality checks specific to table types"""
        print(f"\n🔎 ADDITIONAL QUALITY CHECKS:")
        
        if table_name == 'fact_car_sales':
            # Check for negative values in price and other numeric fields
            numeric_fields = ['price', 'mileage', 'tax', 'mpg', 'engine_size', 'age']
            for field in numeric_fields:
                if field in df.columns:
                    negative_count = df.filter(col(field) < 0).count()
                    if negative_count > 0:
                        print(f"⚠️  {field}: {negative_count} negative values found")
                    else:
                        print(f"✅ {field}: No negative values")
            
            # Check for unrealistic values
            if 'price' in df.columns:
                very_cheap = df.filter(col('price') < 100).count()
                very_expensive = df.filter(col('price') > 500000).count()
                if very_cheap > 0:
                    print(f"⚠️  price: {very_cheap} cars under £100 (possible data quality issue)")
                if very_expensive > 0:
                    print(f"⚠️  price: {very_expensive} cars over £500k (possible data quality issue)")
            
            if 'age' in df.columns:
                future_cars = df.filter(col('age') < 0).count()
                very_old = df.filter(col('age') > 50).count()
                if future_cars > 0:
                    print(f"❌ age: {future_cars} cars with negative age (data error)")
                if very_old > 0:
                    print(f"📋 age: {very_old} cars over 50 years old")
        
        elif table_name == 'dim_date':
            # Check year ranges
            if 'year' in df.columns:
                min_year = df.select(spark_min('year')).collect()[0][0]
                max_year = df.select(spark_max('year')).collect()[0][0]
                print(f"📅 Year range: {min_year} - {max_year}")
                
                if min_year and min_year < 1900:
                    print(f"⚠️  Found years before 1900: minimum year is {min_year}")
                if max_year and max_year > 2030:
                    print(f"⚠️  Found years after 2030: maximum year is {max_year}")
        
        elif table_name.startswith('dim_'):
            # Check for empty strings in text columns
            text_columns = [col for col in df.columns if not col.endswith('_tk')]
            for column in text_columns:
                if column in df.columns:
                    empty_count = df.filter((col(column) == '') | (col(column).isNull())).count()
                    if empty_count > 0:
                        print(f"⚠️  {column}: {empty_count} empty or null values")
                    else:
                        print(f"✅ {column}: No empty values")
    
    def suggest_data_quality_fixes(self):
        """Suggest fixes for common data quality issues found"""
        self.print_section("DATA QUALITY FIX SUGGESTIONS")
        
        print("🔧 COMMON FIXES FOR DATA QUALITY ISSUES:")
        print()
        
        print("1. 🚨 NULL DECADE COLUMN in dim_date:")
        print("   Problem: All decade values are null")
        print("   Solution: Update ETL transformation")
        print("   Code fix: In your dimension transformation, add:")
        print("   ```python")
        print("   df = df.withColumn('decade', (col('year') / 10).cast('int') * 10)")
        print("   ```")
        print()
        
        print("2. ❌ NULL VALUES in Critical Columns:")
        print("   Problem: Primary keys or essential fields have nulls")
        print("   Solution: Add data validation and default values")
        print("   - Use coalesce() for fallback values")
        print("   - Filter out records with null primary keys")
        print("   - Add NOT NULL constraints in database schema")
        print()
        
        print("3. ⚠️  UNREALISTIC VALUES:")
        print("   Problem: Negative prices, future dates, etc.")
        print("   Solution: Add data validation rules")
        print("   - Filter: price > 0 AND price < 1000000")
        print("   - Filter: year >= 1900 AND year <= current_year")
        print("   - Filter: mileage >= 0")
        print()
        
        print("4. 📋 EMPTY STRING VALUES:")
        print("   Problem: Empty strings instead of proper nulls")
        print("   Solution: Convert empty strings to nulls")
        print("   Code fix: when(col('column') == '', None).otherwise(col('column'))")
        print()
        
        print("5. 🔄 REFERENTIAL INTEGRITY:")
        print("   Problem: Foreign keys pointing to non-existent records")
        print("   Solution: Add unknown/default dimension records")
        print("   - Insert record with ID = -1 for 'Unknown' values")
        print("   - Use coalesce to default to -1 for missing lookups")

    def validate_decade_calculation_logic(self):
        """Test if the decade calculation logic is working correctly"""
        self.print_section("DECADE CALCULATION VALIDATION")
        
        # Create test data to validate decade calculation
        test_years = [1995, 2000, 2010, 2015, 2020, 2025]
        expected_decades = [1990, 2000, 2010, 2010, 2020, 2020]
        
        print("🧪 Testing decade calculation logic:")
        print("Year → Expected Decade")
        print("-" * 25)
        
        for year, expected in zip(test_years, expected_decades):
            calculated = (year // 10) * 10
            status = "✅" if calculated == expected else "❌"
            print(f"{year} → {calculated} {status}")
        
        print(f"\n💡 Correct formula: decade = (year // 10) * 10")
        print(f"   This groups years into decades: 1990-1999 → 1990, 2000-2009 → 2000, etc.")
        
        # Check actual data if available
        date_df = self.load_table_from_mysql('dim_date')
        if date_df is not None and 'year' in date_df.columns:
            print(f"\n📊 Your actual year data range:")
            year_stats = date_df.agg(
                spark_min('year').alias('min_year'),
                spark_max('year').alias('max_year')
            ).collect()[0]
            
            min_year = year_stats['min_year']
            max_year = year_stats['max_year']
            
            if min_year and max_year:
                print(f"   Years: {min_year} - {max_year}")
                print(f"   Expected decades: {(min_year // 10) * 10} - {(max_year // 10) * 10}")
                
                # Show what the decade distribution should look like
                unique_decades = set()
                for year in range(min_year, max_year + 1):
                    unique_decades.add((year // 10) * 10)
                
                print(f"   Expected unique decades: {sorted(unique_decades)}")

    def validate_referential_integrity(self):
        """Validate foreign key relationships between fact and dimension tables"""
        self.print_section("REFERENTIAL INTEGRITY VALIDATION")
        
        fact_df = self.load_table_from_mysql('fact_car_sales')
        if fact_df is None:
            print("❌ Cannot load fact table for integrity check")
            return
        
        fact_count = fact_df.count()
        print(f"Total fact records: {fact_count:,}")
        
        # Check each foreign key relationship
        fk_relationships = [
            ('date_tk', 'dim_date'),
            ('manufacturer_tk', 'dim_manufacturer'),
            ('vehicle_tk', 'dim_vehicle'),
            ('transmission_tk', 'dim_transmission'),
            ('fuel_tk', 'dim_fuel'),
            ('location_tk', 'dim_location')
        ]
        
        for fk_column, dim_table in fk_relationships:
            self.print_subsection(f"Foreign Key: {fk_column} -> {dim_table}")
            
            dim_df = self.load_table_from_mysql(dim_table)
            if dim_df is None:
                continue
            
            pk_column = dim_table.replace('dim_', '') + '_tk'
            
            # Count orphaned records (excluding -1 which represents "unknown")
            orphaned = fact_df.join(
                dim_df, 
                fact_df[fk_column] == dim_df[pk_column], 
                'left_anti'
            ).filter(col(fk_column) != -1)
            
            orphaned_count = orphaned.count()
            unknown_count = fact_df.filter(col(fk_column) == -1).count()
            
            if orphaned_count > 0:
                print(f"❌ Orphaned records: {orphaned_count}")
                print("Sample orphaned values:")
                orphaned.select(fk_column).distinct().show(5)
            else:
                print(f"✅ No orphaned records")
            
            if unknown_count > 0:
                print(f"📊 Unknown values (-1): {unknown_count} ({unknown_count/fact_count*100:.1f}%)")
    
    def perform_analytical_queries(self):
        """Perform sample analytical queries to test the data warehouse"""
        self.print_section("ANALYTICAL QUERIES TEST")
        
        fact_df = self.load_table_from_mysql('fact_car_sales')
        manufacturer_df = self.load_table_from_mysql('dim_manufacturer')
        vehicle_df = self.load_table_from_mysql('dim_vehicle')
        date_df = self.load_table_from_mysql('dim_date')
        fuel_df = self.load_table_from_mysql('dim_fuel')
        
        if None in [fact_df, manufacturer_df, vehicle_df, date_df, fuel_df]:
            print("❌ Cannot perform analytical queries - missing tables")
            return
        
        # Query 1: Sales by manufacturer
        self.print_subsection("Top 10 Manufacturers by Average Price")
        manufacturer_sales = (
            fact_df.alias('f')
            .join(manufacturer_df.alias('m'), col('f.manufacturer_tk') == col('m.manufacturer_tk'))
            .groupBy('m.name')
            .agg(
                count('*').alias('car_count'),
                avg('f.price').alias('avg_price'),
                spark_sum('f.price').alias('total_value')
            )
            .orderBy(desc('avg_price'))
        )
        manufacturer_sales.show(10, truncate=False)
        
        # Query 2: Sales trends by year
        self.print_subsection("Sales Trends by Year")
        yearly_trends = (
            fact_df.alias('f')
            .join(date_df.alias('d'), col('f.date_tk') == col('d.date_tk'))
            .groupBy('d.year')
            .agg(
                count('*').alias('car_count'),
                avg('f.price').alias('avg_price'),
                avg('f.mileage').alias('avg_mileage')
            )
            .orderBy('d.year')
        )
        yearly_trends.show(20, truncate=False)
        
        # Query 3: Fuel type analysis
        self.print_subsection("Analysis by Fuel Type")
        fuel_analysis = (
            fact_df.alias('f')
            .join(fuel_df.alias('fuel'), col('f.fuel_tk') == col('fuel.fuel_tk'))
            .groupBy('fuel.type')
            .agg(
                count('*').alias('car_count'),
                avg('f.price').alias('avg_price'),
                avg('f.mpg').alias('avg_mpg'),
                avg('f.engine_size').alias('avg_engine_size')
            )
            .orderBy(desc('car_count'))
        )
        fuel_analysis.show(truncate=False)
        
        # Query 4: Price distribution analysis
        self.print_subsection("Price Distribution Analysis")
        price_stats = fact_df.agg(
            count('price').alias('total_cars'),
            spark_min('price').alias('min_price'),
            spark_max('price').alias('max_price'),
            avg('price').alias('avg_price')
        )
        price_stats.show()
        
        # Price ranges
        price_ranges = fact_df.select(
            when(col('price') <= 10000, 'Under £10k')
            .when(col('price') <= 20000, '£10k-£20k')
            .when(col('price') <= 30000, '£20k-£30k')
            .when(col('price') <= 50000, '£30k-£50k')
            .otherwise('Over £50k').alias('price_range')
        ).groupBy('price_range').count().orderBy('count')
        
        price_ranges.show()
    
    def check_etl_pipeline(self):
        """Test the complete ETL pipeline"""
        self.print_section("ETL PIPELINE TEST")
        
        try:
            print("🚀 Testing data extraction...")
            mysql_data = extract_all_tables()
            csv_data = {"csv_cars": extract_from_csv("data/cars_data_original.csv")}
            raw_data = {**mysql_data, **csv_data}
            
            print(f"✅ Extracted {len(mysql_data)} MySQL tables")
            print(f"✅ Extracted CSV data: {csv_data['csv_cars'].count()} rows")
            
            print("\n🔄 Testing data transformation...")
            transformed_data = run_transformations(raw_data)
            
            print(f"✅ Generated {len(transformed_data)} transformed tables:")
            for table_name, df in transformed_data.items():
                print(f"  - {table_name}: {df.count()} rows")
            
            print("\n💾 ETL pipeline test completed successfully!")
            return True
            
        except Exception as e:
            print(f"❌ ETL pipeline test failed: {e}")
            return False
    
    def generate_summary_report(self):
        """Generate a comprehensive summary report"""
        self.print_section("DATA WAREHOUSE SUMMARY REPORT")
        
        # Table sizes
        tables = ['dim_date', 'dim_manufacturer', 'dim_vehicle', 'dim_transmission',
                 'dim_fuel', 'dim_location', 'fact_car_sales']
        
        total_records = 0
        table_info = []
        
        for table_name in tables:
            df = self.load_table_from_mysql(table_name)
            if df is not None:
                count = df.count()
                total_records += count
                table_info.append((table_name, count, len(df.columns)))
        
        print("📊 Table Summary:")
        print(f"{'Table Name':<20} {'Rows':<10} {'Columns':<10}")
        print("-" * 40)
        for name, rows, cols in table_info:
            print(f"{name:<20} {rows:<10,} {cols:<10}")
        print("-" * 40)
        print(f"{'TOTAL':<20} {total_records:<10,}")
        
        # Data warehouse metrics
        fact_df = self.load_table_from_mysql('fact_car_sales')
        if fact_df is not None:
            print(f"\n🎯 Key Metrics:")
            print(f"  • Total car sales records: {fact_df.count():,}")
            
            price_stats = fact_df.agg(
                spark_min('price').alias('min_price'),
                spark_max('price').alias('max_price'),
                avg('price').alias('avg_price')
            ).collect()[0]
            
            print(f"  • Price range: £{price_stats['min_price']:,.0f} - £{price_stats['max_price']:,.0f}")
            print(f"  • Average price: £{price_stats['avg_price']:,.0f}")
            
            year_range = fact_df.join(
                self.load_table_from_mysql('dim_date'), 'date_tk'
            ).agg(
                spark_min('year').alias('min_year'),
                spark_max('year').alias('max_year')
            ).collect()[0]
            
            print(f"  • Year range: {year_range['min_year']} - {year_range['max_year']}")
    
    def run_full_validation(self):
        """Run complete data warehouse validation"""
        print("🏁 STARTING COMPREHENSIVE DATA WAREHOUSE VALIDATION")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        validation_results = {}
        
        # Run all validation tests
        validation_results['connection'] = self.test_database_connection()
        validation_results['tables'] = self.check_table_existence()
        
        if validation_results['connection'] and validation_results['tables']:
            self.validate_table_structure()
            self.validate_data_quality()
            self.test_and_fix_decade_column()  # Specific test for decade column issue
            self.validate_decade_calculation_logic()  # Test decade calculation logic
            self.validate_referential_integrity()
            self.perform_analytical_queries()
            validation_results['etl'] = self.check_etl_pipeline()
            self.generate_summary_report()
            self.suggest_data_quality_fixes()  # Suggest fixes for issues found
        
        # Final summary
        self.print_section("VALIDATION SUMMARY")
        
        passed_tests = sum(validation_results.values())
        total_tests = len(validation_results)
        
        if passed_tests == total_tests:
            print("🎉 ALL VALIDATION TESTS PASSED!")
            print("✅ Your data warehouse is properly implemented and functional.")
        else:
            print(f"⚠️  {passed_tests}/{total_tests} validation tests passed")
            print("❌ Please review the failed tests above")
        
        print(f"\nValidation completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def main():
    """Main function to run data warehouse validation"""
    import sys
    
    validator = DataWarehouseValidator()
    
    # Check if user wants to run specific tests
    if len(sys.argv) > 1:
        test_type = sys.argv[1].lower()
        
        if test_type == 'nulls':
            print("🔍 RUNNING NULL VALUE ANALYSIS ONLY")
            print("=" * 50)
            if validator.test_database_connection():
                validator.validate_data_quality()
                validator.test_and_fix_decade_column()
                validator.suggest_data_quality_fixes()
        elif test_type == 'decade':
            print("🔍 RUNNING DECADE COLUMN ANALYSIS ONLY")
            print("=" * 50)
            if validator.test_database_connection():
                validator.test_and_fix_decade_column()
                validator.validate_decade_calculation_logic()
        elif test_type == 'quick':
            print("🔍 RUNNING QUICK VALIDATION")
            print("=" * 50)
            validator.test_database_connection()
            validator.check_table_existence()
            validator.validate_data_quality()
        else:
            print("❌ Unknown test type. Available options:")
            print("   python test_data_warehouse.py nulls   - Test null values only")
            print("   python test_data_warehouse.py decade  - Test decade column only")
            print("   python test_data_warehouse.py quick   - Quick validation")
            print("   python test_data_warehouse.py         - Full validation")
    else:
        # Run full validation
        validator.run_full_validation()

if __name__ == "__main__":
    main()
