# ETL Pipeline - Car Sales Data Warehouse

This ETL pipeline transforms car sales data from both MySQL relational database and CSV files into a dimensional star schema for analytical processing.

## Pipeline Overview

### Extract Phase
- **MySQL Database**: Extracts normalized relational data from the `cars` database
- **CSV Files**: Extracts raw car data from processed CSV files
- **Technology**: PySpark with JDBC connector for MySQL

### Transform Phase
Transforms relational data into dimensional model with:

#### Dimension Tables
1. **Dim_Manufacturer** (SCD Type 2)
   - Manufacturer information with country and region
   - Tracks historical changes in manufacturer data

2. **Dim_Vehicle** (SCD Type 2) 
   - Vehicle model information with categories
   - Includes mileage, engine size, and age categories

3. **Dim_Transmission** (SCD Type 1)
   - Transmission types (Manual, Automatic, Semi-Auto)
   - Simple overwrite strategy

4. **Dim_Fuel** (SCD Type 2)
   - Fuel types with historical tracking
   - Supports evolution of fuel technologies

5. **Dim_Location** (SCD Type 1)
   - Geographic information (country, region)
   - Rarely changing geographic data

6. **Dim_Date** (SCD Type 0)
   - Year and decade information
   - Static temporal dimension

#### Fact Table
- **Fact_Car_Sales**: Central fact table with measures:
  - Price, mileage, tax, MPG, engine size, age
  - Foreign keys to all dimensions

### Load Phase
- **Target**: MySQL data warehouse (`cars_dw` database)
- **Mode**: Overwrite for full refresh
- **Fallback**: CSV export if database unavailable

## File Structure

```
4_etl/
├── main.py                    # Main ETL orchestrator
├── spark_session.py           # Spark session configuration
├── extract/
│   ├── extract_mysql.py       # MySQL data extraction
│   └── extract_csv.py         # CSV data extraction
├── transform/
│   ├── pipeline.py            # Transform orchestrator
│   ├── dimensions/
│   │   ├── manufacturer_dim.py
│   │   ├── vehicle_dim.py
│   │   ├── transmission_dim.py
│   │   ├── fuel_dim.py
│   │   ├── location_dim.py
│   │   └── date_dim.py
│   └── facts/
│       └── car_sales_fact.py
├── load/
│   └── run_loading.py         # Data warehouse loading
└── output/                    # CSV fallback exports
```

## Prerequisites

1. **MySQL Server** running on localhost:3306
2. **MySQL Databases**:
   - `cars` (source relational database)
   - `cars_dw` (target data warehouse)
3. **MySQL Connector JAR**: `Connectors/mysql-connector-j-9.2.0.jar`
4. **Python Dependencies**: PySpark, PyMySQL (see requirements.txt)

## Running the Pipeline

```bash
cd 4_etl
python main.py
```

## Data Flow

1. **Extract**: Load data from MySQL `cars` database and CSV files
2. **Transform**: 
   - Create dimension tables with proper SCD implementation
   - Build fact table with dimension key lookups
   - Handle data quality and missing values
3. **Load**: Write dimensional model to `cars_dw` database

## Key Features

- **Hybrid Source Support**: Processes both database and CSV data
- **SCD Implementation**: Multiple slowly changing dimension strategies
- **Data Quality**: Handles missing values and data inconsistencies  
- **Scalability**: Built on PySpark for large dataset processing
- **Fault Tolerance**: CSV fallback if database unavailable
- **Dimensional Modeling**: Proper star schema with surrogate keys

## Configuration

Update database connection settings in:
- `extract/extract_mysql.py`
- `load/run_loading.py`

Default settings:
- Host: 127.0.0.1:3306
- User: root
- Password: root
- Source DB: cars
- Target DB: cars_dw

