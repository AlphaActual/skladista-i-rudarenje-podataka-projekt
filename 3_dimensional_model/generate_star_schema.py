"""
Skripta za generiranje dimenzijskog modela podataka -> star schema za automobile

U ovom koraku generiramo dimenzijski model podataka.
Dimenzijski model podataka je zvjezdasti model koji se sastoji od jedne tablice činjenica i više tablica dimenzija (data mart).
Ovom skriptom samo stvaramo shemu, popunjavanje ostavljamo za ETL proces.

Ovaj model je optimiziran za analizu automobila na tržištu s fokusom na cijenovno-prodajne analize.
"""

from sqlalchemy import create_engine, Column, Integer, BigInteger, String, DateTime, ForeignKey, Float, Boolean, DECIMAL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Define the database connection
DATABASE_URL = "mysql+pymysql://root:root@localhost:3306/cars_dw"
engine = create_engine(DATABASE_URL, echo=True)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# Create data warehouse database if it doesn't exist
from sqlalchemy import create_engine, text
temp_engine = create_engine('mysql+pymysql://root:root@localhost', echo=False)
with temp_engine.connect() as conn:
    conn.execute(text("CREATE DATABASE IF NOT EXISTS cars_dw CHARACTER SET utf8mb4"))
    conn.commit()

# Define Dimensional Model Tables
#=====================================================================================

class DimDate(Base):
    """Vremenska dimenzija za analizu trendova po godinama i desetljećima"""
    __tablename__ = 'dim_date'
    __table_args__ = {'schema': 'cars_dw'}

    date_tk = Column(Integer, primary_key=True)
    year = Column(Integer, nullable=False)
    decade = Column(String(10), nullable=False)
    created_date = Column(DateTime, default=datetime.now)


class DimManufacturer(Base):
    """Informacije o proizvođačima automobila s praćenjem promjena kroz vrijeme (SCD Type 2)"""
    __tablename__ = 'dim_manufacturer'
    __table_args__ = {'schema': 'cars_dw'}

    manufacturer_tk = Column(BigInteger, primary_key=True, autoincrement=True)
    version = Column(Integer, nullable=False, default=1)
    date_from = Column(DateTime, nullable=False, default=datetime.now)
    date_to = Column(DateTime, nullable=True)
    manufacturer_id = Column(Integer, nullable=False, index=True)  # Business key
    name = Column(String(100), nullable=False)
    country = Column(String(50), nullable=False)
    region = Column(String(50), nullable=False)
    is_current = Column(Boolean, nullable=False, default=True)
    created_date = Column(DateTime, default=datetime.now)


class DimVehicle(Base):
    """Informacije o vozilima (model i kategorije) s praćenjem promjena (SCD Type 2)"""
    __tablename__ = 'dim_vehicle'
    __table_args__ = {'schema': 'cars_dw'}

    vehicle_tk = Column(BigInteger, primary_key=True, autoincrement=True)
    version = Column(Integer, nullable=False, default=1)
    date_from = Column(DateTime, nullable=False, default=datetime.now)
    date_to = Column(DateTime, nullable=True)
    vehicle_id = Column(Integer, nullable=False, index=True)  # Business key
    model_name = Column(String(100), nullable=False)
    mileage_category = Column(String(20), nullable=False)
    engine_size_class = Column(String(20), nullable=False)
    age_category = Column(String(20), nullable=False)
    is_current = Column(Boolean, nullable=False, default=True)
    created_date = Column(DateTime, default=datetime.now)


class DimTransmission(Base):
    """Tipovi mjenjača - promjene su rijetke i povijest nije kritična (SCD Type 1)"""
    __tablename__ = 'dim_transmission'
    __table_args__ = {'schema': 'cars_dw'}

    transmission_tk = Column(BigInteger, primary_key=True, autoincrement=True)
    transmission_id = Column(Integer, nullable=False, index=True)  # Business key
    type = Column(String(20), nullable=False)
    last_updated = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    created_date = Column(DateTime, default=datetime.now)


class DimFuel(Base):
    """Tipovi goriva s praćenjem evolucije tehnologija kroz vrijeme (SCD Type 2)"""
    __tablename__ = 'dim_fuel'
    __table_args__ = {'schema': 'cars_dw'}

    fuel_tk = Column(BigInteger, primary_key=True, autoincrement=True)
    version = Column(Integer, nullable=False, default=1)
    date_from = Column(DateTime, nullable=False, default=datetime.now)
    date_to = Column(DateTime, nullable=True)
    fuel_id = Column(Integer, nullable=False, index=True)  # Business key
    type = Column(String(30), nullable=False)
    is_current = Column(Boolean, nullable=False, default=True)
    created_date = Column(DateTime, default=datetime.now)


class DimLocation(Base):
    """Geografske informacije o tržištu (SCD Type 1)"""
    __tablename__ = 'dim_location'
    __table_args__ = {'schema': 'cars_dw'}

    location_tk = Column(BigInteger, primary_key=True, autoincrement=True)
    location_id = Column(Integer, nullable=False, index=True)  # Business key
    country = Column(String(50), nullable=False)
    region = Column(String(50), nullable=False)
    last_updated = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    created_date = Column(DateTime, default=datetime.now)


class FactCarSales(Base):
    """Glavni repozitorij svih prodajnih činjenica vezanih uz automobile na tržištu"""
    __tablename__ = 'fact_car_sales'
    __table_args__ = {'schema': 'cars_dw'}

    car_sales_tk = Column(BigInteger, primary_key=True, autoincrement=True)
    
    # Foreign keys to dimension tables
    date_tk = Column(Integer, ForeignKey('cars_dw.dim_date.date_tk'), nullable=False)
    manufacturer_tk = Column(BigInteger, ForeignKey('cars_dw.dim_manufacturer.manufacturer_tk'), nullable=False)
    vehicle_tk = Column(BigInteger, ForeignKey('cars_dw.dim_vehicle.vehicle_tk'), nullable=False)
    transmission_tk = Column(BigInteger, ForeignKey('cars_dw.dim_transmission.transmission_tk'), nullable=False)
    fuel_tk = Column(BigInteger, ForeignKey('cars_dw.dim_fuel.fuel_tk'), nullable=False)
    location_tk = Column(BigInteger, ForeignKey('cars_dw.dim_location.location_tk'), nullable=False)
    
    # Measures
    price = Column(DECIMAL(10, 2), nullable=False)  # Glavna mjera
    mileage = Column(Integer, nullable=False)
    tax = Column(Integer, nullable=False)
    mpg = Column(DECIMAL(5, 2), nullable=False)
    engine_size = Column(DECIMAL(3, 1), nullable=False)
    age = Column(Integer, nullable=False)
    
    # Audit fields
    created_date = Column(DateTime, default=datetime.now)
    updated_date = Column(DateTime, default=datetime.now, onupdate=datetime.now)


# Create Tables in the Database
try:
    # Drop all tables first (for development)
    print("Dropping existing tables...")
    Base.metadata.drop_all(engine)
    
    # Create all tables
    print("Creating dimensional model tables...")
    Base.metadata.create_all(engine)
    
    print("✅ Star Schema tables created successfully!")
    print("\n=== Created Tables ===")
    print("📊 Fact Table:")
    print("  - fact_car_sales")
    print("\n📋 Dimension Tables:")
    print("  - dim_date (SCD Type 0)")
    print("  - dim_manufacturer (SCD Type 2)")
    print("  - dim_vehicle (SCD Type 2)")
    print("  - dim_transmission (SCD Type 1)")
    print("  - dim_fuel (SCD Type 2)")
    print("  - dim_location (SCD Type 1)")
    
    print("\n🎯 Ready for ETL process!")
    
except Exception as e:
    print(f"❌ Error creating tables: {e}")
    
finally:
    session.close()

