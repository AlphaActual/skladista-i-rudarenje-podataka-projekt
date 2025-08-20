import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, text
from sqlalchemy.orm import sessionmaker, relationship, declarative_base

CSV_FILE_PATH = "2_relational_model/processed/cars_data_80.csv"
df = pd.read_csv(CSV_FILE_PATH)
print("CSV size: ", df.shape)
print(df.head())

Base = declarative_base()

# Add this before create_engine()
from sqlalchemy import create_engine, text

# Create a temporary engine to connect to MySQL server without specifying a database
temp_engine = create_engine('mysql+pymysql://root:root@localhost', echo=False)

# Create database if it doesn't exist
with temp_engine.connect() as conn:
    conn.execute(text("CREATE DATABASE IF NOT EXISTS cars CHARACTER SET utf8mb4"))
    conn.commit()

# Now create the main engine connecting to the cars database
engine = create_engine('mysql+pymysql://root:root@localhost:3306/cars', echo=False)

# Define database schema
#-----------------------------------------------------------------------------------------------------
class Manufacturer(Base):
    __tablename__ = 'manufacturer'
    manufacturer_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(45), nullable=False, unique=True)
    cars = relationship('Car', back_populates='manufacturer')

class TransmissionType(Base):
    __tablename__ = 'transmission_type'
    transmission_id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String(45), nullable=False, unique=True)
    cars = relationship('Car', back_populates='transmission')

class FuelType(Base):
    __tablename__ = 'fuel_type'
    fuel_id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String(45), nullable=False, unique=True)
    cars = relationship('Car', back_populates='fuel')

class Car(Base):
    __tablename__ = 'car'
    car_id = Column(Integer, primary_key=True, autoincrement=True)
    model = Column(String(45), nullable=False)
    year = Column(Integer, nullable=False)
    price = Column(Integer, nullable=False)
    mileage = Column(Integer, nullable=False)
    tax = Column(Integer, nullable=False)
    mpg = Column(Float, nullable=False)
    engineSize = Column(Float, nullable=False)
    
    # Foreign keys
    manufacturer_id = Column(Integer, ForeignKey('manufacturer.manufacturer_id'))
    transmission_id = Column(Integer, ForeignKey('transmission_type.transmission_id'))
    fuel_id = Column(Integer, ForeignKey('fuel_type.fuel_id'))
    
    # Relationships
    manufacturer = relationship('Manufacturer', back_populates='cars')
    transmission = relationship('TransmissionType', back_populates='cars')
    fuel = relationship('FuelType', back_populates='cars')

# Setup database connection
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# Populate tables
#-----------------------------------------------------------------------------------------------------
# Populate Manufacturer table
manufacturers = {}  # Cache for manufacturers
for name in df['Manufacturer'].unique():
    manufacturer = Manufacturer(name=name)
    session.add(manufacturer)
    manufacturers[name] = manufacturer
session.commit()

# Populate TransmissionType table
transmissions = {}  # Cache for transmission types
for type in df['transmission'].unique():
    transmission = TransmissionType(type=type)
    session.add(transmission)
    transmissions[type] = transmission
session.commit()

# Populate FuelType table
fueltypes = {}  # Cache for fuel types
for type in df['fuelType'].unique():
    fueltype = FuelType(type=type)
    session.add(fueltype)
    fueltypes[type] = fueltype
session.commit()

# Populate Car table
for _, row in df.iterrows():
    car = Car(
        model=row['model'],
        year=row['year'],
        price=row['price'],
        mileage=row['mileage'],
        tax=row['tax'],
        mpg=row['mpg'],
        engineSize=row['engineSize'],
        manufacturer=manufacturers[row['Manufacturer']],
        transmission=transmissions[row['transmission']],
        fuel=fueltypes[row['fuelType']]
    )
    session.add(car)

session.commit()
print("Database population completed successfully!")
