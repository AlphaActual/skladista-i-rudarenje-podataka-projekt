import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, text
from sqlalchemy.orm import sessionmaker, relationship, declarative_base

CSV_FILE_PATH = "2_relational_model/processed/cars_data_80.csv"
df = pd.read_csv(CSV_FILE_PATH)
print("CSV size: ", df.shape)
print("Data types:")
print(df.dtypes)
print("Missing values per column:")
print(df.isnull().sum())

# Clean the data - handle NaN values
print("\n=== Data Cleaning ===")
# Fill NaN values in numeric columns with appropriate defaults
numeric_columns = ['price', 'mileage', 'tax', 'mpg', 'engineSize', 'age']
for col in numeric_columns:
    if col in df.columns:
        before_nan = df[col].isnull().sum()
        if before_nan > 0:
            if col in ['price', 'mileage', 'tax', 'age']:
                # Use median for these columns
                df[col] = df[col].fillna(df[col].median())
            else:
                # Use mean for mpg and engineSize
                df[col] = df[col].fillna(df[col].mean())
            print(f"Filled {before_nan} NaN values in {col} column")

# Fill NaN values in categorical columns with 'Unknown'
categorical_columns = ['model', 'transmission', 'fuelType', 'manufacturer', 
                      'decade', 'country', 'region', 'mileageCategory', 
                      'engineSizeClass', 'ageCategory']
for col in categorical_columns:
    if col in df.columns:
        before_nan = df[col].isnull().sum()
        if before_nan > 0:
            df[col] = df[col].fillna('Unknown')
            print(f"Filled {before_nan} NaN values in {col} column")

print("After cleaning - Missing values per column:")
print(df.isnull().sum())
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
    country = Column(String(45), nullable=False)
    region = Column(String(45), nullable=False)
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

class Decade(Base):
    __tablename__ = 'decade'
    decade_id = Column(Integer, primary_key=True, autoincrement=True)
    decade = Column(String(20), nullable=False, unique=True)
    cars = relationship('Car', back_populates='decade_ref')

class MileageCategory(Base):
    __tablename__ = 'mileage_category'
    mileage_category_id = Column(Integer, primary_key=True, autoincrement=True)
    category = Column(String(45), nullable=False, unique=True)
    cars = relationship('Car', back_populates='mileage_category_ref')

class EngineSizeClass(Base):
    __tablename__ = 'engine_size_class'
    engine_size_class_id = Column(Integer, primary_key=True, autoincrement=True)
    size_class = Column(String(45), nullable=False, unique=True)
    cars = relationship('Car', back_populates='engine_size_class_ref')

class AgeCategory(Base):
    __tablename__ = 'age_category'
    age_category_id = Column(Integer, primary_key=True, autoincrement=True)
    category = Column(String(45), nullable=False, unique=True)
    cars = relationship('Car', back_populates='age_category_ref')

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
    age = Column(Integer, nullable=False)
    
    # Foreign keys
    manufacturer_id = Column(Integer, ForeignKey('manufacturer.manufacturer_id'))
    transmission_id = Column(Integer, ForeignKey('transmission_type.transmission_id'))
    fuel_id = Column(Integer, ForeignKey('fuel_type.fuel_id'))
    decade_id = Column(Integer, ForeignKey('decade.decade_id'))
    mileage_category_id = Column(Integer, ForeignKey('mileage_category.mileage_category_id'))
    engine_size_class_id = Column(Integer, ForeignKey('engine_size_class.engine_size_class_id'))
    age_category_id = Column(Integer, ForeignKey('age_category.age_category_id'))
    
    # Relationships
    manufacturer = relationship('Manufacturer', back_populates='cars')
    transmission = relationship('TransmissionType', back_populates='cars')
    fuel = relationship('FuelType', back_populates='cars')
    decade_ref = relationship('Decade', back_populates='cars')
    mileage_category_ref = relationship('MileageCategory', back_populates='cars')
    engine_size_class_ref = relationship('EngineSizeClass', back_populates='cars')
    age_category_ref = relationship('AgeCategory', back_populates='cars')

# Setup database connection
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# Populate tables
#-----------------------------------------------------------------------------------------------------
# Populate Manufacturer table
manufacturers = {}  # Cache for manufacturers
manufacturer_data = df[['manufacturer', 'country', 'region']].drop_duplicates()
for _, row in manufacturer_data.iterrows():
    manufacturer = Manufacturer(
        name=row['manufacturer'],
        country=row['country'],
        region=row['region']
    )
    session.add(manufacturer)
    manufacturers[row['manufacturer']] = manufacturer
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

# Populate Decade table
decades = {}  # Cache for decades
for decade in df['decade'].unique():
    decade_obj = Decade(decade=decade)
    session.add(decade_obj)
    decades[decade] = decade_obj
session.commit()

# Populate MileageCategory table
mileage_categories = {}  # Cache for mileage categories
for category in df['mileageCategory'].unique():
    mileage_category = MileageCategory(category=category)
    session.add(mileage_category)
    mileage_categories[category] = mileage_category
session.commit()

# Populate EngineSizeClass table
engine_size_classes = {}  # Cache for engine size classes
for size_class in df['engineSizeClass'].unique():
    engine_size_class = EngineSizeClass(size_class=size_class)
    session.add(engine_size_class)
    engine_size_classes[size_class] = engine_size_class
session.commit()

# Populate AgeCategory table
age_categories = {}  # Cache for age categories
for category in df['ageCategory'].unique():
    age_category = AgeCategory(category=category)
    session.add(age_category)
    age_categories[category] = age_category
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
        age=row['age'],
        manufacturer=manufacturers[row['manufacturer']],
        transmission=transmissions[row['transmission']],
        fuel=fueltypes[row['fuelType']],
        decade_ref=decades[row['decade']],
        mileage_category_ref=mileage_categories[row['mileageCategory']],
        engine_size_class_ref=engine_size_classes[row['engineSizeClass']],
        age_category_ref=age_categories[row['ageCategory']]
    )
    session.add(car)

session.commit()
print("Database population completed successfully!")
print(f"Total cars inserted: {len(df)}")

# Print summary statistics
print("\n=== Database Population Summary ===")
print(f"Manufacturers: {len(manufacturers)}")
print(f"Transmission types: {len(transmissions)}")
print(f"Fuel types: {len(fueltypes)}")
print(f"Decades: {len(decades)}")
print(f"Mileage categories: {len(mileage_categories)}")
print(f"Engine size classes: {len(engine_size_classes)}")
print(f"Age categories: {len(age_categories)}")
print(f"Cars: {len(df)}")

session.close()
