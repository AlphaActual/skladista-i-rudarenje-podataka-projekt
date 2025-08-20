import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, text, UniqueConstraint
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
class Region(Base):
    __tablename__ = 'region'
    region_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(45), nullable=False, unique=True)
    countries = relationship('Country', back_populates='region')

class Country(Base):
    __tablename__ = 'country'
    country_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(45), nullable=False, unique=True)
    region_id = Column(Integer, ForeignKey('region.region_id'))
    region = relationship('Region', back_populates='countries')
    manufacturers = relationship('Manufacturer', back_populates='country')

class Manufacturer(Base):
    __tablename__ = 'manufacturer'
    manufacturer_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(45), nullable=False, unique=True)
    country_id = Column(Integer, ForeignKey('country.country_id'))
    country = relationship('Country', back_populates='manufacturers')
    models = relationship('Model', back_populates='manufacturer')

class Model(Base):
    __tablename__ = 'model'
    model_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(45), nullable=False)
    manufacturer_id = Column(Integer, ForeignKey('manufacturer.manufacturer_id'))
    manufacturer = relationship('Manufacturer', back_populates='models')
    cars = relationship('Car', back_populates='model')
    
    # Ensure unique model names per manufacturer
    __table_args__ = (UniqueConstraint('name', 'manufacturer_id', name='unique_model_per_manufacturer'),)

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
    year = Column(Integer, nullable=False)
    price = Column(Integer, nullable=False)
    mileage = Column(Integer, nullable=False)
    tax = Column(Integer, nullable=False)
    mpg = Column(Float, nullable=False)
    engineSize = Column(Float, nullable=False)
    age = Column(Integer, nullable=False)
    
    # Foreign keys
    model_id = Column(Integer, ForeignKey('model.model_id'))
    transmission_id = Column(Integer, ForeignKey('transmission_type.transmission_id'))
    fuel_id = Column(Integer, ForeignKey('fuel_type.fuel_id'))
    decade_id = Column(Integer, ForeignKey('decade.decade_id'))
    mileage_category_id = Column(Integer, ForeignKey('mileage_category.mileage_category_id'))
    engine_size_class_id = Column(Integer, ForeignKey('engine_size_class.engine_size_class_id'))
    age_category_id = Column(Integer, ForeignKey('age_category.age_category_id'))
    
    # Relationships
    model = relationship('Model', back_populates='cars')
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
# Populate Region table first
regions = {}  # Cache for regions
for region in df['region'].unique():
    region_obj = Region(name=region)
    session.add(region_obj)
    regions[region] = region_obj
session.commit()

# Populate Country table
countries = {}  # Cache for countries
country_data = df[['country', 'region']].drop_duplicates()
for _, row in country_data.iterrows():
    country = Country(
        name=row['country'],
        region=regions[row['region']]
    )
    session.add(country)
    countries[row['country']] = country
session.commit()

# Populate Manufacturer table
manufacturers = {}  # Cache for manufacturers
manufacturer_data = df[['manufacturer', 'country']].drop_duplicates()
for _, row in manufacturer_data.iterrows():
    manufacturer = Manufacturer(
        name=row['manufacturer'],
        country=countries[row['country']]
    )
    session.add(manufacturer)
    manufacturers[row['manufacturer']] = manufacturer
session.commit()

# Populate Model table
models = {}  # Cache for models
model_data = df[['model', 'manufacturer']].drop_duplicates()
for _, row in model_data.iterrows():
    model = Model(
        name=row['model'],
        manufacturer=manufacturers[row['manufacturer']]
    )
    session.add(model)
    # Use tuple as key to handle same model names across manufacturers
    models[(row['model'], row['manufacturer'])] = model
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
        year=row['year'],
        price=row['price'],
        mileage=row['mileage'],
        tax=row['tax'],
        mpg=row['mpg'],
        engineSize=row['engineSize'],
        age=row['age'],
        model=models[(row['model'], row['manufacturer'])],
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
print(f"Regions: {len(regions)}")
print(f"Countries: {len(countries)}")
print(f"Manufacturers: {len(manufacturers)}")
print(f"Models: {len(models)}")
print(f"Transmission types: {len(transmissions)}")
print(f"Fuel types: {len(fueltypes)}")
print(f"Decades: {len(decades)}")
print(f"Mileage categories: {len(mileage_categories)}")
print(f"Engine size classes: {len(engine_size_classes)}")
print(f"Age categories: {len(age_categories)}")
print(f"Cars: {len(df)}")

session.close()
