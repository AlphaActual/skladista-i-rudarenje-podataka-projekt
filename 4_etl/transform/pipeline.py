from transform.dimensions.manufacturer_dim import transform_manufacturer_dim
from transform.dimensions.vehicle_dim import transform_vehicle_dim
from transform.dimensions.transmission_dim import transform_transmission_dim
from transform.dimensions.fuel_dim import transform_fuel_dim
from transform.dimensions.location_dim import transform_location_dim
from transform.dimensions.date_dim import transform_date_dim
from transform.dimensions.category_dims import transform_mileage_category_dim, transform_engine_size_class_dim, transform_age_category_dim
from transform.facts.car_sales_fact import transform_car_sales_fact
from spark_session import get_spark_session


def run_transformations(raw_data):
    """Transform raw car data into dimensional model"""
    
    # Get Spark session for category dimensions
    spark = get_spark_session("Transform_Categories")
    
    # Transform dimensions
    manufacturer_dim = transform_manufacturer_dim(
        raw_data["manufacturer"],
        raw_data["country"],
        raw_data["region"],
        csv_cars_df=raw_data.get("csv_cars")
    )
    print("1️⃣ Manufacturer dimension complete")
    
    vehicle_dim = transform_vehicle_dim(
        raw_data["model"],
        raw_data["manufacturer"],
        csv_cars_df=raw_data.get("csv_cars")
    )
    print("2️⃣ Vehicle dimension complete")

    # Transform category dimensions separately (static categories)
    mileage_category_dim = transform_mileage_category_dim(spark)
    print("2a️⃣ Mileage category dimension complete")
    
    engine_size_class_dim = transform_engine_size_class_dim(spark)
    print("2b️⃣ Engine size class dimension complete")
    
    age_category_dim = transform_age_category_dim(spark)
    print("2c️⃣ Age category dimension complete")

    transmission_dim = transform_transmission_dim(
        raw_data["transmission_type"],
        csv_cars_df=raw_data.get("csv_cars")
    )
    print("3️⃣ Transmission dimension complete")
    
    fuel_dim = transform_fuel_dim(
        raw_data["fuel_type"],
        csv_cars_df=raw_data.get("csv_cars")
    )
    print("4️⃣ Fuel dimension complete")
    
    location_dim = transform_location_dim(
        raw_data["country"],
        raw_data["region"]
    )
    print("5️⃣ Location dimension complete")

    date_dim = transform_date_dim(
        raw_data["car"],
        csv_cars_df=raw_data.get("csv_cars")
    )
    print("6️⃣ Date dimension complete")

    # Transform fact table
    car_sales_fact = transform_car_sales_fact(
        raw_data,
        manufacturer_dim,
        vehicle_dim,
        transmission_dim,
        fuel_dim,
        location_dim,
        date_dim,
        mileage_category_dim,
        engine_size_class_dim,
        age_category_dim
    )
    print("7️⃣ Car sales fact table complete")

    return {
        "dim_manufacturer": manufacturer_dim,
        "dim_vehicle": vehicle_dim,
        "dim_transmission": transmission_dim,
        "dim_fuel": fuel_dim,
        "dim_location": location_dim,
        "dim_date": date_dim,
        "dim_mileage_category": mileage_category_dim,
        "dim_engine_size_class": engine_size_class_dim,
        "dim_age_category": age_category_dim,
        "fact_car_sales": car_sales_fact
    }