from pyspark.sql.functions import col, lit, when, isnotnull, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def transform_car_sales_fact(raw_data, manufacturer_dim, vehicle_dim, transmission_dim, fuel_dim, location_dim, date_dim, 
                           mileage_category_dim, engine_size_class_dim, age_category_dim):
    """
    Transform car sales fact table with proper dimension key lookups
    This is the central fact table containing all measures and foreign keys
    """
    
    # Get the main car data - priority to database data if available
    if "car" in raw_data and raw_data["car"].count() > 0:
        main_df = raw_data["car"]
        source_type = "database"
    else:
        # Use CSV data as fallback
        main_df = raw_data.get("csv_cars")
        source_type = "csv"
    
    if main_df is None:
        raise ValueError("No car data found in raw_data")
    
    # Process based on source type
    if source_type == "database":
        # Database source - join with dimension tables for lookups
        fact_df = prepare_fact_from_database(main_df, raw_data)
    else:
        # CSV source - prepare fact table from CSV data
        fact_df = prepare_fact_from_csv(main_df)
    
    # Now perform dimension lookups
    fact_with_dims = perform_dimension_lookups(
        fact_df, manufacturer_dim, vehicle_dim, transmission_dim, 
        fuel_dim, location_dim, date_dim, mileage_category_dim, 
        engine_size_class_dim, age_category_dim
    )
    
    # Add surrogate key for fact table
    window = Window.orderBy("year", "price", "mileage")
    fact_with_dims = fact_with_dims.withColumn("car_sales_tk", row_number().over(window))
    
    # Final fact table structure
    final_fact = fact_with_dims.select(
        "car_sales_tk",
        "date_tk",
        "manufacturer_tk", 
        "vehicle_tk",
        "transmission_tk",
        "fuel_tk",
        "location_tk",
        "mileage_category_tk",
        "engine_size_class_tk", 
        "age_category_tk",
        "price",
        "mileage",
        "tax",
        "mpg",
        "engine_size",
        "age"
    )
    
    return final_fact

def prepare_fact_from_database(car_df, raw_data):
    """Prepare fact table from database source with joins"""
    
    # Join with related tables to get business keys for dimension lookups
    manufacturer_df = raw_data["manufacturer"]
    model_df = raw_data["model"]
    transmission_df = raw_data["transmission_type"]
    fuel_df = raw_data["fuel_type"]
    country_df = raw_data["country"]
    region_df = raw_data["region"]
    
    fact_df = (
        car_df.alias("c")
        .join(model_df.alias("m"), col("c.model_id") == col("m.model_id"), "left")
        .join(manufacturer_df.alias("man"), col("m.manufacturer_id") == col("man.manufacturer_id"), "left")
        .join(transmission_df.alias("t"), col("c.transmission_id") == col("t.transmission_id"), "left")
        .join(fuel_df.alias("f"), col("c.fuel_id") == col("f.fuel_id"), "left")
        .join(country_df.alias("co"), col("man.country_id") == col("co.country_id"), "left")
        .join(region_df.alias("r"), col("co.region_id") == col("r.region_id"), "left")
        .select(
            col("c.year"),
            col("c.price"),
            col("c.mileage"),
            col("c.tax"),
            col("c.mpg"),
            col("c.engineSize").alias("engine_size"),
            col("c.age"),
            col("man.name").alias("manufacturer_name"),
            col("m.name").alias("model_name"),
            col("t.type").alias("transmission_type"),
            col("f.type").alias("fuel_type"),
            col("co.name").alias("country_name"),
            col("r.name").alias("region_name")
        )
    )
    
    return fact_df

def prepare_fact_from_csv(csv_df):
    """Prepare fact table from CSV source"""
    
    fact_df = (
        csv_df.select(
            col("year"),
            col("price"),
            col("mileage"),
            col("tax"),
            col("mpg"),
            col("engineSize").alias("engine_size"),
            when(col("year").isNotNull(), 
                 lit(2025) - col("year")).otherwise(lit(0)).alias("age"),  # Calculate age
            col("manufacturer").alias("manufacturer_name"),
            col("model").alias("model_name"),
            col("transmission").alias("transmission_type"),
            col("fuelType").alias("fuel_type"),
            lit("Unknown").alias("country_name"),
            lit("Unknown").alias("region_name")
        )
    )
    
    return fact_df

def perform_dimension_lookups(fact_df, manufacturer_dim, vehicle_dim, transmission_dim, fuel_dim, location_dim, date_dim,
                            mileage_category_dim, engine_size_class_dim, age_category_dim):
    """Perform dimension key lookups to get surrogate keys"""
    
    # Lookup manufacturer dimension key
    fact_with_man = (
        fact_df.alias("f")
        .join(manufacturer_dim.alias("md"), 
              col("f.manufacturer_name") == col("md.name"), "left")
        .select(col("f.*"), col("md.manufacturer_tk"))
    )
    
    # Lookup vehicle dimension key (now based on model_name only)
    fact_with_veh = (
        fact_with_man.alias("f")
        .join(vehicle_dim.alias("vd"),
              col("f.model_name") == col("vd.model_name"), "left")
        .select(col("f.*"), col("vd.vehicle_tk"))
    )
    
    # Lookup transmission dimension key
    fact_with_trans = (
        fact_with_veh.alias("f")
        .join(transmission_dim.alias("td"),
              col("f.transmission_type") == col("td.type"), "left")
        .select(col("f.*"), col("td.transmission_tk"))
    )
    
    # Lookup fuel dimension key
    fact_with_fuel = (
        fact_with_trans.alias("f")
        .join(fuel_dim.alias("fd"),
              col("f.fuel_type") == col("fd.type"), "left")
        .select(col("f.*"), col("fd.fuel_tk"))
    )
    
    # Lookup location dimension key
    fact_with_loc = (
        fact_with_fuel.alias("f")
        .join(location_dim.alias("ld"),
              col("f.country_name") == col("ld.country"), "left")
        .select(col("f.*"), col("ld.location_tk"))
    )
    
    # Lookup date dimension key
    fact_with_date = (
        fact_with_loc.alias("f")
        .join(date_dim.alias("dd"),
              col("f.year") == col("dd.year"), "left")
        .select(col("f.*"), col("dd.date_tk"))
    )
    
    # Add category classifications based on actual values
    fact_with_categories = (
        fact_with_date
        .withColumn("mileage_category", 
                   when(col("mileage") < 10000, "Low")
                   .when(col("mileage") < 50000, "Medium") 
                   .when(col("mileage") < 100000, "High")
                   .otherwise("Very High"))
        .withColumn("engine_size_class",
                   when(col("engine_size") < 1.5, "Small")
                   .when(col("engine_size") < 2.5, "Medium")
                   .when(col("engine_size") < 4.0, "Large") 
                   .otherwise("Very Large"))
        .withColumn("age_category",
                   when(col("age") < 3, "New")
                   .when(col("age") < 7, "Recent")
                   .when(col("age") < 15, "Used")
                   .otherwise("Old"))
    )
    
    # Lookup mileage category dimension key
    fact_with_mileage_cat = (
        fact_with_categories.alias("f")
        .join(mileage_category_dim.alias("mcd"),
              col("f.mileage_category") == col("mcd.mileage_category"), "left")
        .select(col("f.*"), col("mcd.mileage_category_tk"))
    )
    
    # Lookup engine size class dimension key
    fact_with_engine_cat = (
        fact_with_mileage_cat.alias("f")
        .join(engine_size_class_dim.alias("ecd"),
              col("f.engine_size_class") == col("ecd.engine_size_class"), "left")
        .select(col("f.*"), col("ecd.engine_size_class_tk"))
    )
    
    # Lookup age category dimension key
    fact_with_age_cat = (
        fact_with_engine_cat.alias("f")
        .join(age_category_dim.alias("acd"),
              col("f.age_category") == col("acd.age_category"), "left")
        .select(col("f.*"), col("acd.age_category_tk"))
    )
    
    # Fill null foreign keys with -1 (unknown)
    fact_final = fact_with_age_cat.fillna({
        "manufacturer_tk": -1,
        "vehicle_tk": -1,
        "transmission_tk": -1,
        "fuel_tk": -1,
        "location_tk": -1,
        "date_tk": -1,
        "mileage_category_tk": -1,
        "engine_size_class_tk": -1,
        "age_category_tk": -1
    })
    
    return fact_final
