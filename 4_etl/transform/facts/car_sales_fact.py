from pyspark.sql.functions import col, lit, when, isnotnull, current_timestamp, trim, initcap, rank
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def transform_car_sales_fact(raw_data, manufacturer_dim, vehicle_dim, transmission_dim, fuel_dim, location_dim, date_dim, 
                           mileage_category_dim, engine_size_class_dim, age_category_dim):
    """
    Transform car sales fact table with proper dimension key lookups
    This is the central fact table containing all measures and foreign keys
    """
    
    # Prepare data from both sources and merge them
    fact_dfs = []
    
    # Process database data if available
    if "car" in raw_data and raw_data["car"].count() > 0:
        db_fact_df = prepare_fact_from_database(raw_data["car"], raw_data)
        fact_dfs.append(db_fact_df)
        print(f"Database records: {db_fact_df.count()}")
    
    # Process CSV data if available
    if "csv_cars" in raw_data and raw_data["csv_cars"] is not None:
        csv_fact_df = prepare_fact_from_csv(raw_data["csv_cars"])
        fact_dfs.append(csv_fact_df)
        print(f"CSV records: {csv_fact_df.count()}")
    
    if not fact_dfs:
        raise ValueError("No car data found in raw_data")
    
    # Union all fact dataframes
    if len(fact_dfs) == 1:
        fact_df = fact_dfs[0]
    else:
        fact_df = fact_dfs[0]
        for df in fact_dfs[1:]:
            fact_df = fact_df.unionByName(df, allowMissingColumns=True)
        print(f"Total merged records: {fact_df.count()}")
    
    
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
        "age",
        "source"
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
            initcap(trim(col("man.name"))).alias("manufacturer_name"),
            initcap(trim(col("m.name"))).alias("model_name"),
            initcap(trim(col("t.type"))).alias("transmission_type"),
            initcap(trim(col("f.type"))).alias("fuel_type"),
            col("co.name").alias("country_name"),
            col("r.name").alias("region_name"),
            lit("database").alias("source")
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
            initcap(trim(col("manufacturer"))).alias("manufacturer_name"),
            initcap(trim(col("model"))).alias("model_name"),
            initcap(trim(col("transmission"))).alias("transmission_type"),
            initcap(trim(col("fuelType"))).alias("fuel_type"),
            col("country").alias("country_name"),
            col("region").alias("region_name"),  
            lit("csv").alias("source")
        )
    )
    
    return fact_df

def perform_dimension_lookups(fact_df, manufacturer_dim, vehicle_dim, transmission_dim, fuel_dim, location_dim, date_dim,
                            mileage_category_dim, engine_size_class_dim, age_category_dim):
    """Perform dimension key lookups to get surrogate keys"""
    
    # DEBUG: Check all dimension join values
    # print("=== DEBUGGING ALL DIMENSION JOINS ===")
    
    # print("Unique manufacturer names in fact table:")
    # fact_df.select("manufacturer_name").distinct().show(10)
    # print("Manufacturer dimension table (first 10):")
    # manufacturer_dim.select("manufacturer_tk", "name").show(10)
    
    # print("Unique model names in fact table (first 10):")
    # fact_df.select("model_name").distinct().show(10)
    # print("Vehicle dimension table (first 10):")
    # vehicle_dim.select("vehicle_tk", "model_name").show(10)
    
    # print("Unique fuel types in fact table:")
    # fact_df.select("fuel_type").distinct().show()
    # print("Fuel dimension table:")
    # fuel_dim.select("fuel_tk", "type").show()
    
    # print("Unique countries in fact table:")
    # fact_df.select("country_name").distinct().show()
    # print("Location dimension table:")
    # location_dim.select("location_tk", "country").show()
    
    # print("Unique years in fact table:")
    # fact_df.select("year").distinct().orderBy("year").show()
    # print("Date dimension table (first 10):")
    # date_dim.select("date_tk", "year").show(10)
    
    # Lookup manufacturer dimension key
    fact_with_man = (
        fact_df.alias("f")
        .join(manufacturer_dim.alias("md"), 
              col("f.manufacturer_name") == col("md.name"), "left")
        .select(col("f.*"), col("md.manufacturer_tk"))
    )
    
    # print("Manufacturer join results (showing null manufacturer_tk):")
    # fact_with_man.filter(col("manufacturer_tk").isNull()).select("manufacturer_name").distinct().show()
    
    # Lookup vehicle dimension key (now based on model_name only)
    fact_with_veh = (
        fact_with_man.alias("f")
        .join(vehicle_dim.alias("vd"),
              col("f.model_name") == col("vd.model_name"), "left")
        .select(col("f.*"), col("vd.vehicle_tk"))
    )
    
    # print("Vehicle join results (showing null vehicle_tk):")
    # fact_with_veh.filter(col("vehicle_tk").isNull()).select("model_name").distinct().show()
    
    # Lookup transmission dimension key
    fact_with_trans = (
        fact_with_veh.alias("f")
        .join(transmission_dim.alias("td"),
              col("f.transmission_type") == col("td.type"), "left")
        .select(col("f.*"), col("td.transmission_tk"))
    )
    
    # print("Transmission join results (showing null transmission_tk):")
    # fact_with_trans.filter(col("transmission_tk").isNull()).select("transmission_type").distinct().show()
    
    # Lookup fuel dimension key
    fact_with_fuel = (
        fact_with_trans.alias("f")
        .join(fuel_dim.alias("fd"),
              col("f.fuel_type") == col("fd.type"), "left")
        .select(col("f.*"), col("fd.fuel_tk"))
    )
    
    # print("Fuel join results (showing null fuel_tk):")
    # fact_with_fuel.filter(col("fuel_tk").isNull()).select("fuel_type").distinct().show()
    
    # Lookup location dimension key
    fact_with_loc = (
        fact_with_fuel.alias("f")
        .join(location_dim.alias("ld"),
              col("f.country_name") == col("ld.country"), "left")
        .select(col("f.*"), col("ld.location_tk"))
    )
    
    # print("Location join results (showing null location_tk):")
    # fact_with_loc.filter(col("location_tk").isNull()).select("country_name").distinct().show()
    
    # Lookup date dimension key
    fact_with_date = (
        fact_with_loc.alias("f")
        .join(date_dim.alias("dd"),
              col("f.year") == col("dd.year"), "left")
        .select(col("f.*"), col("dd.date_tk"))
    )
    
    # print("Date join results (showing null date_tk):")
    # fact_with_date.filter(col("date_tk").isNull()).select("year").distinct().show()
    
    fact_with_categories = (
        fact_with_date
        .withColumn("mileage_category", 
                   when(col("mileage") < 5000, "Very Low")
                   .when(col("mileage") < 20000, "Low")
                   .when(col("mileage") < 50000, "Medium") 
                   .when(col("mileage") < 100000, "High")
                   .when(col("mileage") < 150000, "Very High")
                   .otherwise("Extreme"))
        .withColumn("engine_size_class",
                   when(col("engine_size") < 1.5, "Small")
                   .when(col("engine_size") < 2.5, "Medium")
                   .otherwise("Large"))
        .withColumn("age_category",
                   when(col("age") < 3, "New")
                   .when(col("age") < 7, "Recent")
                   .when(col("age") < 12, "Mature")
                   .when(col("age") < 20, "Old")
                   .otherwise("Vintage"))
    )
    
    # Lookup mileage category dimension key
    fact_with_mileage_cat = (
        fact_with_categories.alias("f")
        .join(mileage_category_dim.alias("mcd"),
              col("f.mileage_category") == col("mcd.mileage_category"), "left")
        .select(col("f.*"), col("mcd.mileage_category_tk"))
    )
    
    # print("Mileage category join results (showing null mileage_category_tk):")
    # fact_with_mileage_cat.filter(col("mileage_category_tk").isNull()).select("mileage_category").distinct().show()
    
    # Lookup engine size class dimension key
    fact_with_engine_cat = (
        fact_with_mileage_cat.alias("f")
        .join(engine_size_class_dim.alias("ecd"),
              col("f.engine_size_class") == col("ecd.engine_size_class"), "left")
        .select(col("f.*"), col("ecd.engine_size_class_tk"))
    )
    
    # print("Engine size class join results (showing null engine_size_class_tk):")
    # fact_with_engine_cat.filter(col("engine_size_class_tk").isNull()).select("engine_size_class").distinct().show()
    
    # Lookup age category dimension key
    fact_with_age_cat = (
        fact_with_engine_cat.alias("f")
        .join(age_category_dim.alias("acd"),
              col("f.age_category") == col("acd.age_category"), "left")
        .select(col("f.*"), col("acd.age_category_tk"))
    )
    
    # print("Age category join results (showing null age_category_tk):")
    # fact_with_age_cat.filter(col("age_category_tk").isNull()).select("age_category").distinct().show()
    
    # Check final counts before fillna
    # print("=== FINAL JOIN SUMMARY ===")
    # print("Total records:", fact_with_age_cat.count())
    # print("Records with null manufacturer_tk:", fact_with_age_cat.filter(col("manufacturer_tk").isNull()).count())
    # print("Records with null vehicle_tk:", fact_with_age_cat.filter(col("vehicle_tk").isNull()).count())
    # print("Records with null transmission_tk:", fact_with_age_cat.filter(col("transmission_tk").isNull()).count())
    # print("Records with null fuel_tk:", fact_with_age_cat.filter(col("fuel_tk").isNull()).count())
    # print("Records with null location_tk:", fact_with_age_cat.filter(col("location_tk").isNull()).count())
    # print("Records with null date_tk:", fact_with_age_cat.filter(col("date_tk").isNull()).count())
    # print("Records with null mileage_category_tk:", fact_with_age_cat.filter(col("mileage_category_tk").isNull()).count())
    # print("Records with null engine_size_class_tk:", fact_with_age_cat.filter(col("engine_size_class_tk").isNull()).count())
    # print("Records with null age_category_tk:", fact_with_age_cat.filter(col("age_category_tk").isNull()).count())
    
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
