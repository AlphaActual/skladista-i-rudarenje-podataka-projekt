import unittest
import pandas as pd
import sqlalchemy
from pandas.testing import assert_frame_equal

class TestDatabase(unittest.TestCase):
    def setUp(self):
        # Connect to database
        self.engine = sqlalchemy.create_engine('mysql+pymysql://root:root@localhost:3306/cars')
        self.connection = self.engine.connect()

        # Load CSV file - use the same file that was used to populate the database (80% split)
        self.df = pd.read_csv("2_relational_model/processed/cars_data_80.csv")

        # Query to fetch all data from the database tables including all expanded columns
        query = """
        SELECT mfg.name as 'manufacturer'
        , mdl.name as 'model'
        , c.year
        , c.price
        , c.mileage
        , c.tax
        , c.mpg
        , c.engineSize
        , t.type as 'transmission'
        , f.type as 'fuelType'
        , d.decade
        , cnt.name as 'country'
        , r.name as 'region'
        , mc.category as 'mileageCategory'
        , esc.size_class as 'engineSizeClass'
        , c.age
        , ac.category as 'ageCategory'
        FROM car c
        JOIN model mdl ON c.model_id = mdl.model_id
        JOIN manufacturer mfg ON mdl.manufacturer_id = mfg.manufacturer_id
        JOIN country cnt ON mfg.country_id = cnt.country_id
        JOIN region r ON cnt.region_id = r.region_id
        JOIN transmission_type t ON c.transmission_id = t.transmission_id
        JOIN fuel_type f ON c.fuel_id = f.fuel_id
        JOIN decade d ON c.decade_id = d.decade_id
        JOIN mileage_category mc ON c.mileage_category_id = mc.mileage_category_id
        JOIN engine_size_class esc ON c.engine_size_class_id = esc.engine_size_class_id
        JOIN age_category ac ON c.age_category_id = ac.age_category_id
        ORDER BY c.car_id ASC
        """
        result = self.connection.execute(sqlalchemy.text(query))
        self.db_df = pd.DataFrame(result.fetchall())
        self.db_df.columns = result.keys()

    def test_columns(self):
        csv_columns = set(self.df.columns)
        db_columns = set(self.db_df.columns)
        self.assertEqual(csv_columns, db_columns, 
                        f"Column mismatch.\nCSV columns: {csv_columns}\nDB columns: {db_columns}")

    def test_dataframes(self):
        # Define the desired column order - all columns from the expanded dataset
        column_order = ['manufacturer', 'model', 'year', 'price', 'mileage', 'tax', 'mpg', 
                       'engineSize', 'transmission', 'fuelType', 'decade', 'country', 'region',
                       'mileageCategory', 'engineSizeClass', 'age', 'ageCategory']
        
        # Reorder columns in both dataframes
        self.df = self.df[column_order]
        self.db_df = self.db_df[column_order]

        # Sort both dataframes by the same columns to ensure proper comparison
        sort_columns = ['manufacturer', 'model', 'year', 'price']
        self.df = self.df.sort_values(sort_columns).reset_index(drop=True)
        self.db_df = self.db_df.sort_values(sort_columns).reset_index(drop=True)
        
        # Compare dataframes
        try:
            assert_frame_equal(self.df, self.db_df)
        except AssertionError as e:
            print("Differences found between CSV and database:")
            print(e)
            raise

    def tearDown(self):
        self.connection.close()

if __name__ == '__main__':
    unittest.main()