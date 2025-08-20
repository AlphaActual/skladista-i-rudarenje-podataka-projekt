import unittest
import pandas as pd
import sqlalchemy
from pandas.testing import assert_frame_equal

class TestDatabase(unittest.TestCase):
    def setUp(self):
        # Connect to database
        self.engine = sqlalchemy.create_engine('mysql+pymysql://root:root@localhost:3306/cars')
        self.connection = self.engine.connect()

        # Load CSV file
        self.df = pd.read_csv("CarsData_PROCESSED.csv")

        # Query to fetch all data from the database tables
        query = """
        SELECT m.name as 'Manufacturer'
        , c.model
        , c.year
        , c.price
        , c.mileage
        , c.tax
        , c.mpg
        , c.engineSize
        , t.type as 'transmission'
        , f.type as 'fuelType'
        FROM car c
        JOIN manufacturer m ON c.manufacturer_id = m.manufacturer_id
        JOIN transmission_type t ON c.transmission_id = t.transmission_id
        JOIN fuel_type f ON c.fuel_id = f.fuel_id
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
        # Define the desired column order
        column_order = ['Manufacturer', 'model', 'year', 'price', 'mileage', 'tax', 'mpg', 
                       'engineSize', 'transmission', 'fuelType']
        
        # Reorder columns in both dataframes
        self.df = self.df[column_order]
        self.db_df = self.db_df[column_order]

        # Sort both dataframes by the same columns to ensure proper comparison
        sort_columns = ['Manufacturer', 'model', 'year', 'price']
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