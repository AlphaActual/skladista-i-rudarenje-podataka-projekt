import pandas as pd

def expand_cars_data(input_file, output_file):
    # Read the CSV file
    print(f"Reading data from {input_file}...")
    df = pd.read_csv(input_file)
    
    # Create a new 'decade' column based on the 'year' column
    print("Adding decade column...")
    df['decade'] = (df['year'] // 10 * 10).astype(str) + 's'
    
    # Define manufacturer to country and region mapping
    print("Adding country and region columns...")
    manufacturer_mapping = {
        'Hyundai': {'country': 'South Korea', 'region': 'Asia'},
        'Volkswagen': {'country': 'Germany', 'region': 'Europe'},
        'BMW': {'country': 'Germany', 'region': 'Europe'},
        'Skoda': {'country': 'Czech Republic', 'region': 'Europe'},
        'Ford': {'country': 'United States', 'region': 'North America'},
        'Toyota': {'country': 'Japan', 'region': 'Asia'},
        'Mercedes-Benz': {'country': 'Germany', 'region': 'Europe'},
        'Vauxhall': {'country': 'United Kingdom', 'region': 'Europe'},
        'Audi': {'country': 'Germany', 'region': 'Europe'}
    }
    
    # Add country and region columns based on manufacturer
    df['country'] = df['manufacturer'].map(lambda mfr: manufacturer_mapping.get(mfr, {}).get('country', 'Unknown'))
    df['region'] = df['manufacturer'].map(lambda mfr: manufacturer_mapping.get(mfr, {}).get('region', 'Unknown'))
    
    
    # Add mileage range categorization
    print("Adding mileage range column...")
    mileage_ranges = [
        (0, 5000, 'Very Low'),
        (5000, 20000, 'Low'),
        (20000, 50000, 'Medium'),
        (50000, 100000, 'High'),
        (100000, 150000, 'Very High'),
        (150000, float('inf'), 'Extreme')
    ]
    
    df['mileageCategory'] = 'Unknown'
    for low, high, category in mileage_ranges:
        mask = (df['mileage'] >= low) & (df['mileage'] < high)
        df.loc[mask, 'mileageCategory'] = category
    
    # Add engine size classification
    print("Adding engine size classification...")
    # Define engine size classes
    engine_size_classes = [
        (0.1, 1.5, 'Small'),  # Start from 0.1 to exclude 0.0 values
        (1.5, 2.5, 'Medium'),
        (2.5, float('inf'), 'Large')
    ]

    # Apply categorization
    df['engineSizeClass'] = 'Unknown'
    for low, high, category in engine_size_classes:
        mask = (df['engineSize'] >= low) & (df['engineSize'] < high)
        df.loc[mask, 'engineSizeClass'] = category    # Add age column based on year (assuming current year is 2025)
    print("Adding age column and categorization...")
    current_year = 2025
    df['age'] = current_year - df['year']
    
    # Define age categories
    age_categories = [
        (0, 3, 'New'),
        (3, 7, 'Recent'),
        (7, 12, 'Mature'),
        (12, 20, 'Old'),
        (20, float('inf'), 'Vintage')
    ]
    
    # Apply age categorization
    df['ageCategory'] = 'Unknown'
    for low, high, category in age_categories:
        mask = (df['age'] >= low) & (df['age'] < high)
        df.loc[mask, 'ageCategory'] = category
    
    # Save the expanded data to a new CSV file
    print(f"Saving expanded data to {output_file}...")
    df.to_csv(output_file, index=False)
    
    print("Done!")

if __name__ == "__main__":
    input_file = "2_relational_model/processed/cars_data_PROCESSED.csv"
    output_file = "2_relational_model/processed/cars_data_EXPANDED.csv"
    expand_cars_data(input_file, output_file)




