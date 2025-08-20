import pandas as pd

CSV_FILE_PATH = "2_relational_model/processed/cars_data_EXPANDED.csv"
# Random split of dataset into 80:20 (will need later)
df = pd.read_csv(CSV_FILE_PATH, delimiter=',')

df20 = df.sample(frac=0.2, random_state=1)
df = df.drop(df20.index)
print("CSV size 80: ", df.shape)
print("CSV size 20: ", df20.shape)

# Save preprocessed dataset to new CSV file
df.to_csv("2_relational_model/processed/cars_data_80.csv", index=False)
df20.to_csv("2_relational_model/processed/cars_data_20.csv", index=False)