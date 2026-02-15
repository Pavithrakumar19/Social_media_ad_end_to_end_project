import pandas as pd

# Loading  the data
df = pd.read_csv('social_media_ad_optimization.csv')

# Basic exploration
print("Dataset Overview")
print("=" * 50)
print(f"Total rows: {len(df)}")
print(f"Total columns: {len(df.columns)}")
print("\n" + "=" * 50)
print("Column names:")
print(df.columns.tolist())
print("\n" + "=" * 50)
print("First 5 rows:")
print(df.head())
print("\n" + "=" * 50)
print("Data types:")
print(df.dtypes)
print("\n" + "=" * 50)
print("Missing values:")
print(df.isnull().sum())
print("\n" + "=" * 50)
print("Summary statistics:")
print(df.describe())
