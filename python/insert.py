import pandas as pd
import glob
from sqlalchemy import create_engine

# Path to all CSV files in the folder
csv_files = glob.glob('./data/employees_pyspark.csv/*.csv')

# Read and concatenate all CSVs into a single DataFrame
df_list = [pd.read_csv(file) for file in csv_files]
df_combined = pd.concat(df_list, ignore_index=True)

# PostgreSQL connection
engine = create_engine('postgresql://admin:admin123@localhost:5432/postgres')

# Upload all data at once
df_combined.to_sql('employees', engine, if_exists='append', index=False)

print("All data inserted successfully.")