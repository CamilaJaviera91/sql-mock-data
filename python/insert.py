import pandas as pd
from sqlalchemy import create_engine

csv_file = './data/employees_pyspark.csv/part-00000-23f83fe9-9d46-4973-865d-a3915cc7ab63-c000.csv'

df = pd.read_csv(csv_file)

engine = create_engine('postgresql://admin:admin123@localhost:5432/postgres')

df.to_sql('employees', engine, if_exists='append', index=False)

print("Data inserted successfully.")
