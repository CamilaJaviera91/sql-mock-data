import pandas as pd
import glob
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

def load_env():
    load_dotenv()
    config = {
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "admin123"),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "db": os.getenv("DB_NAME", "postgres"),
        "table": os.getenv("TABLE_NAME", "employees")
    }
    
    # Validar que ninguna variable sea "None"
    for key, value in config.items():
        if value is None or value == "None":
            raise ValueError(f"La variable de entorno {key} no está configurada correctamente.")
    
    return config

def get_csv_files(path='./data_enriched/*.csv'):
    files = glob.glob(path)
    if not files:
        raise FileNotFoundError("No CSV files found in ./data/")
    return files

def read_and_combine_csv(files):
    df_list = [pd.read_csv(f) for f in files]
    
    # Validate columns
    base_columns = list(df_list[0].columns)
    for df in df_list:
        if list(df.columns) != base_columns:
            raise ValueError("CSV column mismatch detected.")
    
    df_combined = pd.concat(df_list, ignore_index=True)
    df_combined.drop_duplicates(inplace=True)
    return df_combined

def create_pg_engine(config):
    url = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['db']}"
    return create_engine(url)

def read_and_combine_csv(files):
    df_list = [pd.read_csv(f) for f in files]
    
    # Validate columns
    base_columns = list(df_list[0].columns)
    for df in df_list:
        if list(df.columns) != base_columns:
            raise ValueError("CSV column mismatch detected.")
    
    df_combined = pd.concat(df_list, ignore_index=True)
    df_combined.drop_duplicates(inplace=True)
    
    # Replace string 'None' and other bad strings with actual NaN
    df_combined.replace(['None', 'none', 'NULL', 'null', 'NaN', 'nan'], pd.NA, inplace=True)

    # Optional: convert columns that should be numeric
    for col in df_combined.columns:
        if df_combined[col].dtype == 'object':
            try:
                df_combined[col] = pd.to_numeric(df_combined[col])
            except:
                continue  # Skip non-numeric columns
    
    return df_combined


def upload_to_postgres(df, engine, table_name):
    try:
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"✅ Inserted {len(df)} rows into '{table_name}'.")
    except Exception as e:
        print(f"❌ Failed to insert data: {e}")

def main():
    try:
        config = load_env()
        files = get_csv_files()
        df = read_and_combine_csv(files)
        engine = create_pg_engine(config)
        read_and_combine_csv(files)
        upload_to_postgres(df, engine, config['table'])
    except Exception as err:
        print(f"❌ Error: {err}")

if __name__ == "__main__":
    main()