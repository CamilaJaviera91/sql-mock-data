import sys
sys.path.append('./sql/python/')

from connection import connection
from queries import hired_and_terminated

import psycopg2
import locale
import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np
import matplotlib.pyplot as plt

def set_locale():
    """Set the locale to Spanish (Spain) for formatting."""
    try:
        locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')
    except locale.Error:
        print("Warning: Could not establish the regional settings (locale).")

def get_connection():
    """Establish and return a database connection."""
    con = connection()
    if con is None:
        print("Error: Could not establish a connection to the database.")
    return con

def run_query(query: str) -> pd.DataFrame | None:
    """Execute a SQL query and return the results as a DataFrame."""
    con = get_connection()
    if not con:
        return None

    try:
        with con.cursor() as cursor:
            cursor.execute(query)
            records = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(records, columns=columns)
            print(df)
            return df
    except psycopg2.Error as e:
        print(f"Error executing the query: {e}")
        return None
    finally:
        con.close()
        print("Connection closed successfully.")

def predict_hires_terminations(df: pd.DataFrame, forecast_years: int = 3):
    # Ensure 'year' is integer
    df['year'] = df['year'].astype(int)

    X = df[['year']]
    y_hires = df['hired_count']
    y_terminations = df['terminated_count']

    model_hires = LinearRegression().fit(X, y_hires)
    model_terms = LinearRegression().fit(X, y_terminations)

    last_year = df['year'].max()
    future_years = np.array([[y] for y in range(last_year + 1, last_year + 1 + forecast_years)])

    hires_pred = model_hires.predict(future_years)
    terms_pred = model_terms.predict(future_years)

    # Show forecast results
    forecast_df = pd.DataFrame({
        'year': future_years.flatten(),
        'hired_prediction': hires_pred.round().astype(int),
        'terminated_prediction': terms_pred.round().astype(int)
    })

    print("\nPredictions:")
    print(forecast_df)

    # Plot results
    plt.figure(figsize=(10, 5))
    plt.plot(df['year'], y_hires, label='Actual Hires', marker='o')
    plt.plot(df['year'], y_terminations, label='Actual Terminations', marker='o')
    plt.plot(forecast_df['year'], forecast_df['hired_prediction'], label='Predicted Hires', linestyle='--')
    plt.plot(forecast_df['year'], forecast_df['terminated_prediction'], label='Predicted Terminations', linestyle='--')
    plt.xlabel('Year')
    plt.ylabel('Count')
    plt.title('Hires and Terminations per Year (with Predictions)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    return forecast_df

def main():
    set_locale()
    df = hired_and_terminated()
    if df is not None and not df.empty:
        forecast_df = predict_hires_terminations(df)

if __name__ == "__main__":
    main()
