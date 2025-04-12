import sys
sys.path.append('./sql/python/')

from connection import connection
import psycopg2
import locale
import pandas as pd

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

def by_city() -> pd.DataFrame | None:
    """Query turnover data by city."""
    query = '''
        WITH by_city AS (
            SELECT 
                e.city,
                COUNT(e."name") AS employees,
                COUNT(e.termination_date) AS terminated,
                (COUNT(e."name") - COUNT(e.termination_date)) AS active_employees
            FROM employees e 
            GROUP BY e.city
        )
        SELECT 
            bc.city, 
            bc.employees, 
            bc.terminated, 
            bc.active_employees,
            ROUND((bc.terminated * 1.0 / bc.employees), 2) AS turnover_rate
        FROM by_city bc
        ORDER BY bc.active_employees DESC
        LIMIT 10;
    '''
    return run_query(query)

def by_department() -> pd.DataFrame | None:
    """Query turnover data by department."""
    query = '''
        WITH by_department AS (
            SELECT 
                e.department,
                COUNT(e."name") AS employees,
                COUNT(e.termination_date) AS terminated,
                (COUNT(e."name") - COUNT(e.termination_date)) AS active_employees
            FROM employees e 
            GROUP BY e.department
        )
        SELECT 
            bd.department, 
            bd.employees, 
            bd.terminated, 
            bd.active_employees,
            ROUND((bd.terminated * 1.0 / bd.employees), 2) AS turnover_rate
        FROM by_department bd
        ORDER BY bd.active_employees DESC;
    '''
    return run_query(query)

def main():
    set_locale()
    print("=== Turnover by City ===")
    by_city()
    print("\n=== Turnover by Department ===")
    by_department()

if __name__ == "__main__":
    main()