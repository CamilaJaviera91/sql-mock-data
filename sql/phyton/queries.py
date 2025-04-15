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

def by_age() -> pd.DataFrame | None:
    """Query turnover data by age."""
    query = '''
        WITH by_age AS (
            SELECT 
                EXTRACT(YEAR FROM NOW()::DATE) - EXTRACT(YEAR FROM e.date_birth::DATE) AS age,
                COUNT(e.name) AS employees,
                COUNT(e.termination_date) AS terminated,
                COUNT(e.name) - COUNT(e.termination_date) AS active_employees
            FROM employees e 
            GROUP BY EXTRACT(YEAR FROM e.date_birth::DATE)
        )
        SELECT 
            ba.age, 
            ba.employees, 
            ba.terminated, 
            ba.active_employees,
            ROUND(ba.terminated * 1.0 / ba.employees, 2) AS turnover_rate
        FROM by_age ba
        ORDER BY ba.active_employees desc;
    '''
    return run_query(query)

def salary_by_city() -> pd.DataFrame | None:
    """Query salary by city."""
    query = '''
        WITH salary AS (
            SELECT 
                e.city,
                ROUND(SUM(e.yearly_salary)/12) AS total_salary,
                COUNT(e."name") AS employees
            FROM employees e 
            WHERE e.termination_date IS NOT NULL
            GROUP BY e.city
        )
        SELECT 
            s.city,
            s.employees,
            s.total_salary
        FROM salary s
        ORDER BY s.total_salary DESC
        LIMIT 10;
    '''
    return run_query(query)

def salary_by_department() -> pd.DataFrame | None:
    """Query salary by department."""
    query = '''
        WITH salary AS (
            SELECT 
                e.department,
                ROUND(SUM(e.yearly_salary)/12) AS total_salary,
                COUNT(e."name") AS employees
            FROM employees e 
            WHERE e.termination_date IS NOT NULL
            GROUP BY e.department
        )
        SELECT 
            s.department,
            s.employees,
            s.total_salary
        FROM salary s
        ORDER BY s.total_salary DESC;
    '''
    return run_query(query)

def salary_by_age() -> pd.DataFrame | None:
    """Query salary by age."""
    query = '''
        WITH salary AS (
            SELECT 
                EXTRACT(YEAR FROM NOW()::DATE) - EXTRACT(YEAR FROM e.date_birth::DATE) AS age,
                ROUND(SUM(e.yearly_salary)/12) AS total_salary,
                COUNT(e."name") AS employees
            FROM employees e 
            WHERE e.termination_date IS NOT NULL
            GROUP BY EXTRACT(YEAR FROM NOW()::DATE) - EXTRACT(YEAR FROM e.date_birth::DATE)
        )
        SELECT 
            s.age,
            s.employees,
            s.total_salary
        FROM salary s
        ORDER BY s.total_salary DESC;
    '''
    return run_query(query)

def hired_and_terminated() -> pd.DataFrame | None:
    """Query salary by age."""
    query = '''
        SELECT 
            EXTRACT(YEAR FROM COALESCE(hire_date::DATE, termination_date::DATE)) AS year,
            COUNT(*) FILTER (WHERE hire_date IS NOT NULL) AS hired_count,
            COUNT(*) FILTER (WHERE termination_date IS NOT NULL) AS terminated_count
        FROM employees
        WHERE hire_date IS NOT NULL OR termination_date IS NOT NULL
        GROUP BY EXTRACT(YEAR FROM COALESCE(hire_date::DATE, termination_date::DATE))
        ORDER BY year;
    '''
    return run_query(query)

def main():
    set_locale()
    print("=== Turnover by City ===")
    by_city()
    print("\n=== Turnover by Department ===")
    by_department()
    print("\n=== Turnover by Age ===")
    by_age()
    print("\n=== Salary by City ===")
    salary_by_city()
    print("\n=== Salary by Department ===")
    salary_by_department()
    print("\n=== Salary by Age ===")
    salary_by_age()

if __name__ == "__main__":
    main()