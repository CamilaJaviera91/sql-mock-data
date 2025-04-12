# Import the connection function from the 'connection' file
import sys
sys.path.append('./sql/python/')

from connection import connection

# Import necessary libraries
import psycopg2
import locale
import pandas as pd

def by_city():

    # Set the locale to Spanish (Spain) to ensure proper formatting
    try:
        locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')
    except locale.Error:
        print("Error: Could not establish the regional settings.")
    
    # Establish a connection using the connection function from 'connection.py'
    con = connection()
    if con is None:
        print("Error: Could not establish a connection to the database.")
        return

    try:
        cursor = con.cursor()  # Create a cursor to interact with the database

        # Execute the SQL query to retrieve the sales data
        cursor.execute('''
            with by_city as (
                            select 
                                e.city,
                                count(e."name") as employees,
                                count(e.termination_date) as terminated,
                                (count(e."name") - count(e.termination_date)) as active_employees
                            from employees e 
                            group by e.city
                        )
                            select 
                                bc.city, 
                                bc.employees, 
                                bc.terminated, 
                                bc.active_employees,
                                round((bc.terminated * 1.0/ bc.employees), 2) as turnover_rate
                            from by_city bc
                            order by bc.active_employees desc
                            limit 10;
                       ''')

        records = cursor.fetchall()  # Fetch all the results

        # Convert results into a DataFrame for better visualization.
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(records, columns=columns)

        print(df)

        return df

    except psycopg2.Error as e:
        print(f"Error executing the query: {e}")
        return None

    finally:
        # Close cursor and connection safely
        cursor.close()
        con.close()
        print("Connection closed successfully.")

def by_department():

    # Set the locale to Spanish (Spain) to ensure proper formatting
    try:
        locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')
    except locale.Error:
        print("Error: Could not establish the regional settings.")
    
    # Establish a connection using the connection function from 'connection.py'
    con = connection()
    if con is None:
        print("Error: Could not establish a connection to the database.")
        return

    try:
        cursor = con.cursor()  # Create a cursor to interact with the database

        # Execute the SQL query to retrieve the sales data
        cursor.execute('''
            with by_department as (
                                select 
                                    e.department,
                                    count(e."name") as employees,
                                    count(e.termination_date) as terminated,
                                    (count(e."name") - count(e.termination_date)) as active_employees
                                from employees e 
                                group by e.department
                                )
                                select 
                                    bd.department, 
                                    bd.employees, 
                                    bd.terminated, 
                                    bd.active_employees,
                                    round((bd.terminated * 1.0/ bd.employees), 2) as turnover_rate
                                from by_department bd
                                order by bd.active_employees desc;
                       ''')

        records = cursor.fetchall()  # Fetch all the results

        # Convert results into a DataFrame for better visualization.
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(records, columns=columns)

        print(df)

        return df

    except psycopg2.Error as e:
        print(f"Error executing the query: {e}")
        return None

    finally:
        # Close cursor and connection safely
        cursor.close()
        con.close()
        print("Connection closed successfully.")

if __name__== "__main__":
    
    by_city()

    by_department()