# Import the connection function from the 'connection' file
import sys
sys.path.append('./sql/python/')

from connection import connection

# Import necessary libraries
import psycopg2
import locale
import pandas as pd

def query_city():
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
            select 
                employees.city as City, 
                ARRAY_AGG(employees."name") as Employees
            from employees
            group by employees.city
            order by employees.city;
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