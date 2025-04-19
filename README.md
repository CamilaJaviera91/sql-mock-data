# 🪪 Synthetic Employee Dataset: SQL, PySpark & ML Pipeline

## SQL Mock Data

- This project generates synthetic employee data using Python and Faker, stores it in a PostgreSQL database, and performs analytics and machine learning modeling using PySpark and Scikit-learn. It's designed for data engineering and data science practice, focusing on realistic HR-style datasets and workflows.

- **Key features include:**
    - Synthetic data generation with customizable logic
    - PostgreSQL integration
    - PySpark data processing and transformations
    - Predictive modeling for employee attrition

## 🚀 Getting Started

### 1. Clone the repository
```
git clone https://github.com/CamilaJaviera91/sql-mock-data.git
```

### 2. Open the folder in your computer
```
cd your/route/sql-mock-data
```

### 3. Create a file named **requirements.txt** with the following content:

```
pandas
numpy
faker
psycopg2-binary
pyspark
scikit-learn
matplotlib
seaborn
```

### 4. Create a virtual environment and install dependencies
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 5. Set up the PostgreSQL database

1. Create a new database called **employees**.

2. Generate mock data.

3. Insert mock data into our new schema.

### 6. Generate and insert mock data into the database
```
python your/route/sql-mock-data/sql_mock_data.py

python your/route/sql-mock-data/insert.py
```

# 📚 Data Dictionary

| Column           | Description                         | Type    |
|------------------|-------------------------------------|---------|
| id               | Unique identifier                   | Integer |
| name             | full name of the employee           | Text    |
| date_birth       | Date of birth if the employee       | Date    |
| department       | Department where the employee works | Text    |
| email            | Employee work email                 | Text    |
| phonenumber      | Work phonenumber of the employee    | Text    |
| yearly_salary    | Yearly salary in USD                | Integer |
| city             | City where the employee lives       | Text    |
| hire_date        | Date when the employee was hired    | Date    |
| termination_date | Date when the employee was fired    | Date    |

# 📁 Project Structure

```
sql-mock-data/
├── data/
│   └── *.csv                  # Synthetic employee data files
├── images/
│   └── pic*.png               # Visualizations and example outputs
├── python/
│   ├── sql_mock_data.py       # Script to generate synthetic data
│   ├── insert.py              # Script to insert data into PostgreSQL
│   ├── analysis.py            # Data analysis using PySpark
│   ├── queries.py             # SQL queries for data retrieval
│   ├── show_results.py        # Visualization of query results
│   └── connection.py          # Database connection setup
├── sql/
│   └── schema.sql             # SQL schema definitions
├── .gitignore                 # Specifies files to ignore in Git
└── README.md                  # Project documentation
```

# 🔥 Introduction to PySpark
- **PySpark** it's the Python API for Apache Spark, enabling the use of Spark with Python.

## 🔑 Key Features:

1. **Distributed Computing:** Processes large datasets across a cluster of computers for scalability.

2. **In-Memory Processing:** Speeds up computation by reducing disk I/O.

3. **Lazy Evaluation:** Operations are only executed when an action is triggered, optimizing performance.

4. **Rich Libraries:**
    - **Spark SQL:** Structured data processing (like SQL operations).
    - **MLlib:** Machine learning library for scalable algorithms.
    - **GraphX:** Graph processing (via RDD API).
    - **Spark Streaming:** Real-time stream processing.

5. **Compatibility:** Works with Hadoop, HDFS, Hive, Cassandra, etc.

6. **Resilient Distributed Datasets (RDDs):** Low-level API for distributed data handling.

7. **DataFrames & Datasets:** High-level APIs for structured data with SQL-like operations.

## ✅ Pros — ❌ Cons

| Pros                                                  | Cons                                            |
|-------------------------------------------------------|-------------------------------------------------|
| Handles massive datasets efficiently.                 | Can be memory-intensive.                        |
| Compatible with many tools (Hadoop, Cassandra, etc.). | Complex configuration for cluster environments. |
| Built-in libraries for SQL, Machine Learning.         |                                                 |

## 🔧 Install pyspark

1. Install via pip

```
pip install pyspark
```

2. Verify installation

```
python3 -c "import pyspark; print(pyspark.__version__)"
```

---

# 🗃️ Introduction to SQL (Structured Query Language)

- **SQL** is how we read, write, and manage data stored in databases.

## 🔑 Key Features:

1. **Data Querying:** You can retrieve exactly the data you need using the SELECT statement.
```
SELECT * FROM employees WHERE department = 'HR';
```

2.**Data Manipulation:** SQL lets you insert, update, or delete records.

    - INSERT
    - UPDATE
    - DELETE

3. **Data Definition:** You can create or change the structure of tables and databases.

    - CREATE
    - ALTER
    - DROP

4. **Data Control:** SQL allows you to control access to the data.

    - GRANT
    - REVOKE

5. **Transaction Control:** Manage multiple steps as a single unit.

    - BEGIN
    - COMMIT
    - ROLLBACK

6. **Filtering and Sorting:**
    
    - WHERE
    - ORDER BY
    - GROUP BY
    - HAVING

7. **Joins:** Combine data from multiple tables.

8. **Built-in Functions:** SQL includes powerful functions for calculations, text handling, dates, etc.

9. **Standardized Language:** SQL is used across most relational database systems (like PostgreSQL, MySQL, SQL Server, etc.), with only slight differences.

10. **Declarative Nature:** You tell SQL what you want, not how to do it. The database figures out the best way.

## ✅ Pros — ❌ Cons

| Pros                            | Cons                           |
|---------------------------------|--------------------------------|
| Easy to Learn and Use.          | Not Ideal for Complex Logic.   |
| Efficient Data Management.      | Different Dialects.            |
| Powerful Querying Capabilities. | Can Get Complicated.           |
| Standardized Language.          | Limited for Unstructured Data. |
| Scalable.                       | Performance Tuning Required.   |
| Secure.                         |                                |
| Supports Transactions.          |                                |

---

# 🐳 Introduction to Docker

- **Docker** is a tool that lets you package your app with everything it needs, so it can run anywhere, without problems.

- It does this using something called containers, which are like small, lightweight virtual machines.

## 🔑 Key Features:

1. **Containers:** Run apps in isolated environments.

2. **Images:** Blueprints for containers (created using a Dockerfile).

3. **Portability:** Works the same on any system with Docker.

4. **Speed:** Starts apps quickly.

5. **Docker Hub:** A place to share and download app images.

## ✅ Pros — ❌ Cons

| Pros                              | Cons                                                  |
|-----------------------------------|-------------------------------------------------------|
| Works the same everywhere.        | Takes some time to learn.                             |
| Fast and lightweight.             | Not ideal for apps that need a full operating system. |
| Easy to share apps.               | Security risks if not set up properly.                |
| Good for automating deployments.  | Managing data storage can be tricky.                  |
| Great for teams working together. |                                                       |

## 🔧 Install Docker on Fedora

1. Update the system:

```
sudo dnf update -y
```

2. Install necessary packages for using HTTPS repositories:

```
sudo dnf install dnf-plugins-core -y
```

3. Add the official Docker repository:

```
sudo dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo
```

4. Install Docker Engine:

```
sudo dnf install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin -y
```

5. Enable and start the Docker service:

```
sudo systemctl enable docker
sudo systemctl start docker
```

6. Verify that Docker is running:

```
sudo docker run hello-world
```

7. (Optional) Run Docker without sudo:

- If you want to use Docker without typing sudo every time:

```
sudo usermod -aG docker $USER
```

Then, log out and log back in (or reboot your system) for the change to take effect.

---

# 🛠️ Code Explanation

## 👩‍💻 Script 1: sql_mock_data.py — Generate Mock Data

### 🔧 Libraries that we are going to need:

| Library   | Description                                                    |
|-----------|----------------------------------------------------------------|
| PySpark   | Apache Spark Python API (for big data).                        |
| Faker	    | Fake data generator (used for names, etc.).                    |
| unidecode | Removes accents from characters (e.g., é → e).                 |
| random    | For generating random numbers, probabilities, selections, etc. |
| os        | For cross-platform file handling and directory management.     |
| shutil    | For managing file system operations in automation scripts.     |

### 📖 Explanation of the Code:

- This script:

    - Creates 1 million fake employee records.

    - Each with realistic personal and job data.

    - Saves them across 12 cleanly named CSV files.

    - Makes sure names and phones are unique.

    - Can be scaled easily or reused for testing, demos, or training.

### ✅ Example Output:

<img src="./images/pic2.png" alt="mock_data" width="500"/>

---

## 👩‍💻 Script 2: edit_data.py — Edit Mock Data

### 🔧 Libraries that we are going to need:

| Library | Description                                                            |
|---------|------------------------------------------------------------------------|
| pandas  | For working with CSVs and DataFrames.                                  |
| os      | For cross-platform file handling and directory management.             |
| random  | For generate random numbers, shuffle data, and make random selections. |

### 📖 Explanation of the Code:

### ✅ Example Output:

<img src="./images/pic23.png" alt="mock_data" width="500"/>

---

## 👩‍💻 Script 3: insert.py — Insert data into postgres

### 🔧 Libraries that we are going to need:

| Library       | Description                                                |
|---------------|------------------------------------------------------------|
| pandas        | For working with CSVs and DataFrames.                      |
| sqlalchemy    | Python SQL toolkit and ORM.                                |
| psycopg2      | PostgreSQL driver required by SQLAlchemy.                  |
| python-dotenv | helps you load environment variables from `.env` file.     |
| glob	        | Standard library for file pattern matching.                |
| os	        | For cross-platform file handling and directory management. |

### 📖 Explanation of the Code:

- This script:

    - Finds all CSV files in the ./data/ folder using glob.

    - Reads and combines all the CSVs into a single pandas DataFrame.

    - Creates a connection to a PostgreSQL database using SQLAlchemy.

    - Uploads the combined data to the employees table in the database.

### ✅ Example Output:

<img src="./images/pic3.png" alt="mock_data" width="500"/>
---

## 👩‍💻 Script 4: analysis.py — First analysis of the data

### 🔧 Libraries that we are going to need:

| Library            | Description                                           |
|--------------------|-------------------------------------------------------|
| PySpark            | Apache Spark Python API (for big data).               |
| matplotlib.pyplot  | To create visualizations (histograms and bar charts). |
| logging            | To track execution flow and info messages.            |

### 📖 Explanation of the Code:

- This script:

    - Reads multiple CSV files using PySpark and combines them into a single DataFrame.

    - Calculates the age of each employee based on their date of birth and shows basic statistics.

    - Generates age distribution plots using matplotlib (histogram + bar chart with labels).

    - Performs department and city analysis, including counts and turnover (employees who left).

    - Logs activity and minimizes Spark output verbosity for clarity.

### ✅ Example Output:

<img src="./images/pic4.png" alt="mock_data" width="500"/>

<br>

<img src="./images/pic5.png" alt="mock_data" width="500"/>

<br>

<img src="./images/pic6.png" alt="mock_data" width="500"/>

<br>

<img src="./images/pic7.png" alt="mock_data" width="500"/>

<br>

<img src="./images/pic8.png" alt="mock_data" width="500"/>

---

## 👩‍💻 Script 5: queries.py — Create SQL queries

### 🔧 Libraries that we are going to need:

| Library    | Description                                     |
|------------|-------------------------------------------------|
| psycopg2   | PostgreSQL driver required by SQLAlchemy.       |
| pandas     | For working with CSVs and DataFrames.           |
| connection | Custom local module to establish DB connection. |
| locale     | Built-in module for localization/formatting.    |
| sys        | Built-in module to modify the system path.      |

### 📖 Explanation of the Code:

- This script:

    - Uses a custom connection() function to establish a PostgreSQL connection.

    - Tries to set locale to Spanish (es_ES.UTF-8) for formatting purposes.

    - Runs SQL queries using run_query(), returning results as a pandas DataFrame.

    - Includes 6 analysis (more to add) functions by city, department, and age, calculating turnover rates and salaries for active employees.

    - Executes all analyses and prints them when the script is run directly.

### ✅ Example Output:

- **by_city()**

<img src="./images/pic9.png" alt="mock_data" width="500"/>

- **by_department()**

<img src="./images/pic10.png" alt="mock_data" width="500"/>

- **by_age()**

<img src="./images/pic11.png" alt="mock_data" width="500"/>

- **salary_by_city()**

<img src="./images/pic12.png" alt="mock_data" width="500"/>

- **salary_by_department()**

<img src="./images/pic14.png" alt="mock_data" width="500"/>

- **salary_by_age()**

<img src="./images/pic13.png" alt="mock_data" width="500"/>

- **hired_and_terminated()**

<img src="./images/pic15.png" alt="mock_data" width="500"/>

- **hired_and_terminated_department()**

<img src="./images/pic16.png" alt="mock_data" width="500"/>

---

## 👩‍💻 Script 6: show_results.py — Plot SQL queries

### 🔧 Libraries that we are going to need:

| Library           | Description                                           |
|-------------------|-------------------------------------------------------|
| matplotlib.pyplot | To create visualizations (histograms and bar charts). |
| seaborn           | For making nice statistical plots easily.             |
| queries           | Custom local module to establish DB connection.       |

### 📖 Explanation of the Code:

- This script:

    - Imports data from predefined SQL queries (like by_city, by_age, etc.) using custom functions.

    - Creates charts with Seaborn and Matplotlib to visualize employee data.

    - Plots bar charts for active employees and salaries by city and department.

    - Plots a line chart showing turnover rate by age, with value labels.

    - Plots a line chart showing yearly hires and terminations, including count labels.

### ✅ Example Output:

- **plot_by_city()**

<img src="./images/pic17.png" alt="mock_data" width="500"/>

- **plot_by_department()**

<img src="./images/pic18.png" alt="mock_data" width="500"/>

- **plot_by_age()**

<img src="./images/pic19.png" alt="mock_data" width="500"/>

- **plot_salary_by_city()**

<img src="./images/pic20.png" alt="mock_data" width="500"/>

- **plot_salary_by_department()**

<img src="./images/pic21.png" alt="mock_data" width="500"/>

- **plot_hired_and_terminated()**

<img src="./images/pic22.png" alt="mock_data" width="500"/>

---

# 🔮 Future Enhancements

- [ ] Add DBT models for transformation and documentation.
- [ ] Streamline data generation for large-scale datasets.
- [ ] Add Airflow DAG for orchestration.
- [ ] Deploy insights via Looker Studio or Power BI dashboard.