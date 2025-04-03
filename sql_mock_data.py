from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when
from pyspark.sql.types import StringType, FloatType, DateType
import random
import unidecode
from faker import Faker
from datetime import date

# Create Spark session
spark = SparkSession.builder \
    .appName("Synthetic Employee Data") \
    .getOrCreate()

# Initialize Faker
fake = Faker("en_US")
Faker.seed(42)
random.seed(42)

# List of departments
departments = ['Sales', 'IT', 'Human Resources', 'Marketing', 'Finance', 'Operations']

# Sets for uniqueness
unique_names = set()
unique_phones = set()

# UDF Functions
def get_unique_name():
    name = fake.name()
    while name in unique_names:
        name = fake.name()
    unique_names.add(name)
    return name.replace("'", "''")

def get_unique_phone():
    phone = f"569-{random.randint(100, 999)}{random.randint(100, 999)}{random.randint(10, 99)}"
    while phone in unique_phones:
        phone = f"569-{random.randint(100, 999)}{random.randint(100, 999)}{random.randint(10, 99)}"
    unique_phones.add(phone)
    return phone

def generate_email(name, department):
    name_clean = unidecode.unidecode(name.replace(" ", "").lower())
    department_clean = department.replace(" ", "").lower()
    return f"{name_clean}@{department_clean}.com"

def generate_birthdate():
    return fake.date_of_birth(minimum_age=30, maximum_age=50)

def generate_hiredate():
    return fake.date_between(start_date="-5y", end_date="today")

def generate_salary():
    return round(random.uniform(30000, 50000), 2)

def generate_termination_date(hire_date):
    """ Assigns a termination date to approximately 30% of employees. """
    if random.random() < 0.3:  # 30% of employees will be terminated
        return fake.date_between(start_date=hire_date, end_date="today")
    return None

# Register functions as UDFs
udf_get_unique_name = udf(get_unique_name, StringType())
udf_get_unique_phone = udf(get_unique_phone, StringType())
udf_generate_email = udf(generate_email, StringType())
udf_generate_birthdate = udf(generate_birthdate, DateType())
udf_generate_hiredate = udf(generate_hiredate, DateType())
udf_generate_salary = udf(generate_salary, FloatType())
udf_generate_termination_date = udf(generate_termination_date, DateType())

# Number of records to generate
records = 1_000_000

# Create DataFrame with Spark
df = spark.range(1, records + 1).toDF("id") \
    .withColumn("name", udf_get_unique_name()) \
    .withColumn("date_birth", udf_generate_birthdate()) \
    .withColumn("department", udf_get_unique_name()) \
    .withColumn("email", udf_generate_email("name", "department")) \
    .withColumn("phonenumber", udf_get_unique_phone()) \
    .withColumn("yearly_salary", udf_generate_salary()) \
    .withColumn("city", udf_get_unique_name()) \
    .withColumn("hire_date", udf_generate_hiredate())

# Add termination date based on hire date
df = df.withColumn("termination_date", udf_generate_termination_date(col("hire_date")))

# Save to CSV
df.write.csv("data/employees_pyspark.csv", header=True, mode="overwrite")

df.show()

print("File 'employees_pyspark.csv' successfully generated.")

# Close Spark session
spark.stop()