from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, FloatType, DateType
import random
from faker import Faker
import os
import shutil
import unidecode

# Create Spark session
spark = SparkSession.builder \
    .appName("Synthetic Employee Data") \
    .getOrCreate()

# Initialize Faker
fake = Faker("es_CL")
Faker.seed(42)
random.seed(42)

# List of departments
departments = ['Sales', 'IT', 'Human Resources', 'Marketing', 'Finance', 'Operations']

company = "COD. DIY"

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

def generate_email_simple(name):
    parts = name.strip().split()
    if len(parts) == 2 or len(parts) == 3:
        first_name = parts[0]
        last_name = parts[1]
    elif  len(parts) == 4:
        first_name = parts[0]
        last_name = parts[2]
    else:
        first_name = parts[0]
        last_name = ""

    first_clean = unidecode.unidecode(first_name.lower())
    last_clean = unidecode.unidecode(last_name.lower())
    company_clean = company.replace(".", "").replace(" ", "").lower()

    return f"{first_clean}.{last_clean}@{company_clean}.com"

def generate_birthdate():
    return fake.date_of_birth(minimum_age=30, maximum_age=50)

def generate_city():
    return fake.city()

def generate_hiredate():
    return fake.date_between(start_date="-5y", end_date="today")

def generate_salary():
    return round(random.uniform(30000, 50000), 2)

def generate_department():
    return fake.random_element(departments)

def generate_termination_date(hire_date):
    """ Assigns a termination date to approximately 30% of employees. """
    if random.random() < 0.3:  # 30% of employees will be terminated
        return fake.date_between(start_date=hire_date, end_date="today")
    return None

# Register functions as UDFs
udf_get_unique_name = udf(get_unique_name, StringType())
udf_get_unique_phone = udf(get_unique_phone, StringType())
udf_generate_email = udf(generate_email_simple, StringType())
udf_generate_birthdate = udf(generate_birthdate, DateType())
udf_generate_city = udf(generate_city, StringType())
udf_generate_hiredate = udf(generate_hiredate, DateType())
udf_generate_salary = udf(generate_salary, FloatType())
udf_generate_department = udf(generate_department, StringType( ))
udf_generate_termination_date = udf(generate_termination_date, DateType())

# Number of records to generate
records = 1_000_000

# Create DataFrame with Spark
df = spark.range(1, records + 1).toDF("id") \
    .withColumn("name", udf_get_unique_name()) \
    .withColumn("date_birth", udf_generate_birthdate()) \
    .withColumn("department", udf_generate_department()) \
    .withColumn("email", udf_generate_email(col("name"))) \
    .withColumn("phonenumber", udf_get_unique_phone()) \
    .withColumn("yearly_salary", udf_generate_salary()) \
    .withColumn("city", udf_generate_city()) \
    .withColumn("hire_date", udf_generate_hiredate())

# Add termination date based on hire date
df = df.withColumn("termination_date", udf_generate_termination_date(col("hire_date")))

# Save to CSV
df.repartition(12).write.csv("./data/temp_employees/", header=True, mode="overwrite")

# Rename part file to desired name
input_folder = "./data/temp_employees/"
output_folder = "./data/"

# Create output folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# List all files in the folder
files = sorted(f for f in os.listdir(input_folder) if f.startswith("part-") and f.endswith(".csv"))

for i, filename in enumerate(files):
    src = os.path.join(input_folder, filename)
    dst = os.path.join(output_folder, f"employees_part_{i+1:02d}.csv")  # e.g., employees_part_01.csv
    shutil.move(src, dst)

# Remove _SUCCESS and temp folder
success_file = os.path.join(input_folder, "_SUCCESS")
if os.path.exists(success_file):
    os.remove(success_file)

shutil.rmtree(input_folder)

df.show(5)

print("Split files successfully generated in 'data/'")

# Close Spark session
spark.stop()