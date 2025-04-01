from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType, DateType
import random
import unidecode
from faker import Faker
from datetime import date

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("Synthetic Employee Data") \
    .getOrCreate()

# Inicializar Faker
fake = Faker("es_CL")
Faker.seed(42)
random.seed(42)

# Lista de departamentos
departments = ['Sales', 'IT', 'Human Resources', 'Marketing', 'Finance', 'Operations']

# Conjuntos para unicidad
unique_names = set()
unique_phones = set()

# Funciones UDFs
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
    return f"{name_clean}@{department_clean}.cl"

def generate_birthdate():
    return fake.date_of_birth(minimum_age=30, maximum_age=50)

def generate_hiredate():
    return fake.date_between(start_date="-5y", end_date="today")

def generate_salary():
    return round(random.uniform(30000, 50000), 2)

# Registrar funciones como UDFs
udf_get_unique_name = udf(get_unique_name, StringType())
udf_get_unique_phone = udf(get_unique_phone, StringType())
udf_generate_email = udf(generate_email, StringType())
udf_generate_birthdate = udf(generate_birthdate, DateType())
udf_generate_hiredate = udf(generate_hiredate, DateType())
udf_generate_salary = udf(generate_salary, FloatType())

# Número de registros a generar
records = 1_000_000

# Crear DataFrame con Spark
df = spark.range(1, records + 1).toDF("id") \
    .withColumn("name", udf_get_unique_name()) \
    .withColumn("date_birth", udf_generate_birthdate()) \
    .withColumn("department", udf_get_unique_name()) \
    .withColumn("email", udf_generate_email("name", "department")) \
    .withColumn("phonenumber", udf_get_unique_phone()) \
    .withColumn("yearly_salary", udf_generate_salary()) \
    .withColumn("city", udf_get_unique_name()) \
    .withColumn("hire_date", udf_generate_hiredate())

# Guardar en CSV
df.write.csv("employees_pyspark.csv", header=True, mode="overwrite")

df.show()

print("Archivo 'employees_pyspark.csv' generado exitosamente.")

# Cerrar sesión de Spark
spark.stop()
