from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import logging
import matplotlib.pyplot as plt

def mean_age(df):

    # Add a new column "age"
    df_age = df.withColumn(
        "age",
        F.floor(F.months_between(F.current_date(), F.col("date_birth")) / 12)
    )

    # Show descriptive statistics
    df_age.select("age").summary("count", "min", "max", "mean").show()

    # Convert the age column to Pandas for plotting
    ages_pd = df_age.select("age").dropna().toPandas()

    # Histogram with Matplotlib
    plt.figure(figsize=(10, 6))
    plt.hist(ages_pd["age"], bins=30, color='skyblue', edgecolor='black')
    plt.title("Employee Age Distribution")
    plt.xlabel("Age")
    plt.ylabel("Frequency")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # Count ages
    age_counts = ages_pd["age"].value_counts().sort_index()

    # Create full age range (even missing ones)
    min_age = ages_pd["age"].min()
    max_age = ages_pd["age"].max()
    full_range = range(min_age, max_age + 1)

    # Reindex to include all ages
    age_counts_full = age_counts.reindex(full_range, fill_value=0)

    # Plot the bar chart
    plt.figure(figsize=(14, 6))
    bars = plt.bar(age_counts_full.index, age_counts_full.values, color='skyblue', edgecolor='black')

    # Show values on top
    for bar in bars:
        height = bar.get_height()
        if height > 0:
            plt.text(bar.get_x() + bar.get_width() / 2, height + 1, str(int(height)),
                    ha='center', va='bottom', fontsize=8)

    plt.title("Employee Age Distribution (All Ages)")
    plt.xlabel("Age")
    plt.ylabel("Number of Employees")
    plt.xticks(list(full_range), rotation=45) # Show all x-tick labels
    plt.grid(axis='y')
    plt.tight_layout()
    plt.show()

def departments(df):

    # Count employees by city
    df.groupBy("city").count().orderBy("count", ascending=False).show(5)

    # Count employees by department
    department_counts = df.groupBy("department").count().orderBy("count", ascending=False)
    department_counts.show(6)

    # Employees who left
    terminated = df.filter(F.col("termination_date").isNotNull())

    # Turnover by city
    terminated.groupBy("city").count().orderBy("count", ascending=False).show(6)

    # Turnover by department
    terminated.groupBy("department").count().orderBy("count", ascending=False).show(6)

    # Total employees per department
    total_by_dept = df.groupBy("department").count().withColumnRenamed("count", "total_employees")

    # Terminations per department
    terminated_by_dept = terminated.groupBy("department").count().withColumnRenamed("count", "terminated_employees")

    # Join and calculate turnover
    rotation_rate = total_by_dept.join(terminated_by_dept, on="department", how="left") \
        .fillna(0) \
        .withColumn("turnover_rate", F.round(F.col("terminated_employees") / F.col("total_employees") * 100, 2))

    rotation_rate.orderBy("turnover_rate", ascending=False).show(6)

    # Convert to pandas for plotting
    pandas_df = rotation_rate.orderBy("turnover_rate", ascending=False).toPandas()

    # Plot turnover rate by department
    plt.figure(figsize=(14, 6))
    bars = plt.bar(pandas_df["department"], pandas_df["turnover_rate"], color='skyblue', edgecolor='black')

    for bar in bars:
        height = bar.get_height()
        if height > 0:
            plt.text(bar.get_x() + bar.get_width() / 2, height + 0.5, f"{height}%", ha='center', va='bottom', fontsize=8)

    plt.title("Turnover Rate by Department")
    plt.xlabel("Department")
    plt.ylabel("Turnover Rate (%)")
    plt.xticks(rotation=45)
    plt.grid(axis='y')
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    
    # Create a Spark session
    spark = SparkSession.builder.appName("Pyspark").getOrCreate()

    # Set logging level to 'ERROR' to minimize logs
    spark.sparkContext.setLogLevel("ERROR")

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.StreamHandler()])
    
    # Load all CSV files from the directory
    df = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("./data/*.csv")
    
    logger = logging.getLogger(__name__)

    logger.info("Starting mean age calculation...")

    # Pass spark session as parameter
    mean_age(df)

    departments(df)