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