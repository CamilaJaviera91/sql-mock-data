from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import logging

def mean_age(spark):

    # Load all CSV files from the directory
    df = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("./data/*.csv")  # Corrige la ruta aquí si es necesario

    # Add a new column "age"
    df_age = df.withColumn(
        "age",
        F.floor(F.months_between(F.current_date(), F.col("date_birth")) / 12)
    )

    # Show descriptive statistics
    df_age.select("age").summary("count", "min", "max", "mean").show()

if __name__ == "__main__":
    
    # Create a Spark session
    spark = SparkSession.builder.appName("Pyspark").getOrCreate()

    # Set logging level to 'ERROR' to minimize logs
    spark.sparkContext.setLogLevel("ERROR")

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.StreamHandler()])
    
    logger = logging.getLogger(__name__)

    logger.info("Starting mean age calculation...")

    # Pass spark session as parameter
    mean_age(spark)