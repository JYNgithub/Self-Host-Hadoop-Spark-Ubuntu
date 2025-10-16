import logging
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType 
import os
from dotenv import load_dotenv

#  Init spark session
spark = SparkSession.builder \
    .appName("ValSilverToGold") \
    .master("yarn") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.jars", "postgresql-42.7.8.jar") \
    .getOrCreate()

# JDBC connection details
jdbc_url = "jdbc:postgresql://localhost:5432/app_dm"

# Database credentials
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
db_properties = {
    "user": f"{DB_USER}",
    "password": f"{DB_PASSWORD}",
    "driver": "org.postgresql.Driver"
}

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define schema for expense data
schema_expense = StructType([
    StructField("expense_id", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("expense_amount", DoubleType(), nullable=True),
    StructField("discount_percentage", DoubleType(), nullable=True),
    StructField("frequency", IntegerType(), nullable=True),
    StructField("expense_type", StringType(), nullable=True),
    StructField("rating", IntegerType(), nullable=True)
])

# Define schema for user data
schema_user = StructType([
    StructField("user_id", StringType(), nullable=False),
    StructField("user_name", StringType(), nullable=True),
    StructField("user_email", StringType(), nullable=True),
    StructField("user_job", StringType(), nullable=True)
])

def read_csv_data(csv_path, schema):
    try:
        df = (
            spark.read
            .option("header", True)
            .schema(schema)
            .csv(f"hdfs://localhost:9000/{csv_path}")
        )
        logger.info(f"Read CSV file(s) from {csv_path}")
        return df

    except Exception as e:
        logger.error(f"Error in read_csv_data: {e}", exc_info=True)
        raise

def write_to_postgres(df, table_name):
    try:
        logger.info(f"Writing DataFrame to PostgreSQL table {table_name}")
        
        df.write \
            .jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=db_properties)
        
        logger.info(f"Data successfully written to PostgreSQL table {table_name}")
    
    except Exception as e:
        logger.error(f"Error writing DataFrame to PostgreSQL: {e}", exc_info=True)
        raise

def main():
    try:
        df_exp = read_csv_data("app_gold/expense_data", schema_expense)
        write_to_postgres(df_exp, "app_data.expense_data")

        df_us = read_csv_data("app_gold/user_data", schema_user)
        write_to_postgres(df_us, "app_data.user_data")
    finally:
        spark.stop()
        logger.info("Spark session ended")

if __name__ == "__main__":
    main()

