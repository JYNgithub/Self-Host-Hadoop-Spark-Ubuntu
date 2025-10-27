import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Configuration
JDBC_DRIVER_PATH = "etc/postgresql-42.7.7.jar"
JDBC_URL = "jdbc:postgresql://localhost:5432/App_WH"
JDBC_USER = "postgres"
JDBC_PASSWORD = "1234"
JDBC_DRIVER = "org.postgresql.Driver"

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a Spark session with the driver
spark = SparkSession.builder \
    .config("spark.jars", JDBC_DRIVER_PATH) \
    .getOrCreate()

# Define schema for user data
schema_user = StructType([
    StructField("user_id", StringType(), nullable=False),
    StructField("user_name", StringType(), nullable=True),
    StructField("user_email", StringType(), nullable=True),
    StructField("user_job", StringType(), nullable=True)
])

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

def _read_csv_with_schema(path, schema, sep=";", header=True, quote='"'):
    logger.info(f"Reading CSV from {path}")
    try:
        df = spark.read.option("header", header) \
                       .option("sep", sep) \
                       .option("quote", quote) \
                       .schema(schema) \
                       .csv(path)
        return df
    except Exception as e:
        logger.error(f"Failed to read CSV {path}: {e}")
        raise

def _write_df_to_postgres(df, table_name, mode="overwrite", batch_size=500):
    logger.info(f"Writing DataFrame to {table_name} in PostgreSQL")
    try:
        df.write.format("jdbc") \
          .option("url", JDBC_URL) \
          .option("dbtable", table_name) \
          .option("user", JDBC_USER) \
          .option("password", JDBC_PASSWORD) \
          .option("driver", JDBC_DRIVER) \
          .option("batchsize", batch_size) \
          .mode(mode) \
          .save()
        logger.info(f"Successfully wrote to {table_name}")
    except Exception as e:
        logger.error(f"Write failed for {table_name}: {e}")
        raise

def ingest_data_staging(csv_path, schema, table_name, mode):
    
    # Read data
    df = _read_csv_with_schema(csv_path, schema)
    
    # Ingest data to staging
    staging_table = table_name + "_staging"
    _write_df_to_postgres(df, staging_table, mode=mode)
    
    logger.info(f"Data ingested to staging table {staging_table}")

def main():
    try:
        ingest_data_staging("user_data_AU.csv", schema_user, "app_data.user_data_au", mode="overwrite")
        ingest_data_staging("user_data_EU.csv", schema_user, "app_data.user_data_eu", mode="overwrite")
        ingest_data_staging("user_data_US.csv", schema_user, "app_data.user_data_us", mode="overwrite")
        ingest_data_staging("expense_data.csv", schema_expense, "app_data.expense_data", mode="overwrite")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")

    # Stop Spark session when done
    spark.stop()

if __name__ == "__main__":
    main()