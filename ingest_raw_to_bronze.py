import os
import logging
from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType 

#  Init spark session
spark = SparkSession.builder \
    .appName("IngestRawToBronze") \
    .master("yarn") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

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

def create_hdfs_dir(path):
    try:
        hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jsc.hadoopConfiguration()
        )
        hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(path)
        if not hadoop_fs.exists(hdfs_path):
            logger.info(f"Creating HDFS directory at {path}")
            hadoop_fs.mkdirs(hdfs_path)
        else:
            logger.info(f"HDFS directory {path} already exists")
    except Exception as e:
        logger.error(f"Error in creating_hdfs_dir: {e}", exc_info=True)
        raise

def _read_csv_with_schema(raw_path, schema, sep, quote):
    try:
        df = spark.read.option("header", True) \
            .option("sep", sep) \
            .option("quote", quote) \
            .schema(schema) \
            .csv(raw_path)
        logger.info(f"Successfully read CSV from {raw_path}")
        return df
    except Exception as e:
        logger.error(f"Error in _read_csv_with_schema for {raw_path}: {e}", exc_info=True)
        raise

def write_to_bronze_parquet(raw_path, schema, sep, quote, hdfs_path):
    try:
        # Prefix local files with 'file://' so Spark reads from local FS
        local_path = f"file://{os.path.abspath(raw_path)}"
        
        df = _read_csv_with_schema(local_path, schema, sep, quote)

        df.write.mode("overwrite").parquet(hdfs_path)
        logger.info(f"Successfully wrote DataFrame to {hdfs_path}")
    except Exception as e:
        logger.error(f"Error in write_to_bronze_parquet for {hdfs_path}: {e}", exc_info=True)
        raise

def main():
    try:
        create_hdfs_dir("/app_bronze")
        write_to_bronze_parquet("data/expense_data.csv", 
                            schema_expense, 
                            ";",
                            '"',
                            "hdfs://localhost:9000/app_bronze/expense_data")
        write_to_bronze_parquet("data/user_data_AU.csv", 
                            schema_user, 
                            ";",
                            '"',
                            "hdfs://localhost:9000/app_bronze/user_data_AU")
        write_to_bronze_parquet("data/user_data_EU.csv", 
                            schema_user, 
                            ";",
                            '"',
                            "hdfs://localhost:9000/app_bronze/user_data_EU")
        write_to_bronze_parquet("data/user_data_US.csv", 
                            schema_user, 
                            ";",
                            '"',
                            "hdfs://localhost:9000/app_bronze/user_data_US")
    finally:
        spark.stop()
        logger.info("Spark session ended")

if __name__ == "__main__":
    main()

