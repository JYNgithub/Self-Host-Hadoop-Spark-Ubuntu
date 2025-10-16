import logging
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType 

#  Init spark session
spark = SparkSession.builder \
    .appName("ValSilverToGold") \
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

def validate_data(df):
    try:
        count = df.count()
        if count > 1:
            print(f"DataFrame validation passed: {count} rows found.")
        else:
            raise ValueError(f"DataFrame validation failed: only {count} row(s) found.")
    except Exception as e:
        logger.error(f"Error in validate_data: {e}", exc_info=True)
        raise

def write_to_gold_csv(gold_path, df):
    try:
        df.write.mode("overwrite") \
            .option("header", True) \
            .option("encoding", "UTF-8") \
            .option("sep", ",") \
            .csv(f"hdfs://localhost:9000/{gold_path}")
        logger.info(f"Data saved successfully to {gold_path} as CSV")

    except Exception as e:
        logger.error(f"Error in write_to_gold_csv: {e}", exc_info=True)
        raise

def main():
    try:
        create_hdfs_dir("/app_gold")

        df_exp = read_csv_data("app_silver/expense_data", schema_expense)
        validate_data(df_exp)
        write_to_gold_csv("app_gold/expense_data", df_exp)


        df_user = read_csv_data("app_silver/user_data", schema_user)
        validate_data(df_user)
        write_to_gold_csv("app_gold/user_data", df_user)

    finally:
        spark.stop()
        logger.info("Spark session ended")

if __name__ == "__main__":
    main()




