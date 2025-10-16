import logging
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col

#  Init spark session
spark = SparkSession.builder \
    .appName("TransformBronzeToSilver") \
    .master("yarn") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def read_parquet_data(parquet_path): 
    try: 
        df = spark.read.parquet(f"hdfs://localhost:9000/{parquet_path}") 
        logger.info(f"Read parquet file at {parquet_path}") 

        return df 
    except Exception as e: 
        logger.error(f"Error in read_parquet_data: {e}", exc_info=True) 
        raise


def transform_expense_data(df):
    try:
        # Drop rows with any null values
        missing_count = df.filter(
            sum(col(c).isNull().cast("int") for c in df.columns) > 0
        ).count()
        logger.info(f"Number of rows with missing values in expense data: {missing_count}")
        df = df.dropna()

        # Create 'discount_amount' column
        df = df.withColumn(
            "discount_amount", 
            col("expense_amount") * col("discount_percentage") * 100
        )

        return df
    except Exception as e:
        logger.error(f"Error in transform_expense_data: {e}", exc_info=True)
        raise

def transform_user_data(dfs):
    try:
        if not dfs:
            raise ValueError("Input list is empty")
        
        # Union all DataFrames
        joined_df = dfs[0]
        for df in dfs[1:]:
            joined_df = joined_df.unionByName(df)

        return joined_df
    except Exception as e:
        logger.error(f"Error in transform_user_data: {e}", exc_info=True)
        raise

def write_to_silver_csv(silver_path, df):
    try:
        df.write.mode("overwrite") \
            .option("header", True) \
            .option("encoding", "UTF-8") \
            .option("sep", ",") \
            .csv(f"hdfs://localhost:9000/{silver_path}")
        logger.info(f"Data saved successfully to {silver_path} as CSV")

    except Exception as e:
        logger.error(f"Error in write_to_silver_csv: {e}", exc_info=True)
        raise

def main():
    try:
        # Create silver dir
        create_hdfs_dir("/app_silver")

        # Transform expense data from bronze to silver
        df = read_parquet_data("app_bronze/expense_data")
        df = transform_expense_data(df)
        write_to_silver_csv("app_silver/expense_data", df)

        # Transform user data from bronze to silver
        df_u1 = read_parquet_data("app_bronze/user_data_AU")
        df_u2 = read_parquet_data("app_bronze/user_data_EU")
        df_u3 = read_parquet_data("app_bronze/user_data_US")
        df_user = transform_user_data([df_u1, df_u2, df_u3])
        write_to_silver_csv("app_silver/user_data", df_user)

    finally:
        spark.stop()
        logger.info("Spark session ended")

if __name__ == "__main__":
    main()