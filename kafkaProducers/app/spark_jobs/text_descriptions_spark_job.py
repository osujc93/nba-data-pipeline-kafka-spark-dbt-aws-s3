from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging
import os
from azure.storage.blob import BlobServiceClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='app.log', filemode='w')
logger = logging.getLogger(__name__)

bootstrap_servers = ['kafka1:9092', 'kafka2:9093', 'kafka3:9094']

azure_blob_service_client = BlobServiceClient.from_connection_string(
    f"DefaultEndpointsProtocol=https;AccountName={os.getenv('AZURE_ACCOUNT_NAME')};AccountKey={os.getenv('AZURE_ACCOUNT_KEY')};EndpointSuffix=core.windows.net"
)

def create_spark_connection():
    try:
        azure_account_name = os.getenv('AZURE_ACCOUNT_NAME')
        azure_account_key = os.getenv('AZURE_ACCOUNT_KEY')
        azure_container = os.getenv('AZURE_CONTAINER')
        
        spark = SparkSession.builder \
            .appName('TextDescriptions') \
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                    "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.2,"
                    "org.apache.hadoop:hadoop-azure:3.3.4,"
                    "org.apache.hadoop:hadoop-azure-datalake:3.3.4") \
            .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem") \
            .config(f"spark.hadoop.fs.azure.account.key.{azure_account_name}.blob.core.windows.net", azure_account_key) \
            .config("spark.sql.warehouse.dir", f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/warehouse") \
            .enableHiveSupport() \
            .getOrCreate()
        
        spark.sql("CREATE DATABASE IF NOT EXISTS mlb_db")
        
        logger.info("Spark connection created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark connection: {e}", exc_info=True)
        raise

def create_text_descriptions_table(spark, azure_container, azure_account_name):
    try:
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.game_descriptions (
            game_id STRING,
            headline STRING,
            title STRING,
            description STRING,
            blurb STRING,
            season INT,
            month INT,
            day INT
        ) USING iceberg
        PARTITIONED BY (season, month, day, game_id)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/game_descriptions'
        """)
        logger.info("Game descriptions table created successfully.")
    except Exception as e:
        logger.error(f"Error creating game descriptions table: {e}", exc_info=True)
        raise

def consume_and_write_to_iceberg(spark):
    # Define the schema for the incoming Kafka data
    schema = StructType([
        StructField("game_id", StringType(), True),
        StructField("headline", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("blurb", StringType(), True),
        StructField("season", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True)
    ])

    # Read streaming data from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(bootstrap_servers)) \
        .option("subscribe", "text_descriptions") \
        .option("startingOffsets", "earliest") \
        .load()

    logger.info("Kafka stream read successfully.")

    # Parse the JSON data and select the relevant fields
    value_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    logger.info("JSON parsing completed successfully.")

    # Write the streaming data to Iceberg table
    query = value_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("path", f"abfss://{os.getenv('AZURE_CONTAINER')}@{os.getenv('AZURE_ACCOUNT_NAME')}.dfs.core.windows.net/mlb_db/game_descriptions") \
        .option("checkpointLocation", f"abfss://{os.getenv('AZURE_CONTAINER')}@{os.getenv('AZURE_ACCOUNT_NAME')}.dfs.core.windows.net/mlb_db/game_descriptions/checkpoints") \
        .start()

    logger.info("Started writing stream to Iceberg.")

    # Await termination of the streaming query
    query.awaitTermination()

def upload_to_storage(data_frame, storage_name, file_name):
    try:
        csv_data = data_frame.toPandas().to_csv(index=False).encode('utf-8')
        if len(csv_data) > 1000000000:
            raise ValueError("Data size is too large to upload.")
        azure_blob_client = azure_blob_service_client.get_blob_client(container=storage_name, blob=f"{file_name}.csv")
        azure_blob_client.upload_blob(io.BytesIO(csv_data), overwrite=True)
        logger.info(f"Data uploaded to Azure Blob Storage in container {storage_name} with file name {file_name}.csv")
    except Exception as e:
        logger.error(f"Failed to upload data to storage: {e}")

if __name__ == "__main__":
    spark = create_spark_connection()
    azure_container = os.getenv('AZURE_CONTAINER')
    azure_account_name = os.getenv('AZURE_ACCOUNT_NAME')
    create_text_descriptions_table(spark, azure_container, azure_account_name)
    consume_and_write_to_iceberg(spark)
