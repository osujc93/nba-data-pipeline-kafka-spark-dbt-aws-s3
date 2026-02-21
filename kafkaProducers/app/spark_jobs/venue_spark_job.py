from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, split, current_date, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
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
            .appName('VenueInfo') \
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

def create_tables(spark, azure_container, azure_account_name):
    try:
        # Create the dimension table for venues
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.dim_venue (
            game_id STRING,
            game_time STRING,                  
            venue_id INT,
            venue_name STRING,
            venue_active BOOLEAN,
            venue_location_address1 STRING,
            venue_location_city STRING,
            venue_location_state STRING,
            venue_location_stateAbbrev STRING,
            venue_location_postalCode STRING,
            venue_timeZone_id STRING,
            venue_timeZone_tz STRING,
            venue_fieldInfo_capacity INT,
            venue_fieldInfo_turfType STRING,
            venue_fieldInfo_roofType STRING,
            record_start_date DATE,
            record_end_date DATE,
            is_current BOOLEAN,
            season STRING,
            month STRING,
            day STRING                  
        ) USING iceberg
        PARTITIONED BY (venue_id, season, month, day)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/dim_venue'
        """)
        
        # Create the fact table for venue info
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.fact_venue (
            game_id STRING,
            game_time STRING,
            venue_id INT,
            venue_name STRING,  
            venue_location_city STRING,
            venue_location_state STRING,
            venue_location_stateAbbrev STRING,                                  
            season STRING,
            month STRING,
            day STRING
        ) USING iceberg
        PARTITIONED BY (venue_id, season, month, day)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_venue'
        """)

        logger.info("Tables created successfully!")
    except Exception as e:
        logger.error(f"Error creating tables: {e}", exc_info=True)
        raise

def handle_scd_type_2(df, spark, table_name, join_columns, compare_columns):
    try:
        df = df.withColumn('record_start_date', current_date())
        df = df.withColumn('record_end_date', lit(None).cast('date'))
        df = df.withColumn('is_current', lit(True))

        updates_df = df.selectExpr("*")
        updates_df.createOrReplaceTempView("updates_df")

        join_condition = " AND ".join([f"t.{col} = u.{col}" for col in join_columns])
        compare_condition = " OR ".join([f"t.{col} <> u.{col}" for col in compare_columns])

        merge_query = f"""
        MERGE INTO {table_name} AS t
        USING updates_df AS u
        ON {join_condition} AND t.is_current = true
        WHEN MATCHED AND ({compare_condition}) THEN
          UPDATE SET t.is_current = false, t.record_end_date = current_date()
        WHEN NOT MATCHED THEN
          INSERT (
            {", ".join(df.columns)}
          )
          VALUES (
            {", ".join([f"u.{col}" for col in df.columns])}
          )
        """

        spark.sql(merge_query)
        logger.info("SCD Type 2 handling completed successfully.")
    except Exception as e:
        logger.error(f"Error handling SCD Type 2: {e}", exc_info=True)
        raise

def read_and_write_stream(spark, azure_container, azure_account_name):
    try:
        schema = StructType([
            StructField("venue_id", IntegerType(), True),
            StructField("venue_name", StringType(), True),
            StructField("venue_active", BooleanType(), True),
            StructField("venue_season", StringType(), True),
            StructField("venue_location.address1", StringType(), True),
            StructField("venue_location.city", StringType(), True),
            StructField("venue_location.state", StringType(), True),
            StructField("venue_location.stateAbbrev", StringType(), True),
            StructField("venue_location.postalCode", StringType(), True),
            StructField("venue_timeZone.id", StringType(), True),
            StructField("venue_timeZone.tz", StringType(), True),
            StructField("venue_fieldInfo.capacity", IntegerType(), True),
            StructField("venue_fieldInfo.turfType", StringType(), True),
            StructField("venue_fieldInfo.roofType", StringType(), True),
            StructField("game_time", StringType(), True),
            StructField("game_id", StringType(), True),
            StructField("season", StringType(), True),
            StructField("month", StringType(), True),
            StructField("day", StringType(), True)
        ])

        kafka_df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', ','.join(bootstrap_servers)) \
            .option('subscribe', 'venue_info') \
            .option('startingOffsets', 'earliest') \
            .load()

        game_info_df = kafka_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")).select("data.*")

        venue_df = game_info_df.select(
            "venue_id", "venue_name", "venue_active",
            col("season"),
            col("venue_location.address1").alias("venue_location_address1"),
            col("venue_location.city").alias("venue_location_city"),
            col("venue_location.state").alias("venue_location_state"),
            col("venue_location.stateAbbrev").alias("venue_location_stateAbbrev"),
            col("venue_location.postalCode").alias("venue_location_postalCode"),
            col("venue_timeZone.id").alias("venue_timeZone_id"),
            col("venue_timeZone.tz").alias("venue_timeZone_tz"),
            col("venue_fieldInfo.capacity").alias("venue_fieldInfo_capacity"),
            col("venue_fieldInfo.turfType").alias("venue_fieldInfo_turfType"),
            col("venue_fieldInfo.roofType").alias("venue_fieldInfo_roofType")
        )

        game_info_fact_df = game_info_df.select("game_id", "game_time", "venue_id", "season", "month", "day")

        handle_scd_type_2(venue_df, spark, "mlb_db.dim_venue",
                          join_columns=["venue_id"],
                          compare_columns=["venue_name", "venue_location_address1", "venue_location_city", "venue_active"])

        query = game_info_fact_df.writeStream \
            .format("iceberg") \
            .option("path", f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_venue") \
            .option("checkpointLocation", "/tmp/checkpoints/fact_venue") \
            .outputMode("append") \
            .start()

        query.awaitTermination()
        
        logger.info("Streaming data from Kafka to Iceberg table initiated successfully.")
    except Exception as e:
        logger.error(f"Error in read_and_write_stream function: {e}", exc_info=True)
        raise

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

def main():
    try:
        spark = create_spark_connection()
        azure_container = os.getenv('AZURE_CONTAINER')
        azure_account_name = os.getenv('AZURE_ACCOUNT_NAME')
        create_tables(spark, azure_container, azure_account_name)
        read_and_write_stream(spark, azure_container, azure_account_name)
        spark.stop()
        logging.info("Streaming pipeline executed successfully.")
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)

if __name__ == "__main__":
    main()
