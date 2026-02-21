from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, month, dayofmonth, year, lit, current_date, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging
import os
from azure.storage.blob import BlobServiceClient
import io

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
            .appName('PitchersInfo') \
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
        # Create probable_pitchers_info table
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.probable_pitchers_info (
            probablePitchers_home_id INT,
            probablePitchers_home_fullName STRING,
            probablePitchers_away_id INT,
            probablePitchers_away_fullName STRING,
            game_time STRING,
            game_id STRING,
            season STRING,
            month INT,
            day INT
        ) USING iceberg
        PARTITIONED BY (season, month)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/probable_pitchers_info'
        """)

        # Create star schema tables
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.dim_pitchers (
            pitcher_id INT,
            pitcher_fullName STRING,
            team_id INT,
            start_date DATE,
            end_date DATE,
            is_current BOOLEAN,
            is_retired BOOLEAN,
            season STRING,
            month INT
        ) USING iceberg
        PARTITIONED BY (season, month)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/dim_pitchers'
        """)

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.fact_pitchers (
            pitcher_id INT,
            game_id STRING,
            game_time STRING,
            home_or_away STRING,
            season STRING,
            month INT,
            day INT
        ) USING iceberg
        PARTITIONED BY (season, month)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_pitchers'
        """)

        # Create tables grouped by different dimensions
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.fact_games_by_game_id (
            game_id STRING,
            season STRING,
            month INT,
            day INT,
            home_team_id INT,
            away_team_id INT,
            home_team_name STRING,
            away_team_name STRING,
            home_pitcher_id INT,
            away_pitcher_id INT,
            game_time STRING
        ) USING iceberg
        PARTITIONED BY (season, month)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_games_by_game_id'
        """)

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.fact_games_by_home_pitcher (
            home_pitcher_id INT,
            game_id STRING,
            season STRING,
            month INT,
            day INT,
            home_team_id INT,
            away_team_id INT,
            home_team_name STRING,
            away_team_name STRING,
            game_time STRING
        ) USING iceberg
        PARTITIONED BY (season, month)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_games_by_home_pitcher'
        """)

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.fact_games_by_away_pitcher (
            away_pitcher_id INT,
            game_id STRING,
            season STRING,
            month INT,
            day INT,
            home_team_id INT,
            away_team_id INT,
            home_team_name STRING,
            away_team_name STRING,
            game_time STRING
        ) USING iceberg
        PARTITIONED BY (season, month)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_games_by_away_pitcher'
        """)

        logger.info("Tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating tables: {e}", exc_info=True)
        raise

def handle_scd_type_2(spark, target_table, incoming_updates_view, key_columns, other_columns):
    try:
        conditions = " OR ".join([f"target.{col} != source.{col}" for col in other_columns])
        
        merge_query = f"""
        MERGE INTO {target_table} AS target
        USING {incoming_updates_view} AS source
        ON {" AND ".join([f"target.{col} = source.{col}" for col in key_columns])} AND target.is_current = true
        WHEN MATCHED AND ({conditions}) THEN
          UPDATE SET end_date = current_date(), is_current = false
        WHEN NOT MATCHED THEN
          INSERT (
            {", ".join(key_columns + other_columns + ['start_date', 'end_date', 'is_current'])}
          )
          VALUES (
            {", ".join([f"source.{col}" for col in key_columns + other_columns])}, current_date(), null, true
          )
        """
        
        spark.sql(merge_query)
        logger.info(f"Upsert into '{target_table}' completed successfully.")
    except Exception as e:
        logger.error(f"Error in handle_scd_type_2 function: {e}", exc_info=True)
        raise

def read_and_write_stream(spark):
    try:
        schema = StructType([
            StructField("probablePitchers_home_id", IntegerType(), True),
            StructField("probablePitchers_home_fullName", StringType(), True),
            StructField("probablePitchers_away_id", IntegerType(), True),
            StructField("probablePitchers_away_fullName", StringType(), True),
            StructField("game_time", StringType(), True),
            StructField("game_id", StringType(), True),
            StructField("season", StringType(), True),
            StructField("month", IntegerType(), True),
            StructField("day", IntegerType(), True)
        ])

        kafka_df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', ','.join(bootstrap_servers)) \
            .option('subscribe', 'pitchers_info') \
            .option('startingOffsets', 'earliest') \
            .load()

        probable_pitchers_df = kafka_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")).select("data.*")

        # Transform and add new columns for date parts
        probable_pitchers_df = probable_pitchers_df.withColumn("season", col("season")) \
            .withColumn("month", col("month")) \
            .withColumn("day", col("day"))

        # Write probable_pitchers_info table
        probable_pitchers_df.writeStream \
            .format("iceberg") \
            .option("path", f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/probable_pitchers_info") \
            .option("checkpointLocation", "/tmp/checkpoints/probable_pitchers_info") \
            .outputMode("append") \
            .start()

        # Write to star schema tables
        dim_pitchers_home_df = probable_pitchers_df.select(
            col("probablePitchers_home_id").alias("pitcher_id"),
            col("probablePitchers_home_fullName").alias("pitcher_fullName"),
            lit(None).cast("int").alias("team_id"),
            current_date().alias("start_date"),
            lit(None).cast("date").alias("end_date"),
            lit(True).alias("is_current"),
            lit(False).alias("is_retired"),
            col("season"),
            col("month")
        ).distinct()

        dim_pitchers_away_df = probable_pitchers_df.select(
            col("probablePitchers_away_id").alias("pitcher_id"),
            col("probablePitchers_away_fullName").alias("pitcher_fullName"),
            lit(None).cast("int").alias("team_id"),
            current_date().alias("start_date"),
            lit(None).cast("date").alias("end_date"),
            lit(True).alias("is_current"),
            lit(False).alias("is_retired"),
            col("season"),
            col("month")
        ).distinct()

        dim_pitchers_df = dim_pitchers_home_df.union(dim_pitchers_away_df)
        dim_pitchers_df.createOrReplaceTempView("incoming_pitchers")

        # Upsert SCD Type 2 for pitchers dimension table
        handle_scd_type_2(spark, "mlb_db.dim_pitchers", "incoming_pitchers", ["pitcher_id"], ["pitcher_fullName", "team_id", "is_retired"])

        fact_pitchers_home_df = probable_pitchers_df.select(
            col("probablePitchers_home_id").alias("pitcher_id"),
            "game_id", "game_time",
            lit("home").alias("home_or_away"),
            col("season"),
            col("month"),
            col("day")
        )

        fact_pitchers_away_df = probable_pitchers_df.select(
            col("probablePitchers_away_id").alias("pitcher_id"),
            "game_id", "game_time",
            lit("away").alias("home_or_away"),
            col("season"),
            col("month"),
            col("day")
        )

        fact_pitchers_df = fact_pitchers_home_df.union(fact_pitchers_away_df)

        fact_pitchers_df.writeStream \
            .format("iceberg") \
            .option("path", f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_pitchers") \
            .option("checkpointLocation", "/tmp/checkpoints/fact_pitchers") \
            .outputMode("append") \
            .start()

        # Create additional fact tables grouped by different dimensions
        fact_games_by_game_id_df = probable_pitchers_df.select(
            "game_id", "season", "month", "day",
            col("probablePitchers_home_id").alias("home_pitcher_id"),
            col("probablePitchers_away_id").alias("away_pitcher_id"),
            lit(None).cast("int").alias("home_team_id"),
            lit(None).cast("int").alias("away_team_id"),
            lit(None).cast("string").alias("home_team_name"),
            lit(None).cast("string").alias("away_team_name"),
            "game_time"
        )

        fact_games_by_game_id_df.writeStream \
            .format("iceberg") \
            .option("path", f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_games_by_game_id") \
            .option("checkpointLocation", "/tmp/checkpoints/fact_games_by_game_id") \
            .outputMode("append") \
            .start()

        fact_games_by_home_pitcher_df = probable_pitchers_df.select(
            col("probablePitchers_home_id").alias("home_pitcher_id"),
            "game_id", "season", "month", "day",
            lit(None).cast("int").alias("home_team_id"),
            lit(None).cast("int").alias("away_team_id"),
            lit(None).cast("string").alias("home_team_name"),
            lit(None).cast("string").alias("away_team_name"),
            "game_time"
        )

        fact_games_by_home_pitcher_df.writeStream \
            .format("iceberg") \
            .option("path", f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_games_by_home_pitcher") \
            .option("checkpointLocation", "/tmp/checkpoints/fact_games_by_home_pitcher") \
            .outputMode("append") \
            .start()

        fact_games_by_away_pitcher_df = probable_pitchers_df.select(
            col("probablePitchers_away_id").alias("away_pitcher_id"),
            "game_id", "season", "month", "day",
            lit(None).cast("int").alias("home_team_id"),
            lit(None).cast("int").alias("away_team_id"),
            lit(None).cast("string").alias("home_team_name"),
            lit(None).cast("string").alias("away_team_name"),
            "game_time"
        )

        fact_games_by_away_pitcher_df.writeStream \
            .format("iceberg") \
            .option("path", f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_games_by_away_pitcher") \
            .option("checkpointLocation", "/tmp/checkpoints/fact_games_by_away_pitcher") \
            .outputMode("append") \
            .start()

        logger.info("Streaming data from Kafka to Iceberg tables initiated successfully.")
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
        read_and_write_stream(spark)
        spark.stop()
        logger.info("Main function executed successfully.")
    except Exception as e:
        logger.error(f"Error in main function: {e}", exc_info=True)

if __name__ == "__main__":
    main()
