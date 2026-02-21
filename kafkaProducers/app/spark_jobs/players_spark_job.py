from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_date, lit, explode, substring
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DateType
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
            .appName('PlayersInfo') \
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

def create_playersinfo_table(spark, azure_container, azure_account_name):
    try:
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.playersinfo (
            player_id STRING,
            player_name STRING,
            team_id STRING,
            team_name STRING,
            link STRING,
            first_name STRING,
            last_name STRING,
            primary_number STRING,
            birth_date STRING,
            current_age INT,
            birth_city STRING,
            birth_country STRING,
            height STRING,
            weight INT,
            active BOOLEAN,
            primary_position_code STRING,
            primary_position_name STRING,
            primary_position_type STRING,
            primary_position_abbreviation STRING,
            gender STRING,
            mlb_debut_date STRING,
            bat_side_code STRING,
            bat_side_description STRING,
            pitch_hand_code STRING,
            pitch_hand_description STRING,
            start_date DATE,
            end_date DATE,
            is_current BOOLEAN,
            season STRING,
            month INT,
            day INT
        ) USING iceberg
        PARTITIONED BY (season, team_id)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/playersinfo'
        """)
        logger.info("Table 'playersinfo' created successfully!")
    except Exception as e:
        logger.error(f"Error creating 'playersinfo' table: {e}", exc_info=True)

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

def process_kafka_stream(spark):
    try:
        schema = StructType([
            StructField('season', StringType(), True),
            StructField('month', IntegerType(), True),
            StructField('day', IntegerType(), True),
            StructField('game_time', StringType(), True),
            StructField('game_id', StringType(), True),
            StructField('teams', StructType([
                StructField('team_id', StringType(), True),
                StructField('team_name', StringType(), True),
                StructField('players', StructType([
                    StructField('id', StringType(), True),
                    StructField('fullName', StringType(), True),
                    StructField('link', StringType(), True),
                    StructField('firstName', StringType(), True),
                    StructField('lastName', StringType(), True),
                    StructField('primaryNumber', StringType(), True),
                    StructField('birthDate', StringType(), True),
                    StructField('currentAge', IntegerType(), True),
                    StructField('birthCity', StringType(), True),
                    StructField('birthCountry', StringType(), True),
                    StructField('height', StringType(), True),
                    StructField('weight', IntegerType(), True),
                    StructField('active', BooleanType(), True),
                    StructField('primaryPosition', StructType([
                        StructField('code', StringType(), True),
                        StructField('name', StringType(), True),
                        StructField('type', StringType(), True),
                        StructField('abbreviation', StringType(), True)
                    ]), True),
                    StructField('gender', StringType(), True),
                    StructField('mlbDebutDate', StringType(), True),
                    StructField('batSide', StructType([
                        StructField('code', StringType(), True),
                        StructField('description', StringType(), True)
                    ]), True),
                    StructField('pitchHand', StructType([
                        StructField('code', StringType(), True),
                        StructField('description', StringType(), True)
                    ]), True)
                ]), True)
            ]), True)
        ])

        kafka_df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'kafka1:9092,kafka2:9093,kafka3:9094') \
            .option('subscribe', 'players_info') \
            .option('startingOffsets', 'earliest') \
            .load()

        value_df = kafka_df.selectExpr("CAST(value AS STRING)")
        json_df = value_df.select(from_json(col("value"), schema).alias("data"))

        players_info_df = json_df.select(
            col("data.season"),
            col("data.month"),
            col("data.day"),
            col("data.game_time"),
            col("data.game_id"),
            explode(col("data.teams")).alias("team_key", "team_info")
        ).select(
            col("season"),
            col("month"),
            col("day"),
            col("game_time"),
            col("game_id"),
            col("team_info.team_id").alias("team_id"),
            col("team_info.team_name").alias("team_name"),
            explode(col("team_info.players")).alias("player_id", "player_info")
        )

        player_df = players_info_df.select(
            col("season"),
            col("month"),
            col("day"),
            col("game_time"),
            col("game_id"),
            col("team_id"),
            col("team_name"),
            col("player_id"),
            col("player_info.id").alias("player_id"),
            col("player_info.fullName").alias("player_name"),
            col("player_info.link"),
            col("player_info.firstName"),
            col("player_info.lastName"),
            col("player_info.primaryNumber"),
            col("player_info.birthDate"),
            col("player_info.currentAge"),
            col("player_info.birthCity"),
            col("player_info.birthCountry"),
            col("player_info.height"),
            col("player_info.weight"),
            col("player_info.active"),
            col("player_info.primaryPosition.code").alias("primary_position_code"),
            col("player_info.primaryPosition.name").alias("primary_position_name"),
            col("player_info.primary_position.type").alias("primary_position_type"),
            col("player_info.primary_position.abbreviation").alias("primary_position_abbreviation"),
            col("player_info.gender"),
            col("player_info.mlbDebutDate"),
            col("player_info.batSide.code").alias("bat_side_code"),
            col("player_info.batSide.description").alias("bat_side_description"),
            col("player_info.pitchHand.code").alias("pitch_hand_code"),
            col("player_info.pitchHand.description").alias("pitch_hand_description"),
            lit(current_date()).alias("start_date"),
            lit(None).cast(DateType()).alias("end_date"),
            lit(True).alias("is_current")
        )

        upsert_dim_players(spark, player_df)
        logger.info("Processed Kafka stream and upserted player data successfully.")
    except Exception as e:
        logger.error(f"Error processing Kafka stream: {e}", exc_info=True)

def upsert_dim_players(spark, player_df):
    try:
        player_df.createOrReplaceTempView("incoming_updates")

        key_columns = ['player_id']
        other_columns = [
            'player_name', 'team_id', 'team_name', 'link', 'first_name', 'last_name', 
            'primary_number', 'birth_date', 'current_age', 'birth_city', 'birth_country', 
            'height', 'weight', 'active', 'primary_position_code', 'primary_position_name', 
            'primary_position_type', 'primary_position_abbreviation', 'gender', 
            'mlb_debut_date', 'bat_side_code', 'bat_side_description', 'pitch_hand_code', 
            'pitch_hand_description', 'season', 'month', 'day'
        ]
        
        handle_scd_type_2(spark, "mlb_db.playersinfo", "incoming_updates", key_columns, other_columns)
    except Exception as e:
        logger.error(f"Error upserting player data: {e}", exc_info=True)

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
        create_playersinfo_table(spark, azure_container, azure_account_name)
        process_kafka_stream(spark)
        spark.stop()
        logger.info("Main function executed successfully.")
    except Exception as e:
        logger.error(f"Error in main function: {e}", exc_info=True)

if __name__ == "__main__":
    main()
