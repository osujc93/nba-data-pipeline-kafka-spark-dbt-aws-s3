from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, year, month, dayofmonth, lit, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DateType
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
            .appName('AllPlaysInfo') \
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
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS spark_catalog.mlb_db.all_plays (
            game_id STRING,
            game_date DATE,
            season INT,
            month INT,
            day INT,
            game_time STRING,
            pitchIndex ARRAY<INT>,
            actionIndex ARRAY<INT>,
            runnerIndex ARRAY<INT>,
            runners ARRAY<STRUCT<details:STRING>>,
            playEvents ARRAY<STRUCT<details:STRING>>,
            atBatIndex INT,
            result_type STRING,
            result_event STRING,
            result_eventType STRING,
            result_description STRING,
            result_rbi INT,
            result_awayScore INT,
            result_homeScore INT,
            result_isOut BOOLEAN,
            about_atBatIndex INT,
            about_halfInning STRING,
            about_isTopInning BOOLEAN,
            about_inning INT,
            about_isComplete BOOLEAN,
            about_isScoringPlay BOOLEAN,
            about_hasReview BOOLEAN,
            about_hasOut BOOLEAN,
            about_captivatingIndex INT,
            count_balls INT,
            count_strikes INT,
            count_outs INT,
            matchup_batter_id INT,
            matchup_batter_fullName STRING,
            matchup_batSide_code STRING,
            matchup_batSide_description STRING,
            matchup_pitcher_id INT,
            matchup_pitcher_fullName STRING,
            matchup_pitchHand_code STRING,
            matchup_pitchHand_description STRING,
            matchup_batterHotColdZones ARRAY<STRUCT<details:STRING>>,
            matchup_pitcherHotColdZones ARRAY<STRUCT<details:STRING>>,
            matchup_splits_batter STRING,
            matchup_splits_pitcher STRING,
            matchup_splits_menOnBase STRING,
            matchup_postOnSecond_id INT,
            matchup_postOnSecond_fullName STRING,
            matchup_postOnFirst_id INT,
            matchup_postOnFirst_fullName STRING,
            matchup_postOnThird_id INT,
            matchup_postOnThird_fullName STRING
        ) STORED BY ICEBERG
        PARTITIONED BY (season, month, day)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/allplays'
        """)

        logger.info("AllPlays info table created successfully.")
    except Exception as e:
        logger.error(f"Error creating table: {e}", exc_info=True)
        raise

def process_stream(spark):
    schema = StructType([
        StructField("game_id", StringType(), True),
        StructField("game_date", StringType(), True),
        StructField("game_time", StringType(), True),
        StructField("allPlays_pitchIndex", StringType(), True),
        StructField("allPlays_actionIndex", StringType(), True),
        StructField("allPlays_runnerIndex", StringType(), True),
        StructField("allPlays_runners", StringType(), True),
        StructField("allPlays_playEvents", StringType(), True),
        StructField("allPlays_atBatIndex", IntegerType(), True),
        StructField("allPlays_result.type", StringType(), True),
        StructField("allPlays_result.event", StringType(), True),
        StructField("allPlays_result.eventType", StringType(), True),
        StructField("allPlays_result.description", StringType(), True),
        StructField("allPlays_result.rbi", IntegerType(), True),
        StructField("allPlays_result.awayScore", IntegerType(), True),
        StructField("allPlays_result.homeScore", IntegerType(), True),
        StructField("allPlays_result.isOut", BooleanType(), True),
        StructField("allPlays_about.atBatIndex", IntegerType(), True),
        StructField("allPlays_about.halfInning", StringType(), True),
        StructField("allPlays_about.isTopInning", BooleanType(), True),
        StructField("allPlays_about.inning", IntegerType(), True),
        StructField("allPlays_about.isComplete", BooleanType(), True),
        StructField("allPlays_about.isScoringPlay", BooleanType(), True),
        StructField("allPlays_about.hasReview", BooleanType(), True),
        StructField("allPlays_about.hasOut", BooleanType(), True),
        StructField("allPlays_about.captivatingIndex", IntegerType(), True),
        StructField("allPlays_count.balls", IntegerType(), True),
        StructField("allPlays_count.strikes", IntegerType(), True),
        StructField("allPlays_count.outs", IntegerType(), True),
        StructField("allPlays_matchup.batter.id", IntegerType(), True),
        StructField("allPlays_matchup.batter.fullName", StringType(), True),
        StructField("allPlays_matchup.batSide.code", StringType(), True),
        StructField("allPlays_matchup.batSide.description", StringType(), True),
        StructField("allPlays_matchup.pitcher.id", IntegerType(), True),
        StructField("allPlays_matchup.pitcher.fullName", StringType(), True),
        StructField("allPlays_matchup.pitchHand.code", StringType(), True),
        StructField("allPlays_matchup.pitchHand.description", StringType(), True),
        StructField("allPlays_matchup.batterHotColdZones", StringType(), True),
        StructField("allPlays_matchup.pitcherHotColdZones", StringType(), True),
        StructField("allPlays_matchup.splits.batter", StringType(), True),
        StructField("allPlays_matchup.splits.pitcher", StringType(), True),
        StructField("allPlays_matchup.splits.menOnBase", StringType(), True),
        StructField("allPlays_matchup.postOnSecond.id", IntegerType(), True),
        StructField("allPlays_matchup.postOnSecond.fullName", StringType(), True),
        StructField("allPlays_matchup.postOnFirst.id", IntegerType(), True),
        StructField("allPlays_matchup.postOnFirst.fullName", StringType(), True),
        StructField("allPlays_matchup.postOnThird.id", IntegerType(), True),
        StructField("allPlays_matchup.postOnThird.fullName", StringType(), True)
    ])

    kafka_df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'kafka1:9092,kafka2:9093,kafka3:9094') \
        .option('subscribe', 'allplays_info') \
        .option('startingOffsets', 'earliest') \
        .load()

    allplays_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*") \
        .withColumn("game_date", col("game_date").cast(DateType())) \
        .withColumn("season", year(col("game_date"))) \
        .withColumn("month", month(col("game_date"))) \
        .withColumn("day", dayofmonth(col("game_date"))) \
        .drop("allPlays_about.endTime", "allPlays_about.startTime", "allPlays_playEndTime") \
        .drop("allPlays_matchup.postOnThird.link", "allPlays_matchup.postOnFirst.link", "allPlays_matchup.postOnSecond.link") \
        .drop("allPlays_matchup.pitcher.link", "allPlays_matchup.batter.link") \
        .withColumnRenamed("allPlays_pitchIndex", "pitchIndex") \
        .withColumnRenamed("allPlays_actionIndex", "actionIndex") \
        .withColumnRenamed("allPlays_runnerIndex", "runnerIndex") \
        .withColumnRenamed("allPlays_runners", "runners") \
        .withColumnRenamed("allPlays_playEvents", "playEvents") \
        .withColumnRenamed("allPlays_atBatIndex", "atBatIndex") \
        .withColumnRenamed("allPlays_result.type", "result_type") \
        .withColumnRenamed("allPlays_result.event", "result_event") \
        .withColumnRenamed("allPlays_result.eventType", "result_eventType") \
        .withColumnRenamed("allPlays_result.description", "result_description") \
        .withColumnRenamed("allPlays_result.rbi", "result_rbi") \
        .withColumnRenamed("allPlays_result.awayScore", "result_awayScore") \
        .withColumnRenamed("allPlays_result.homeScore", "result_homeScore") \
        .withColumnRenamed("allPlays_result.isOut", "result_isOut") \
        .withColumnRenamed("allPlays_about.atBatIndex", "about_atBatIndex") \
        .withColumnRenamed("allPlays_about.halfInning", "about_halfInning") \
        .withColumnRenamed("allPlays_about.isTopInning", "about_isTopInning") \
        .withColumnRenamed("allPlays_about.inning", "about_inning") \
        .withColumnRenamed("allPlays_about.isComplete", "about_isComplete") \
        .withColumnRenamed("allPlays_about.isScoringPlay", "about_isScoringPlay") \
        .withColumnRenamed("allPlays_about.hasReview", "about_hasReview") \
        .withColumnRenamed("allPlays_about.hasOut", "about_hasOut") \
        .withColumnRenamed("allPlays_about.captivatingIndex", "about_captivatingIndex") \
        .withColumnRenamed("allPlays_count.balls", "count_balls") \
        .withColumnRenamed("allPlays_count.strikes", "count_strikes") \
        .withColumnRenamed("allPlays_count.outs", "count_outs") \
        .withColumnRenamed("allPlays_matchup.batter.id", "matchup_batter_id") \
        .withColumnRenamed("allPlays_matchup.batter.fullName", "matchup_batter_fullName") \
        .withColumnRenamed("allPlays_matchup.batSide.code", "matchup_batSide_code") \
        .withColumnRenamed("allPlays_matchup.batSide.description", "matchup_batSide_description") \
        .withColumnRenamed("allPlays_matchup.pitcher.id", "matchup_pitcher_id") \
        .withColumnRenamed("allPlays_matchup.pitcher.fullName", "matchup_pitcher_fullName") \
        .withColumnRenamed("allPlays_matchup.pitchHand.code", "matchup_pitchHand_code") \
        .withColumnRenamed("allPlays_matchup.pitchHand.description", "matchup_pitchHand_description") \
        .withColumnRenamed("allPlays_matchup.batterHotColdZones", "matchup_batterHotColdZones") \
        .withColumnRenamed("allPlays_matchup.pitcherHotColdZones", "matchup_pitcherHotColdZones") \
        .withColumnRenamed("allPlays_matchup.splits.batter", "matchup_splits_batter") \
        .withColumnRenamed("allPlays_matchup.splits.pitcher", "matchup_splits_pitcher") \
        .withColumnRenamed("allPlays_matchup.splits.menOnBase", "matchup_splits_menOnBase") \
        .withColumnRenamed("allPlays_matchup.postOnSecond.id", "matchup_postOnSecond_id") \
        .withColumnRenamed("allPlays_matchup.postOnSecond.fullName", "matchup_postOnSecond_fullName") \
        .withColumnRenamed("allPlays_matchup.postOnFirst.id", "matchup_postOnFirst_id") \
        .withColumnRenamed("allPlays_matchup.postOnFirst.fullName", "matchup_postOnFirst_fullName") \
        .withColumnRenamed("allPlays_matchup.postOnThird.id", "matchup_postOnThird_id") \
        .withColumnRenamed("allPlays_matchup.postOnThird.fullName", "matchup_postOnThird_fullName")

    query = allplays_df.writeStream \
        .format("iceberg") \
        .option("path", f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/allplays") \
        .option("checkpointLocation", "/tmp/checkpoints/allplays_info") \
        .outputMode("append") \
        .start()

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
    create_tables(spark, azure_container, azure_account_name)
    process_stream(spark)
