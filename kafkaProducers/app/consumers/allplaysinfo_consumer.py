from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, year, month, dayofmonth, current_date, lit, explode, sum as spark_sum, avg, window, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType, DateType
import logging
import re
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
            .appName('AllPlaysInfo') \
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                    "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.2,"
                    "org.apache.hadoop:hadoop-azure:3.4.0,"
                    "org.apache.hadoop:hadoop-azure-datalake:3.4.0") \
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
            year INT,
            month INT,
            day INT,
            game_time STRING,
            pitchIndex ARRAY<INT>,
            actionIndex ARRAY<INT>,
            runnerIndex ARRAY<INT>,
            runners ARRAY<STRUCT<details:STRING>>,
            playEvents ARRAY<STRUCT<details:STRING>>,
            playEndTime STRING,
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
            about_startTime STRING,
            about_endTime STRING,
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
            matchup_batter_link STRING,
            matchup_batSide_code STRING,
            matchup_batSide_description STRING,
            matchup_pitcher_id INT,
            matchup_pitcher_fullName STRING,
            matchup_pitcher_link STRING,
            matchup_pitchHand_code STRING,
            matchup_pitchHand_description STRING,
            matchup_batterHotColdZones ARRAY<STRUCT<details:STRING>>,
            matchup_pitcherHotColdZones ARRAY<STRUCT<details:STRING>>,
            matchup_splits_batter STRING,
            matchup_splits_pitcher STRING,
            matchup_splits_menOnBase STRING,
            matchup_postOnSecond_id INT,
            matchup_postOnSecond_fullName STRING,
            matchup_postOnSecond_link STRING,
            matchup_postOnFirst_id INT,
            matchup_postOnFirst_fullName STRING,
            matchup_postOnFirst_link STRING,
            matchup_postOnThird_id INT,
            matchup_postOnThird_fullName STRING,
            matchup_postOnThird_link STRING
        ) STORED BY ICEBERG
        PARTITIONED BY (year, month, day)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/allplays'
        """)

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS spark_catalog.mlb_db.allplays_aggregates (
            game_id STRING,
            total_rbi INT,
            total_awayScore INT,
            total_homeScore INT,
            total_pitches INT,
            total_actions INT,
            total_runners INT,
            total_playEvents INT,
            avg_captivatingIndex DOUBLE,
            avg_balls DOUBLE,
            avg_strikes DOUBLE,
            avg_outs DOUBLE,
            avg_pitches DOUBLE,
            avg_actions DOUBLE,
            avg_runners DOUBLE,
            avg_playEvents DOUBLE
        ) STORED BY ICEBERG
        PARTITIONED BY (year, month)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/allplays_aggregates'
        """)

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS spark_catalog.mlb_db.season_totals (
            year INT,
            total_rbi INT,
            total_awayScore INT,
            total_homeScore INT,
            total_pitches INT,
            total_actions INT,
            total_runners INT,
            total_playEvents INT
        ) STORED BY ICEBERG
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/season_totals'
        """)

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS spark_catalog.mlb_db.season_averages (
            year INT,
            avg_captivatingIndex DOUBLE,
            avg_balls DOUBLE,
            avg_strikes DOUBLE,
            avg_outs DOUBLE,
            avg_pitches DOUBLE,
            avg_actions DOUBLE,
            avg_runners DOUBLE,
            avg_playEvents DOUBLE
        ) STORED BY ICEBERG
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/season_averages'
        """)

        logger.info("AllPlays info, aggregation, and cumulative tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating tables: {e}", exc_info=True)
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
        StructField("allPlays_playEndTime", StringType(), True),
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
        StructField("allPlays_about.startTime", StringType(), True),
        StructField("allPlays_about.endTime", StringType(), True),
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
        StructField("allPlays_matchup.batter.link", StringType(), True),
        StructField("allPlays_matchup.batSide.code", StringType(), True),
        StructField("allPlays_matchup.batSide.description", StringType(), True),
        StructField("allPlays_matchup.pitcher.id", IntegerType(), True),
        StructField("allPlays_matchup.pitcher.fullName", StringType(), True),
        StructField("allPlays_matchup.pitcher.link", StringType(), True),
        StructField("allPlays_matchup.pitchHand.code", StringType(), True),
        StructField("allPlays_matchup.pitchHand.description", StringType(), True),
        StructField("allPlays_matchup.batterHotColdZones", StringType(), True),
        StructField("allPlays_matchup.pitcherHotColdZones", StringType(), True),
        StructField("allPlays_matchup.splits.batter", StringType(), True),
        StructField("allPlays_matchup.splits.pitcher", StringType(), True),
        StructField("allPlays_matchup.splits.menOnBase", StringType(), True),
        StructField("allPlays_matchup.postOnSecond.id", IntegerType(), True),
        StructField("allPlays_matchup.postOnSecond.fullName", StringType(), True),
        StructField("allPlays_matchup.postOnSecond.link", StringType(), True),
        StructField("allPlays_matchup.postOnFirst.id", IntegerType(), True),
        StructField("allPlays_matchup.postOnFirst.fullName", StringType(), True),
        StructField("allPlays_matchup.postOnFirst.link", StringType(), True),
        StructField("allPlays_matchup.postOnThird.id", IntegerType(), True),
        StructField("allPlays_matchup.postOnThird.fullName", StringType(), True),
        StructField("allPlays_matchup.postOnThird.link", StringType(), True)
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
        .withColumn("year", year(col("game_date"))) \
        .withColumn("month", month(col("game_date"))) \
        .withColumn("day", dayofmonth(col("game_date"))) \
        .withColumnRenamed("allPlays_pitchIndex", "pitchIndex") \
        .withColumnRenamed("allPlays_actionIndex", "actionIndex") \
        .withColumnRenamed("allPlays_runnerIndex", "runnerIndex") \
        .withColumnRenamed("allPlays_runners", "runners") \
        .withColumnRenamed("allPlays_playEvents", "playEvents") \
        .withColumnRenamed("allPlays_playEndTime", "playEndTime") \
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
        .withColumnRenamed("allPlays_about.startTime", "about_startTime") \
        .withColumnRenamed("allPlays_about.endTime", "about_endTime") \
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
        .withColumnRenamed("allPlays_matchup.batter.link", "matchup_batter_link") \
        .withColumnRenamed("allPlays_matchup.batSide.code", "matchup_batSide_code") \
        .withColumnRenamed("allPlays_matchup.batSide.description", "matchup_batSide_description") \
        .withColumnRenamed("allPlays_matchup.pitcher.id", "matchup_pitcher_id") \
        .withColumnRenamed("allPlays_matchup.pitcher.fullName", "matchup_pitcher_fullName") \
        .withColumnRenamed("allPlays_matchup.pitcher.link", "matchup_pitcher_link") \
        .withColumnRenamed("allPlays_matchup.pitchHand.code", "matchup_pitchHand_code") \
        .withColumnRenamed("allPlays_matchup.pitchHand.description", "matchup_pitchHand_description") \
        .withColumnRenamed("allPlays_matchup.batterHotColdZones", "matchup_batterHotColdZones") \
        .withColumnRenamed("allPlays_matchup.pitcherHotColdZones", "matchup_pitcherHotColdZones") \
        .withColumnRenamed("allPlays_matchup.splits.batter", "matchup_splits_batter") \
        .withColumnRenamed("allPlays_matchup.splits.pitcher", "matchup_splits_pitcher") \
        .withColumnRenamed("allPlays_matchup.splits.menOnBase", "matchup_splits_menOnBase") \
        .withColumnRenamed("allPlays_matchup.postOnSecond.id", "matchup_postOnSecond_id") \
        .withColumnRenamed("allPlays_matchup.postOnSecond.fullName", "matchup_postOnSecond_fullName") \
        .withColumnRenamed("allPlays_matchup.postOnSecond.link", "matchup_postOnSecond_link") \
        .withColumnRenamed("allPlays_matchup.postOnFirst.id", "matchup_postOnFirst_id") \
        .withColumnRenamed("allPlays_matchup.postOnFirst.fullName", "matchup_postOnFirst_fullName") \
        .withColumnRenamed("allPlays_matchup.postOnFirst.link", "matchup_postOnFirst_link") \
        .withColumnRenamed("allPlays_matchup.postOnThird.id", "matchup_postOnThird_id") \
        .withColumnRenamed("allPlays_matchup.postOnThird.fullName", "matchup_postOnThird_fullName") \
        .withColumnRenamed("allPlays_matchup.postOnThird.link", "matchup_postOnThird_link")

    query = allplays_df.writeStream \
        .format("iceberg") \
        .option("path", f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/allplays") \
        .option("checkpointLocation", "/tmp/checkpoints/allplays_info") \
        .outputMode("append") \
        .start()

    aggregate_df = allplays_df.groupBy("game_id") \
        .agg(
            spark_sum("result_rbi").alias("total_rbi"),
            spark_sum("result_awayScore").alias("total_awayScore"),
            spark_sum("result_homeScore").alias("total_homeScore"),
            spark_sum("pitchIndex").alias("total_pitches"),
            spark_sum("actionIndex").alias("total_actions"),
            spark_sum("runnerIndex").alias("total_runners"),
            spark_sum("playEvents").alias("total_playEvents"),
            avg("about_captivatingIndex").alias("avg_captivatingIndex"),
            avg("count_balls").alias("avg_balls"),
            avg("count_strikes").alias("avg_strikes"),
            avg("count_outs").alias("avg_outs"),
            avg("pitchIndex").alias("avg_pitches"),
            avg("actionIndex").alias("avg_actions"),
            avg("runnerIndex").alias("avg_runners"),
            avg("playEvents").alias("avg_playEvents")
        )

    aggregate_query = aggregate_df.writeStream \
        .format("iceberg") \
        .option("path", f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/allplays_aggregates") \
        .option("checkpointLocation", "/tmp/checkpoints/allplays_aggregates") \
        .outputMode("complete") \
        .start()

    cumulative_df = allplays_df.withColumn("game_date", col("game_date").cast("date"))

    season_totals_df = cumulative_df.groupBy("year") \
        .agg(
            spark_sum("result_rbi").alias("total_rbi"),
            spark_sum("result_awayScore").alias("total_awayScore"),
            spark_sum("result_homeScore").alias("total_homeScore"),
            spark_sum("pitchIndex").alias("total_pitches"),
            spark_sum("actionIndex").alias("total_actions"),
            spark_sum("runnerIndex").alias("total_runners"),
            spark_sum("playEvents").alias("total_playEvents")
        )

    season_totals_query = season_totals_df.writeStream \
        .format("iceberg") \
        .option("path", f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/season_totals") \
        .option("checkpointLocation", "/tmp/checkpoints/season_totals") \
        .outputMode("complete") \
        .start()

    season_averages_df = cumulative_df.groupBy("year") \
        .agg(
            avg("about_captivatingIndex").alias("avg_captivatingIndex"),
            avg("count_balls").alias("avg_balls"),
            avg("count_strikes").alias("avg_strikes"),
            avg("count_outs").alias("avg_outs"),
            avg("pitchIndex").alias("avg_pitches"),
            avg("actionIndex").alias("avg_actions"),
            avg("runnerIndex").alias("avg_runners"),
            avg("playEvents").alias("avg_playEvents")
        )

    season_averages_query = season_averages_df.writeStream \
        .format("iceberg") \
        .option("path", f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/season_averages") \
        .option("checkpointLocation", "/tmp/checkpoints/season_averages") \
        .outputMode("complete") \
        .start()

    query.awaitTermination()
    aggregate_query.awaitTermination()
    season_totals_query.awaitTermination()
    season_averages_query.awaitTermination()

if __name__ == "__main__":
    spark = create_spark_connection()
    azure_container = os.getenv('AZURE_CONTAINER')
    azure_account_name = os.getenv('AZURE_ACCOUNT_NAME')
    create_tables(spark, azure_container, azure_account_name)
    process_stream(spark)
