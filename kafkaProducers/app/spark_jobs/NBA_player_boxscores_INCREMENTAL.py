#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import sys
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    year,
    month,
    dayofmonth
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Kafka bootstrap servers
BOOTSTRAP_SERVERS: List[str] = [
    "172.16.10.2:9092",
    "172.16.10.3:9093",
    "172.16.10.4:9094",
]

def create_spark_connection() -> SparkSession:
    """
    Creates and returns a SparkSession with the necessary configurations.
    This time, we specifically set 'startingOffsets' = 'latest' later when reading from Kafka.
    """
    try:
        spark = (
            SparkSession.builder.appName("NBA_Player_Boxscores_Incremental")
            .config(
                "spark.jars.packages",
                ",".join(
                    [
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1",
                        "org.apache.hadoop:hadoop-common:3.4.0",
                        "org.apache.hadoop:hadoop-hdfs:3.4.0",
                        "org.apache.commons:commons-pool2:2.12.0",
                    ]
                ),
            )
            .config("spark.sql.iceberg.target-file-size-bytes", "134217728")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog",
            )
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.warehouse.dir", "hdfs://mycluster/nelo_sports_warehouse")
            .enableHiveSupport()
            .getOrCreate()
        )

        # Create the Iceberg DB if not exists
        spark.sql("CREATE DATABASE IF NOT EXISTS iceberg_nba_player_boxscores")

        logger.info("Spark connection (incremental) created successfully.")

        spark.conf.set("spark.sql.shuffle.partitions", 25)
        spark.conf.set("spark.sql.adaptive.enabled", "false")

        return spark
    except Exception as e:
        logger.error(f"[Incremental] Error creating Spark connection: {e}", exc_info=True)
        sys.exit(1)

def create_main_boxscore_table_iceberg(spark: SparkSession) -> None:
    """
    Creates or ensures existence of the main boxscore table in Iceberg.
    We'll re-use the same table as before or create a new one, depending on your preference.
    """
    try:
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS spark_catalog.iceberg_nba_player_boxscores.nba_player_boxscores (
                season_id STRING,
                player_id INT,
                player_name STRING,
                team_id INT,
                team_abbreviation STRING,
                team_name STRING,
                game_id STRING,
                game_date DATE,
                year INT,
                month INT,
                day INT,
                matchup STRING,
                win_loss STRING,
                minutes_played INT,
                field_goals_made INT,
                field_goals_attempted INT,
                field_goal_percentage DOUBLE,
                three_point_field_goals_made INT,
                three_point_field_goals_attempted INT,
                three_point_field_goal_percentage DOUBLE,
                free_throws_made INT,
                free_throws_attempted INT,
                free_throw_percentage DOUBLE,
                offensive_rebounds INT,
                defensive_rebounds INT,
                rebounds INT,
                assists INT,
                steals INT,
                blocks INT,
                turnovers INT,
                personal_fouls INT,
                points INT,
                plus_minus DOUBLE,
                fantasy_points DOUBLE,
                video_available INT,
                season STRING,
                season_type STRING,
                game_date_param STRING
            )
            USING ICEBERG
            PARTITIONED BY (season, season_type, team_name, month)
            """
        )
        logger.info("[Incremental] Iceberg table verified/created.")
    except Exception as e:
        logger.error(f"[Incremental] Error creating main boxscore table: {e}", exc_info=True)
        sys.exit(1)

def read_kafka_incremental_and_process(spark: SparkSession) -> None:
    """
    Reads only new offsets from Kafka using startingOffsets='latest', then appends to Iceberg.
    """
    try:
        schema = StructType([
            StructField("SEASON_ID", StringType(), True),
            StructField("PLAYER_ID", IntegerType(), True),
            StructField("PLAYER_NAME", StringType(), True),
            StructField("TEAM_ID", IntegerType(), True),
            StructField("TEAM_ABBREVIATION", StringType(), True),
            StructField("TEAM_NAME", StringType(), True),
            StructField("GAME_ID", StringType(), True),
            StructField("GAME_DATE", StringType(), True),
            StructField("MATCHUP", StringType(), True),
            StructField("WL", StringType(), True),
            StructField("MIN", IntegerType(), True),
            StructField("FGM", IntegerType(), True),
            StructField("FGA", IntegerType(), True),
            StructField("FG_PCT", DoubleType(), True),
            StructField("FG3M", IntegerType(), True),
            StructField("FG3A", IntegerType(), True),
            StructField("FG3_PCT", DoubleType(), True),
            StructField("FTM", IntegerType(), True),
            StructField("FTA", IntegerType(), True),
            StructField("FT_PCT", DoubleType(), True),
            StructField("OREB", IntegerType(), True),
            StructField("DREB", IntegerType(), True),
            StructField("REB", IntegerType(), True),
            StructField("AST", IntegerType(), True),
            StructField("STL", IntegerType(), True),
            StructField("BLK", IntegerType(), True),
            StructField("TOV", IntegerType(), True),
            StructField("PF", IntegerType(), True),
            StructField("PTS", IntegerType(), True),
            StructField("PLUS_MINUS", DoubleType(), True),
            StructField("FANTASY_PTS", DoubleType(), True),
            StructField("VIDEO_AVAILABLE", IntegerType(), True),
            StructField("DateFrom", StringType(), True),
            StructField("DateTo", StringType(), True),
            StructField("SeasonType", StringType(), True),
        ])

        # IMPORTANT: The incremental difference here is we use 'startingOffsets' = 'latest'
        # so that we do NOT reprocess old messages in the topic.
        df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", ",".join(BOOTSTRAP_SERVERS))
            .option("subscribe", "NBA_player_boxscores_incr")
            .option("startingOffsets", "latest")
            .option("endingOffsets", "latest")  # we process only what's there
            .load()
        )

        if df.rdd.isEmpty():
            logger.info("[Incremental] No new records found in Kafka.")
            return

        parsed_df = (
            df.selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*")
        )

        final_df = (
            parsed_df
            .withColumnRenamed("SEASON_ID", "season_id")
            .withColumnRenamed("PLAYER_ID", "player_id")
            .withColumnRenamed("PLAYER_NAME", "player_name")
            .withColumnRenamed("TEAM_ID", "team_id")
            .withColumnRenamed("TEAM_ABBREVIATION", "team_abbreviation")
            .withColumnRenamed("TEAM_NAME", "team_name")
            .withColumnRenamed("GAME_ID", "game_id")
            .withColumnRenamed("GAME_DATE", "game_date")
            .withColumnRenamed("MATCHUP", "matchup")
            .withColumnRenamed("WL", "win_loss")
            .withColumnRenamed("MIN", "minutes_played")
            .withColumnRenamed("FGM", "field_goals_made")
            .withColumnRenamed("FGA", "field_goals_attempted")
            .withColumnRenamed("FG_PCT", "field_goal_percentage")
            .withColumnRenamed("FG3M", "three_point_field_goals_made")
            .withColumnRenamed("FG3A", "three_point_field_goals_attempted")
            .withColumnRenamed("FG3_PCT", "three_point_field_goal_percentage")
            .withColumnRenamed("FTM", "free_throws_made")
            .withColumnRenamed("FTA", "free_throws_attempted")
            .withColumnRenamed("FT_PCT", "free_throw_percentage")
            .withColumnRenamed("OREB", "offensive_rebounds")
            .withColumnRenamed("DREB", "defensive_rebounds")
            .withColumnRenamed("REB", "rebounds")
            .withColumnRenamed("AST", "assists")
            .withColumnRenamed("STL", "steals")
            .withColumnRenamed("BLK", "blocks")
            .withColumnRenamed("TOV", "turnovers")
            .withColumnRenamed("PF", "personal_fouls")
            .withColumnRenamed("PTS", "points")
            .withColumnRenamed("PLUS_MINUS", "plus_minus")
            .withColumnRenamed("FANTASY_PTS", "fantasy_points")
            .withColumnRenamed("VIDEO_AVAILABLE", "video_available")
            .withColumnRenamed("SeasonType", "season_type")
            .withColumnRenamed("DateFrom", "date_from")
            .withColumnRenamed("DateTo", "date_to")
        )

        # If your "season_type" values are "Regular%20Season", "Playoffs", etc., you can replace them:
        final_df = final_df.replace(
            {
                "Regular%20Season": "regular_season",
                "Pre%20Season": "pre_season",
                "Playoffs": "playoffs",
                "All%20Star": "all_star",
                "PlayIn": "play_in",
                "IST": "nba_cup"
            },
            subset=["season_type"]
        )

        # Convert game_date to date
        final_df = final_df.withColumn("game_date", col("game_date").cast(DateType()))
        # Add year, month, day
        final_df = final_df.withColumn("year", year(col("game_date")))
        final_df = final_df.withColumn("month", month(col("game_date")))
        final_df = final_df.withColumn("day", dayofmonth(col("game_date")))

        # Write to the same Iceberg table
        final_df.write.format("iceberg") \
            .mode("append") \
            .save("spark_catalog.iceberg_nba_player_boxscores.nba_player_boxscores")

        logger.info(f"[Incremental] Appended {final_df.count()} records to Iceberg table.")

    except Exception as e:
        logger.error(f"[Incremental] Error processing Kafka incremental data: {e}", exc_info=True)
        sys.exit(1)

def main() -> None:
    try:
        spark = create_spark_connection()
        create_main_boxscore_table_iceberg(spark)
        read_kafka_incremental_and_process(spark)
        spark.stop()
        logger.info("[Incremental] Spark job completed successfully.")
    except Exception as e:
        logger.error(f"[Incremental] Error in main: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
