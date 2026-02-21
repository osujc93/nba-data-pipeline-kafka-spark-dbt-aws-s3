#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Module for ingesting NBA player boxscore data from Kafka into Spark and storing in Iceberg.
"""

import logging
import sys
from typing import Union
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, year, month, dayofmonth
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
)

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Kafka bootstrap servers
BOOTSTRAP_SERVERS: list[str] = [
    "172.16.10.2:9092",
    "172.16.10.3:9093",
    "172.16.10.4:9094",
]


def create_spark_connection() -> SparkSession:
    """
    Create and return a SparkSession with the necessary configurations.

    The SparkSession is set up with Kafka, Iceberg, and Hadoop packages.
    The function also ensures that the needed databases are created.
    Returns:
        SparkSession: Configured Spark session object.
    """
    try:
        spark = (
            SparkSession.builder.appName("NBA_Player_Boxscores_Batch")
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

        # Create a separate Hive DB so we don't conflict
        spark.sql("CREATE DATABASE IF NOT EXISTS hive_nba_player_boxscores")

        logger.info("Spark connection (batch mode) created successfully.")

        # Shuffle partitions
        spark.conf.set("spark.sql.shuffle.partitions", 25)
        # Disable adaptive execution if desired
        spark.conf.set("spark.sql.adaptive.enabled", "false")

        return spark
    except Exception as e:
        logger.error("Error creating Spark connection: %s", e, exc_info=True)
        sys.exit(1)


def create_main_boxscore_table_iceberg(spark: SparkSession) -> None:
    """
    Create or ensure existence of the main boxscore table in Iceberg (spark_catalog).

    Args:
        spark (SparkSession): The active Spark session.
    """
    try:
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS 
            spark_catalog.iceberg_nba_player_boxscores.nba_player_boxscores (
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
                season_type STRING
            )
            USING ICEBERG
            PARTITIONED BY (season, season_type, team_name, month)
            """
        )

        logger.info(
            "Base Iceberg boxscore table created/verified in "
            "spark_catalog.iceberg_nba_player_boxscores."
        )
    except Exception as e:
        logger.error("Error creating main boxscore table (Iceberg): %s", e, exc_info=True)
        sys.exit(1)


def read_kafka_batch_and_process(spark: SparkSession) -> None:
    """
    Read data from Kafka in a single batch, then append to Iceberg table.

    The function:
      1) Reads from Kafka topic "NBA_player_boxscores"
      2) Parses the JSON data
      3) Renames columns to a standard naming convention
      4) Cleans up season_type values
      5) Casts game_date to DateType and adds derived columns (year, month, day)
      6) Appends data to Iceberg table 
         spark_catalog.iceberg_nba_player_boxscores.nba_player_boxscores

    Args:
        spark (SparkSession): The active Spark session.
    """
    try:
        schema = StructType(
            [
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
                StructField("Season", StringType(), True),
                StructField("SeasonType", StringType(), True),
            ]
        )

        kafka_batch_df: DataFrame = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", ",".join(BOOTSTRAP_SERVERS))
            .option("subscribe", "NBA_player_boxscores")
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        if kafka_batch_df.rdd.isEmpty():
            logger.info("No new records found in Kafka. Exiting.")
            return

        parsed_df: DataFrame = (
            kafka_batch_df.selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*")
        )

        final_df: DataFrame = (
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
            .withColumnRenamed("Season", "season")
            .withColumnRenamed("SeasonType", "season_type")
        )

        final_df = final_df.replace(
            {
                "Regular%20Season": "regular_season",
                "Pre%20Season": "pre_season",
                "Playoffs": "playoffs",
                "All%20Star": "all_star",
                "PlayIn": "play_in",
                "IST": "nba_cup",
            },
            subset=["season_type"],
        )

        final_df = final_df.withColumn("game_date", col("game_date").cast(DateType()))

        final_df = (
            final_df
            .withColumn("year", year(col("game_date")))
            .withColumn("month", month(col("game_date")))
            .withColumn("day", dayofmonth(col("game_date")))
        )

        final_df.write.format("iceberg").mode("append").save(
            "spark_catalog.iceberg_nba_player_boxscores.nba_player_boxscores"
        )

        logger.info(
            "[ICEBERG] Wrote %d records to "
            "spark_catalog.iceberg_nba_player_boxscores.nba_player_boxscores",
            final_df.count(),
        )

    except Exception as e:
        logger.error("Error in Kafka batch processing: %s", e, exc_info=True)
        sys.exit(1)


def main() -> None:
    """
    Main entry point for the Spark job (batch ingestion).
    """
    try:
        spark = create_spark_connection()

        # Ensure the Iceberg table is created
        create_main_boxscore_table_iceberg(spark)

        # Now read from Kafka and append to Iceberg
        read_kafka_batch_and_process(spark)

        spark.stop()
        logger.info("Batch ingestion pipeline executed successfully.")
    except Exception as e:
        logger.error("Error in main: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
