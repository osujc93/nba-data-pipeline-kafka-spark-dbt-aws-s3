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

# Configure logging
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
    Only includes settings needed for Hive.
    """
    try:
        spark = (
            SparkSession.builder.appName("NBA_Player_Boxscores_Batch_HIVE_ONLY")
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
            # We still configure spark_catalog for iceberg packages to load, 
            # but we do NOT create or write to any Iceberg table.
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.warehouse.dir", "hdfs://mycluster/nelo_sports_warehouse")
            .enableHiveSupport()
            .getOrCreate()
        )

        # Create a Hive DB if not exists
        spark.sql("CREATE DATABASE IF NOT EXISTS hive_nba_player_boxscores")

        logger.info("Spark connection (Hive ONLY) created successfully.")

        # Shuffle partitions
        spark.conf.set("spark.sql.shuffle.partitions", 25)
        # Disable adaptive execution if desired
        spark.conf.set("spark.sql.adaptive.enabled", "false")

        return spark
    except Exception as e:
        logger.error(f"Error creating Spark connection: {e}", exc_info=True)
        sys.exit(1)

def read_kafka_batch_and_process(spark: SparkSession) -> None:
    """
    Reads data from Kafka in a single batch, then appends to:
      1) hive_nba_player_boxscores.nba_player_boxscores (Hive)
    """
    try:
        # Define schema matching the original Kafka JSON
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
            StructField("Season", StringType(), True),
            StructField("SeasonType", StringType(), True),
            StructField("GameDateParam", StringType(), True),
        ])

        # Single batch read from Kafka
        kafka_batch_df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", ",".join(BOOTSTRAP_SERVERS))
            .option("subscribe", "NBA_player_boxscores")
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        if kafka_batch_df.rdd.isEmpty():
            logger.info("No new records found in Kafka. Exiting.")
            return

        # Parse JSON
        parsed_df = (
            kafka_batch_df
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*")
        )

        # Rename columns
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
            .withColumnRenamed("Season", "season")
            .withColumnRenamed("SeasonType", "season_type")
            .withColumnRenamed("GameDateParam", "game_date_param")
        )

        # Cast game_date to DateType
        final_df = final_df.withColumn("game_date", col("game_date").cast(DateType()))

        # Add year, month, day derived from game_date
        final_df = (
            final_df
            .withColumn("year", year(col("game_date")))
            .withColumn("month", month(col("game_date")))
            .withColumn("day", dayofmonth(col("game_date")))
        )

        # Write to Hive table in DB: hive_nba_player_boxscores
        final_df.write \
            .mode("append") \
            .partitionBy("month") \
            .saveAsTable("hive_nba_player_boxscores.nba_player_boxscores")

        record_count = final_df.count()
        logger.info(f"[HIVE] Wrote {record_count} records to hive_nba_player_boxscores.nba_player_boxscores")

    except Exception as e:
        logger.error(f"Error in Kafka batch processing (Hive only): {e}", exc_info=True)
        sys.exit(1)

def main() -> None:
    """
    Main entry point for the Spark job (batch ingestion) - HIVE ONLY
    """
    try:
        spark = create_spark_connection()

        # Read from Kafka and append to Hive
        read_kafka_batch_and_process(spark)

        spark.stop()
        logger.info("Batch ingestion pipeline (Hive only) executed successfully.")
    except Exception as e:
        logger.error(f"Error in main (Hive only): {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
