#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""

1) Reads from Kafka "NBA_player_boxscores" in batch mode (earliest->latest).
2) Parses JSON data and renames columns.
3) Cleans up season_type values.
4) Adds derived columns (year, month, day).
5) Appends to the Iceberg table spark_catalog.iceberg_nba_player_boxscores.nba_player_boxscores
6) Extract the latest consumed offsets from the batch read and store them in Postgres
   so the next incremental job can pick up from there.
"""

import logging
import sys
import json
import psycopg2
from typing import Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json,
    col,
    year,
    month,
    dayofmonth,
    to_date,
    when
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

logger = logging.getLogger(__name__)

POSTGRES_HOST = "postgres"
POSTGRES_DB   = "nelonba"
POSTGRES_USER = "nelonba"
POSTGRES_PWD  = "Password123456789"
POSTGRES_PORT = 5432

class BatchProcessor:
    def __init__(self, spark: SparkSession, bootstrap_servers: list[str]):
        try:
            self.spark = spark
            self.bootstrap_servers = bootstrap_servers
            self.topic_name = "NBA_player_boxscores"

            self._ensure_kafka_offsets_table()

            # Defined schema matching the JSON in Kafka messages
            self.schema = StructType([
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
            ])
        finally:
            pass

    def _ensure_kafka_offsets_table(self) -> None:
        """
        Creates the kafka_offsets table in Postgres if it does not already exist.
        """
        conn = None
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PWD,
                port=POSTGRES_PORT
            )
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS kafka_offsets (
                        id SERIAL PRIMARY KEY,
                        topic VARCHAR NOT NULL,
                        offsets_json TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    """
                )
            conn.commit()
        except Exception as e:
            logger.error("Error ensuring kafka_offsets table exists: %s", e, exc_info=True)
        finally:
            if conn:
                conn.close()

    def _extract_latest_offsets_from_batch_df(self, df: DataFrame) -> Dict:
        """
        Extract the highest consumed offsets from the DataFrame that was used to read from Kafka.
        Returns a dictionary of the form:
            {
                "NBA_player_boxscores": {
                    "0": <last-offset+1>,
                    "1": <last-offset+1>,
                    ...
                }
            }
        If no data, returns {}.
        """
        try:
            if df.rdd.isEmpty():
                logger.info("[Batch] No rows read. No offsets to save.")
                return {}

            # We group by (topic, partition) and get the max offset
            offsets_agg = (
                df.groupBy("topic", "partition")
                  .agg({"offset": "max"})
                  .withColumnRenamed("max(offset)", "max_offset")
            )

            rows = offsets_agg.collect()
            offsets_dict = {}
            for r in rows:
                t = r["topic"]
                p = str(r["partition"])
                # next offset to consume is current offset + 1
                next_offset = r["max_offset"] + 1
                if t not in offsets_dict:
                    offsets_dict[t] = {}
                offsets_dict[t][p] = next_offset

            return offsets_dict

        finally:
            pass

    def _save_offsets_to_postgres(self, offsets: Dict) -> None:
        """
        Store the offsets dictionary as JSON in a Postgres table so the incremental job
        can resume from these offsets.
        We store one row per ingestion. The incremental job always takes the most recent row.
        """
        if not offsets:
            logger.info("No offsets to save.")
            return

        conn = None
        try:
            offsets_json_str = json.dumps(offsets)
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PWD,
                port=POSTGRES_PORT
            )
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO kafka_offsets (topic, offsets_json)
                    VALUES (%s, %s);
                    """,
                    (self.topic_name, offsets_json_str)
                )
            conn.commit()
            logger.info("[Batch] Successfully saved offsets to Postgres: %s", offsets)
        except Exception as e:
            logger.error("Error saving offsets to Postgres: %s", e, exc_info=True)
        finally:
            if conn:
                conn.close()

    def read_kafka_batch_and_process(self) -> None:
        try:
            # Read from earliest->latest in batch mode
            kafka_batch_df: DataFrame = (
                self.spark.read.format("kafka")
                .option("kafka.bootstrap.servers", ",".join(self.bootstrap_servers))
                .option("subscribe", self.topic_name)
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load()
                .selectExpr(
                    "CAST(topic AS STRING) as topic",
                    "CAST(partition AS INT) as partition",
                    "CAST(offset AS LONG) as offset",
                    "CAST(value AS STRING) as value"
                )
            )

            # If no data was found, exit
            if kafka_batch_df.rdd.isEmpty():
                logger.info("No new records found in Kafka. Exiting.")
                return

            # Step 2: Parse JSON
            parsed_df: DataFrame = (
                kafka_batch_df
                .select(
                    "topic",
                    "partition",
                    "offset",
                    from_json(col("value"), self.schema).alias("data")
                )
                .select("topic", "partition", "offset", "data.*")
            )

            # Step 3: Rename columns
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

            # Step 4: Cast game_date to DateType & add derived columns
            final_df = (
                final_df
                .withColumn("game_date", to_date(col("game_date"), "yyyy-MM-dd"))
                .withColumn("year", year(col("game_date")))
                .withColumn("month", month(col("game_date")))
                .withColumn("day", dayofmonth(col("game_date")))
            )

            # Clean up season_type
            final_df = final_df.withColumn(
                "season_type",
                when(col("season_type") == "Regular%20Season", "regular_season")
                .when(col("season_type") == "Pre%20Season", "pre_season")
                .when(col("season_type") == "All%20Star", "all_star")
                .when(col("season_type") == "PlayIn", "play_in")
                .when(col("season_type") == "Playoffs", "playoffs")
                .otherwise(col("season_type"))
            )

            # exclude columns that are not in the target Iceberg table:
            final_df = final_df.select(
                "season_id",
                "player_id",
                "player_name",
                "team_id",
                "team_abbreviation",
                "team_name",
                "game_id",
                "game_date",
                "matchup",
                "win_loss",
                "minutes_played",
                "field_goals_made",
                "field_goals_attempted",
                "field_goal_percentage",
                "three_point_field_goals_made",
                "three_point_field_goals_attempted",
                "three_point_field_goal_percentage",
                "free_throws_made",
                "free_throws_attempted",
                "free_throw_percentage",
                "offensive_rebounds",
                "defensive_rebounds",
                "rebounds",
                "assists",
                "steals",
                "blocks",
                "turnovers",
                "personal_fouls",
                "points",
                "plus_minus",
                "fantasy_points",
                "video_available",
                "season",
                "season_type",
                "year",
                "month",
                "day"
            )

            # Fill nulls in numeric columns with 0
            numeric_cols = [
                field.name
                for field in final_df.schema.fields
                if isinstance(field.dataType, (IntegerType, DoubleType))
            ]
            final_df = final_df.fillna(0, subset=numeric_cols)

            # Step 5: Final Write to Iceberg
            final_df.writeTo("spark_catalog.iceberg_nba_player_boxscores.nba_player_boxscores") \
                    .append()

            logger.info("Successfully wrote batch data into Iceberg table.")

            # Step 6: Extract the newly consumed offsets and store them in Postgres
            latest_offsets = self._extract_latest_offsets_from_batch_df(kafka_batch_df)
            self._save_offsets_to_postgres(latest_offsets)

        except Exception as e:
            logger.error("Error in read_kafka_batch_and_process(): %s", e, exc_info=True)
            sys.exit(1)
        finally:
            pass
