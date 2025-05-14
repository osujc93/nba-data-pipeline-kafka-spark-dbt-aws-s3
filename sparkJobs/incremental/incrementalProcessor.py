#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import sys
import json
import psycopg2
from typing import Optional, Dict, Set

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, year, month, dayofmonth, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

from kafka import KafkaConsumer

logger = logging.getLogger(__name__)

POSTGRES_HOST = "postgres"
POSTGRES_DB   = "nelonba"
POSTGRES_USER = "nelonba"
POSTGRES_PWD  = "Password123456789"
POSTGRES_PORT = 5432

class IncrementalKafkaProcessor:
    """
    Manages reading only new offsets from Kafka in batch mode. We do this by:
      1. Reading the last committed offsets from Postgres
      2. Telling Spark to start from those offsets
      3. Reading all data up to 'latest'
      4. Saving the newly consumed offsets back to Postgres
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.topic_name = "NBA_player_boxscores_incr"

        # Ensure kafka_offsets table exists before use
        self._ensure_kafka_offsets_table()

        # Schema definition for incoming JSON
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

    def _get_last_offsets_from_postgres(self) -> Optional[Dict]:
        """
        Fetch the last saved offsets (in JSON form) from Postgres.
        Returns None if no offsets exist yet.
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
                    "SELECT offsets_json FROM kafka_offsets WHERE topic = %s ORDER BY id DESC LIMIT 1;",
                    (self.topic_name,)
                )
                row = cur.fetchone()
                if not row:
                    return None
                offsets_json_str = row[0]
                return json.loads(offsets_json_str) if offsets_json_str else None
        except Exception as e:
            logger.error("Error retrieving offsets from Postgres: %s", e, exc_info=True)
            return None
        finally:
            if conn:
                conn.close()

    def _save_offsets_to_postgres(self, offsets: Dict) -> None:
        """
        Save the new offsets as JSON into Postgres, under this topic.
        """
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
        except Exception as e:
            logger.error("Error saving offsets to Postgres: %s", e, exc_info=True)
        finally:
            if conn:
                conn.close()

    # Use Kafka metadata directly to ensure all partitions are discovered.
    def _get_all_partitions_for_topic(self) -> Set[int]:
        """
        Returns all partition IDs for the specified Kafka topic by querying
        KafkaConsumer metadata (independent of whether the partition has data).
        """
        consumer = None
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=["kafka2:9092", "kafka3:9093", "kafka4:9094"],
                # group_id is not strictly needed for listing partitions
                consumer_timeout_ms=3000
            )
            partitions = consumer.partitions_for_topic(self.topic_name)
            if partitions is None:
                # If the topic does not exist or has no metadata, return empty set
                return set()
            return partitions
        finally:
            if consumer:
                consumer.close()

    def _get_starting_offset_option(self) -> str:
        """
        Construct the correct JSON for Spark's 'startingOffsets' option.
        If we have saved offsets in Postgres, use them. Otherwise use 'earliest'.
        Additionally, fill in any newly discovered partitions with -2 (earliest).
        """
        last_offsets = self._get_last_offsets_from_postgres()
        all_partitions = self._get_all_partitions_for_topic()

        if last_offsets:
            logger.info(
                "[Incremental] Found previously stored offsets. Using: %s",
                json.dumps(last_offsets)
            )
            existing_topic_partitions = last_offsets.get(self.topic_name, {})

            # Add any newly discovered partitions with offset = -2
            for p in all_partitions:
                p_str = str(p)
                if p_str not in existing_topic_partitions:
                    existing_topic_partitions[p_str] = -2  # earliest

            # Overwrite the offsets in our dictionary
            last_offsets[self.topic_name] = existing_topic_partitions
            return json.dumps(last_offsets)
        else:
            logger.info("[Incremental] No stored offsets found. Using 'earliest' for all partitions.")
            # If no offsets at all, we can either return "earliest" string
            # or build a JSON with -2 for each partition. For a brand-new start,
            # just returning "earliest" is fine:
            return "earliest"

    def _extract_latest_offsets(self, df: DataFrame) -> Dict:
        """
        Query the batch DataFrame for the largest offsets actually consumed.
        Return them in the JSON structure Spark expects:
           {
               "NBA_player_boxscores_incr": {
                   "0": 123,
                   "1": 456
               }
           }
        """
        if df.rdd.isEmpty():
            logger.info("[Incremental] No rows consumed, won't update offsets.")
            return {}

        df_offsets = (
            df.select("topic", "partition", "offset")
              .groupBy("topic", "partition")
              .agg({"offset": "max"})
              .withColumnRenamed("max(offset)", "max_offset")
        )

        rows = df_offsets.collect()
        offsets_dict = {}
        for r in rows:
            t = r["topic"]
            p = str(r["partition"])
            o = r["max_offset"]
            if t not in offsets_dict:
                offsets_dict[t] = {}
            # Next offset to start from will be current offset + 1
            offsets_dict[t][p] = o + 1

        return offsets_dict

    def read_kafka_incremental_and_process(self) -> None:
        """
        Main method to read new Kafka messages (based on last offsets), process them,
        and write to Iceberg.
        """
        try:
            # 1) Figure out the correct starting offsets (from DB or "earliest")
            starting_offsets_json = self._get_starting_offset_option()

            raw_kafka_df = (
                self.spark.read
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "kafka2:9092,kafka3:9093,kafka4:9094")
                    .option("subscribe", self.topic_name)
                    .option("startingOffsets", starting_offsets_json)
                    .option("endingOffsets", "latest")
                    .option("failOnDataLoss", "false")
                    .load()
            )

            if raw_kafka_df.rdd.isEmpty():
                logger.info("[Incremental] No new records found in Kafka.")
                return

            raw_kafka_df = raw_kafka_df.selectExpr(
                "CAST(topic AS STRING) as topic",
                "CAST(partition AS INT) as partition",
                "CAST(offset AS LONG) as offset",
                "CAST(value AS STRING) as value"
            )

            # 3) Parse JSON
            parsed_df = (
                raw_kafka_df
                .select(
                    "topic", "partition", "offset",
                    from_json(col("value"), self.schema).alias("data")
                )
                .select("topic", "partition", "offset", "data.*")
            )

            # 4) Rename & Transform columns
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
            )

            final_df = final_df.withColumn("game_date", col("game_date").cast(DateType()))
            final_df = (
                final_df
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

            final_df.write.format("iceberg") \
                .mode("append") \
                .save("spark_catalog.iceberg_nba_player_boxscores.nba_player_boxscores")

            appended_count = final_df.count()
            logger.info(
                "[Incremental] Appended %d records to Iceberg table.",
                appended_count
            )

            # 5) Extract & Save the newest offsets
            latest_offsets = self._extract_latest_offsets(raw_kafka_df)
            if latest_offsets:
                self._save_offsets_to_postgres(latest_offsets)
                logger.info("[Incremental] Updated offsets saved to Postgres: %s", latest_offsets)

        except Exception as e:
            logger.error("[Incremental] Error processing Kafka incremental data: %s", e, exc_info=True)
            sys.exit(1)
