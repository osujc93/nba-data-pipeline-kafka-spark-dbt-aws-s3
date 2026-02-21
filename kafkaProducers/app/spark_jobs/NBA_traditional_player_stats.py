#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import sys
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json,
    col,
    current_date,
    lit,
    sum as spark_sum,
    avg,
    sha2,
    concat_ws,
    when
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
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
    Note we are removing streaming triggers and focusing on batch usage.
    """
    try:
        spark = (
            SparkSession.builder.appName("NBA_Traditional_Player_Stats_Batch")
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

        spark.sql("CREATE DATABASE IF NOT EXISTS NBA_player_stats")
        logger.info("Spark connection (batch mode) created successfully.")

        # Example: setting shuffle partitions to match Kafka partition count
        spark.conf.set("spark.sql.shuffle.partitions", 25)

        # Example: disabling adaptive execution if you want consistent partitioning
        spark.conf.set("spark.sql.adaptive.enabled", "false")

        return spark
    except Exception as e:
        logger.error(f"Error creating Spark connection: {e}", exc_info=True)
        sys.exit(1)

def create_tables(spark: SparkSession) -> None:
    """
    Creates necessary Iceberg tables in the Hive metastore for TRADITIONAL player stats,
    using fully spelled-out, lowercase column names with underscores.
    """
    try:
        # Drop old tables so new schema is correct
        for table_name in [
            "NBA_player_stats.traditional_player_stats",
            "NBA_player_stats.fact_traditional_player_stats",
            "NBA_player_stats.dim_players",
        ]:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

        #
        # 1) Create the main table for the TRADITIONAL player stats
        #
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.traditional_player_stats (
                player_id INT,
                player_name STRING,
                nickname STRING,
                team_id INT,
                team_abbreviation STRING,
                age DOUBLE,
                games_played INT,
                wins INT,
                losses INT,
                win_percentage DOUBLE,
                minutes_played DOUBLE,
                field_goals_made DOUBLE,
                field_goals_attempted DOUBLE,
                field_goal_percentage DOUBLE,
                three_pointers_made DOUBLE,
                three_pointers_attempted DOUBLE,
                three_point_percentage DOUBLE,
                free_throws_made DOUBLE,
                free_throws_attempted DOUBLE,
                free_throw_percentage DOUBLE,
                offensive_rebounds DOUBLE,
                defensive_rebounds DOUBLE,
                total_rebounds DOUBLE,
                assists DOUBLE,
                turnovers DOUBLE,
                steals DOUBLE,
                blocks DOUBLE,
                blocked_attempts DOUBLE,
                personal_fouls DOUBLE,
                fouls_drawn DOUBLE,
                points DOUBLE,
                plus_minus DOUBLE,
                nba_fantasy_points DOUBLE,
                double_doubles INT,
                triple_doubles INT,
                wnba_fantasy_points DOUBLE,
                games_played_rank INT,
                wins_rank INT,
                losses_rank INT,
                win_percentage_rank INT,
                minutes_played_rank INT,
                field_goals_made_rank INT,
                field_goals_attempted_rank INT,
                field_goal_percentage_rank INT,
                three_pointers_made_rank INT,
                three_pointers_attempted_rank INT,
                three_point_percentage_rank INT,
                free_throws_made_rank INT,
                free_throws_attempted_rank INT,
                free_throw_percentage_rank INT,
                offensive_rebounds_rank INT,
                defensive_rebounds_rank INT,
                total_rebounds_rank INT,
                assists_rank INT,
                turnovers_rank INT,
                steals_rank INT,
                blocks_rank INT,
                blocked_attempts_rank INT,
                personal_fouls_rank INT,
                fouls_drawn_rank INT,
                points_rank INT,
                plus_minus_rank INT,
                nba_fantasy_points_rank INT,
                double_doubles_rank INT,
                triple_doubles_rank INT,
                wnba_fantasy_points_rank INT,
                season STRING,
                season_type STRING,
                month INT,
                per_mode STRING,
                record_start_date DATE,
                record_end_date DATE,
                is_current BOOLEAN,
                hash_val STRING
            )
            USING ICEBERG
            PARTITIONED BY (season, month)
            """
        )

        #
        # 2) Create a simple dimension table for players (SCD Type 2 logic can apply)
        #
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.dim_players (
                player_id INT,
                player_name STRING,
                nickname STRING,
                team_id INT,
                team_abbreviation STRING,
                record_start_date DATE,
                record_end_date DATE,
                is_current BOOLEAN
            )
            USING ICEBERG
            PARTITIONED BY (player_id)
            """
        )

        #
        # 3) Create a fact table for aggregated TRADITIONAL player stats
        #
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.fact_traditional_player_stats (
                player_id INT,
                season STRING,
                month INT,
                per_mode STRING,
                plus_minus_bucket STRING,
                total_points DOUBLE,
                total_offensive_rebounds DOUBLE,
                total_defensive_rebounds DOUBLE,
                total_assists DOUBLE,
                total_steals DOUBLE,
                total_blocks DOUBLE,
                total_field_goals_made DOUBLE,
                total_field_goals_attempted DOUBLE,
                total_three_pointers_made DOUBLE,
                total_three_pointers_attempted DOUBLE,
                total_free_throws_made DOUBLE,
                total_free_throws_attempted DOUBLE,
                total_turnovers DOUBLE,
                avg_field_goal_percentage DOUBLE,
                avg_three_point_percentage DOUBLE,
                avg_free_throw_percentage DOUBLE,
                avg_plus_minus DOUBLE,
                avg_minutes_played DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season, player_id)
            """
        )

        logger.info("Tables for TRADITIONAL player stats created successfully.")
    except Exception as e:
        logger.error(f"Error creating tables: {e}", exc_info=True)
        sys.exit(1)

def handle_scd_type_2(
    df: DataFrame,
    spark: SparkSession,
    table_name: str,
    join_columns: List[str],
    compare_columns: List[str],
) -> None:
    """
    Handles Slowly Changing Dimension Type 2 for the given DataFrame,
    using a hash-based compare to detect changes.
    """
    try:
        # 1) Add SCD Type 2 columns
        df = (
            df.withColumn("record_start_date", current_date())
            .withColumn("record_end_date", lit(None).cast(DateType()))
            .withColumn("is_current", lit(True))
        )

        # 2) Build a single hash to detect changes
        df = df.withColumn(
            "hash_val",
            sha2(concat_ws("||", *[col(c).cast("string") for c in compare_columns]), 256)
        )

        # 3) Create or replace a GLOBAL temp view
        df.createOrReplaceGlobalTempView("updates_df")

        # 4) Build the join condition
        join_condition = " AND ".join([f"t.{jc} = u.{jc}" for jc in join_columns])

        # 5) MERGE statement comparing hash_val
        merge_query = f"""
            MERGE INTO {table_name} t
            USING global_temp.updates_df u
            ON {join_condition} AND t.is_current = true
            WHEN MATCHED AND t.hash_val <> u.hash_val THEN
              UPDATE SET
                t.is_current = false,
                t.record_end_date = current_date()
            WHEN NOT MATCHED THEN
              INSERT *
        """

        spark.sql(merge_query)
        logger.info(f"SCD Type 2 handling completed successfully (hash-based) for table: {table_name}")
    except Exception as e:
        logger.error(f"Error handling SCD Type 2: {e}", exc_info=True)
        sys.exit(1)

def scd_and_aggregate_batch(df: DataFrame, spark: SparkSession) -> None:
    """
    Performs SCD Type 2 merges into traditional_player_stats,
    then aggregates & writes to fact_traditional_player_stats in normal batch mode.
    Demonstrates a CASE WHEN approach to bucketize plus_minus.
    """
    record_count = df.count()
    logger.info(f"[Batch-Job] Dataset has {record_count} records to process in this run.")

    if record_count == 0:
        logger.info("[Batch-Job] No records to merge or aggregate.")
        return

    #
    # 1) Handle SCD merges into NBA_player_stats.traditional_player_stats
    #
    handle_scd_type_2(
        df=df,
        spark=spark,
        table_name="NBA_player_stats.traditional_player_stats",
        join_columns=["player_id", "season", "per_mode", "month"],
        compare_columns=[
            "player_name",
            "nickname",
            "team_id",
            "team_abbreviation",
            "age",
            "games_played",
            "wins",
            "losses",
            "win_percentage",
            "minutes_played",
            "field_goals_made",
            "field_goals_attempted",
            "field_goal_percentage",
            "three_pointers_made",
            "three_pointers_attempted",
            "three_point_percentage",
            "free_throws_made",
            "free_throws_attempted",
            "free_throw_percentage",
            "offensive_rebounds",
            "defensive_rebounds",
            "total_rebounds",
            "assists",
            "turnovers",
            "steals",
            "blocks",
            "blocked_attempts",
            "personal_fouls",
            "fouls_drawn",
            "points",
            "plus_minus",
            "nba_fantasy_points",
            "double_doubles",
            "triple_doubles",
            "wnba_fantasy_points",
            "games_played_rank",
            "wins_rank",
            "losses_rank",
            "win_percentage_rank",
            "minutes_played_rank",
            "field_goals_made_rank",
            "field_goals_attempted_rank",
            "field_goal_percentage_rank",
            "three_pointers_made_rank",
            "three_pointers_attempted_rank",
            "three_point_percentage_rank",
            "free_throws_made_rank",
            "free_throws_attempted_rank",
            "free_throw_percentage_rank",
            "offensive_rebounds_rank",
            "defensive_rebounds_rank",
            "total_rebounds_rank",
            "assists_rank",
            "turnovers_rank",
            "steals_rank",
            "blocks_rank",
            "blocked_attempts_rank",
            "personal_fouls_rank",
            "fouls_drawn_rank",
            "points_rank",
            "plus_minus_rank",
            "nba_fantasy_points_rank",
            "double_doubles_rank",
            "triple_doubles_rank",
            "wnba_fantasy_points_rank",
        ],
    )

    #
    # 2) CASE WHEN bucketization for plus_minus before aggregation
    #
    df_buckets = df.withColumn(
        "plus_minus_bucket",
        when(col("plus_minus") < -10, "LOW")
        .when((col("plus_minus") >= -10) & (col("plus_minus") <= 10), "MED")
        .otherwise("HIGH")
    )

    #
    # 3) Aggregate and write to fact_traditional_player_stats in batch
    #
    fact_aggregated = (
        df_buckets.groupBy(
            "player_id", "season", "month", "per_mode", "plus_minus_bucket"
        )
        .agg(
            spark_sum(col("points")).alias("total_points"),
            spark_sum(col("offensive_rebounds")).alias("total_offensive_rebounds"),
            spark_sum(col("defensive_rebounds")).alias("total_defensive_rebounds"),
            spark_sum(col("assists")).alias("total_assists"),
            spark_sum(col("steals")).alias("total_steals"),
            spark_sum(col("blocks")).alias("total_blocks"),
            spark_sum(col("field_goals_made")).alias("total_field_goals_made"),
            spark_sum(col("field_goals_attempted")).alias("total_field_goals_attempted"),
            spark_sum(col("three_pointers_made")).alias("total_three_pointers_made"),
            spark_sum(col("three_pointers_attempted")).alias("total_three_pointers_attempted"),
            spark_sum(col("free_throws_made")).alias("total_free_throws_made"),
            spark_sum(col("free_throws_attempted")).alias("total_free_throws_attempted"),
            spark_sum(col("turnovers")).alias("total_turnovers"),
            avg("field_goal_percentage").alias("avg_field_goal_percentage"),
            avg("three_point_percentage").alias("avg_three_point_percentage"),
            avg("free_throw_percentage").alias("avg_free_throw_percentage"),
            avg("plus_minus").alias("avg_plus_minus"),
            avg("minutes_played").alias("avg_minutes_played"),
        )
    )

    fact_aggregated.write.format("iceberg") \
        .mode("append") \
        .save("spark_catalog.NBA_player_stats.fact_traditional_player_stats")

    logger.info("[Batch-Job] Wrote aggregated data to fact_traditional_player_stats.")

def create_cumulative_tables(spark: SparkSession) -> None:
    """
    Creates additional cumulative Iceberg tables for TRADITIONAL stats
    (current season, all seasons, etc.) in standardized columns.
    Adjust or remove if not needed, but here we keep the structure for example.
    """
    try:
        # OPTIONAL: Drop if needed
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.current_season_player_traditional_totals")
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.current_season_player_traditional_averages")
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.all_season_player_traditional_totals")
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.all_season_player_traditional_averages")

        #
        # Example TOTALS table for the CURRENT season
        #
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.current_season_player_traditional_totals (
                season STRING,
                player_name STRING,
                total_points DOUBLE,
                total_field_goals_made DOUBLE,
                total_field_goals_attempted DOUBLE,
                total_three_pointers_made DOUBLE,
                total_three_pointers_attempted DOUBLE,
                total_free_throws_made DOUBLE,
                total_free_throws_attempted DOUBLE,
                total_offensive_rebounds DOUBLE,
                total_defensive_rebounds DOUBLE,
                total_rebounds DOUBLE,
                total_assists DOUBLE,
                total_steals DOUBLE,
                total_blocks DOUBLE,
                total_turnovers DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        #
        # Example AVERAGES table for the CURRENT season
        #
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.current_season_player_traditional_averages (
                season STRING,
                player_name STRING,
                avg_field_goal_percentage DOUBLE,
                avg_three_point_percentage DOUBLE,
                avg_free_throw_percentage DOUBLE,
                avg_plus_minus DOUBLE,
                avg_minutes_played DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        #
        # Example TOTALS table for ALL seasons
        #
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.all_season_player_traditional_totals (
                player_name STRING,
                total_points DOUBLE,
                total_field_goals_made DOUBLE,
                total_field_goals_attempted DOUBLE,
                total_three_pointers_made DOUBLE,
                total_three_pointers_attempted DOUBLE,
                total_free_throws_made DOUBLE,
                total_free_throws_attempted DOUBLE,
                total_offensive_rebounds DOUBLE,
                total_defensive_rebounds DOUBLE,
                total_rebounds DOUBLE,
                total_assists DOUBLE,
                total_steals DOUBLE,
                total_blocks DOUBLE,
                total_turnovers DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (player_name)
            """
        )

        #
        # Example AVERAGES table for ALL seasons
        #
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.all_season_player_traditional_averages (
                player_name STRING,
                avg_field_goal_percentage DOUBLE,
                avg_three_point_percentage DOUBLE,
                avg_free_throw_percentage DOUBLE,
                avg_plus_minus DOUBLE,
                avg_minutes_played DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (player_name)
            """
        )

        logger.info("Cumulative tables (TRADITIONAL) created successfully.")
    except Exception as e:
        logger.error(
            f"Error creating cumulative tables: {e}",
            exc_info=True,
        )
        sys.exit(1)

def update_cumulative_tables(spark: SparkSession) -> None:
    """
    Example function to update the cumulative TRADITIONAL tables
    with the latest data from NBA_player_stats.traditional_player_stats.
    Adjust or remove as needed.
    """
    try:
        # Example of how you might define "current_season"
        current_season_row = spark.sql(
            """
            SELECT MAX(CAST(SUBSTRING(season, 1, 4) AS INT)) AS current_season
            FROM NBA_player_stats.traditional_player_stats
            """
        ).collect()[0]

        current_season = current_season_row["current_season"]
        if current_season is None:
            logger.warning("No current season found. Skipping cumulative tables update.")
            return

        #
        # ========== CURRENT SEASON TOTALS ==========
        #
        current_season_totals = spark.sql(
            f"""
            SELECT
                season AS season,
                player_name,
                SUM(points) AS total_points,
                SUM(field_goals_made) AS total_field_goals_made,
                SUM(field_goals_attempted) AS total_field_goals_attempted,
                SUM(three_pointers_made) AS total_three_pointers_made,
                SUM(three_pointers_attempted) AS total_three_pointers_attempted,
                SUM(free_throws_made) AS total_free_throws_made,
                SUM(free_throws_attempted) AS total_free_throws_attempted,
                SUM(offensive_rebounds) AS total_offensive_rebounds,
                SUM(defensive_rebounds) AS total_defensive_rebounds,
                SUM(total_rebounds) AS total_rebounds,
                SUM(assists) AS total_assists,
                SUM(steals) AS total_steals,
                SUM(blocks) AS total_blocks,
                SUM(turnovers) AS total_turnovers
            FROM NBA_player_stats.traditional_player_stats
            WHERE CAST(SUBSTRING(season,1,4) AS INT) = {current_season}
            GROUP BY season, player_name
            """
        )
        current_season_totals.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_player_stats.current_season_player_traditional_totals"
        )

        #
        # ========== CURRENT SEASON AVERAGES ==========
        #
        current_season_averages = spark.sql(
            f"""
            SELECT
                season AS season,
                player_name,
                AVG(field_goal_percentage) AS avg_field_goal_percentage,
                AVG(three_point_percentage) AS avg_three_point_percentage,
                AVG(free_throw_percentage) AS avg_free_throw_percentage,
                AVG(plus_minus) AS avg_plus_minus,
                AVG(minutes_played) AS avg_minutes_played
            FROM NBA_player_stats.traditional_player_stats
            WHERE CAST(SUBSTRING(season,1,4) AS INT) = {current_season}
            GROUP BY season, player_name
            """
        )
        current_season_averages.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_player_stats.current_season_player_traditional_averages"
        )

        #
        # ========== ALL SEASON TOTALS ==========
        #
        all_season_totals = spark.sql(
            """
            SELECT
                player_name,
                SUM(points) AS total_points,
                SUM(field_goals_made) AS total_field_goals_made,
                SUM(field_goals_attempted) AS total_field_goals_attempted,
                SUM(three_pointers_made) AS total_three_pointers_made,
                SUM(three_pointers_attempted) AS total_three_pointers_attempted,
                SUM(free_throws_made) AS total_free_throws_made,
                SUM(free_throws_attempted) AS total_free_throws_attempted,
                SUM(offensive_rebounds) AS total_offensive_rebounds,
                SUM(defensive_rebounds) AS total_defensive_rebounds,
                SUM(total_rebounds) AS total_rebounds,
                SUM(assists) AS total_assists,
                SUM(steals) AS total_steals,
                SUM(blocks) AS total_blocks,
                SUM(turnovers) AS total_turnovers
            FROM NBA_player_stats.traditional_player_stats
            GROUP BY player_name
            """
        )
        all_season_totals.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_player_stats.all_season_player_traditional_totals"
        )

        #
        # ========== ALL SEASON AVERAGES ==========
        #
        all_season_averages = spark.sql(
            """
            SELECT
                player_name,
                AVG(field_goal_percentage) AS avg_field_goal_percentage,
                AVG(three_point_percentage) AS avg_three_point_percentage,
                AVG(free_throw_percentage) AS avg_free_throw_percentage,
                AVG(plus_minus) AS avg_plus_minus,
                AVG(minutes_played) AS avg_minutes_played
            FROM NBA_player_stats.traditional_player_stats
            GROUP BY player_name
            """
        )
        all_season_averages.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_player_stats.all_season_player_traditional_averages"
        )

        logger.info("Cumulative TRADITIONAL tables updated successfully.")
    except Exception as e:
        logger.error(f"Error updating cumulative tables: {e}", exc_info=True)
        sys.exit(1)

def read_kafka_batch_and_process(spark: SparkSession) -> None:
    """
    Reads data from Kafka in a single batch (using startingOffsets=earliest, endingOffsets=latest),
    then merges into NBA_player_stats.traditional_player_stats + writes aggregated results to fact_traditional_player_stats.
    """
    try:
        #
        # 1) Define schema matching the *original* Kafka JSON fields for TRADITIONAL stats.
        #    The example from the user:
        #
        #    {
        #       "PLAYER_ID": 947,
        #       "PLAYER_NAME": "Allen Iverson",
        #       "NICKNAME": "Allen",
        #       "TEAM_ID": 1610612755,
        #       "TEAM_ABBREVIATION": "PHI",
        #       "AGE": 22.0,
        #       "GP": 12,
        #       "W": 6,
        #       "L": 6,
        #       "W_PCT": 0.5,
        #       "MIN": 37.2,
        #       "FGM": 7.3,
        #       "FGA": 17.6,
        #       "FG_PCT": 0.417,
        #       "FG3M": 2.2,
        #       "FG3A": 5.5,
        #       "FG3_PCT": 0.394,
        #       "FTM": 4.9,
        #       "FTA": 7.9,
        #       "FT_PCT": 0.621,
        #       "OREB": 1.9,
        #       "DREB": 3.3,
        #       "REB": 5.3,
        #       "AST": 6.4,
        #       "TOV": 5.3,
        #       "STL": 2.7,
        #       "BLK": 0.5,
        #       "BLKA": 1.3,
        #       "PF": 2.6,
        #       "PFD": 0.0,
        #       "PTS": 21.8,
        #       "PLUS_MINUS": -1.3,
        #       "NBA_FANTASY_PTS": 41.8,
        #       "DD2": 2,
        #       "TD3": 0,
        #       "WNBA_FANTASY_PTS": 41.9,
        #       "GP_RANK": 213,
        #       "W_RANK": 155,
        #       ...
        #       "Season": "1996-97",
        #       "SeasonType": "Regular%20Season",
        #       "Month": 2,
        #       "PerMode": "PerGame"
        #    }
        #
        schema = StructType([
            StructField("PLAYER_ID", IntegerType(), True),
            StructField("PLAYER_NAME", StringType(), True),
            StructField("NICKNAME", StringType(), True),
            StructField("TEAM_ID", IntegerType(), True),
            StructField("TEAM_ABBREVIATION", StringType(), True),
            StructField("AGE", DoubleType(), True),
            StructField("GP", IntegerType(), True),
            StructField("W", IntegerType(), True),
            StructField("L", IntegerType(), True),
            StructField("W_PCT", DoubleType(), True),
            StructField("MIN", DoubleType(), True),
            StructField("FGM", DoubleType(), True),
            StructField("FGA", DoubleType(), True),
            StructField("FG_PCT", DoubleType(), True),
            StructField("FG3M", DoubleType(), True),
            StructField("FG3A", DoubleType(), True),
            StructField("FG3_PCT", DoubleType(), True),
            StructField("FTM", DoubleType(), True),
            StructField("FTA", DoubleType(), True),
            StructField("FT_PCT", DoubleType(), True),
            StructField("OREB", DoubleType(), True),
            StructField("DREB", DoubleType(), True),
            StructField("REB", DoubleType(), True),
            StructField("AST", DoubleType(), True),
            StructField("TOV", DoubleType(), True),
            StructField("STL", DoubleType(), True),
            StructField("BLK", DoubleType(), True),
            StructField("BLKA", DoubleType(), True),
            StructField("PF", DoubleType(), True),
            StructField("PFD", DoubleType(), True),
            StructField("PTS", DoubleType(), True),
            StructField("PLUS_MINUS", DoubleType(), True),
            StructField("NBA_FANTASY_PTS", DoubleType(), True),
            StructField("DD2", IntegerType(), True),
            StructField("TD3", IntegerType(), True),
            StructField("WNBA_FANTASY_PTS", DoubleType(), True),
            StructField("GP_RANK", IntegerType(), True),
            StructField("W_RANK", IntegerType(), True),
            StructField("L_RANK", IntegerType(), True),
            StructField("W_PCT_RANK", IntegerType(), True),
            StructField("MIN_RANK", IntegerType(), True),
            StructField("FGM_RANK", IntegerType(), True),
            StructField("FGA_RANK", IntegerType(), True),
            StructField("FG_PCT_RANK", IntegerType(), True),
            StructField("FG3M_RANK", IntegerType(), True),
            StructField("FG3A_RANK", IntegerType(), True),
            StructField("FG3_PCT_RANK", IntegerType(), True),
            StructField("FTM_RANK", IntegerType(), True),
            StructField("FTA_RANK", IntegerType(), True),
            StructField("FT_PCT_RANK", IntegerType(), True),
            StructField("OREB_RANK", IntegerType(), True),
            StructField("DREB_RANK", IntegerType(), True),
            StructField("REB_RANK", IntegerType(), True),
            StructField("AST_RANK", IntegerType(), True),
            StructField("TOV_RANK", IntegerType(), True),
            StructField("STL_RANK", IntegerType(), True),
            StructField("BLK_RANK", IntegerType(), True),
            StructField("BLKA_RANK", IntegerType(), True),
            StructField("PF_RANK", IntegerType(), True),
            StructField("PFD_RANK", IntegerType(), True),
            StructField("PTS_RANK", IntegerType(), True),
            StructField("PLUS_MINUS_RANK", IntegerType(), True),
            StructField("NBA_FANTASY_PTS_RANK", IntegerType(), True),
            StructField("DD2_RANK", IntegerType(), True),
            StructField("TD3_RANK", IntegerType(), True),
            StructField("WNBA_FANTASY_PTS_RANK", IntegerType(), True),
            StructField("Season", StringType(), True),
            StructField("SeasonType", StringType(), True),
            StructField("Month", IntegerType(), True),
            StructField("PerMode", StringType(), True),
        ])

        #
        # 2) Do a single batch read from Kafka, subscribing to NBA_traditional_player_stats
        #
        kafka_batch_df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", ",".join(BOOTSTRAP_SERVERS))
            .option("subscribe", "NBA_traditional_player_stats")
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        #
        # 3) Parse the JSON messages
        #
        parsed_df = (
            kafka_batch_df
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*")
        )

        #
        # 4) Rename columns from uppercase/abbrev to fully spelled-out lowercase
        #
        renamed_df = (
            parsed_df
            .withColumnRenamed("PLAYER_ID", "player_id")
            .withColumnRenamed("PLAYER_NAME", "player_name")
            .withColumnRenamed("NICKNAME", "nickname")
            .withColumnRenamed("TEAM_ID", "team_id")
            .withColumnRenamed("TEAM_ABBREVIATION", "team_abbreviation")
            .withColumnRenamed("AGE", "age")
            .withColumnRenamed("GP", "games_played")
            .withColumnRenamed("W", "wins")
            .withColumnRenamed("L", "losses")
            .withColumnRenamed("W_PCT", "win_percentage")
            .withColumnRenamed("MIN", "minutes_played")
            .withColumnRenamed("FGM", "field_goals_made")
            .withColumnRenamed("FGA", "field_goals_attempted")
            .withColumnRenamed("FG_PCT", "field_goal_percentage")
            .withColumnRenamed("FG3M", "three_pointers_made")
            .withColumnRenamed("FG3A", "three_pointers_attempted")
            .withColumnRenamed("FG3_PCT", "three_point_percentage")
            .withColumnRenamed("FTM", "free_throws_made")
            .withColumnRenamed("FTA", "free_throws_attempted")
            .withColumnRenamed("FT_PCT", "free_throw_percentage")
            .withColumnRenamed("OREB", "offensive_rebounds")
            .withColumnRenamed("DREB", "defensive_rebounds")
            .withColumnRenamed("REB", "total_rebounds")
            .withColumnRenamed("AST", "assists")
            .withColumnRenamed("TOV", "turnovers")
            .withColumnRenamed("STL", "steals")
            .withColumnRenamed("BLK", "blocks")
            .withColumnRenamed("BLKA", "blocked_attempts")
            .withColumnRenamed("PF", "personal_fouls")
            .withColumnRenamed("PFD", "fouls_drawn")
            .withColumnRenamed("PTS", "points")
            .withColumnRenamed("PLUS_MINUS", "plus_minus")
            .withColumnRenamed("NBA_FANTASY_PTS", "nba_fantasy_points")
            .withColumnRenamed("DD2", "double_doubles")
            .withColumnRenamed("TD3", "triple_doubles")
            .withColumnRenamed("WNBA_FANTASY_PTS", "wnba_fantasy_points")
            .withColumnRenamed("GP_RANK", "games_played_rank")
            .withColumnRenamed("W_RANK", "wins_rank")
            .withColumnRenamed("L_RANK", "losses_rank")
            .withColumnRenamed("W_PCT_RANK", "win_percentage_rank")
            .withColumnRenamed("MIN_RANK", "minutes_played_rank")
            .withColumnRenamed("FGM_RANK", "field_goals_made_rank")
            .withColumnRenamed("FGA_RANK", "field_goals_attempted_rank")
            .withColumnRenamed("FG_PCT_RANK", "field_goal_percentage_rank")
            .withColumnRenamed("FG3M_RANK", "three_pointers_made_rank")
            .withColumnRenamed("FG3A_RANK", "three_pointers_attempted_rank")
            .withColumnRenamed("FG3_PCT_RANK", "three_point_percentage_rank")
            .withColumnRenamed("FTM_RANK", "free_throws_made_rank")
            .withColumnRenamed("FTA_RANK", "free_throws_attempted_rank")
            .withColumnRenamed("FT_PCT_RANK", "free_throw_percentage_rank")
            .withColumnRenamed("OREB_RANK", "offensive_rebounds_rank")
            .withColumnRenamed("DREB_RANK", "defensive_rebounds_rank")
            .withColumnRenamed("REB_RANK", "total_rebounds_rank")
            .withColumnRenamed("AST_RANK", "assists_rank")
            .withColumnRenamed("TOV_RANK", "turnovers_rank")
            .withColumnRenamed("STL_RANK", "steals_rank")
            .withColumnRenamed("BLK_RANK", "blocks_rank")
            .withColumnRenamed("BLKA_RANK", "blocked_attempts_rank")
            .withColumnRenamed("PF_RANK", "personal_fouls_rank")
            .withColumnRenamed("PFD_RANK", "fouls_drawn_rank")
            .withColumnRenamed("PTS_RANK", "points_rank")
            .withColumnRenamed("PLUS_MINUS_RANK", "plus_minus_rank")
            .withColumnRenamed("NBA_FANTASY_PTS_RANK", "nba_fantasy_points_rank")
            .withColumnRenamed("DD2_RANK", "double_doubles_rank")
            .withColumnRenamed("TD3_RANK", "triple_doubles_rank")
            .withColumnRenamed("WNBA_FANTASY_PTS_RANK", "wnba_fantasy_points_rank")
            .withColumnRenamed("Season", "season")
            .withColumnRenamed("SeasonType", "season_type")
            .withColumnRenamed("Month", "month")
            .withColumnRenamed("PerMode", "per_mode")
        )

        #
        # 5) Repartition to reduce shuffle overhead
        #
        repartitioned_df = renamed_df.repartition(25)

        #
        # (Optional) Example join with dim_players
        #
        dim_players_df = spark.read.table("NBA_player_stats.dim_players")  # hypothetical dimension
        joined_df = repartitioned_df.join(dim_players_df.hint("merge"), on="player_id", how="left")

        # Keep only the newly standardized columns from the Kafka data
        final_df = joined_df.select(repartitioned_df["*"])

        #
        # 6) Run SCD merges + aggregator in batch
        #
        scd_and_aggregate_batch(final_df, spark)

        logger.info("Batch read from Kafka (NBA_traditional_player_stats) + SCD merges + aggregator completed.")

        # Example: You can optionally invoke Iceberg maintenance here:
        # spark.sql("CALL spark_catalog.system.rewrite_data_files(table => 'NBA_player_stats.traditional_player_stats')")

    except Exception as e:
        logger.error(f"Error in batch processing from Kafka: {e}", exc_info=True)
        sys.exit(1)

def main() -> None:
    """
    Main function to execute the pipeline in batch mode for NBA TRADITIONAL Player Stats.
    """
    try:
        spark = create_spark_connection()
        create_tables(spark)
        create_cumulative_tables(spark)

        # Single run of reading from Kafka + merging + writing aggregator
        read_kafka_batch_and_process(spark)

        # Example: call update_cumulative_tables(spark) once data accumulates
        update_cumulative_tables(spark)

        spark.stop()
        logger.info("Batch pipeline executed successfully for TRADITIONAL player stats.")
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
