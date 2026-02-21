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

# --------------------------------------------------------------------------------
# Configure logging
# --------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------------
# Kafka bootstrap servers
# --------------------------------------------------------------------------------
BOOTSTRAP_SERVERS: List[str] = [
    "172.16.10.2:9092",
    "172.16.10.3:9093",
    "172.16.10.4:9094",
]

# --------------------------------------------------------------------------------
# Spark Connection
# --------------------------------------------------------------------------------
def create_spark_connection() -> SparkSession:
    """
    Creates and returns a SparkSession with the necessary configurations.
    Note we are removing streaming triggers and focusing on batch usage.
    """
    try:
        spark = (
            SparkSession.builder.appName("NBA_Violations_Stats_Batch")
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

        # Create separate databases for player vs team
        spark.sql("CREATE DATABASE IF NOT EXISTS NBA_player_stats")
        spark.sql("CREATE DATABASE IF NOT EXISTS NBA_team_stats")
        logger.info("Spark connection (batch mode) created successfully.")

        # Example: setting shuffle partitions
        spark.conf.set("spark.sql.shuffle.partitions", 25)

        # Example: disabling adaptive execution for consistent partitioning
        spark.conf.set("spark.sql.adaptive.enabled", "false")

        return spark
    except Exception as e:
        logger.error(f"Error creating Spark connection: {e}", exc_info=True)
        sys.exit(1)

# --------------------------------------------------------------------------------
# CREATE TABLES (Team + Player Violations)
# including dimension, main, and fact tables
# --------------------------------------------------------------------------------
def create_team_violations_tables(spark: SparkSession) -> None:
    """
    Creates necessary Iceberg tables for TEAM violations in the Hive metastore 
    using fully spelled-out, lowercase, underscore-based column names.
    """
    try:
        # Drop old tables if they exist
        for table_name in [
            "NBA_team_stats.team_violations_stats",
            "NBA_team_stats.fact_team_violations_stats",
            "NBA_team_stats.dim_teams",
        ]:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

        # Main table: team_violations_stats
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_stats.team_violations_stats (
                team_id INT,
                team_name STRING,
                games_played INT,
                wins INT,
                losses INT,
                win_percentage DOUBLE,

                traveling_violations DOUBLE,
                double_dribble_violations DOUBLE,
                discontinued_dribble_violations DOUBLE,
                offensive_three_seconds_violations DOUBLE,
                five_seconds_violations DOUBLE,
                eight_seconds_violations DOUBLE,
                shot_clock_violations DOUBLE,
                inbound_violations DOUBLE,
                backcourt_violations DOUBLE,
                offensive_goaltending_violations DOUBLE,
                palming_violations DOUBLE,
                offensive_foul_violations DOUBLE,
                defensive_three_seconds_violations DOUBLE,
                charging_violations DOUBLE,
                delay_of_game_violations DOUBLE,
                defensive_goaltending_violations DOUBLE,
                lane_violations DOUBLE,
                jump_ball_violations DOUBLE,
                kicked_ball_violations DOUBLE,

                games_played_rank INT,
                wins_rank INT,
                losses_rank INT,
                win_percentage_rank INT,
                traveling_violations_rank INT,
                double_dribble_violations_rank INT,
                discontinued_dribble_violations_rank INT,
                offensive_three_seconds_violations_rank INT,
                five_seconds_violations_rank INT,
                eight_seconds_violations_rank INT,
                shot_clock_violations_rank INT,
                inbound_violations_rank INT,
                backcourt_violations_rank INT,
                offensive_goaltending_violations_rank INT,
                palming_violations_rank INT,
                offensive_foul_violations_rank INT,
                defensive_three_seconds_violations_rank INT,
                charging_violations_rank INT,
                delay_of_game_violations_rank INT,
                defensive_goaltending_violations_rank INT,
                lane_violations_rank INT,
                jump_ball_violations_rank INT,
                kicked_ball_violations_rank INT,

                season STRING,
                season_type STRING,
                month INT,
                per_mode STRING,
                last_n_games INT,

                record_start_date DATE,
                record_end_date DATE,
                is_current BOOLEAN,
                hash_val STRING
            )
            USING ICEBERG
            PARTITIONED BY (season, month)
            """
        )

        # Dimension table: dim_teams
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_stats.dim_teams (
                team_id INT,
                team_name STRING,
                record_start_date DATE,
                record_end_date DATE,
                is_current BOOLEAN,
                hash_val STRING
            )
            USING ICEBERG
            PARTITIONED BY (team_id)
            """
        )

        # Fact table: fact_team_violations_stats
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_stats.fact_team_violations_stats (
                team_id INT,
                season STRING,
                month INT,
                per_mode STRING,

                total_traveling_violations DOUBLE,
                total_double_dribble_violations DOUBLE,
                total_discontinued_dribble_violations DOUBLE,
                total_offensive_three_seconds_violations DOUBLE,
                total_five_seconds_violations DOUBLE,
                total_eight_seconds_violations DOUBLE,
                total_shot_clock_violations DOUBLE,
                total_inbound_violations DOUBLE,
                total_backcourt_violations DOUBLE,
                total_offensive_goaltending_violations DOUBLE,
                total_palming_violations DOUBLE,
                total_offensive_foul_violations DOUBLE,
                total_defensive_three_seconds_violations DOUBLE,
                total_charging_violations DOUBLE,
                total_delay_of_game_violations DOUBLE,
                total_defensive_goaltending_violations DOUBLE,
                total_lane_violations DOUBLE,
                total_jump_ball_violations DOUBLE,
                total_kicked_ball_violations DOUBLE,

                avg_traveling_violations DOUBLE,
                avg_double_dribble_violations DOUBLE,
                avg_discontinued_dribble_violations DOUBLE,
                avg_offensive_three_seconds_violations DOUBLE,
                avg_five_seconds_violations DOUBLE,
                avg_eight_seconds_violations DOUBLE,
                avg_shot_clock_violations DOUBLE,
                avg_inbound_violations DOUBLE,
                avg_backcourt_violations DOUBLE,
                avg_offensive_goaltending_violations DOUBLE,
                avg_palming_violations DOUBLE,
                avg_offensive_foul_violations DOUBLE,
                avg_defensive_three_seconds_violations DOUBLE,
                avg_charging_violations DOUBLE,
                avg_delay_of_game_violations DOUBLE,
                avg_defensive_goaltending_violations DOUBLE,
                avg_lane_violations DOUBLE,
                avg_jump_ball_violations DOUBLE,
                avg_kicked_ball_violations DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season, team_id)
            """
        )

        logger.info("Team violations tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating team violations tables: {e}", exc_info=True)
        sys.exit(1)

def create_player_violations_tables(spark: SparkSession) -> None:
    """
    Creates necessary Iceberg tables for PLAYER violations in the Hive metastore 
    using fully spelled-out, lowercase, underscore-based column names.
    """
    try:
        # Drop old tables if they exist
        for table_name in [
            "NBA_player_stats.player_violations_stats",
            "NBA_player_stats.fact_player_violations_stats",
            "NBA_player_stats.dim_players",
        ]:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

        # Main table: player_violations_stats
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.player_violations_stats (
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

                traveling_violations DOUBLE,
                double_dribble_violations DOUBLE,
                discontinued_dribble_violations DOUBLE,
                offensive_three_seconds_violations DOUBLE,
                inbound_violations DOUBLE,
                backcourt_violations DOUBLE,
                offensive_goaltending_violations DOUBLE,
                palming_violations DOUBLE,
                offensive_foul_violations DOUBLE,
                defensive_three_seconds_violations DOUBLE,
                charging_violations DOUBLE,
                delay_of_game_violations DOUBLE,
                defensive_goaltending_violations DOUBLE,
                lane_violations DOUBLE,
                jump_ball_violations DOUBLE,
                kicked_ball_violations DOUBLE,

                games_played_rank INT,
                wins_rank INT,
                losses_rank INT,
                win_percentage_rank INT,
                traveling_violations_rank INT,
                double_dribble_violations_rank INT,
                discontinued_dribble_violations_rank INT,
                offensive_three_seconds_violations_rank INT,
                inbound_violations_rank INT,
                backcourt_violations_rank INT,
                offensive_goaltending_violations_rank INT,
                palming_violations_rank INT,
                offensive_foul_violations_rank INT,
                defensive_three_seconds_violations_rank INT,
                charging_violations_rank INT,
                delay_of_game_violations_rank INT,
                defensive_goaltending_violations_rank INT,
                lane_violations_rank INT,
                jump_ball_violations_rank INT,
                kicked_ball_violations_rank INT,

                season STRING,
                season_type STRING,
                month INT,
                per_mode STRING,
                last_n_games INT,

                record_start_date DATE,
                record_end_date DATE,
                is_current BOOLEAN,
                hash_val STRING
            )
            USING ICEBERG
            PARTITIONED BY (season, month)
            """
        )

        # Dimension table: dim_players
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
                is_current BOOLEAN,
                hash_val STRING
            )
            USING ICEBERG
            PARTITIONED BY (player_id)
            """
        )

        # Fact table: fact_player_violations_stats
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.fact_player_violations_stats (
                player_id INT,
                season STRING,
                month INT,
                per_mode STRING,

                total_traveling_violations DOUBLE,
                total_double_dribble_violations DOUBLE,
                total_discontinued_dribble_violations DOUBLE,
                total_offensive_three_seconds_violations DOUBLE,
                total_inbound_violations DOUBLE,
                total_backcourt_violations DOUBLE,
                total_offensive_goaltending_violations DOUBLE,
                total_palming_violations DOUBLE,
                total_offensive_foul_violations DOUBLE,
                total_defensive_three_seconds_violations DOUBLE,
                total_charging_violations DOUBLE,
                total_delay_of_game_violations DOUBLE,
                total_defensive_goaltending_violations DOUBLE,
                total_lane_violations DOUBLE,
                total_jump_ball_violations DOUBLE,
                total_kicked_ball_violations DOUBLE,

                avg_traveling_violations DOUBLE,
                avg_double_dribble_violations DOUBLE,
                avg_discontinued_dribble_violations DOUBLE,
                avg_offensive_three_seconds_violations DOUBLE,
                avg_inbound_violations DOUBLE,
                avg_backcourt_violations DOUBLE,
                avg_offensive_goaltending_violations DOUBLE,
                avg_palming_violations DOUBLE,
                avg_offensive_foul_violations DOUBLE,
                avg_defensive_three_seconds_violations DOUBLE,
                avg_charging_violations DOUBLE,
                avg_delay_of_game_violations DOUBLE,
                avg_defensive_goaltending_violations DOUBLE,
                avg_lane_violations DOUBLE,
                avg_jump_ball_violations DOUBLE,
                avg_kicked_ball_violations DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season, player_id)
            """
        )

        logger.info("Player violations tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating player violations tables: {e}", exc_info=True)
        sys.exit(1)

# --------------------------------------------------------------------------------
# SCD Type 2 Handling
# --------------------------------------------------------------------------------
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

        # 2) Build a single hash to detect changes across compare_columns
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

# --------------------------------------------------------------------------------
# TEAM VIOLATIONS: SCD + Aggregation
# --------------------------------------------------------------------------------
def scd_and_aggregate_team_violations(df: DataFrame, spark: SparkSession) -> None:
    """
    Performs SCD Type 2 merges into NBA_team_stats.team_violations_stats,
    then aggregates & writes to NBA_team_stats.fact_team_violations_stats in normal batch mode.
    """
    record_count = df.count()
    logger.info(f"[Batch-Job:TEAM] Dataset has {record_count} team-violation records to process.")

    if record_count == 0:
        logger.info("[Batch-Job:TEAM] No records to merge or aggregate for Team Violations.")
        return

    # 1) Handle SCD merges into NBA_team_stats.team_violations_stats
    handle_scd_type_2(
        df=df,
        spark=spark,
        table_name="NBA_team_stats.team_violations_stats",
        join_columns=["team_id", "season", "per_mode", "month", "last_n_games"],
        compare_columns=[
            "team_name",
            "games_played",
            "wins",
            "losses",
            "win_percentage",

            "traveling_violations",
            "double_dribble_violations",
            "discontinued_dribble_violations",
            "offensive_three_seconds_violations",
            "five_seconds_violations",
            "eight_seconds_violations",
            "shot_clock_violations",
            "inbound_violations",
            "backcourt_violations",
            "offensive_goaltending_violations",
            "palming_violations",
            "offensive_foul_violations",
            "defensive_three_seconds_violations",
            "charging_violations",
            "delay_of_game_violations",
            "defensive_goaltending_violations",
            "lane_violations",
            "jump_ball_violations",
            "kicked_ball_violations",

            "games_played_rank",
            "wins_rank",
            "losses_rank",
            "win_percentage_rank",
            "traveling_violations_rank",
            "double_dribble_violations_rank",
            "discontinued_dribble_violations_rank",
            "offensive_three_seconds_violations_rank",
            "five_seconds_violations_rank",
            "eight_seconds_violations_rank",
            "shot_clock_violations_rank",
            "inbound_violations_rank",
            "backcourt_violations_rank",
            "offensive_goaltending_violations_rank",
            "palming_violations_rank",
            "offensive_foul_violations_rank",
            "defensive_three_seconds_violations_rank",
            "charging_violations_rank",
            "delay_of_game_violations_rank",
            "defensive_goaltending_violations_rank",
            "lane_violations_rank",
            "jump_ball_violations_rank",
            "kicked_ball_violations_rank",
        ],
    )

    # 2) Aggregate (sum and avg) to store in fact_team_violations_stats
    fact_aggregated = (
        df.groupBy("team_id", "season", "month", "per_mode")
        .agg(
            spark_sum("traveling_violations").alias("total_traveling_violations"),
            spark_sum("double_dribble_violations").alias("total_double_dribble_violations"),
            spark_sum("discontinued_dribble_violations").alias("total_discontinued_dribble_violations"),
            spark_sum("offensive_three_seconds_violations").alias("total_offensive_three_seconds_violations"),
            spark_sum("five_seconds_violations").alias("total_five_seconds_violations"),
            spark_sum("eight_seconds_violations").alias("total_eight_seconds_violations"),
            spark_sum("shot_clock_violations").alias("total_shot_clock_violations"),
            spark_sum("inbound_violations").alias("total_inbound_violations"),
            spark_sum("backcourt_violations").alias("total_backcourt_violations"),
            spark_sum("offensive_goaltending_violations").alias("total_offensive_goaltending_violations"),
            spark_sum("palming_violations").alias("total_palming_violations"),
            spark_sum("offensive_foul_violations").alias("total_offensive_foul_violations"),
            spark_sum("defensive_three_seconds_violations").alias("total_defensive_three_seconds_violations"),
            spark_sum("charging_violations").alias("total_charging_violations"),
            spark_sum("delay_of_game_violations").alias("total_delay_of_game_violations"),
            spark_sum("defensive_goaltending_violations").alias("total_defensive_goaltending_violations"),
            spark_sum("lane_violations").alias("total_lane_violations"),
            spark_sum("jump_ball_violations").alias("total_jump_ball_violations"),
            spark_sum("kicked_ball_violations").alias("total_kicked_ball_violations"),

            avg("traveling_violations").alias("avg_traveling_violations"),
            avg("double_dribble_violations").alias("avg_double_dribble_violations"),
            avg("discontinued_dribble_violations").alias("avg_discontinued_dribble_violations"),
            avg("offensive_three_seconds_violations").alias("avg_offensive_three_seconds_violations"),
            avg("five_seconds_violations").alias("avg_five_seconds_violations"),
            avg("eight_seconds_violations").alias("avg_eight_seconds_violations"),
            avg("shot_clock_violations").alias("avg_shot_clock_violations"),
            avg("inbound_violations").alias("avg_inbound_violations"),
            avg("backcourt_violations").alias("avg_backcourt_violations"),
            avg("offensive_goaltending_violations").alias("avg_offensive_goaltending_violations"),
            avg("palming_violations").alias("avg_palming_violations"),
            avg("offensive_foul_violations").alias("avg_offensive_foul_violations"),
            avg("defensive_three_seconds_violations").alias("avg_defensive_three_seconds_violations"),
            avg("charging_violations").alias("avg_charging_violations"),
            avg("delay_of_game_violations").alias("avg_delay_of_game_violations"),
            avg("defensive_goaltending_violations").alias("avg_defensive_goaltending_violations"),
            avg("lane_violations").alias("avg_lane_violations"),
            avg("jump_ball_violations").alias("avg_jump_ball_violations"),
            avg("kicked_ball_violations").alias("avg_kicked_ball_violations"),
        )
    )

    fact_aggregated.write.format("iceberg") \
        .mode("append") \
        .save("spark_catalog.NBA_team_stats.fact_team_violations_stats")

    logger.info("[Batch-Job:TEAM] Wrote aggregated data to fact_team_violations_stats.")

# --------------------------------------------------------------------------------
# PLAYER VIOLATIONS: SCD + Aggregation
# --------------------------------------------------------------------------------
def scd_and_aggregate_player_violations(df: DataFrame, spark: SparkSession) -> None:
    """
    Performs SCD Type 2 merges into NBA_player_stats.player_violations_stats,
    then aggregates & writes to NBA_player_stats.fact_player_violations_stats in normal batch mode.
    """
    record_count = df.count()
    logger.info(f"[Batch-Job:PLAYER] Dataset has {record_count} player-violation records to process.")

    if record_count == 0:
        logger.info("[Batch-Job:PLAYER] No records to merge or aggregate for Player Violations.")
        return

    # 1) Handle SCD merges into NBA_player_stats.player_violations_stats
    handle_scd_type_2(
        df=df,
        spark=spark,
        table_name="NBA_player_stats.player_violations_stats",
        join_columns=["player_id", "season", "per_mode", "month", "last_n_games"],
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

            "traveling_violations",
            "double_dribble_violations",
            "discontinued_dribble_violations",
            "offensive_three_seconds_violations",
            "inbound_violations",
            "backcourt_violations",
            "offensive_goaltending_violations",
            "palming_violations",
            "offensive_foul_violations",
            "defensive_three_seconds_violations",
            "charging_violations",
            "delay_of_game_violations",
            "defensive_goaltending_violations",
            "lane_violations",
            "jump_ball_violations",
            "kicked_ball_violations",

            "games_played_rank",
            "wins_rank",
            "losses_rank",
            "win_percentage_rank",
            "traveling_violations_rank",
            "double_dribble_violations_rank",
            "discontinued_dribble_violations_rank",
            "offensive_three_seconds_violations_rank",
            "inbound_violations_rank",
            "backcourt_violations_rank",
            "offensive_goaltending_violations_rank",
            "palming_violations_rank",
            "offensive_foul_violations_rank",
            "defensive_three_seconds_violations_rank",
            "charging_violations_rank",
            "delay_of_game_violations_rank",
            "defensive_goaltending_violations_rank",
            "lane_violations_rank",
            "jump_ball_violations_rank",
            "kicked_ball_violations_rank",
        ],
    )

    # 2) Aggregate (sum and avg) to store in fact_player_violations_stats
    fact_aggregated = (
        df.groupBy("player_id", "season", "month", "per_mode")
        .agg(
            spark_sum("traveling_violations").alias("total_traveling_violations"),
            spark_sum("double_dribble_violations").alias("total_double_dribble_violations"),
            spark_sum("discontinued_dribble_violations").alias("total_discontinued_dribble_violations"),
            spark_sum("offensive_three_seconds_violations").alias("total_offensive_three_seconds_violations"),
            spark_sum("inbound_violations").alias("total_inbound_violations"),
            spark_sum("backcourt_violations").alias("total_backcourt_violations"),
            spark_sum("offensive_goaltending_violations").alias("total_offensive_goaltending_violations"),
            spark_sum("palming_violations").alias("total_palming_violations"),
            spark_sum("offensive_foul_violations").alias("total_offensive_foul_violations"),
            spark_sum("defensive_three_seconds_violations").alias("total_defensive_three_seconds_violations"),
            spark_sum("charging_violations").alias("total_charging_violations"),
            spark_sum("delay_of_game_violations").alias("total_delay_of_game_violations"),
            spark_sum("defensive_goaltending_violations").alias("total_defensive_goaltending_violations"),
            spark_sum("lane_violations").alias("total_lane_violations"),
            spark_sum("jump_ball_violations").alias("total_jump_ball_violations"),
            spark_sum("kicked_ball_violations").alias("total_kicked_ball_violations"),

            avg("traveling_violations").alias("avg_traveling_violations"),
            avg("double_dribble_violations").alias("avg_double_dribble_violations"),
            avg("discontinued_dribble_violations").alias("avg_discontinued_dribble_violations"),
            avg("offensive_three_seconds_violations").alias("avg_offensive_three_seconds_violations"),
            avg("inbound_violations").alias("avg_inbound_violations"),
            avg("backcourt_violations").alias("avg_backcourt_violations"),
            avg("offensive_goaltending_violations").alias("avg_offensive_goaltending_violations"),
            avg("palming_violations").alias("avg_palming_violations"),
            avg("offensive_foul_violations").alias("avg_offensive_foul_violations"),
            avg("defensive_three_seconds_violations").alias("avg_defensive_three_seconds_violations"),
            avg("charging_violations").alias("avg_charging_violations"),
            avg("delay_of_game_violations").alias("avg_delay_of_game_violations"),
            avg("defensive_goaltending_violations").alias("avg_defensive_goaltending_violations"),
            avg("lane_violations").alias("avg_lane_violations"),
            avg("jump_ball_violations").alias("avg_jump_ball_violations"),
            avg("kicked_ball_violations").alias("avg_kicked_ball_violations"),
        )
    )

    fact_aggregated.write.format("iceberg") \
        .mode("append") \
        .save("spark_catalog.NBA_player_stats.fact_player_violations_stats")

    logger.info("[Batch-Job:PLAYER] Wrote aggregated data to fact_player_violations_stats.")

# --------------------------------------------------------------------------------
# CREATE CUMULATIVE TABLES (TEAM + PLAYER)
# --------------------------------------------------------------------------------
def create_cumulative_team_tables(spark: SparkSession) -> None:
    """
    Creates additional cumulative Iceberg tables for TEAM violations 
    using the new standardized column names.
    """
    try:
        # Drop if needed
        spark.sql("DROP TABLE IF EXISTS NBA_team_stats.current_season_team_violations_totals")
        spark.sql("DROP TABLE IF EXISTS NBA_team_stats.current_season_team_violations_averages")
        spark.sql("DROP TABLE IF EXISTS NBA_team_stats.all_season_team_violations_totals")
        spark.sql("DROP TABLE IF EXISTS NBA_team_stats.all_season_team_violations_averages")

        # Current Season Totals
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_stats.current_season_team_violations_totals (
                season STRING,
                team_id INT,
                total_traveling_violations DOUBLE,
                total_double_dribble_violations DOUBLE,
                total_discontinued_dribble_violations DOUBLE,
                total_offensive_three_seconds_violations DOUBLE,
                total_five_seconds_violations DOUBLE,
                total_eight_seconds_violations DOUBLE,
                total_shot_clock_violations DOUBLE,
                total_inbound_violations DOUBLE,
                total_backcourt_violations DOUBLE,
                total_offensive_goaltending_violations DOUBLE,
                total_palming_violations DOUBLE,
                total_offensive_foul_violations DOUBLE,
                total_defensive_three_seconds_violations DOUBLE,
                total_charging_violations DOUBLE,
                total_delay_of_game_violations DOUBLE,
                total_defensive_goaltending_violations DOUBLE,
                total_lane_violations DOUBLE,
                total_jump_ball_violations DOUBLE,
                total_kicked_ball_violations DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        # Current Season Averages
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_stats.current_season_team_violations_averages (
                season STRING,
                team_id INT,
                avg_traveling_violations DOUBLE,
                avg_double_dribble_violations DOUBLE,
                avg_discontinued_dribble_violations DOUBLE,
                avg_offensive_three_seconds_violations DOUBLE,
                avg_five_seconds_violations DOUBLE,
                avg_eight_seconds_violations DOUBLE,
                avg_shot_clock_violations DOUBLE,
                avg_inbound_violations DOUBLE,
                avg_backcourt_violations DOUBLE,
                avg_offensive_goaltending_violations DOUBLE,
                avg_palming_violations DOUBLE,
                avg_offensive_foul_violations DOUBLE,
                avg_defensive_three_seconds_violations DOUBLE,
                avg_charging_violations DOUBLE,
                avg_delay_of_game_violations DOUBLE,
                avg_defensive_goaltending_violations DOUBLE,
                avg_lane_violations DOUBLE,
                avg_jump_ball_violations DOUBLE,
                avg_kicked_ball_violations DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        # All Season Totals
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_stats.all_season_team_violations_totals (
                team_id INT,
                total_traveling_violations DOUBLE,
                total_double_dribble_violations DOUBLE,
                total_discontinued_dribble_violations DOUBLE,
                total_offensive_three_seconds_violations DOUBLE,
                total_five_seconds_violations DOUBLE,
                total_eight_seconds_violations DOUBLE,
                total_shot_clock_violations DOUBLE,
                total_inbound_violations DOUBLE,
                total_backcourt_violations DOUBLE,
                total_offensive_goaltending_violations DOUBLE,
                total_palming_violations DOUBLE,
                total_offensive_foul_violations DOUBLE,
                total_defensive_three_seconds_violations DOUBLE,
                total_charging_violations DOUBLE,
                total_delay_of_game_violations DOUBLE,
                total_defensive_goaltending_violations DOUBLE,
                total_lane_violations DOUBLE,
                total_jump_ball_violations DOUBLE,
                total_kicked_ball_violations DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (team_id)
            """
        )

        # All Season Averages
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_stats.all_season_team_violations_averages (
                team_id INT,
                avg_traveling_violations DOUBLE,
                avg_double_dribble_violations DOUBLE,
                avg_discontinued_dribble_violations DOUBLE,
                avg_offensive_three_seconds_violations DOUBLE,
                avg_five_seconds_violations DOUBLE,
                avg_eight_seconds_violations DOUBLE,
                avg_shot_clock_violations DOUBLE,
                avg_inbound_violations DOUBLE,
                avg_backcourt_violations DOUBLE,
                avg_offensive_goaltending_violations DOUBLE,
                avg_palming_violations DOUBLE,
                avg_offensive_foul_violations DOUBLE,
                avg_defensive_three_seconds_violations DOUBLE,
                avg_charging_violations DOUBLE,
                avg_delay_of_game_violations DOUBLE,
                avg_defensive_goaltending_violations DOUBLE,
                avg_lane_violations DOUBLE,
                avg_jump_ball_violations DOUBLE,
                avg_kicked_ball_violations DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (team_id)
            """
        )

        logger.info("Cumulative Team Violations tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating cumulative team tables: {e}", exc_info=True)
        sys.exit(1)

def create_cumulative_player_tables(spark: SparkSession) -> None:
    """
    Creates additional cumulative Iceberg tables for PLAYER violations 
    using the new standardized column names.
    """
    try:
        # Drop if needed
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.current_season_player_violations_totals")
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.current_season_player_violations_averages")
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.all_season_player_violations_totals")
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.all_season_player_violations_averages")

        # Current Season Totals
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.current_season_player_violations_totals (
                season STRING,
                player_id INT,
                total_traveling_violations DOUBLE,
                total_double_dribble_violations DOUBLE,
                total_discontinued_dribble_violations DOUBLE,
                total_offensive_three_seconds_violations DOUBLE,
                total_inbound_violations DOUBLE,
                total_backcourt_violations DOUBLE,
                total_offensive_goaltending_violations DOUBLE,
                total_palming_violations DOUBLE,
                total_offensive_foul_violations DOUBLE,
                total_defensive_three_seconds_violations DOUBLE,
                total_charging_violations DOUBLE,
                total_delay_of_game_violations DOUBLE,
                total_defensive_goaltending_violations DOUBLE,
                total_lane_violations DOUBLE,
                total_jump_ball_violations DOUBLE,
                total_kicked_ball_violations DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        # Current Season Averages
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.current_season_player_violations_averages (
                season STRING,
                player_id INT,
                avg_traveling_violations DOUBLE,
                avg_double_dribble_violations DOUBLE,
                avg_discontinued_dribble_violations DOUBLE,
                avg_offensive_three_seconds_violations DOUBLE,
                avg_inbound_violations DOUBLE,
                avg_backcourt_violations DOUBLE,
                avg_offensive_goaltending_violations DOUBLE,
                avg_palming_violations DOUBLE,
                avg_offensive_foul_violations DOUBLE,
                avg_defensive_three_seconds_violations DOUBLE,
                avg_charging_violations DOUBLE,
                avg_delay_of_game_violations DOUBLE,
                avg_defensive_goaltending_violations DOUBLE,
                avg_lane_violations DOUBLE,
                avg_jump_ball_violations DOUBLE,
                avg_kicked_ball_violations DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        # All Season Totals
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.all_season_player_violations_totals (
                player_id INT,
                total_traveling_violations DOUBLE,
                total_double_dribble_violations DOUBLE,
                total_discontinued_dribble_violations DOUBLE,
                total_offensive_three_seconds_violations DOUBLE,
                total_inbound_violations DOUBLE,
                total_backcourt_violations DOUBLE,
                total_offensive_goaltending_violations DOUBLE,
                total_palming_violations DOUBLE,
                total_offensive_foul_violations DOUBLE,
                total_defensive_three_seconds_violations DOUBLE,
                total_charging_violations DOUBLE,
                total_delay_of_game_violations DOUBLE,
                total_defensive_goaltending_violations DOUBLE,
                total_lane_violations DOUBLE,
                total_jump_ball_violations DOUBLE,
                total_kicked_ball_violations DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (player_id)
            """
        )

        # All Season Averages
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.all_season_player_violations_averages (
                player_id INT,
                avg_traveling_violations DOUBLE,
                avg_double_dribble_violations DOUBLE,
                avg_discontinued_dribble_violations DOUBLE,
                avg_offensive_three_seconds_violations DOUBLE,
                avg_inbound_violations DOUBLE,
                avg_backcourt_violations DOUBLE,
                avg_offensive_goaltending_violations DOUBLE,
                avg_palming_violations DOUBLE,
                avg_offensive_foul_violations DOUBLE,
                avg_defensive_three_seconds_violations DOUBLE,
                avg_charging_violations DOUBLE,
                avg_delay_of_game_violations DOUBLE,
                avg_defensive_goaltending_violations DOUBLE,
                avg_lane_violations DOUBLE,
                avg_jump_ball_violations DOUBLE,
                avg_kicked_ball_violations DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (player_id)
            """
        )

        logger.info("Cumulative Player Violations tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating cumulative player tables: {e}", exc_info=True)
        sys.exit(1)

# --------------------------------------------------------------------------------
# UPDATE CUMULATIVE TABLES (TEAM + PLAYER) EXAMPLES
# --------------------------------------------------------------------------------
def update_cumulative_team_tables(spark: SparkSession) -> None:
    """
    Example logic to update the newly created cumulative TEAM tables from
    NBA_team_stats.team_violations_stats. Adjust the queries as needed.
    """
    try:
        # Example: define "current_season" by the largest season year found
        current_season_row = spark.sql(
            """
            SELECT MAX(CAST(SUBSTRING(season, 1, 4) AS INT)) AS current_season
            FROM NBA_team_stats.team_violations_stats
            """
        ).collect()[0]

        current_season = current_season_row["current_season"]
        if current_season is None:
            logger.warning("No current season found in team_violations_stats. Skipping update.")
            return

        # --------------------------------------------------
        # 1) Current Season Totals
        # --------------------------------------------------
        current_season_totals = spark.sql(
            f"""
            SELECT
                season,
                team_id,
                SUM(traveling_violations) AS total_traveling_violations,
                SUM(double_dribble_violations) AS total_double_dribble_violations,
                SUM(discontinued_dribble_violations) AS total_discontinued_dribble_violations,
                SUM(offensive_three_seconds_violations) AS total_offensive_three_seconds_violations,
                SUM(five_seconds_violations) AS total_five_seconds_violations,
                SUM(eight_seconds_violations) AS total_eight_seconds_violations,
                SUM(shot_clock_violations) AS total_shot_clock_violations,
                SUM(inbound_violations) AS total_inbound_violations,
                SUM(backcourt_violations) AS total_backcourt_violations,
                SUM(offensive_goaltending_violations) AS total_offensive_goaltending_violations,
                SUM(palming_violations) AS total_palming_violations,
                SUM(offensive_foul_violations) AS total_offensive_foul_violations,
                SUM(defensive_three_seconds_violations) AS total_defensive_three_seconds_violations,
                SUM(charging_violations) AS total_charging_violations,
                SUM(delay_of_game_violations) AS total_delay_of_game_violations,
                SUM(defensive_goaltending_violations) AS total_defensive_goaltending_violations,
                SUM(lane_violations) AS total_lane_violations,
                SUM(jump_ball_violations) AS total_jump_ball_violations,
                SUM(kicked_ball_violations) AS total_kicked_ball_violations
            FROM NBA_team_stats.team_violations_stats
            WHERE CAST(SUBSTRING(season,1,4) AS INT) = {current_season}
            GROUP BY season, team_id
            """
        )
        current_season_totals.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_team_stats.current_season_team_violations_totals"
        )

        # --------------------------------------------------
        # 2) Current Season Averages
        # --------------------------------------------------
        current_season_averages = spark.sql(
            f"""
            SELECT
                season,
                team_id,
                AVG(traveling_violations) AS avg_traveling_violations,
                AVG(double_dribble_violations) AS avg_double_dribble_violations,
                AVG(discontinued_dribble_violations) AS avg_discontinued_dribble_violations,
                AVG(offensive_three_seconds_violations) AS avg_offensive_three_seconds_violations,
                AVG(five_seconds_violations) AS avg_five_seconds_violations,
                AVG(eight_seconds_violations) AS avg_eight_seconds_violations,
                AVG(shot_clock_violations) AS avg_shot_clock_violations,
                AVG(inbound_violations) AS avg_inbound_violations,
                AVG(backcourt_violations) AS avg_backcourt_violations,
                AVG(offensive_goaltending_violations) AS avg_offensive_goaltending_violations,
                AVG(palming_violations) AS avg_palming_violations,
                AVG(offensive_foul_violations) AS avg_offensive_foul_violations,
                AVG(defensive_three_seconds_violations) AS avg_defensive_three_seconds_violations,
                AVG(charging_violations) AS avg_charging_violations,
                AVG(delay_of_game_violations) AS avg_delay_of_game_violations,
                AVG(defensive_goaltending_violations) AS avg_defensive_goaltending_violations,
                AVG(lane_violations) AS avg_lane_violations,
                AVG(jump_ball_violations) AS avg_jump_ball_violations,
                AVG(kicked_ball_violations) AS avg_kicked_ball_violations
            FROM NBA_team_stats.team_violations_stats
            WHERE CAST(SUBSTRING(season,1,4) AS INT) = {current_season}
            GROUP BY season, team_id
            """
        )
        current_season_averages.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_team_stats.current_season_team_violations_averages"
        )

        # --------------------------------------------------
        # 3) All Season Totals
        # --------------------------------------------------
        all_season_totals = spark.sql(
            """
            SELECT
                team_id,
                SUM(traveling_violations) AS total_traveling_violations,
                SUM(double_dribble_violations) AS total_double_dribble_violations,
                SUM(discontinued_dribble_violations) AS total_discontinued_dribble_violations,
                SUM(offensive_three_seconds_violations) AS total_offensive_three_seconds_violations,
                SUM(five_seconds_violations) AS total_five_seconds_violations,
                SUM(eight_seconds_violations) AS total_eight_seconds_violations,
                SUM(shot_clock_violations) AS total_shot_clock_violations,
                SUM(inbound_violations) AS total_inbound_violations,
                SUM(backcourt_violations) AS total_backcourt_violations,
                SUM(offensive_goaltending_violations) AS total_offensive_goaltending_violations,
                SUM(palming_violations) AS total_palming_violations,
                SUM(offensive_foul_violations) AS total_offensive_foul_violations,
                SUM(defensive_three_seconds_violations) AS total_defensive_three_seconds_violations,
                SUM(charging_violations) AS total_charging_violations,
                SUM(delay_of_game_violations) AS total_delay_of_game_violations,
                SUM(defensive_goaltending_violations) AS total_defensive_goaltending_violations,
                SUM(lane_violations) AS total_lane_violations,
                SUM(jump_ball_violations) AS total_jump_ball_violations,
                SUM(kicked_ball_violations) AS total_kicked_ball_violations
            FROM NBA_team_stats.team_violations_stats
            GROUP BY team_id
            """
        )
        all_season_totals.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_team_stats.all_season_team_violations_totals"
        )

        # --------------------------------------------------
        # 4) All Season Averages
        # --------------------------------------------------
        all_season_averages = spark.sql(
            """
            SELECT
                team_id,
                AVG(traveling_violations) AS avg_traveling_violations,
                AVG(double_dribble_violations) AS avg_double_dribble_violations,
                AVG(discontinued_dribble_violations) AS avg_discontinued_dribble_violations,
                AVG(offensive_three_seconds_violations) AS avg_offensive_three_seconds_violations,
                AVG(five_seconds_violations) AS avg_five_seconds_violations,
                AVG(eight_seconds_violations) AS avg_eight_seconds_violations,
                AVG(shot_clock_violations) AS avg_shot_clock_violations,
                AVG(inbound_violations) AS avg_inbound_violations,
                AVG(backcourt_violations) AS avg_backcourt_violations,
                AVG(offensive_goaltending_violations) AS avg_offensive_goaltending_violations,
                AVG(palming_violations) AS avg_palming_violations,
                AVG(offensive_foul_violations) AS avg_offensive_foul_violations,
                AVG(defensive_three_seconds_violations) AS avg_defensive_three_seconds_violations,
                AVG(charging_violations) AS avg_charging_violations,
                AVG(delay_of_game_violations) AS avg_delay_of_game_violations,
                AVG(defensive_goaltending_violations) AS avg_defensive_goaltending_violations,
                AVG(lane_violations) AS avg_lane_violations,
                AVG(jump_ball_violations) AS avg_jump_ball_violations,
                AVG(kicked_ball_violations) AS avg_kicked_ball_violations
            FROM NBA_team_stats.team_violations_stats
            GROUP BY team_id
            """
        )
        all_season_averages.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_team_stats.all_season_team_violations_averages"
        )

        logger.info("Cumulative team violation tables updated successfully.")
    except Exception as e:
        logger.error(f"Error updating cumulative team tables: {e}", exc_info=True)
        sys.exit(1)

def update_cumulative_player_tables(spark: SparkSession) -> None:
    """
    Example logic to update the newly created cumulative PLAYER tables from
    NBA_player_stats.player_violations_stats. Adjust as needed.
    """
    try:
        # Example: define "current_season" by the largest season year found
        current_season_row = spark.sql(
            """
            SELECT MAX(CAST(SUBSTRING(season, 1, 4) AS INT)) AS current_season
            FROM NBA_player_stats.player_violations_stats
            """
        ).collect()[0]

        current_season = current_season_row["current_season"]
        if current_season is None:
            logger.warning("No current season found in player_violations_stats. Skipping update.")
            return

        # --------------------------------------------------
        # 1) Current Season Totals
        # --------------------------------------------------
        current_season_totals = spark.sql(
            f"""
            SELECT
                season,
                player_id,
                SUM(traveling_violations) AS total_traveling_violations,
                SUM(double_dribble_violations) AS total_double_dribble_violations,
                SUM(discontinued_dribble_violations) AS total_discontinued_dribble_violations,
                SUM(offensive_three_seconds_violations) AS total_offensive_three_seconds_violations,
                SUM(inbound_violations) AS total_inbound_violations,
                SUM(backcourt_violations) AS total_backcourt_violations,
                SUM(offensive_goaltending_violations) AS total_offensive_goaltending_violations,
                SUM(palming_violations) AS total_palming_violations,
                SUM(offensive_foul_violations) AS total_offensive_foul_violations,
                SUM(defensive_three_seconds_violations) AS total_defensive_three_seconds_violations,
                SUM(charging_violations) AS total_charging_violations,
                SUM(delay_of_game_violations) AS total_delay_of_game_violations,
                SUM(defensive_goaltending_violations) AS total_defensive_goaltending_violations,
                SUM(lane_violations) AS total_lane_violations,
                SUM(jump_ball_violations) AS total_jump_ball_violations,
                SUM(kicked_ball_violations) AS total_kicked_ball_violations
            FROM NBA_player_stats.player_violations_stats
            WHERE CAST(SUBSTRING(season,1,4) AS INT) = {current_season}
            GROUP BY season, player_id
            """
        )
        current_season_totals.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_player_stats.current_season_player_violations_totals"
        )

        # --------------------------------------------------
        # 2) Current Season Averages
        # --------------------------------------------------
        current_season_averages = spark.sql(
            f"""
            SELECT
                season,
                player_id,
                AVG(traveling_violations) AS avg_traveling_violations,
                AVG(double_dribble_violations) AS avg_double_dribble_violations,
                AVG(discontinued_dribble_violations) AS avg_discontinued_dribble_violations,
                AVG(offensive_three_seconds_violations) AS avg_offensive_three_seconds_violations,
                AVG(inbound_violations) AS avg_inbound_violations,
                AVG(backcourt_violations) AS avg_backcourt_violations,
                AVG(offensive_goaltending_violations) AS avg_offensive_goaltending_violations,
                AVG(palming_violations) AS avg_palming_violations,
                AVG(offensive_foul_violations) AS avg_offensive_foul_violations,
                AVG(defensive_three_seconds_violations) AS avg_defensive_three_seconds_violations,
                AVG(charging_violations) AS avg_charging_violations,
                AVG(delay_of_game_violations) AS avg_delay_of_game_violations,
                AVG(defensive_goaltending_violations) AS avg_defensive_goaltending_violations,
                AVG(lane_violations) AS avg_lane_violations,
                AVG(jump_ball_violations) AS avg_jump_ball_violations,
                AVG(kicked_ball_violations) AS avg_kicked_ball_violations
            FROM NBA_player_stats.player_violations_stats
            WHERE CAST(SUBSTRING(season,1,4) AS INT) = {current_season}
            GROUP BY season, player_id
            """
        )
        current_season_averages.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_player_stats.current_season_player_violations_averages"
        )

        # --------------------------------------------------
        # 3) All Season Totals
        # --------------------------------------------------
        all_season_totals = spark.sql(
            """
            SELECT
                player_id,
                SUM(traveling_violations) AS total_traveling_violations,
                SUM(double_dribble_violations) AS total_double_dribble_violations,
                SUM(discontinued_dribble_violations) AS total_discontinued_dribble_violations,
                SUM(offensive_three_seconds_violations) AS total_offensive_three_seconds_violations,
                SUM(inbound_violations) AS total_inbound_violations,
                SUM(backcourt_violations) AS total_backcourt_violations,
                SUM(offensive_goaltending_violations) AS total_offensive_goaltending_violations,
                SUM(palming_violations) AS total_palming_violations,
                SUM(offensive_foul_violations) AS total_offensive_foul_violations,
                SUM(defensive_three_seconds_violations) AS total_defensive_three_seconds_violations,
                SUM(charging_violations) AS total_charging_violations,
                SUM(delay_of_game_violations) AS total_delay_of_game_violations,
                SUM(defensive_goaltending_violations) AS total_defensive_goaltending_violations,
                SUM(lane_violations) AS total_lane_violations,
                SUM(jump_ball_violations) AS total_jump_ball_violations,
                SUM(kicked_ball_violations) AS total_kicked_ball_violations
            FROM NBA_player_stats.player_violations_stats
            GROUP BY player_id
            """
        )
        all_season_totals.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_player_stats.all_season_player_violations_totals"
        )

        # --------------------------------------------------
        # 4) All Season Averages
        # --------------------------------------------------
        all_season_averages = spark.sql(
            """
            SELECT
                player_id,
                AVG(traveling_violations) AS avg_traveling_violations,
                AVG(double_dribble_violations) AS avg_double_dribble_violations,
                AVG(discontinued_dribble_violations) AS avg_discontinued_dribble_violations,
                AVG(offensive_three_seconds_violations) AS avg_offensive_three_seconds_violations,
                AVG(inbound_violations) AS avg_inbound_violations,
                AVG(backcourt_violations) AS avg_backcourt_violations,
                AVG(offensive_goaltending_violations) AS avg_offensive_goaltending_violations,
                AVG(palming_violations) AS avg_palming_violations,
                AVG(offensive_foul_violations) AS avg_offensive_foul_violations,
                AVG(defensive_three_seconds_violations) AS avg_defensive_three_seconds_violations,
                AVG(charging_violations) AS avg_charging_violations,
                AVG(delay_of_game_violations) AS avg_delay_of_game_violations,
                AVG(defensive_goaltending_violations) AS avg_defensive_goaltending_violations,
                AVG(lane_violations) AS avg_lane_violations,
                AVG(jump_ball_violations) AS avg_jump_ball_violations,
                AVG(kicked_ball_violations) AS avg_kicked_ball_violations
            FROM NBA_player_stats.player_violations_stats
            GROUP BY player_id
            """
        )
        all_season_averages.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_player_stats.all_season_player_violations_averages"
        )

        logger.info("Cumulative player violation tables updated successfully.")
    except Exception as e:
        logger.error(f"Error updating cumulative player tables: {e}", exc_info=True)
        sys.exit(1)

# --------------------------------------------------------------------------------
# Read from Kafka (TEAM + PLAYER) in Batch and Process
# --------------------------------------------------------------------------------
def read_kafka_team_violations_batch_and_process(spark: SparkSession) -> None:
    """
    Reads TEAM violations data from the 'NBA_team_violations' Kafka topic in a single batch,
    then merges into NBA_team_stats.team_violations_stats + writes aggregated results 
    to NBA_team_stats.fact_team_violations_stats.
    """
    try:
        # 1) Define schema matching the *original* TEAM Kafka JSON fields (UPPERCASE)
        team_schema = StructType([
            StructField("TEAM_ID", IntegerType(), True),
            StructField("TEAM_NAME", StringType(), True),
            StructField("GP", IntegerType(), True),
            StructField("W", IntegerType(), True),
            StructField("L", IntegerType(), True),
            StructField("W_PCT", DoubleType(), True),
            StructField("TRAVEL", DoubleType(), True),
            StructField("DOUBLE_DRIBBLE", DoubleType(), True),
            StructField("DISCONTINUED_DRIBBLE", DoubleType(), True),
            StructField("OFF_THREE_SEC", DoubleType(), True),
            StructField("FIVE_SEC", DoubleType(), True),
            StructField("EIGHT_SEC", DoubleType(), True),
            StructField("SHOT_CLOCK", DoubleType(), True),
            StructField("INBOUND", DoubleType(), True),
            StructField("BACKCOURT", DoubleType(), True),
            StructField("OFF_GOALTENDING", DoubleType(), True),
            StructField("PALMING", DoubleType(), True),
            StructField("OFF_FOUL", DoubleType(), True),
            StructField("DEF_THREE_SEC", DoubleType(), True),
            StructField("CHARGE", DoubleType(), True),
            StructField("DELAY_OF_GAME", DoubleType(), True),
            StructField("DEF_GOALTENDING", DoubleType(), True),
            StructField("LANE", DoubleType(), True),
            StructField("JUMP_BALL", DoubleType(), True),
            StructField("KICKED_BALL", DoubleType(), True),

            StructField("GP_RANK", IntegerType(), True),
            StructField("W_RANK", IntegerType(), True),
            StructField("L_RANK", IntegerType(), True),
            StructField("W_PCT_RANK", IntegerType(), True),
            StructField("TRAVEL_RANK", IntegerType(), True),
            StructField("DOUBLE_DRIBBLE_RANK", IntegerType(), True),
            StructField("DISCONTINUED_DRIBBLE_RANK", IntegerType(), True),
            StructField("OFF_THREE_SEC_RANK", IntegerType(), True),
            StructField("FIVE_SEC_RANK", IntegerType(), True),
            StructField("EIGHT_SEC_RANK", IntegerType(), True),
            StructField("SHOT_CLOCK_RANK", IntegerType(), True),
            StructField("INBOUND_RANK", IntegerType(), True),
            StructField("BACKCOURT_RANK", IntegerType(), True),
            StructField("OFF_GOALTENDING_RANK", IntegerType(), True),
            StructField("PALMING_RANK", IntegerType(), True),
            StructField("OFF_FOUL_RANK", IntegerType(), True),
            StructField("DEF_THREE_SEC_RANK", IntegerType(), True),
            StructField("CHARGE_RANK", IntegerType(), True),
            StructField("DELAY_OF_GAME_RANK", IntegerType(), True),
            StructField("DEF_GOALTENDING_RANK", IntegerType(), True),
            StructField("LANE_RANK", IntegerType(), True),
            StructField("JUMP_BALL_RANK", IntegerType(), True),
            StructField("KICKED_BALL_RANK", IntegerType(), True),

            StructField("Season", StringType(), True),
            StructField("SeasonType", StringType(), True),
            StructField("Month", IntegerType(), True),
            StructField("PerMode", StringType(), True),
            StructField("LastNGames", IntegerType(), True),
        ])

        # 2) Do a single batch read from Kafka for TEAM violations
        kafka_batch_df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", ",".join(BOOTSTRAP_SERVERS))
            .option("subscribe", "NBA_team_violations")
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        # 3) Parse the JSON messages
        parsed_df = (
            kafka_batch_df
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), team_schema).alias("data"))
            .select("data.*")
        )

        # 4) Rename columns from uppercase to fully spelled-out lowercase with underscores
        renamed_df = (
            parsed_df
            .withColumnRenamed("TEAM_ID", "team_id")
            .withColumnRenamed("TEAM_NAME", "team_name")
            .withColumnRenamed("GP", "games_played")
            .withColumnRenamed("W", "wins")
            .withColumnRenamed("L", "losses")
            .withColumnRenamed("W_PCT", "win_percentage")

            .withColumnRenamed("TRAVEL", "traveling_violations")
            .withColumnRenamed("DOUBLE_DRIBBLE", "double_dribble_violations")
            .withColumnRenamed("DISCONTINUED_DRIBBLE", "discontinued_dribble_violations")
            .withColumnRenamed("OFF_THREE_SEC", "offensive_three_seconds_violations")
            .withColumnRenamed("FIVE_SEC", "five_seconds_violations")
            .withColumnRenamed("EIGHT_SEC", "eight_seconds_violations")
            .withColumnRenamed("SHOT_CLOCK", "shot_clock_violations")
            .withColumnRenamed("INBOUND", "inbound_violations")
            .withColumnRenamed("BACKCOURT", "backcourt_violations")
            .withColumnRenamed("OFF_GOALTENDING", "offensive_goaltending_violations")
            .withColumnRenamed("PALMING", "palming_violations")
            .withColumnRenamed("OFF_FOUL", "offensive_foul_violations")
            .withColumnRenamed("DEF_THREE_SEC", "defensive_three_seconds_violations")
            .withColumnRenamed("CHARGE", "charging_violations")
            .withColumnRenamed("DELAY_OF_GAME", "delay_of_game_violations")
            .withColumnRenamed("DEF_GOALTENDING", "defensive_goaltending_violations")
            .withColumnRenamed("LANE", "lane_violations")
            .withColumnRenamed("JUMP_BALL", "jump_ball_violations")
            .withColumnRenamed("KICKED_BALL", "kicked_ball_violations")

            .withColumnRenamed("GP_RANK", "games_played_rank")
            .withColumnRenamed("W_RANK", "wins_rank")
            .withColumnRenamed("L_RANK", "losses_rank")
            .withColumnRenamed("W_PCT_RANK", "win_percentage_rank")
            .withColumnRenamed("TRAVEL_RANK", "traveling_violations_rank")
            .withColumnRenamed("DOUBLE_DRIBBLE_RANK", "double_dribble_violations_rank")
            .withColumnRenamed("DISCONTINUED_DRIBBLE_RANK", "discontinued_dribble_violations_rank")
            .withColumnRenamed("OFF_THREE_SEC_RANK", "offensive_three_seconds_violations_rank")
            .withColumnRenamed("FIVE_SEC_RANK", "five_seconds_violations_rank")
            .withColumnRenamed("EIGHT_SEC_RANK", "eight_seconds_violations_rank")
            .withColumnRenamed("SHOT_CLOCK_RANK", "shot_clock_violations_rank")
            .withColumnRenamed("INBOUND_RANK", "inbound_violations_rank")
            .withColumnRenamed("BACKCOURT_RANK", "backcourt_violations_rank")
            .withColumnRenamed("OFF_GOALTENDING_RANK", "offensive_goaltending_violations_rank")
            .withColumnRenamed("PALMING_RANK", "palming_violations_rank")
            .withColumnRenamed("OFF_FOUL_RANK", "offensive_foul_violations_rank")
            .withColumnRenamed("DEF_THREE_SEC_RANK", "defensive_three_seconds_violations_rank")
            .withColumnRenamed("CHARGE_RANK", "charging_violations_rank")
            .withColumnRenamed("DELAY_OF_GAME_RANK", "delay_of_game_violations_rank")
            .withColumnRenamed("DEF_GOALTENDING_RANK", "defensive_goaltending_violations_rank")
            .withColumnRenamed("LANE_RANK", "lane_violations_rank")
            .withColumnRenamed("JUMP_BALL_RANK", "jump_ball_violations_rank")
            .withColumnRenamed("KICKED_BALL_RANK", "kicked_ball_violations_rank")

            .withColumnRenamed("Season", "season")
            .withColumnRenamed("SeasonType", "season_type")
            .withColumnRenamed("Month", "month")
            .withColumnRenamed("PerMode", "per_mode")
            .withColumnRenamed("LastNGames", "last_n_games")
        )

        # 5) Repartition
        repartitioned_df = renamed_df.repartition(25)

        # Now run your SCD merges + aggregator in batch for TEAM violations
        scd_and_aggregate_team_violations(repartitioned_df, spark)

        logger.info("Batch read from Kafka (TEAM) + SCD merges + aggregator completed.")
    except Exception as e:
        logger.error(f"Error in batch processing TEAM violations from Kafka: {e}", exc_info=True)
        sys.exit(1)

def read_kafka_player_violations_batch_and_process(spark: SparkSession) -> None:
    """
    Reads PLAYER violations data from the 'NBA_player_violations' Kafka topic in a single batch,
    then merges into NBA_player_stats.player_violations_stats + writes aggregated results 
    to NBA_player_stats.fact_player_violations_stats.
    """
    try:
        # 1) Define schema matching the *original* PLAYER Kafka JSON fields (UPPERCASE)
        player_schema = StructType([
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

            StructField("TRAVEL", DoubleType(), True),
            StructField("DOUBLE_DRIBBLE", DoubleType(), True),
            StructField("DISCONTINUED_DRIBBLE", DoubleType(), True),
            StructField("OFF_THREE_SEC", DoubleType(), True),
            StructField("INBOUND", DoubleType(), True),
            StructField("BACKCOURT", DoubleType(), True),
            StructField("OFF_GOALTENDING", DoubleType(), True),
            StructField("PALMING", DoubleType(), True),
            StructField("OFF_FOUL", DoubleType(), True),
            StructField("DEF_THREE_SEC", DoubleType(), True),
            StructField("CHARGE", DoubleType(), True),
            StructField("DELAY_OF_GAME", DoubleType(), True),
            StructField("DEF_GOALTENDING", DoubleType(), True),
            StructField("LANE", DoubleType(), True),
            StructField("JUMP_BALL", DoubleType(), True),
            StructField("KICKED_BALL", DoubleType(), True),

            StructField("GP_RANK", IntegerType(), True),
            StructField("W_RANK", IntegerType(), True),
            StructField("L_RANK", IntegerType(), True),
            StructField("W_PCT_RANK", IntegerType(), True),
            StructField("TRAVEL_RANK", IntegerType(), True),
            StructField("DOUBLE_DRIBBLE_RANK", IntegerType(), True),
            StructField("DISCONTINUED_DRIBBLE_RANK", IntegerType(), True),
            StructField("OFF_THREE_SEC_RANK", IntegerType(), True),
            StructField("INBOUND_RANK", IntegerType(), True),
            StructField("BACKCOURT_RANK", IntegerType(), True),
            StructField("OFF_GOALTENDING_RANK", IntegerType(), True),
            StructField("PALMING_RANK", IntegerType(), True),
            StructField("OFF_FOUL_RANK", IntegerType(), True),
            StructField("DEF_THREE_SEC_RANK", IntegerType(), True),
            StructField("CHARGE_RANK", IntegerType(), True),
            StructField("DELAY_OF_GAME_RANK", IntegerType(), True),
            StructField("DEF_GOALTENDING_RANK", IntegerType(), True),
            StructField("LANE_RANK", IntegerType(), True),
            StructField("JUMP_BALL_RANK", IntegerType(), True),
            StructField("KICKED_BALL_RANK", IntegerType(), True),

            StructField("Season", StringType(), True),
            StructField("SeasonType", StringType(), True),
            StructField("Month", IntegerType(), True),
            StructField("PerMode", StringType(), True),
            StructField("LastNGames", IntegerType(), True),
        ])

        # 2) Do a single batch read from Kafka for PLAYER violations
        kafka_batch_df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", ",".join(BOOTSTRAP_SERVERS))
            .option("subscribe", "NBA_player_violations")
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        # 3) Parse the JSON messages
        parsed_df = (
            kafka_batch_df
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), player_schema).alias("data"))
            .select("data.*")
        )

        # 4) Rename columns
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

            .withColumnRenamed("TRAVEL", "traveling_violations")
            .withColumnRenamed("DOUBLE_DRIBBLE", "double_dribble_violations")
            .withColumnRenamed("DISCONTINUED_DRIBBLE", "discontinued_dribble_violations")
            .withColumnRenamed("OFF_THREE_SEC", "offensive_three_seconds_violations")
            .withColumnRenamed("INBOUND", "inbound_violations")
            .withColumnRenamed("BACKCOURT", "backcourt_violations")
            .withColumnRenamed("OFF_GOALTENDING", "offensive_goaltending_violations")
            .withColumnRenamed("PALMING", "palming_violations")
            .withColumnRenamed("OFF_FOUL", "offensive_foul_violations")
            .withColumnRenamed("DEF_THREE_SEC", "defensive_three_seconds_violations")
            .withColumnRenamed("CHARGE", "charging_violations")
            .withColumnRenamed("DELAY_OF_GAME", "delay_of_game_violations")
            .withColumnRenamed("DEF_GOALTENDING", "defensive_goaltending_violations")
            .withColumnRenamed("LANE", "lane_violations")
            .withColumnRenamed("JUMP_BALL", "jump_ball_violations")
            .withColumnRenamed("KICKED_BALL", "kicked_ball_violations")

            .withColumnRenamed("GP_RANK", "games_played_rank")
            .withColumnRenamed("W_RANK", "wins_rank")
            .withColumnRenamed("L_RANK", "losses_rank")
            .withColumnRenamed("W_PCT_RANK", "win_percentage_rank")
            .withColumnRenamed("TRAVEL_RANK", "traveling_violations_rank")
            .withColumnRenamed("DOUBLE_DRIBBLE_RANK", "double_dribble_violations_rank")
            .withColumnRenamed("DISCONTINUED_DRIBBLE_RANK", "discontinued_dribble_violations_rank")
            .withColumnRenamed("OFF_THREE_SEC_RANK", "offensive_three_seconds_violations_rank")
            .withColumnRenamed("INBOUND_RANK", "inbound_violations_rank")
            .withColumnRenamed("BACKCOURT_RANK", "backcourt_violations_rank")
            .withColumnRenamed("OFF_GOALTENDING_RANK", "offensive_goaltending_violations_rank")
            .withColumnRenamed("PALMING_RANK", "palming_violations_rank")
            .withColumnRenamed("OFF_FOUL_RANK", "offensive_foul_violations_rank")
            .withColumnRenamed("DEF_THREE_SEC_RANK", "defensive_three_seconds_violations_rank")
            .withColumnRenamed("CHARGE_RANK", "charging_violations_rank")
            .withColumnRenamed("DELAY_OF_GAME_RANK", "delay_of_game_violations_rank")
            .withColumnRenamed("DEF_GOALTENDING_RANK", "defensive_goaltending_violations_rank")
            .withColumnRenamed("LANE_RANK", "lane_violations_rank")
            .withColumnRenamed("JUMP_BALL_RANK", "jump_ball_violations_rank")
            .withColumnRenamed("KICKED_BALL_RANK", "kicked_ball_violations_rank")

            .withColumnRenamed("Season", "season")
            .withColumnRenamed("SeasonType", "season_type")
            .withColumnRenamed("Month", "month")
            .withColumnRenamed("PerMode", "per_mode")
            .withColumnRenamed("LastNGames", "last_n_games")
        )

        # 5) Repartition
        repartitioned_df = renamed_df.repartition(25)

        # Now run your SCD merges + aggregator in batch for PLAYER violations
        scd_and_aggregate_player_violations(repartitioned_df, spark)

        logger.info("Batch read from Kafka (PLAYER) + SCD merges + aggregator completed.")
    except Exception as e:
        logger.error(f"Error in batch processing PLAYER violations from Kafka: {e}", exc_info=True)
        sys.exit(1)

# --------------------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------------------
def main() -> None:
    """
    Main function to execute the pipeline in batch mode for both TEAM and PLAYER violations.
    """
    try:
        spark = create_spark_connection()

        # Create the needed tables for Team + Player
        create_team_violations_tables(spark)
        create_player_violations_tables(spark)

        # Create cumulative tables (Team + Player)
        create_cumulative_team_tables(spark)
        create_cumulative_player_tables(spark)

        # TEAM: Single run of reading from Kafka + SCD merges + writing aggregator
        read_kafka_team_violations_batch_and_process(spark)

        # PLAYER: Single run of reading from Kafka + SCD merges + writing aggregator
        read_kafka_player_violations_batch_and_process(spark)

        # Update cumulative tables (Team + Player)
        update_cumulative_team_tables(spark)
        update_cumulative_player_tables(spark)

        spark.stop()
        logger.info("NBA Violations Batch pipeline executed successfully.")
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        sys.exit(1)

# --------------------------------------------------------------------------------
# ENTRY POINT
# --------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
