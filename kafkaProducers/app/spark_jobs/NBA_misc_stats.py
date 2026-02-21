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

# ----------------------------------------------------------------------------
# Configure logging
# ----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------
# Kafka bootstrap servers
# ----------------------------------------------------------------------------
BOOTSTRAP_SERVERS: List[str] = [
    "172.16.10.2:9092",
    "172.16.10.3:9093",
    "172.16.10.4:9094",
]

# ----------------------------------------------------------------------------
# Create Spark Session
# ----------------------------------------------------------------------------
def create_spark_connection() -> SparkSession:
    """
    Creates and returns a SparkSession with the necessary configurations.
    Note we are removing streaming triggers and focusing on batch usage.
    """
    try:
        spark = (
            SparkSession.builder.appName("NBA_Misc_Stats_Batch")
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

        # Set shuffle partitions
        spark.conf.set("spark.sql.shuffle.partitions", 25)

        # Disable adaptive execution for consistent partitioning
        spark.conf.set("spark.sql.adaptive.enabled", "false")

        return spark
    except Exception as e:
        logger.error(f"Error creating Spark connection: {e}", exc_info=True)
        sys.exit(1)

# ----------------------------------------------------------------------------
# Create Misc Tables
# ----------------------------------------------------------------------------
def create_misc_tables(spark: SparkSession) -> None:
    """
    Creates necessary Iceberg tables in the Hive metastore for both
    player and team miscellaneous stats, plus their fact tables.
    Follows the same structure/logic as the advanced script but adjusted
    for the 'misc' dataset.
    """
    try:
        # Drop old tables (if they exist) so new schema is correct
        for table_name in [
            "NBA_player_stats.misc_player_stats",
            "NBA_player_stats.fact_misc_player_stats",
            "NBA_player_stats.dim_players",
            "NBA_team_stats.misc_team_stats",
            "NBA_team_stats.fact_misc_team_stats",
            "NBA_team_stats.dim_teams",
        ]:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

        # --------------------------------------------------------------------
        # Create MISC Player Stats Table (SCD Type 2 table)
        # --------------------------------------------------------------------
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.misc_player_stats (
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
                points_off_turnovers DOUBLE,
                points_second_chance DOUBLE,
                points_fast_break DOUBLE,
                points_in_paint DOUBLE,
                opponent_points_off_turnovers DOUBLE,
                opponent_points_second_chance DOUBLE,
                opponent_points_fast_break DOUBLE,
                opponent_points_in_paint DOUBLE,
                blocks INT,
                blocks_against INT,
                personal_fouls INT,
                personal_fouls_drawn INT,
                nba_fantasy_points DOUBLE,
                games_played_rank INT,
                wins_rank INT,
                losses_rank INT,
                win_percentage_rank INT,
                minutes_played_rank INT,
                points_off_turnovers_rank INT,
                points_second_chance_rank INT,
                points_fast_break_rank INT,
                points_in_paint_rank INT,
                opponent_points_off_turnovers_rank INT,
                opponent_points_second_chance_rank INT,
                opponent_points_fast_break_rank INT,
                opponent_points_in_paint_rank INT,
                blocks_rank INT,
                blocks_against_rank INT,
                personal_fouls_rank INT,
                personal_fouls_drawn_rank INT,
                nba_fantasy_points_rank INT,
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

        # --------------------------------------------------------------------
        # Create MISC Team Stats Table (SCD Type 2 table)
        # --------------------------------------------------------------------
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_stats.misc_team_stats (
                team_id INT,
                team_name STRING,
                games_played INT,
                wins INT,
                losses INT,
                win_percentage DOUBLE,
                minutes_played DOUBLE,
                points_off_turnovers DOUBLE,
                points_second_chance DOUBLE,
                points_fast_break DOUBLE,
                points_in_paint DOUBLE,
                opponent_points_off_turnovers DOUBLE,
                opponent_points_second_chance DOUBLE,
                opponent_points_fast_break DOUBLE,
                opponent_points_in_paint DOUBLE,
                games_played_rank INT,
                wins_rank INT,
                losses_rank INT,
                win_percentage_rank INT,
                minutes_played_rank INT,
                points_off_turnovers_rank INT,
                points_second_chance_rank INT,
                points_fast_break_rank INT,
                points_in_paint_rank INT,
                opponent_points_off_turnovers_rank INT,
                opponent_points_second_chance_rank INT,
                opponent_points_fast_break_rank INT,
                opponent_points_in_paint_rank INT,
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

        # --------------------------------------------------------------------
        # Create dimension tables for players and teams (SCD Type 2 style if desired)
        # --------------------------------------------------------------------
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

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_stats.dim_teams (
                team_id INT,
                team_name STRING,
                record_start_date DATE,
                record_end_date DATE,
                is_current BOOLEAN
            )
            USING ICEBERG
            PARTITIONED BY (team_id)
            """
        )

        # --------------------------------------------------------------------
        # Create FACT tables for MISC Player & Team
        # --------------------------------------------------------------------
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.fact_misc_player_stats (
                player_id INT,
                season STRING,
                month INT,
                per_mode STRING,
                last_n_games INT,
                total_blocks BIGINT,
                total_personal_fouls BIGINT,
                total_points_off_turnovers DOUBLE,
                total_points_in_paint DOUBLE,
                total_points_fast_break DOUBLE,
                total_points_second_chance DOUBLE,
                total_opponent_points_off_turnovers DOUBLE,
                total_opponent_points_in_paint DOUBLE,
                total_opponent_points_fast_break DOUBLE,
                total_opponent_points_second_chance DOUBLE,
                total_nba_fantasy_points DOUBLE,
                avg_minutes_played DOUBLE,
                avg_win_percentage DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season, player_id)
            """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_stats.fact_misc_team_stats (
                team_id INT,
                season STRING,
                month INT,
                per_mode STRING,
                last_n_games INT,
                total_points_off_turnovers DOUBLE,
                total_points_second_chance DOUBLE,
                total_points_fast_break DOUBLE,
                total_points_in_paint DOUBLE,
                total_opponent_points_off_turnovers DOUBLE,
                total_opponent_points_second_chance DOUBLE,
                total_opponent_points_fast_break DOUBLE,
                total_opponent_points_in_paint DOUBLE,
                total_wins BIGINT,
                total_losses BIGINT,
                avg_win_percentage DOUBLE,
                avg_minutes_played DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season, team_id)
            """
        )

        logger.info("Misc tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating misc tables: {e}", exc_info=True)
        sys.exit(1)

# ----------------------------------------------------------------------------
# SCD Type 2 Handler
# ----------------------------------------------------------------------------
def handle_scd_type_2(
    df: DataFrame,
    spark: SparkSession,
    table_name: str,
    join_columns: List[str],
    compare_columns: List[str],
) -> None:
    """
    Handles Slowly Changing Dimension Type 2 for the given DataFrame,
    using a hash-based compare to detect changes. The same approach
    can be applied to both player and team tables.
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
        logger.info(f"SCD Type 2 handling completed successfully for table: {table_name}")
    except Exception as e:
        logger.error(f"Error handling SCD Type 2 for {table_name}: {e}", exc_info=True)
        sys.exit(1)

# ----------------------------------------------------------------------------
# Aggregate & SCD for Player Misc Data
# ----------------------------------------------------------------------------
def scd_and_aggregate_batch_player(df: DataFrame, spark: SparkSession) -> None:
    """
    Performs SCD Type 2 merges into NBA_player_stats.misc_player_stats,
    then aggregates & writes to NBA_player_stats.fact_misc_player_stats in batch mode.
    """
    record_count = df.count()
    logger.info(f"[Batch-Job: Player] Dataset has {record_count} records to process for players.")

    if record_count == 0:
        logger.info("[Batch-Job: Player] No player records to merge or aggregate.")
        return

    # ----------------------------------------------------
    # 1) Handle SCD merges into NBA_player_stats.misc_player_stats
    # ----------------------------------------------------
    handle_scd_type_2(
        df=df,
        spark=spark,
        table_name="NBA_player_stats.misc_player_stats",
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
            "minutes_played",
            "points_off_turnovers",
            "points_second_chance",
            "points_fast_break",
            "points_in_paint",
            "opponent_points_off_turnovers",
            "opponent_points_second_chance",
            "opponent_points_fast_break",
            "opponent_points_in_paint",
            "blocks",
            "blocks_against",
            "personal_fouls",
            "personal_fouls_drawn",
            "nba_fantasy_points",
            "games_played_rank",
            "wins_rank",
            "losses_rank",
            "win_percentage_rank",
            "minutes_played_rank",
            "points_off_turnovers_rank",
            "points_second_chance_rank",
            "points_fast_break_rank",
            "points_in_paint_rank",
            "opponent_points_off_turnovers_rank",
            "opponent_points_second_chance_rank",
            "opponent_points_fast_break_rank",
            "opponent_points_in_paint_rank",
            "blocks_rank",
            "blocks_against_rank",
            "personal_fouls_rank",
            "personal_fouls_drawn_rank",
            "nba_fantasy_points_rank",
        ],
    )

    # ----------------------------------------------------
    # 2) Aggregate for FACT: NBA_player_stats.fact_misc_player_stats
    # ----------------------------------------------------
    fact_aggregated = (
        df.groupBy("player_id", "season", "month", "per_mode", "last_n_games")
        .agg(
            spark_sum(col("blocks").cast("long")).alias("total_blocks"),
            spark_sum(col("personal_fouls").cast("long")).alias("total_personal_fouls"),
            spark_sum(col("points_off_turnovers")).alias("total_points_off_turnovers"),
            spark_sum(col("points_in_paint")).alias("total_points_in_paint"),
            spark_sum(col("points_fast_break")).alias("total_points_fast_break"),
            spark_sum(col("points_second_chance")).alias("total_points_second_chance"),
            spark_sum(col("opponent_points_off_turnovers")).alias("total_opponent_points_off_turnovers"),
            spark_sum(col("opponent_points_in_paint")).alias("total_opponent_points_in_paint"),
            spark_sum(col("opponent_points_fast_break")).alias("total_opponent_points_fast_break"),
            spark_sum(col("opponent_points_second_chance")).alias("total_opponent_points_second_chance"),
            spark_sum(col("nba_fantasy_points")).alias("total_nba_fantasy_points"),
            avg("minutes_played").alias("avg_minutes_played"),
            avg("win_percentage").alias("avg_win_percentage"),
        )
    )

    fact_aggregated.write.format("iceberg") \
        .mode("append") \
        .save("spark_catalog.NBA_player_stats.fact_misc_player_stats")

    logger.info("[Batch-Job: Player] Wrote aggregated data to fact_misc_player_stats.")

# ----------------------------------------------------------------------------
# Aggregate & SCD for Team Misc Data
# ----------------------------------------------------------------------------
def scd_and_aggregate_batch_team(df: DataFrame, spark: SparkSession) -> None:
    """
    Performs SCD Type 2 merges into NBA_team_stats.misc_team_stats,
    then aggregates & writes to NBA_team_stats.fact_misc_team_stats in batch mode.
    """
    record_count = df.count()
    logger.info(f"[Batch-Job: Team] Dataset has {record_count} records to process for teams.")

    if record_count == 0:
        logger.info("[Batch-Job: Team] No team records to merge or aggregate.")
        return

    # ----------------------------------------------------
    # 1) Handle SCD merges into NBA_team_stats.misc_team_stats
    # ----------------------------------------------------
    handle_scd_type_2(
        df=df,
        spark=spark,
        table_name="NBA_team_stats.misc_team_stats",
        join_columns=["team_id", "season", "per_mode", "month", "last_n_games"],
        compare_columns=[
            "team_name",
            "games_played",
            "wins",
            "losses",
            "win_percentage",
            "minutes_played",
            "points_off_turnovers",
            "points_second_chance",
            "points_fast_break",
            "points_in_paint",
            "opponent_points_off_turnovers",
            "opponent_points_second_chance",
            "opponent_points_fast_break",
            "opponent_points_in_paint",
            "games_played_rank",
            "wins_rank",
            "losses_rank",
            "win_percentage_rank",
            "minutes_played_rank",
            "points_off_turnovers_rank",
            "points_second_chance_rank",
            "points_fast_break_rank",
            "points_in_paint_rank",
            "opponent_points_off_turnovers_rank",
            "opponent_points_second_chance_rank",
            "opponent_points_fast_break_rank",
            "opponent_points_in_paint_rank",
        ],
    )

    # ----------------------------------------------------
    # 2) Aggregate for FACT: NBA_team_stats.fact_misc_team_stats
    # ----------------------------------------------------
    fact_aggregated = (
        df.groupBy("team_id", "season", "month", "per_mode", "last_n_games")
        .agg(
            spark_sum(col("points_off_turnovers")).alias("total_points_off_turnovers"),
            spark_sum(col("points_second_chance")).alias("total_points_second_chance"),
            spark_sum(col("points_fast_break")).alias("total_points_fast_break"),
            spark_sum(col("points_in_paint")).alias("total_points_in_paint"),
            spark_sum(col("opponent_points_off_turnovers")).alias("total_opponent_points_off_turnovers"),
            spark_sum(col("opponent_points_second_chance")).alias("total_opponent_points_second_chance"),
            spark_sum(col("opponent_points_fast_break")).alias("total_opponent_points_fast_break"),
            spark_sum(col("opponent_points_in_paint")).alias("total_opponent_points_in_paint"),
            spark_sum(col("wins").cast("long")).alias("total_wins"),
            spark_sum(col("losses").cast("long")).alias("total_losses"),
            avg("win_percentage").alias("avg_win_percentage"),
            avg("minutes_played").alias("avg_minutes_played"),
        )
    )

    fact_aggregated.write.format("iceberg") \
        .mode("append") \
        .save("spark_catalog.NBA_team_stats.fact_misc_team_stats")

    logger.info("[Batch-Job: Team] Wrote aggregated data to fact_misc_team_stats.")

# ----------------------------------------------------------------------------
# Create Additional Cumulative Tables (optional)
# ----------------------------------------------------------------------------
def create_cumulative_tables(spark: SparkSession) -> None:
    """
    Creates additional cumulative/derived Iceberg tables for both
    player and team data, just as an example (mirroring the advanced script approach).
    Adjust or expand as needed.
    """
    try:
        # Drop them if needed
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.current_season_misc_player_totals")
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.all_season_misc_player_totals")
        spark.sql("DROP TABLE IF EXISTS NBA_team_stats.current_season_misc_team_totals")
        spark.sql("DROP TABLE IF EXISTS NBA_team_stats.all_season_misc_team_totals")

        # For demonstration, create 4 simple tables (2 for player, 2 for team)

        # Player: current season
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.current_season_misc_player_totals (
                season STRING,
                player_name STRING,
                total_blocks BIGINT,
                total_personal_fouls BIGINT,
                total_points_off_turnovers DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        # Player: all seasons
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.all_season_misc_player_totals (
                player_name STRING,
                total_blocks BIGINT,
                total_personal_fouls BIGINT,
                total_points_off_turnovers DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (player_name)
            """
        )

        # Team: current season
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_stats.current_season_misc_team_totals (
                season STRING,
                team_name STRING,
                total_wins BIGINT,
                total_losses BIGINT,
                total_points_off_turnovers DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        # Team: all seasons
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_stats.all_season_misc_team_totals (
                team_name STRING,
                total_wins BIGINT,
                total_losses BIGINT,
                total_points_off_turnovers DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (team_name)
            """
        )

        logger.info("Cumulative tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating cumulative tables: {e}", exc_info=True)
        sys.exit(1)

# ----------------------------------------------------------------------------
# Update Cumulative Tables
# ----------------------------------------------------------------------------
def update_cumulative_tables(spark: SparkSession) -> None:
    """
    Example function to update the newly created cumulative tables from
    the main SCD table (misc_player_stats, misc_team_stats).
    This mirrors the advanced script logic, but simplified for demonstration.
    """
    try:
        # ==========================================================
        # For PLAYER totals: current season & all seasons
        # ==========================================================
        # 1) Identify the "current" season by the maximum season value
        current_season_row = spark.sql(
            """
            SELECT MAX(CAST(SUBSTRING(season, 1, 4) AS INT)) AS current_season
            FROM NBA_player_stats.misc_player_stats
            """
        ).collect()[0]
        current_season = current_season_row["current_season"]

        if current_season is None:
            logger.warning("No current season found in player stats. Skipping player cumulative update.")
        else:
            # current_season_misc_player_totals
            current_player_totals = spark.sql(
                f"""
                SELECT
                    season,
                    player_name,
                    SUM(blocks) AS total_blocks,
                    SUM(personal_fouls) AS total_personal_fouls,
                    SUM(points_off_turnovers) AS total_points_off_turnovers
                FROM NBA_player_stats.misc_player_stats
                WHERE CAST(SUBSTRING(season, 1, 4) AS INT) = {current_season}
                GROUP BY season, player_name
                """
            )
            current_player_totals.write.format("iceberg") \
                .mode("overwrite") \
                .save("spark_catalog.NBA_player_stats.current_season_misc_player_totals")

        # all_season_misc_player_totals
        all_player_totals = spark.sql(
            """
            SELECT
                player_name,
                SUM(blocks) AS total_blocks,
                SUM(personal_fouls) AS total_personal_fouls,
                SUM(points_off_turnovers) AS total_points_off_turnovers
            FROM NBA_player_stats.misc_player_stats
            GROUP BY player_name
            """
        )
        all_player_totals.write.format("iceberg") \
            .mode("overwrite") \
            .save("spark_catalog.NBA_player_stats.all_season_misc_player_totals")

        # ==========================================================
        # For TEAM totals: current season & all seasons
        # ==========================================================
        current_season_row_team = spark.sql(
            """
            SELECT MAX(CAST(SUBSTRING(season, 1, 4) AS INT)) AS current_season
            FROM NBA_team_stats.misc_team_stats
            """
        ).collect()[0]
        current_season_team = current_season_row_team["current_season"]

        if current_season_team is None:
            logger.warning("No current season found in team stats. Skipping team cumulative update.")
        else:
            # current_season_misc_team_totals
            current_team_totals = spark.sql(
                f"""
                SELECT
                    season,
                    team_name,
                    SUM(wins) AS total_wins,
                    SUM(losses) AS total_losses,
                    SUM(points_off_turnovers) AS total_points_off_turnovers
                FROM NBA_team_stats.misc_team_stats
                WHERE CAST(SUBSTRING(season, 1, 4) AS INT) = {current_season_team}
                GROUP BY season, team_name
                """
            )
            current_team_totals.write.format("iceberg") \
                .mode("overwrite") \
                .save("spark_catalog.NBA_team_stats.current_season_misc_team_totals")

        # all_season_misc_team_totals
        all_team_totals = spark.sql(
            """
            SELECT
                team_name,
                SUM(wins) AS total_wins,
                SUM(losses) AS total_losses,
                SUM(points_off_turnovers) AS total_points_off_turnovers
            FROM NBA_team_stats.misc_team_stats
            GROUP BY team_name
            """
        )
        all_team_totals.write.format("iceberg") \
            .mode("overwrite") \
            .save("spark_catalog.NBA_team_stats.all_season_misc_team_totals")

        logger.info("Cumulative tables updated successfully.")
    except Exception as e:
        logger.error(f"Error updating cumulative tables: {e}", exc_info=True)
        sys.exit(1)

# ----------------------------------------------------------------------------
# Batch Read & Process from Kafka (2 Topics: Player & Team)
# ----------------------------------------------------------------------------
def read_kafka_batch_and_process(spark: SparkSession) -> None:
    """
    Reads data from 2 Kafka topics (NBA_misc_player_stats and NBA_misc_team_stats)
    in batch mode, applies the same SCD Type 2 merges + aggregator approach
    for each dataset, then optionally updates cumulative tables.
    """

    try:
        # --------------------------------------------------------------------
        # 1) SCHEMA for the PLAYER MISC STATS topic
        # --------------------------------------------------------------------
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
            StructField("MIN", DoubleType(), True),
            StructField("PTS_OFF_TOV", DoubleType(), True),
            StructField("PTS_2ND_CHANCE", DoubleType(), True),
            StructField("PTS_FB", DoubleType(), True),
            StructField("PTS_PAINT", DoubleType(), True),
            StructField("OPP_PTS_OFF_TOV", DoubleType(), True),
            StructField("OPP_PTS_2ND_CHANCE", DoubleType(), True),
            StructField("OPP_PTS_FB", DoubleType(), True),
            StructField("OPP_PTS_PAINT", DoubleType(), True),
            StructField("BLK", IntegerType(), True),
            StructField("BLKA", IntegerType(), True),
            StructField("PF", IntegerType(), True),
            StructField("PFD", IntegerType(), True),
            StructField("NBA_FANTASY_PTS", DoubleType(), True),
            StructField("GP_RANK", IntegerType(), True),
            StructField("W_RANK", IntegerType(), True),
            StructField("L_RANK", IntegerType(), True),
            StructField("W_PCT_RANK", IntegerType(), True),
            StructField("MIN_RANK", IntegerType(), True),
            StructField("PTS_OFF_TOV_RANK", IntegerType(), True),
            StructField("PTS_2ND_CHANCE_RANK", IntegerType(), True),
            StructField("PTS_FB_RANK", IntegerType(), True),
            StructField("PTS_PAINT_RANK", IntegerType(), True),
            StructField("OPP_PTS_OFF_TOV_RANK", IntegerType(), True),
            StructField("OPP_PTS_2ND_CHANCE_RANK", IntegerType(), True),
            StructField("OPP_PTS_FB_RANK", IntegerType(), True),
            StructField("OPP_PTS_PAINT_RANK", IntegerType(), True),
            StructField("BLK_RANK", IntegerType(), True),
            StructField("BLKA_RANK", IntegerType(), True),
            StructField("PF_RANK", IntegerType(), True),
            StructField("PFD_RANK", IntegerType(), True),
            StructField("NBA_FANTASY_PTS_RANK", IntegerType(), True),
            StructField("Season", StringType(), True),
            StructField("SeasonType", StringType(), True),
            StructField("Month", IntegerType(), True),
            StructField("PerMode", StringType(), True),
            StructField("LastNGames", IntegerType(), True),
        ])

        # --------------------------------------------------------------------
        # 2) SCHEMA for the TEAM MISC STATS topic
        # --------------------------------------------------------------------
        team_schema = StructType([
            StructField("TEAM_ID", IntegerType(), True),
            StructField("TEAM_NAME", StringType(), True),
            StructField("GP", IntegerType(), True),
            StructField("W", IntegerType(), True),
            StructField("L", IntegerType(), True),
            StructField("W_PCT", DoubleType(), True),
            StructField("MIN", DoubleType(), True),
            StructField("PTS_OFF_TOV", DoubleType(), True),
            StructField("PTS_2ND_CHANCE", DoubleType(), True),
            StructField("PTS_FB", DoubleType(), True),
            StructField("PTS_PAINT", DoubleType(), True),
            StructField("OPP_PTS_OFF_TOV", DoubleType(), True),
            StructField("OPP_PTS_2ND_CHANCE", DoubleType(), True),
            StructField("OPP_PTS_FB", DoubleType(), True),
            StructField("OPP_PTS_PAINT", DoubleType(), True),
            StructField("GP_RANK", IntegerType(), True),
            StructField("W_RANK", IntegerType(), True),
            StructField("L_RANK", IntegerType(), True),
            StructField("W_PCT_RANK", IntegerType(), True),
            StructField("MIN_RANK", IntegerType(), True),
            StructField("PTS_OFF_TOV_RANK", IntegerType(), True),
            StructField("PTS_2ND_CHANCE_RANK", IntegerType(), True),
            StructField("PTS_FB_RANK", IntegerType(), True),
            StructField("PTS_PAINT_RANK", IntegerType(), True),
            StructField("OPP_PTS_OFF_TOV_RANK", IntegerType(), True),
            StructField("OPP_PTS_2ND_CHANCE_RANK", IntegerType(), True),
            StructField("OPP_PTS_FB_RANK", IntegerType(), True),
            StructField("OPP_PTS_PAINT_RANK", IntegerType(), True),
            StructField("Season", StringType(), True),
            StructField("SeasonType", StringType(), True),
            StructField("Month", IntegerType(), True),
            StructField("PerMode", StringType(), True),
            StructField("LastNGames", IntegerType(), True),
        ])

        # --------------------------------------------------------------------
        # 3) Read the PLAYER MISC data from Kafka (batch)
        # --------------------------------------------------------------------
        kafka_batch_player_df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", ",".join(BOOTSTRAP_SERVERS))
            .option("subscribe", "NBA_misc_player_stats")
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        parsed_player_df = (
            kafka_batch_player_df
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), player_schema).alias("data"))
            .select("data.*")
        )

        # Rename columns to full-lowercase, fully spelled out
        renamed_player_df = (
            parsed_player_df
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
            .withColumnRenamed("PTS_OFF_TOV", "points_off_turnovers")
            .withColumnRenamed("PTS_2ND_CHANCE", "points_second_chance")
            .withColumnRenamed("PTS_FB", "points_fast_break")
            .withColumnRenamed("PTS_PAINT", "points_in_paint")
            .withColumnRenamed("OPP_PTS_OFF_TOV", "opponent_points_off_turnovers")
            .withColumnRenamed("OPP_PTS_2ND_CHANCE", "opponent_points_second_chance")
            .withColumnRenamed("OPP_PTS_FB", "opponent_points_fast_break")
            .withColumnRenamed("OPP_PTS_PAINT", "opponent_points_in_paint")
            .withColumnRenamed("BLK", "blocks")
            .withColumnRenamed("BLKA", "blocks_against")
            .withColumnRenamed("PF", "personal_fouls")
            .withColumnRenamed("PFD", "personal_fouls_drawn")
            .withColumnRenamed("NBA_FANTASY_PTS", "nba_fantasy_points")
            .withColumnRenamed("GP_RANK", "games_played_rank")
            .withColumnRenamed("W_RANK", "wins_rank")
            .withColumnRenamed("L_RANK", "losses_rank")
            .withColumnRenamed("W_PCT_RANK", "win_percentage_rank")
            .withColumnRenamed("MIN_RANK", "minutes_played_rank")
            .withColumnRenamed("PTS_OFF_TOV_RANK", "points_off_turnovers_rank")
            .withColumnRenamed("PTS_2ND_CHANCE_RANK", "points_second_chance_rank")
            .withColumnRenamed("PTS_FB_RANK", "points_fast_break_rank")
            .withColumnRenamed("PTS_PAINT_RANK", "points_in_paint_rank")
            .withColumnRenamed("OPP_PTS_OFF_TOV_RANK", "opponent_points_off_turnovers_rank")
            .withColumnRenamed("OPP_PTS_2ND_CHANCE_RANK", "opponent_points_second_chance_rank")
            .withColumnRenamed("OPP_PTS_FB_RANK", "opponent_points_fast_break_rank")
            .withColumnRenamed("OPP_PTS_PAINT_RANK", "opponent_points_in_paint_rank")
            .withColumnRenamed("BLK_RANK", "blocks_rank")
            .withColumnRenamed("BLKA_RANK", "blocks_against_rank")
            .withColumnRenamed("PF_RANK", "personal_fouls_rank")
            .withColumnRenamed("PFD_RANK", "personal_fouls_drawn_rank")
            .withColumnRenamed("NBA_FANTASY_PTS_RANK", "nba_fantasy_points_rank")
            .withColumnRenamed("Season", "season")
            .withColumnRenamed("SeasonType", "season_type")
            .withColumnRenamed("Month", "month")
            .withColumnRenamed("PerMode", "per_mode")
            .withColumnRenamed("LastNGames", "last_n_games")
        )

        repartitioned_player_df = renamed_player_df.repartition(25)

        # ----------------------------------------------------
        # 4) Read the TEAM MISC data from Kafka (batch)
        # ----------------------------------------------------
        kafka_batch_team_df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", ",".join(BOOTSTRAP_SERVERS))
            .option("subscribe", "NBA_misc_team_stats")
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        parsed_team_df = (
            kafka_batch_team_df
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), team_schema).alias("data"))
            .select("data.*")
        )

        renamed_team_df = (
            parsed_team_df
            .withColumnRenamed("TEAM_ID", "team_id")
            .withColumnRenamed("TEAM_NAME", "team_name")
            .withColumnRenamed("GP", "games_played")
            .withColumnRenamed("W", "wins")
            .withColumnRenamed("L", "losses")
            .withColumnRenamed("W_PCT", "win_percentage")
            .withColumnRenamed("MIN", "minutes_played")
            .withColumnRenamed("PTS_OFF_TOV", "points_off_turnovers")
            .withColumnRenamed("PTS_2ND_CHANCE", "points_second_chance")
            .withColumnRenamed("PTS_FB", "points_fast_break")
            .withColumnRenamed("PTS_PAINT", "points_in_paint")
            .withColumnRenamed("OPP_PTS_OFF_TOV", "opponent_points_off_turnovers")
            .withColumnRenamed("OPP_PTS_2ND_CHANCE", "opponent_points_second_chance")
            .withColumnRenamed("OPP_PTS_FB", "opponent_points_fast_break")
            .withColumnRenamed("OPP_PTS_PAINT", "opponent_points_in_paint")
            .withColumnRenamed("GP_RANK", "games_played_rank")
            .withColumnRenamed("W_RANK", "wins_rank")
            .withColumnRenamed("L_RANK", "losses_rank")
            .withColumnRenamed("W_PCT_RANK", "win_percentage_rank")
            .withColumnRenamed("MIN_RANK", "minutes_played_rank")
            .withColumnRenamed("PTS_OFF_TOV_RANK", "points_off_turnovers_rank")
            .withColumnRenamed("PTS_2ND_CHANCE_RANK", "points_second_chance_rank")
            .withColumnRenamed("PTS_FB_RANK", "points_fast_break_rank")
            .withColumnRenamed("PTS_PAINT_RANK", "points_in_paint_rank")
            .withColumnRenamed("OPP_PTS_OFF_TOV_RANK", "opponent_points_off_turnovers_rank")
            .withColumnRenamed("OPP_PTS_2ND_CHANCE_RANK", "opponent_points_second_chance_rank")
            .withColumnRenamed("OPP_PTS_FB_RANK", "opponent_points_fast_break_rank")
            .withColumnRenamed("OPP_PTS_PAINT_RANK", "opponent_points_in_paint_rank")
            .withColumnRenamed("Season", "season")
            .withColumnRenamed("SeasonType", "season_type")
            .withColumnRenamed("Month", "month")
            .withColumnRenamed("PerMode", "per_mode")
            .withColumnRenamed("LastNGames", "last_n_games")
        )

        repartitioned_team_df = renamed_team_df.repartition(25)

        # ----------------------------------------------------
        # 5) Process Player data: SCD + Aggregates
        # ----------------------------------------------------
        scd_and_aggregate_batch_player(repartitioned_player_df, spark)

        # ----------------------------------------------------
        # 6) Process Team data: SCD + Aggregates
        # ----------------------------------------------------
        scd_and_aggregate_batch_team(repartitioned_team_df, spark)

        logger.info("Batch read from Kafka (both player & team) + merges + aggregator completed.")

    except Exception as e:
        logger.error(f"Error in batch processing from Kafka: {e}", exc_info=True)
        sys.exit(1)

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main() -> None:
    """
    Main function to execute the pipeline in batch mode for
    NBA miscellaneous stats (both player and team).
    """
    try:
        spark = create_spark_connection()
        create_misc_tables(spark)          # Create (or recreate) the main SCD & fact tables
        create_cumulative_tables(spark)    # Create optional cumulative tables

        # Single run of reading from Kafka + merging + writing aggregator
        read_kafka_batch_and_process(spark)

        # Optionally update cumulative tables once data is populated
        update_cumulative_tables(spark)

        spark.stop()
        logger.info("NBA Misc Stats batch pipeline executed successfully.")
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        sys.exit(1)

# ----------------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    main()
