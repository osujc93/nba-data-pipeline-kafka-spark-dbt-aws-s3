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
            SparkSession.builder.appName("NBA_Advanced_Player_Stats_Batch")
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
    Creates necessary Iceberg tables in the Hive metastore using the standardized
    (fully spelled-out, lowercase) column names.
    """
    try:
        # Drop old tables so new schema is correct
        for table_name in [
            "NBA_player_stats.advanced_player_stats",
            "NBA_player_stats.fact_advanced_player_stats",
            "NBA_player_stats.dim_players",
        ]:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.advanced_player_stats (
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
                estimated_offensive_rating DOUBLE,
                offensive_rating DOUBLE,
                sports_work_offensive_rating DOUBLE,
                estimated_defensive_rating DOUBLE,
                defensive_rating DOUBLE,
                sports_work_defensive_rating DOUBLE,
                estimated_net_rating DOUBLE,
                net_rating DOUBLE,
                sports_work_net_rating DOUBLE,
                assist_percentage DOUBLE,
                assist_turnover_ratio DOUBLE,
                assist_ratio DOUBLE,
                offensive_rebound_percentage DOUBLE,
                defensive_rebound_percentage DOUBLE,
                rebound_percentage DOUBLE,
                team_turnover_percentage DOUBLE,
                estimated_turnover_percentage DOUBLE,
                effective_field_goal_percentage DOUBLE,
                true_shooting_percentage DOUBLE,
                usage_percentage DOUBLE,
                estimated_usage_percentage DOUBLE,
                estimated_pace DOUBLE,
                pace DOUBLE,
                pace_per_40 DOUBLE,
                sports_work_pace DOUBLE,
                player_impact_estimate DOUBLE,
                possessions INT,
                field_goals_made INT,
                field_goals_attempted INT,
                field_goals_made_per_game DOUBLE,
                field_goals_attempted_per_game DOUBLE,
                field_goal_percentage DOUBLE,
                games_played_rank INT,
                wins_rank INT,
                losses_rank INT,
                win_percentage_rank INT,
                minutes_played_rank INT,
                estimated_offensive_rating_rank INT,
                offensive_rating_rank INT,
                sports_work_offensive_rating_rank INT,
                estimated_defensive_rating_rank INT,
                defensive_rating_rank INT,
                sports_work_defensive_rating_rank INT,
                estimated_net_rating_rank INT,
                net_rating_rank INT,
                sports_work_net_rating_rank INT,
                assist_percentage_rank INT,
                assist_turnover_ratio_rank INT,
                assist_ratio_rank INT,
                offensive_rebound_percentage_rank INT,
                defensive_rebound_percentage_rank INT,
                rebound_percentage_rank INT,
                team_turnover_percentage_rank INT,
                estimated_turnover_percentage_rank INT,
                effective_field_goal_percentage_rank INT,
                true_shooting_percentage_rank INT,
                usage_percentage_rank INT,
                estimated_usage_percentage_rank INT,
                estimated_pace_rank INT,
                pace_rank INT,
                sports_work_pace_rank INT,
                player_impact_estimate_rank INT,
                field_goals_made_rank INT,
                field_goals_attempted_rank INT,
                field_goals_made_per_game_rank INT,
                field_goals_attempted_per_game_rank INT,
                field_goal_percentage_rank INT,
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
            CREATE TABLE IF NOT EXISTS NBA_player_stats.fact_advanced_player_stats (
                player_id INT,
                season STRING,
                month INT,
                per_mode STRING,
                total_possessions BIGINT,
                total_player_impact_estimate DOUBLE,
                total_field_goals_made BIGINT,
                total_field_goals_attempted BIGINT,
                avg_estimated_offensive_rating DOUBLE,
                avg_offensive_rating DOUBLE,
                avg_sports_work_offensive_rating DOUBLE,
                avg_estimated_defensive_rating DOUBLE,
                avg_defensive_rating DOUBLE,
                avg_sports_work_defensive_rating DOUBLE,
                avg_estimated_net_rating DOUBLE,
                avg_net_rating DOUBLE,
                avg_sports_work_net_rating DOUBLE,
                avg_assist_percentage DOUBLE,
                avg_assist_turnover_ratio DOUBLE,
                avg_assist_ratio DOUBLE,
                avg_offensive_rebound_percentage DOUBLE,
                avg_defensive_rebound_percentage DOUBLE,
                avg_rebound_percentage DOUBLE,
                avg_team_turnover_percentage DOUBLE,
                avg_estimated_turnover_percentage DOUBLE,
                avg_effective_field_goal_percentage DOUBLE,
                avg_true_shooting_percentage DOUBLE,
                avg_usage_percentage DOUBLE,
                avg_estimated_usage_percentage DOUBLE,
                avg_estimated_pace DOUBLE,
                avg_pace DOUBLE,
                avg_pace_per_40 DOUBLE,
                avg_sports_work_pace DOUBLE,
                avg_field_goals_made_per_game DOUBLE,
                avg_field_goals_attempted_per_game DOUBLE,
                avg_field_goal_percentage DOUBLE,
                net_rating_bucket STRING
            )
            USING ICEBERG
            PARTITIONED BY (season, player_id)
            """
        )

        logger.info("Tables created successfully.")
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
    Performs SCD Type 2 merges into advanced_player_stats,
    then aggregates & writes to fact_advanced_player_stats in normal batch mode.
    Also demonstrates a CASE WHEN approach to bucketize net_rating.
    """
    record_count = df.count()
    logger.info(f"[Batch-Job] Dataset has {record_count} records to process in this run.")

    if record_count == 0:
        logger.info("[Batch-Job] No records to merge or aggregate.")
        return

    #
    # 1) Handle SCD merges into advanced_player_stats
    #
    handle_scd_type_2(
        df=df,
        spark=spark,
        table_name="NBA_player_stats.advanced_player_stats",
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
            "estimated_offensive_rating",
            "offensive_rating",
            "sports_work_offensive_rating",
            "estimated_defensive_rating",
            "defensive_rating",
            "sports_work_defensive_rating",
            "estimated_net_rating",
            "net_rating",
            "sports_work_net_rating",
            "assist_percentage",
            "assist_turnover_ratio",
            "assist_ratio",
            "offensive_rebound_percentage",
            "defensive_rebound_percentage",
            "rebound_percentage",
            "team_turnover_percentage",
            "estimated_turnover_percentage",
            "effective_field_goal_percentage",
            "true_shooting_percentage",
            "usage_percentage",
            "estimated_usage_percentage",
            "estimated_pace",
            "pace",
            "pace_per_40",
            "sports_work_pace",
            "player_impact_estimate",
            "possessions",
            "field_goals_made",
            "field_goals_attempted",
            "field_goals_made_per_game",
            "field_goals_attempted_per_game",
            "field_goal_percentage",
            "games_played_rank",
            "wins_rank",
            "losses_rank",
            "win_percentage_rank",
            "minutes_played_rank",
            "estimated_offensive_rating_rank",
            "offensive_rating_rank",
            "sports_work_offensive_rating_rank",
            "estimated_defensive_rating_rank",
            "defensive_rating_rank",
            "sports_work_defensive_rating_rank",
            "estimated_net_rating_rank",
            "net_rating_rank",
            "sports_work_net_rating_rank",
            "assist_percentage_rank",
            "assist_turnover_ratio_rank",
            "assist_ratio_rank",
            "offensive_rebound_percentage_rank",
            "defensive_rebound_percentage_rank",
            "rebound_percentage_rank",
            "team_turnover_percentage_rank",
            "estimated_turnover_percentage_rank",
            "effective_field_goal_percentage_rank",
            "true_shooting_percentage_rank",
            "usage_percentage_rank",
            "estimated_usage_percentage_rank",
            "estimated_pace_rank",
            "pace_rank",
            "sports_work_pace_rank",
            "player_impact_estimate_rank",
            "field_goals_made_rank",
            "field_goals_attempted_rank",
            "field_goals_made_per_game_rank",
            "field_goals_attempted_per_game_rank",
            "field_goal_percentage_rank",
        ],
    )

    #
    # 2) CASE WHEN bucketization for net_rating before aggregation
    #
    df_buckets = df.withColumn(
        "net_rating_bucket",
        when(col("net_rating") < -10, "LOW")
        .when((col("net_rating") >= -10) & (col("net_rating") <= 10), "MED")
        .otherwise("HIGH")
    )

    #
    # 3) Aggregate and write to fact_advanced_player_stats in batch
    #
    fact_aggregated = (
        df_buckets.groupBy("player_id", "season", "month", "per_mode", "net_rating_bucket")
        .agg(
            spark_sum(col("possessions").cast("long")).alias("total_possessions"),
            spark_sum(col("player_impact_estimate")).alias("total_player_impact_estimate"),
            spark_sum(col("field_goals_made").cast("long")).alias("total_field_goals_made"),
            spark_sum(col("field_goals_attempted").cast("long")).alias("total_field_goals_attempted"),
            avg("estimated_offensive_rating").alias("avg_estimated_offensive_rating"),
            avg("offensive_rating").alias("avg_offensive_rating"),
            avg("sports_work_offensive_rating").alias("avg_sports_work_offensive_rating"),
            avg("estimated_defensive_rating").alias("avg_estimated_defensive_rating"),
            avg("defensive_rating").alias("avg_defensive_rating"),
            avg("sports_work_defensive_rating").alias("avg_sports_work_defensive_rating"),
            avg("estimated_net_rating").alias("avg_estimated_net_rating"),
            avg("net_rating").alias("avg_net_rating"),
            avg("sports_work_net_rating").alias("avg_sports_work_net_rating"),
            avg("assist_percentage").alias("avg_assist_percentage"),
            avg("assist_turnover_ratio").alias("avg_assist_turnover_ratio"),
            avg("assist_ratio").alias("avg_assist_ratio"),
            avg("offensive_rebound_percentage").alias("avg_offensive_rebound_percentage"),
            avg("defensive_rebound_percentage").alias("avg_defensive_rebound_percentage"),
            avg("rebound_percentage").alias("avg_rebound_percentage"),
            avg("team_turnover_percentage").alias("avg_team_turnover_percentage"),
            avg("estimated_turnover_percentage").alias("avg_estimated_turnover_percentage"),
            avg("effective_field_goal_percentage").alias("avg_effective_field_goal_percentage"),
            avg("true_shooting_percentage").alias("avg_true_shooting_percentage"),
            avg("usage_percentage").alias("avg_usage_percentage"),
            avg("estimated_usage_percentage").alias("avg_estimated_usage_percentage"),
            avg("estimated_pace").alias("avg_estimated_pace"),
            avg("pace").alias("avg_pace"),
            avg("pace_per_40").alias("avg_pace_per_40"),
            avg("sports_work_pace").alias("avg_sports_work_pace"),
            avg("field_goals_made_per_game").alias("avg_field_goals_made_per_game"),
            avg("field_goals_attempted_per_game").alias("avg_field_goals_attempted_per_game"),
            avg("field_goal_percentage").alias("avg_field_goal_percentage"),
        )
    )

    fact_aggregated.write.format("iceberg") \
        .mode("append") \
        .save("spark_catalog.NBA_player_stats.fact_advanced_player_stats")

    logger.info("[Batch-Job] Wrote aggregated data to fact_advanced_player_stats.")

def create_cumulative_tables(spark: SparkSession) -> None:
    """
    Creates additional cumulative Iceberg tables (current season, all seasons, etc.)
    using the new standardized column names.
    """
    try:
        # OPTIONAL: Drop if needed
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.current_season_player_advanced_totals")
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.current_season_player_advanced_averages")
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.all_season_player_advanced_totals")
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.all_season_player_advanced_averages")
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.current_season_player_defensive_totals")
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.current_season_player_defensive_averages")
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.current_season_player_offensive_totals")
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.current_season_player_offensive_averages")
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.all_season_player_defensive_totals")
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.all_season_player_defensive_averages")
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.all_season_player_offensive_totals")
        spark.sql("DROP TABLE IF EXISTS NBA_player_stats.all_season_player_offensive_averages")

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.current_season_player_advanced_totals (
                season STRING,
                player_name STRING,
                total_possessions BIGINT,
                total_player_impact_estimate DOUBLE,
                total_field_goals_made BIGINT,
                total_field_goals_attempted BIGINT
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.current_season_player_advanced_averages (
                season STRING,
                player_name STRING,
                avg_estimated_offensive_rating DOUBLE,
                avg_offensive_rating DOUBLE,
                avg_sports_work_offensive_rating DOUBLE,
                avg_estimated_defensive_rating DOUBLE,
                avg_defensive_rating DOUBLE,
                avg_sports_work_defensive_rating DOUBLE,
                avg_estimated_net_rating DOUBLE,
                avg_net_rating DOUBLE,
                avg_sports_work_net_rating DOUBLE,
                avg_assist_percentage DOUBLE,
                avg_assist_turnover_ratio DOUBLE,
                avg_assist_ratio DOUBLE,
                avg_offensive_rebound_percentage DOUBLE,
                avg_defensive_rebound_percentage DOUBLE,
                avg_rebound_percentage DOUBLE,
                avg_team_turnover_percentage DOUBLE,
                avg_estimated_turnover_percentage DOUBLE,
                avg_effective_field_goal_percentage DOUBLE,
                avg_true_shooting_percentage DOUBLE,
                avg_usage_percentage DOUBLE,
                avg_estimated_usage_percentage DOUBLE,
                avg_estimated_pace DOUBLE,
                avg_pace DOUBLE,
                avg_pace_per_40 DOUBLE,
                avg_sports_work_pace DOUBLE,
                avg_field_goals_made_per_game DOUBLE,
                avg_field_goals_attempted_per_game DOUBLE,
                avg_field_goal_percentage DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.all_season_player_advanced_totals (
                player_name STRING,
                total_possessions BIGINT,
                total_player_impact_estimate DOUBLE,
                total_field_goals_made BIGINT,
                total_field_goals_attempted BIGINT
            )
            USING ICEBERG
            PARTITIONED BY (player_name)
            """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.all_season_player_advanced_averages (
                player_name STRING,
                avg_estimated_offensive_rating DOUBLE,
                avg_offensive_rating DOUBLE,
                avg_sports_work_offensive_rating DOUBLE,
                avg_estimated_defensive_rating DOUBLE,
                avg_defensive_rating DOUBLE,
                avg_sports_work_defensive_rating DOUBLE,
                avg_estimated_net_rating DOUBLE,
                avg_net_rating DOUBLE,
                avg_sports_work_net_rating DOUBLE,
                avg_assist_percentage DOUBLE,
                avg_assist_turnover_ratio DOUBLE,
                avg_assist_ratio DOUBLE,
                avg_offensive_rebound_percentage DOUBLE,
                avg_defensive_rebound_percentage DOUBLE,
                avg_rebound_percentage DOUBLE,
                avg_team_turnover_percentage DOUBLE,
                avg_estimated_turnover_percentage DOUBLE,
                avg_effective_field_goal_percentage DOUBLE,
                avg_true_shooting_percentage DOUBLE,
                avg_usage_percentage DOUBLE,
                avg_estimated_usage_percentage DOUBLE,
                avg_estimated_pace DOUBLE,
                avg_pace DOUBLE,
                avg_pace_per_40 DOUBLE,
                avg_sports_work_pace DOUBLE,
                avg_field_goals_made_per_game DOUBLE,
                avg_field_goals_attempted_per_game DOUBLE,
                avg_field_goal_percentage DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (player_name)
            """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.current_season_player_defensive_totals (
                season STRING,
                player_name STRING,
                total_possessions BIGINT
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.current_season_player_defensive_averages (
                season STRING,
                player_name STRING,
                avg_estimated_defensive_rating DOUBLE,
                avg_defensive_rating DOUBLE,
                avg_sports_work_defensive_rating DOUBLE,
                avg_defensive_rebound_percentage DOUBLE,
                avg_rebound_percentage DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.current_season_player_offensive_totals (
                season STRING,
                player_name STRING,
                total_field_goals_made BIGINT,
                total_field_goals_attempted BIGINT
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.current_season_player_offensive_averages (
                season STRING,
                player_name STRING,
                avg_estimated_offensive_rating DOUBLE,
                avg_offensive_rating DOUBLE,
                avg_sports_work_offensive_rating DOUBLE,
                avg_assist_percentage DOUBLE,
                avg_assist_turnover_ratio DOUBLE,
                avg_assist_ratio DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.all_season_player_defensive_totals (
                player_name STRING,
                total_possessions BIGINT
            )
            USING ICEBERG
            PARTITIONED BY (player_name)
            """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.all_season_player_defensive_averages (
                player_name STRING,
                avg_estimated_defensive_rating DOUBLE,
                avg_defensive_rating DOUBLE,
                avg_sports_work_defensive_rating DOUBLE,
                avg_defensive_rebound_percentage DOUBLE,
                avg_rebound_percentage DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (player_name)
            """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.all_season_player_offensive_totals (
                player_name STRING,
                total_field_goals_made BIGINT,
                total_field_goals_attempted BIGINT
            )
            USING ICEBERG
            PARTITIONED BY (player_name)
            """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_player_stats.all_season_player_offensive_averages (
                player_name STRING,
                avg_estimated_offensive_rating DOUBLE,
                avg_offensive_rating DOUBLE,
                avg_sports_work_offensive_rating DOUBLE,
                avg_assist_percentage DOUBLE,
                avg_assist_turnover_ratio DOUBLE,
                avg_assist_ratio DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (player_name)
            """
        )

        logger.info("Cumulative + Offensive/Defensive tables created successfully.")
    except Exception as e:
        logger.error(
            f"Error creating cumulative tables: {e}",
            exc_info=True,
        )
        sys.exit(1)

def update_cumulative_tables(spark: SparkSession) -> None:
    """
    Example function to update the cumulative + segregated Offensive/Defensive tables
    with the latest data from advanced_player_stats. 
    Adjust column references to your new standardized naming.
    """
    try:
        # Example of how you might define "current_season"
        current_season_row = spark.sql(
            """
            SELECT MAX(CAST(SUBSTRING(season, 1, 4) AS INT)) AS current_season
            FROM NBA_player_stats.advanced_player_stats
            """
        ).collect()[0]

        current_season = current_season_row["current_season"]
        if current_season is None:
            logger.warning("No current season found. Skipping cumulative tables update.")
            return

        # current_season_player_advanced_totals
        current_season_totals = spark.sql(
            f"""
            SELECT
                season AS season,
                player_name,
                SUM(possessions) AS total_possessions,
                SUM(player_impact_estimate) AS total_player_impact_estimate,
                SUM(field_goals_made) AS total_field_goals_made,
                SUM(field_goals_attempted) AS total_field_goals_attempted
            FROM NBA_player_stats.advanced_player_stats
            WHERE CAST(SUBSTRING(season,1,4) AS INT) = {current_season}
            GROUP BY season, player_name
            """
        )
        current_season_totals.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_player_stats.current_season_player_advanced_totals"
        )

        # current_season_player_advanced_averages
        current_season_averages = spark.sql(
            f"""
            SELECT
                season AS season,
                player_name,
                AVG(estimated_offensive_rating) AS avg_estimated_offensive_rating,
                AVG(offensive_rating) AS avg_offensive_rating,
                AVG(sports_work_offensive_rating) AS avg_sports_work_offensive_rating,
                AVG(estimated_defensive_rating) AS avg_estimated_defensive_rating,
                AVG(defensive_rating) AS avg_defensive_rating,
                AVG(sports_work_defensive_rating) AS avg_sports_work_defensive_rating,
                AVG(estimated_net_rating) AS avg_estimated_net_rating,
                AVG(net_rating) AS avg_net_rating,
                AVG(sports_work_net_rating) AS avg_sports_work_net_rating,
                AVG(assist_percentage) AS avg_assist_percentage,
                AVG(assist_turnover_ratio) AS avg_assist_turnover_ratio,
                AVG(assist_ratio) AS avg_assist_ratio,
                AVG(offensive_rebound_percentage) AS avg_offensive_rebound_percentage,
                AVG(defensive_rebound_percentage) AS avg_defensive_rebound_percentage,
                AVG(rebound_percentage) AS avg_rebound_percentage,
                AVG(team_turnover_percentage) AS avg_team_turnover_percentage,
                AVG(estimated_turnover_percentage) AS avg_estimated_turnover_percentage,
                AVG(effective_field_goal_percentage) AS avg_effective_field_goal_percentage,
                AVG(true_shooting_percentage) AS avg_true_shooting_percentage,
                AVG(usage_percentage) AS avg_usage_percentage,
                AVG(estimated_usage_percentage) AS avg_estimated_usage_percentage,
                AVG(estimated_pace) AS avg_estimated_pace,
                AVG(pace) AS avg_pace,
                AVG(pace_per_40) AS avg_pace_per_40,
                AVG(sports_work_pace) AS avg_sports_work_pace,
                AVG(field_goals_made_per_game) AS avg_field_goals_made_per_game,
                AVG(field_goals_attempted_per_game) AS avg_field_goals_attempted_per_game,
                AVG(field_goal_percentage) AS avg_field_goal_percentage
            FROM NBA_player_stats.advanced_player_stats
            WHERE CAST(SUBSTRING(season,1,4) AS INT) = {current_season}
            GROUP BY season, player_name
            """
        )
        current_season_averages.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_player_stats.current_season_player_advanced_averages"
        )

        # all_season_player_advanced_totals
        all_season_totals = spark.sql(
            """
            SELECT
                player_name,
                SUM(possessions) AS total_possessions,
                SUM(player_impact_estimate) AS total_player_impact_estimate,
                SUM(field_goals_made) AS total_field_goals_made,
                SUM(field_goals_attempted) AS total_field_goals_attempted
            FROM NBA_player_stats.advanced_player_stats
            GROUP BY player_name
            """
        )
        all_season_totals.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_player_stats.all_season_player_advanced_totals"
        )

        # all_season_player_advanced_averages
        all_season_averages = spark.sql(
            """
            SELECT
                player_name,
                AVG(estimated_offensive_rating) AS avg_estimated_offensive_rating,
                AVG(offensive_rating) AS avg_offensive_rating,
                AVG(sports_work_offensive_rating) AS avg_sports_work_offensive_rating,
                AVG(estimated_defensive_rating) AS avg_estimated_defensive_rating,
                AVG(defensive_rating) AS avg_defensive_rating,
                AVG(sports_work_defensive_rating) AS avg_sports_work_defensive_rating,
                AVG(estimated_net_rating) AS avg_estimated_net_rating,
                AVG(net_rating) AS avg_net_rating,
                AVG(sports_work_net_rating) AS avg_sports_work_net_rating,
                AVG(assist_percentage) AS avg_assist_percentage,
                AVG(assist_turnover_ratio) AS avg_assist_turnover_ratio,
                AVG(assist_ratio) AS avg_assist_ratio,
                AVG(offensive_rebound_percentage) AS avg_offensive_rebound_percentage,
                AVG(defensive_rebound_percentage) AS avg_defensive_rebound_percentage,
                AVG(rebound_percentage) AS avg_rebound_percentage,
                AVG(team_turnover_percentage) AS avg_team_turnover_percentage,
                AVG(estimated_turnover_percentage) AS avg_estimated_turnover_percentage,
                AVG(effective_field_goal_percentage) AS avg_effective_field_goal_percentage,
                AVG(true_shooting_percentage) AS avg_true_shooting_percentage,
                AVG(usage_percentage) AS avg_usage_percentage,
                AVG(estimated_usage_percentage) AS avg_estimated_usage_percentage,
                AVG(estimated_pace) AS avg_estimated_pace,
                AVG(pace) AS avg_pace,
                AVG(pace_per_40) AS avg_pace_per_40,
                AVG(sports_work_pace) AS avg_sports_work_pace,
                AVG(field_goals_made_per_game) AS avg_field_goals_made_per_game,
                AVG(field_goals_attempted_per_game) AS avg_field_goals_attempted_per_game,
                AVG(field_goal_percentage) AS avg_field_goal_percentage
            FROM NBA_player_stats.advanced_player_stats
            GROUP BY player_name
            """
        )
        all_season_averages.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_player_stats.all_season_player_advanced_averages"
        )

        # current_season_player_defensive_totals
        current_defensive_totals = spark.sql(
            f"""
            SELECT
                season AS season,
                player_name,
                SUM(possessions) AS total_possessions
            FROM NBA_player_stats.advanced_player_stats
            WHERE CAST(SUBSTRING(season,1,4) AS INT) = {current_season}
            GROUP BY season, player_name
            """
        )
        current_defensive_totals.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_player_stats.current_season_player_defensive_totals"
        )

        # current_season_player_defensive_averages
        current_defensive_averages = spark.sql(
            f"""
            SELECT
                season AS season,
                player_name,
                AVG(estimated_defensive_rating) AS avg_estimated_defensive_rating,
                AVG(defensive_rating) AS avg_defensive_rating,
                AVG(sports_work_defensive_rating) AS avg_sports_work_defensive_rating,
                AVG(defensive_rebound_percentage) AS avg_defensive_rebound_percentage,
                AVG(rebound_percentage) AS avg_rebound_percentage
            FROM NBA_player_stats.advanced_player_stats
            WHERE CAST(SUBSTRING(season,1,4) AS INT) = {current_season}
            GROUP BY season, player_name
            """
        )
        current_defensive_averages.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_player_stats.current_season_player_defensive_averages"
        )

        # current_season_player_offensive_totals
        current_offensive_totals = spark.sql(
            f"""
            SELECT
                season AS season,
                player_name,
                SUM(field_goals_made) AS total_field_goals_made,
                SUM(field_goals_attempted) AS total_field_goals_attempted
            FROM NBA_player_stats.advanced_player_stats
            WHERE CAST(SUBSTRING(season,1,4) AS INT) = {current_season}
            GROUP BY season, player_name
            """
        )
        current_offensive_totals.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_player_stats.current_season_player_offensive_totals"
        )

        # current_season_player_offensive_averages
        current_offensive_averages = spark.sql(
            f"""
            SELECT
                season AS season,
                player_name,
                AVG(estimated_offensive_rating) AS avg_estimated_offensive_rating,
                AVG(offensive_rating) AS avg_offensive_rating,
                AVG(sports_work_offensive_rating) AS avg_sports_work_offensive_rating,
                AVG(assist_percentage) AS avg_assist_percentage,
                AVG(assist_turnover_ratio) AS avg_assist_turnover_ratio,
                AVG(assist_ratio) AS avg_assist_ratio
            FROM NBA_player_stats.advanced_player_stats
            WHERE CAST(SUBSTRING(season,1,4) AS INT) = {current_season}
            GROUP BY season, player_name
            """
        )
        current_offensive_averages.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_player_stats.current_season_player_offensive_averages"
        )

        # all_season_player_defensive_totals
        all_season_defensive_totals = spark.sql(
            """
            SELECT
                player_name,
                SUM(possessions) AS total_possessions
            FROM NBA_player_stats.advanced_player_stats
            GROUP BY player_name
            """
        )
        all_season_defensive_totals.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_player_stats.all_season_player_defensive_totals"
        )

        # all_season_player_defensive_averages
        all_season_defensive_averages = spark.sql(
            """
            SELECT
                player_name,
                AVG(estimated_defensive_rating) AS avg_estimated_defensive_rating,
                AVG(defensive_rating) AS avg_defensive_rating,
                AVG(sports_work_defensive_rating) AS avg_sports_work_defensive_rating,
                AVG(defensive_rebound_percentage) AS avg_defensive_rebound_percentage,
                AVG(rebound_percentage) AS avg_rebound_percentage
            FROM NBA_player_stats.advanced_player_stats
            GROUP BY player_name
            """
        )
        all_season_defensive_averages.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_player_stats.all_season_player_defensive_averages"
        )

        # all_season_player_offensive_totals
        all_season_offensive_totals = spark.sql(
            """
            SELECT
                player_name,
                SUM(field_goals_made) AS total_field_goals_made,
                SUM(field_goals_attempted) AS total_field_goals_attempted
            FROM NBA_player_stats.advanced_player_stats
            GROUP BY player_name
            """
        )
        all_season_offensive_totals.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_player_stats.all_season_player_offensive_totals"
        )

        # all_season_player_offensive_averages
        all_season_offensive_averages = spark.sql(
            """
            SELECT
                player_name,
                AVG(estimated_offensive_rating) AS avg_estimated_offensive_rating,
                AVG(offensive_rating) AS avg_offensive_rating,
                AVG(sports_work_offensive_rating) AS avg_sports_work_offensive_rating,
                AVG(assist_percentage) AS avg_assist_percentage,
                AVG(assist_turnover_ratio) AS avg_assist_turnover_ratio,
                AVG(assist_ratio) AS avg_assist_ratio
            FROM NBA_player_stats.advanced_player_stats
            GROUP BY player_name
            """
        )
        all_season_offensive_averages.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_player_stats.all_season_player_offensive_averages"
        )

        logger.info("Cumulative + Offensive/Defensive tables updated successfully.")
    except Exception as e:
        logger.error(f"Error updating cumulative tables: {e}", exc_info=True)
        sys.exit(1)

def read_kafka_batch_and_process(spark: SparkSession) -> None:
    """
    Reads data from Kafka in a single batch (using startingOffsets=earliest, endingOffsets=latest),
    then merges into advanced_player_stats + writes aggregated results to fact_advanced_player_stats.
    """
    try:
        # 1) Define schema matching the *original* Kafka JSON fields (uppercase).
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
            StructField("E_OFF_RATING", DoubleType(), True),
            StructField("OFF_RATING", DoubleType(), True),
            StructField("sp_work_OFF_RATING", DoubleType(), True),
            StructField("E_DEF_RATING", DoubleType(), True),
            StructField("DEF_RATING", DoubleType(), True),
            StructField("sp_work_DEF_RATING", DoubleType(), True),
            StructField("E_NET_RATING", DoubleType(), True),
            StructField("NET_RATING", DoubleType(), True),
            StructField("sp_work_NET_RATING", DoubleType(), True),
            StructField("AST_PCT", DoubleType(), True),
            StructField("AST_TO", DoubleType(), True),
            StructField("AST_RATIO", DoubleType(), True),
            StructField("OREB_PCT", DoubleType(), True),
            StructField("DREB_PCT", DoubleType(), True),
            StructField("REB_PCT", DoubleType(), True),
            StructField("TM_TOV_PCT", DoubleType(), True),
            StructField("E_TOV_PCT", DoubleType(), True),
            StructField("EFG_PCT", DoubleType(), True),
            StructField("TS_PCT", DoubleType(), True),
            StructField("USG_PCT", DoubleType(), True),
            StructField("E_USG_PCT", DoubleType(), True),
            StructField("E_PACE", DoubleType(), True),
            StructField("PACE", DoubleType(), True),
            StructField("PACE_PER40", DoubleType(), True),
            StructField("sp_work_PACE", DoubleType(), True),
            StructField("PIE", DoubleType(), True),
            StructField("POSS", IntegerType(), True),
            StructField("FGM", IntegerType(), True),
            StructField("FGA", IntegerType(), True),
            StructField("FGM_PG", DoubleType(), True),
            StructField("FGA_PG", DoubleType(), True),
            StructField("FG_PCT", DoubleType(), True),
            StructField("GP_RANK", IntegerType(), True),
            StructField("W_RANK", IntegerType(), True),
            StructField("L_RANK", IntegerType(), True),
            StructField("W_PCT_RANK", IntegerType(), True),
            StructField("MIN_RANK", IntegerType(), True),
            StructField("E_OFF_RATING_RANK", IntegerType(), True),
            StructField("OFF_RATING_RANK", IntegerType(), True),
            StructField("sp_work_OFF_RATING_RANK", IntegerType(), True),
            StructField("E_DEF_RATING_RANK", IntegerType(), True),
            StructField("DEF_RATING_RANK", IntegerType(), True),
            StructField("sp_work_DEF_RATING_RANK", IntegerType(), True),
            StructField("E_NET_RATING_RANK", IntegerType(), True),
            StructField("NET_RATING_RANK", IntegerType(), True),
            StructField("sp_work_NET_RATING_RANK", IntegerType(), True),
            StructField("AST_PCT_RANK", IntegerType(), True),
            StructField("AST_TO_RANK", IntegerType(), True),
            StructField("AST_RATIO_RANK", IntegerType(), True),
            StructField("OREB_PCT_RANK", IntegerType(), True),
            StructField("DREB_PCT_RANK", IntegerType(), True),
            StructField("REB_PCT_RANK", IntegerType(), True),
            StructField("TM_TOV_PCT_RANK", IntegerType(), True),
            StructField("E_TOV_PCT_RANK", IntegerType(), True),
            StructField("EFG_PCT_RANK", IntegerType(), True),
            StructField("TS_PCT_RANK", IntegerType(), True),
            StructField("USG_PCT_RANK", IntegerType(), True),
            StructField("E_USG_PCT_RANK", IntegerType(), True),
            StructField("E_PACE_RANK", IntegerType(), True),
            StructField("PACE_RANK", IntegerType(), True),
            StructField("sp_work_PACE_RANK", IntegerType(), True),
            StructField("PIE_RANK", IntegerType(), True),
            StructField("FGM_RANK", IntegerType(), True),
            StructField("FGA_RANK", IntegerType(), True),
            StructField("FGM_PG_RANK", IntegerType(), True),
            StructField("FGA_PG_RANK", IntegerType(), True),
            StructField("FG_PCT_RANK", IntegerType(), True),
            StructField("Season", StringType(), True),
            StructField("SeasonType", StringType(), True),
            StructField("Month", IntegerType(), True),
            StructField("PerMode", StringType(), True),
        ])

        # 2) Do a single batch read from Kafka
        kafka_batch_df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", ",".join(BOOTSTRAP_SERVERS))
            .option("subscribe", "NBA_advanced_player_stats")
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        # 3) Parse the JSON messages
        parsed_df = (
            kafka_batch_df
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*")
        )

        # 4) Rename columns from uppercase/abbrev to fully spelled-out lowercase
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
            .withColumnRenamed("E_OFF_RATING", "estimated_offensive_rating")
            .withColumnRenamed("OFF_RATING", "offensive_rating")
            .withColumnRenamed("sp_work_OFF_RATING", "sports_work_offensive_rating")
            .withColumnRenamed("E_DEF_RATING", "estimated_defensive_rating")
            .withColumnRenamed("DEF_RATING", "defensive_rating")
            .withColumnRenamed("sp_work_DEF_RATING", "sports_work_defensive_rating")
            .withColumnRenamed("E_NET_RATING", "estimated_net_rating")
            .withColumnRenamed("NET_RATING", "net_rating")
            .withColumnRenamed("sp_work_NET_RATING", "sports_work_net_rating")
            .withColumnRenamed("AST_PCT", "assist_percentage")
            .withColumnRenamed("AST_TO", "assist_turnover_ratio")
            .withColumnRenamed("AST_RATIO", "assist_ratio")
            .withColumnRenamed("OREB_PCT", "offensive_rebound_percentage")
            .withColumnRenamed("DREB_PCT", "defensive_rebound_percentage")
            .withColumnRenamed("REB_PCT", "rebound_percentage")
            .withColumnRenamed("TM_TOV_PCT", "team_turnover_percentage")
            .withColumnRenamed("E_TOV_PCT", "estimated_turnover_percentage")
            .withColumnRenamed("EFG_PCT", "effective_field_goal_percentage")
            .withColumnRenamed("TS_PCT", "true_shooting_percentage")
            .withColumnRenamed("USG_PCT", "usage_percentage")
            .withColumnRenamed("E_USG_PCT", "estimated_usage_percentage")
            .withColumnRenamed("E_PACE", "estimated_pace")
            .withColumnRenamed("PACE", "pace")
            .withColumnRenamed("PACE_PER40", "pace_per_40")
            .withColumnRenamed("sp_work_PACE", "sports_work_pace")
            .withColumnRenamed("PIE", "player_impact_estimate")
            .withColumnRenamed("POSS", "possessions")
            .withColumnRenamed("FGM", "field_goals_made")
            .withColumnRenamed("FGA", "field_goals_attempted")
            .withColumnRenamed("FGM_PG", "field_goals_made_per_game")
            .withColumnRenamed("FGA_PG", "field_goals_attempted_per_game")
            .withColumnRenamed("FG_PCT", "field_goal_percentage")
            .withColumnRenamed("GP_RANK", "games_played_rank")
            .withColumnRenamed("W_RANK", "wins_rank")
            .withColumnRenamed("L_RANK", "losses_rank")
            .withColumnRenamed("W_PCT_RANK", "win_percentage_rank")
            .withColumnRenamed("MIN_RANK", "minutes_played_rank")
            .withColumnRenamed("E_OFF_RATING_RANK", "estimated_offensive_rating_rank")
            .withColumnRenamed("OFF_RATING_RANK", "offensive_rating_rank")
            .withColumnRenamed("sp_work_OFF_RATING_RANK", "sports_work_offensive_rating_rank")
            .withColumnRenamed("E_DEF_RATING_RANK", "estimated_defensive_rating_rank")
            .withColumnRenamed("DEF_RATING_RANK", "defensive_rating_rank")
            .withColumnRenamed("sp_work_DEF_RATING_RANK", "sports_work_defensive_rating_rank")
            .withColumnRenamed("E_NET_RATING_RANK", "estimated_net_rating_rank")
            .withColumnRenamed("NET_RATING_RANK", "net_rating_rank")
            .withColumnRenamed("sp_work_NET_RATING_RANK", "sports_work_net_rating_rank")
            .withColumnRenamed("AST_PCT_RANK", "assist_percentage_rank")
            .withColumnRenamed("AST_TO_RANK", "assist_turnover_ratio_rank")
            .withColumnRenamed("AST_RATIO_RANK", "assist_ratio_rank")
            .withColumnRenamed("OREB_PCT_RANK", "offensive_rebound_percentage_rank")
            .withColumnRenamed("DREB_PCT_RANK", "defensive_rebound_percentage_rank")
            .withColumnRenamed("REB_PCT_RANK", "rebound_percentage_rank")
            .withColumnRenamed("TM_TOV_PCT_RANK", "team_turnover_percentage_rank")
            .withColumnRenamed("E_TOV_PCT_RANK", "estimated_turnover_percentage_rank")
            .withColumnRenamed("EFG_PCT_RANK", "effective_field_goal_percentage_rank")
            .withColumnRenamed("TS_PCT_RANK", "true_shooting_percentage_rank")
            .withColumnRenamed("USG_PCT_RANK", "usage_percentage_rank")
            .withColumnRenamed("E_USG_PCT_RANK", "estimated_usage_percentage_rank")
            .withColumnRenamed("E_PACE_RANK", "estimated_pace_rank")
            .withColumnRenamed("PACE_RANK", "pace_rank")
            .withColumnRenamed("sp_work_PACE_RANK", "sports_work_pace_rank")
            .withColumnRenamed("PIE_RANK", "player_impact_estimate_rank")
            .withColumnRenamed("FGM_RANK", "field_goals_made_rank")
            .withColumnRenamed("FGA_RANK", "field_goals_attempted_rank")
            .withColumnRenamed("FGM_PG_RANK", "field_goals_made_per_game_rank")
            .withColumnRenamed("FGA_PG_RANK", "field_goals_attempted_per_game_rank")
            .withColumnRenamed("FG_PCT_RANK", "field_goal_percentage_rank")
            .withColumnRenamed("Season", "season")
            .withColumnRenamed("SeasonType", "season_type")
            .withColumnRenamed("Month", "month")
            .withColumnRenamed("PerMode", "per_mode")
        )

        # 5) Repartition to reduce shuffle overhead
        repartitioned_df = renamed_df.repartition(25)

        # (Optional) Join with dim_players for demonstration; rename your columns as needed
        dim_players_df = spark.read.table("NBA_player_stats.dim_players")  # hypothetical dimension
        joined_df = repartitioned_df.join(dim_players_df.hint("merge"), on="player_id", how="left")

        # Keep only the original measure columns from the Kafka data
        final_df = joined_df.select(repartitioned_df["*"])

        # Now run your SCD merges + aggregator in batch
        scd_and_aggregate_batch(final_df, spark)

        logger.info("Batch read from Kafka + SCD merges + aggregator completed.")

        # Example: You can optionally invoke Iceberg maintenance here:
        # spark.sql("CALL spark_catalog.system.rewrite_data_files(table => 'NBA_player_stats.advanced_player_stats')")

    except Exception as e:
        logger.error(f"Error in batch processing from Kafka: {e}", exc_info=True)
        sys.exit(1)

def main() -> None:
    """
    Main function to execute the pipeline in batch mode.
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
        logger.info("Batch pipeline executed successfully.")
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
