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
            SparkSession.builder.appName("NBA_Traditional_Team_Stats_Batch")
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

        spark.sql("CREATE DATABASE IF NOT EXISTS NBA_team_stats")
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
    Creates necessary Iceberg tables in the Hive metastore for NBA traditional team stats
    using fully spelled-out, lowercase column names with underscores.
    """
    try:
        # Drop old tables so new schema is correct
        for table_name in [
            "NBA_team_stats.traditional_team_stats",
            "NBA_team_stats.fact_traditional_team_stats",
            "NBA_team_stats.dim_teams",
        ]:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

        # Main SCD table for traditional team stats
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_stats.traditional_team_stats (
                team_id INT,
                team_name STRING,

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
                rebounds DOUBLE,

                assists DOUBLE,
                turnovers DOUBLE,
                steals DOUBLE,
                blocks DOUBLE,
                blocks_against DOUBLE,

                personal_fouls DOUBLE,
                personal_fouls_drawn DOUBLE,
                points DOUBLE,
                plus_minus DOUBLE,

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
                rebounds_rank INT,
                assists_rank INT,
                turnovers_rank INT,
                steals_rank INT,
                blocks_rank INT,
                blocks_against_rank INT,
                personal_fouls_rank INT,
                personal_fouls_drawn_rank INT,
                points_rank INT,
                plus_minus_rank INT,

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

        # Dim table for teams (Type 2 SCD)
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

        # Fact table for aggregated traditional team stats
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_stats.fact_traditional_team_stats (
                team_id INT,
                season STRING,
                month INT,
                per_mode STRING,
                plus_minus_bucket STRING,

                total_games_played BIGINT,
                total_points DOUBLE,
                total_rebounds DOUBLE,
                total_field_goals_made DOUBLE,
                total_field_goals_attempted DOUBLE,
                total_three_pointers_made DOUBLE,
                total_three_pointers_attempted DOUBLE,
                total_free_throws_made DOUBLE,
                total_free_throws_attempted DOUBLE,

                avg_win_percentage DOUBLE,
                avg_plus_minus DOUBLE,
                avg_field_goal_percentage DOUBLE,
                avg_three_point_percentage DOUBLE,
                avg_free_throw_percentage DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season, team_id)
            """
        )

        logger.info("Team-based tables created successfully.")
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

        # 3) Create or replace a GLOBAL temp view for MERGE usage
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

def scd_and_aggregate_team_stats(df: DataFrame, spark: SparkSession) -> None:
    """
    Performs SCD Type 2 merges into NBA_team_stats.traditional_team_stats,
    then aggregates & writes to NBA_team_stats.fact_traditional_team_stats in batch mode.
    Demonstrates a CASE WHEN approach to bucketize plus_minus.
    """
    record_count = df.count()
    logger.info(f"[Batch-Job] Dataset has {record_count} records to process in this run.")

    if record_count == 0:
        logger.info("[Batch-Job] No records to merge or aggregate.")
        return

    #
    # 1) Handle SCD merges into NBA_team_stats.traditional_team_stats
    #
    handle_scd_type_2(
        df=df,
        spark=spark,
        table_name="NBA_team_stats.traditional_team_stats",
        join_columns=["team_id", "season", "per_mode", "month"],
        compare_columns=[
            "team_name",
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
            "rebounds",
            "assists",
            "turnovers",
            "steals",
            "blocks",
            "blocks_against",
            "personal_fouls",
            "personal_fouls_drawn",
            "points",
            "plus_minus",
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
            "rebounds_rank",
            "assists_rank",
            "turnovers_rank",
            "steals_rank",
            "blocks_rank",
            "blocks_against_rank",
            "personal_fouls_rank",
            "personal_fouls_drawn_rank",
            "points_rank",
            "plus_minus_rank",
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
    # 3) Aggregate and write to fact_traditional_team_stats in batch
    #
    fact_aggregated = (
        df_buckets.groupBy("team_id", "season", "month", "per_mode", "plus_minus_bucket")
        .agg(
            spark_sum(col("games_played").cast("long")).alias("total_games_played"),
            spark_sum(col("points").cast("double")).alias("total_points"),
            spark_sum(col("rebounds").cast("double")).alias("total_rebounds"),
            spark_sum(col("field_goals_made").cast("double")).alias("total_field_goals_made"),
            spark_sum(col("field_goals_attempted").cast("double")).alias("total_field_goals_attempted"),
            spark_sum(col("three_pointers_made").cast("double")).alias("total_three_pointers_made"),
            spark_sum(col("three_pointers_attempted").cast("double")).alias("total_three_pointers_attempted"),
            spark_sum(col("free_throws_made").cast("double")).alias("total_free_throws_made"),
            spark_sum(col("free_throws_attempted").cast("double")).alias("total_free_throws_attempted"),

            avg("win_percentage").alias("avg_win_percentage"),
            avg("plus_minus").alias("avg_plus_minus"),
            avg("field_goal_percentage").alias("avg_field_goal_percentage"),
            avg("three_point_percentage").alias("avg_three_point_percentage"),
            avg("free_throw_percentage").alias("avg_free_throw_percentage"),
        )
    )

    fact_aggregated.write.format("iceberg") \
        .mode("append") \
        .save("spark_catalog.NBA_team_stats.fact_traditional_team_stats")

    logger.info("[Batch-Job] Wrote aggregated data to fact_traditional_team_stats.")

def create_cumulative_tables(spark: SparkSession) -> None:
    """
    Creates example cumulative Iceberg tables for team-based data, if desired.
    Keeping overall structure for demonstration but adjusted to team stats.
    """
    try:
        # OPTIONAL: Drop if needed
        spark.sql("DROP TABLE IF EXISTS NBA_team_stats.current_season_team_totals")
        spark.sql("DROP TABLE IF EXISTS NBA_team_stats.current_season_team_averages")
        spark.sql("DROP TABLE IF EXISTS NBA_team_stats.all_season_team_totals")
        spark.sql("DROP TABLE IF EXISTS NBA_team_stats.all_season_team_averages")

        #
        # Example basic cumulative tables for demonstration
        #
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_stats.current_season_team_totals (
                season STRING,
                team_name STRING,
                total_points DOUBLE,
                total_rebounds DOUBLE,
                total_field_goals_made DOUBLE,
                total_field_goals_attempted DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_stats.current_season_team_averages (
                season STRING,
                team_name STRING,
                avg_win_percentage DOUBLE,
                avg_field_goal_percentage DOUBLE,
                avg_three_point_percentage DOUBLE,
                avg_free_throw_percentage DOUBLE,
                avg_plus_minus DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_stats.all_season_team_totals (
                team_name STRING,
                total_points DOUBLE,
                total_rebounds DOUBLE,
                total_field_goals_made DOUBLE,
                total_field_goals_attempted DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (team_name)
            """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_stats.all_season_team_averages (
                team_name STRING,
                avg_win_percentage DOUBLE,
                avg_field_goal_percentage DOUBLE,
                avg_three_point_percentage DOUBLE,
                avg_free_throw_percentage DOUBLE,
                avg_plus_minus DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (team_name)
            """
        )

        logger.info("Cumulative team-based tables created successfully.")
    except Exception as e:
        logger.error(
            f"Error creating cumulative tables: {e}",
            exc_info=True,
        )
        sys.exit(1)

def update_cumulative_tables(spark: SparkSession) -> None:
    """
    Example function to update the cumulative tables with the latest data 
    from NBA_team_stats.traditional_team_stats. Adjust as desired for production.
    """
    try:
        # Identify current season by largest year
        current_season_row = spark.sql(
            """
            SELECT MAX(CAST(SUBSTRING(season, 1, 4) AS INT)) AS current_season
            FROM NBA_team_stats.traditional_team_stats
            """
        ).collect()[0]

        current_season = current_season_row["current_season"]
        if current_season is None:
            logger.warning("No current season found. Skipping cumulative tables update.")
            return

        #
        # current_season_team_totals
        #
        current_season_totals = spark.sql(
            f"""
            SELECT
                season,
                team_name,
                SUM(points) AS total_points,
                SUM(rebounds) AS total_rebounds,
                SUM(field_goals_made) AS total_field_goals_made,
                SUM(field_goals_attempted) AS total_field_goals_attempted
            FROM NBA_team_stats.traditional_team_stats
            WHERE CAST(SUBSTRING(season, 1, 4) AS INT) = {current_season}
            GROUP BY season, team_name
            """
        )
        current_season_totals.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_team_stats.current_season_team_totals"
        )

        #
        # current_season_team_averages
        #
        current_season_averages = spark.sql(
            f"""
            SELECT
                season,
                team_name,
                AVG(win_percentage) AS avg_win_percentage,
                AVG(field_goal_percentage) AS avg_field_goal_percentage,
                AVG(three_point_percentage) AS avg_three_point_percentage,
                AVG(free_throw_percentage) AS avg_free_throw_percentage,
                AVG(plus_minus) AS avg_plus_minus
            FROM NBA_team_stats.traditional_team_stats
            WHERE CAST(SUBSTRING(season, 1, 4) AS INT) = {current_season}
            GROUP BY season, team_name
            """
        )
        current_season_averages.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_team_stats.current_season_team_averages"
        )

        #
        # all_season_team_totals
        #
        all_season_totals = spark.sql(
            """
            SELECT
                team_name,
                SUM(points) AS total_points,
                SUM(rebounds) AS total_rebounds,
                SUM(field_goals_made) AS total_field_goals_made,
                SUM(field_goals_attempted) AS total_field_goals_attempted
            FROM NBA_team_stats.traditional_team_stats
            GROUP BY team_name
            """
        )
        all_season_totals.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_team_stats.all_season_team_totals"
        )

        #
        # all_season_team_averages
        #
        all_season_averages = spark.sql(
            """
            SELECT
                team_name,
                AVG(win_percentage) AS avg_win_percentage,
                AVG(field_goal_percentage) AS avg_field_goal_percentage,
                AVG(three_point_percentage) AS avg_three_point_percentage,
                AVG(free_throw_percentage) AS avg_free_throw_percentage,
                AVG(plus_minus) AS avg_plus_minus
            FROM NBA_team_stats.traditional_team_stats
            GROUP BY team_name
            """
        )
        all_season_averages.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.NBA_team_stats.all_season_team_averages"
        )

        logger.info("Cumulative team-based tables updated successfully.")
    except Exception as e:
        logger.error(f"Error updating cumulative tables: {e}", exc_info=True)
        sys.exit(1)

def read_kafka_batch_and_process(spark: SparkSession) -> None:
    """
    Reads NBA traditional team stats data from Kafka in a single batch
    (startingOffsets=earliest, endingOffsets=latest), then merges into 
    NBA_team_stats.traditional_team_stats + writes aggregated results to fact_traditional_team_stats.
    """
    try:
        # 1) Define schema matching the *original* Kafka JSON fields (uppercase).
        #    This matches the example messages for NBA team stats.
        schema = StructType([
            StructField("TEAM_ID", IntegerType(), True),
            StructField("TEAM_NAME", StringType(), True),
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
            .option("subscribe", "NBA_traditional_team_stats")
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

        # 4) Rename columns from uppercase/abbrev to fully spelled-out lowercase with underscores
        renamed_df = (
            parsed_df
            .withColumnRenamed("TEAM_ID", "team_id")
            .withColumnRenamed("TEAM_NAME", "team_name")
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
            .withColumnRenamed("REB", "rebounds")
            .withColumnRenamed("AST", "assists")
            .withColumnRenamed("TOV", "turnovers")
            .withColumnRenamed("STL", "steals")
            .withColumnRenamed("BLK", "blocks")
            .withColumnRenamed("BLKA", "blocks_against")
            .withColumnRenamed("PF", "personal_fouls")
            .withColumnRenamed("PFD", "personal_fouls_drawn")
            .withColumnRenamed("PTS", "points")
            .withColumnRenamed("PLUS_MINUS", "plus_minus")

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
            .withColumnRenamed("REB_RANK", "rebounds_rank")
            .withColumnRenamed("AST_RANK", "assists_rank")
            .withColumnRenamed("TOV_RANK", "turnovers_rank")
            .withColumnRenamed("STL_RANK", "steals_rank")
            .withColumnRenamed("BLK_RANK", "blocks_rank")
            .withColumnRenamed("BLKA_RANK", "blocks_against_rank")
            .withColumnRenamed("PF_RANK", "personal_fouls_rank")
            .withColumnRenamed("PFD_RANK", "personal_fouls_drawn_rank")
            .withColumnRenamed("PTS_RANK", "points_rank")
            .withColumnRenamed("PLUS_MINUS_RANK", "plus_minus_rank")

            .withColumnRenamed("Season", "season")
            .withColumnRenamed("SeasonType", "season_type")
            .withColumnRenamed("Month", "month")
            .withColumnRenamed("PerMode", "per_mode")
        )

        # 5) Repartition to reduce shuffle overhead
        repartitioned_df = renamed_df.repartition(25)

        # (Optional) Join with dim_teams if you wish to do SCD merges for the dimension
        #   We'll show the approach below. Suppose we keep a dim_teams table for Type 2 as well.
        dim_teams_df = spark.read.table("NBA_team_stats.dim_teams")  # hypothetical dimension
        joined_df = repartitioned_df.join(
            dim_teams_df.hint("merge"), on="team_id", how="left"
        )

        # Keep only the original measure columns from the Kafka data (avoid duplicates from the join)
        # If you do want to SCD update dim_teams, do it similarly with handle_scd_type_2, etc.
        final_df = joined_df.select(repartitioned_df["*"])

        # Now run your SCD merges + aggregator in batch
        scd_and_aggregate_team_stats(final_df, spark)

        logger.info("Batch read from Kafka + SCD merges + aggregator completed.")

        # Example: You can optionally invoke Iceberg maintenance here:
        # spark.sql("CALL spark_catalog.system.rewrite_data_files(table => 'NBA_team_stats.traditional_team_stats')")

    except Exception as e:
        logger.error(f"Error in batch processing from Kafka: {e}", exc_info=True)
        sys.exit(1)

def main() -> None:
    """
    Main function to execute the pipeline in batch mode for NBA traditional team stats.
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
        logger.info("Batch pipeline executed successfully for NBA traditional team stats.")
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
