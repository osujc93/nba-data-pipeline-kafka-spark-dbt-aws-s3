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
    when,
    to_date
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
            SparkSession.builder.appName("NBA_Team_Boxscores_Batch")
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

        spark.sql("CREATE DATABASE IF NOT EXISTS NBA_team_boxscores")
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
    Creates necessary Iceberg tables in the Hive metastore for team boxscores,
    using the standardized (fully spelled-out, lowercase, underscored) column names.
    """
    try:
        # Drop old tables to ensure the new schema is correct
        for table_name in [
            "NBA_team_boxscores.team_boxscores",
            "NBA_team_boxscores.fact_team_boxscores",
            "NBA_team_boxscores.dim_teams",
        ]:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

        #
        # Main SCD table for team boxscores
        #
        # NOTE: We removed the line for `game_date_param STRING`
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_boxscores.team_boxscores (
                team_id INT,
                team_name STRING,
                team_abbreviation STRING,
                game_id STRING,
                game_date DATE,
                matchup STRING,
                wl STRING,
                minutes_played INT,
                field_goals_made INT,
                field_goals_attempted INT,
                field_goals_percentage DOUBLE,
                three_point_field_goals_made INT,
                three_point_field_goals_attempted INT,
                three_point_field_goals_percentage DOUBLE,
                free_throws_made INT,
                free_throws_attempted INT,
                free_throws_percentage DOUBLE,
                offensive_rebounds INT,
                defensive_rebounds INT,
                total_rebounds INT,
                assists INT,
                steals INT,
                blocks INT,
                turnovers INT,
                personal_fouls INT,
                points INT,
                plus_minus INT,
                video_available INT,
                season STRING,
                season_type STRING,
                record_start_date DATE,
                record_end_date DATE,
                is_current BOOLEAN,
                hash_val STRING
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        #
        # Dimension table for teams (SCD Type 2)
        #
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_boxscores.dim_teams (
                team_id INT,
                team_name STRING,
                team_abbreviation STRING,
                record_start_date DATE,
                record_end_date DATE,
                is_current BOOLEAN,
                hash_val STRING
            )
            USING ICEBERG
            PARTITIONED BY (team_id)
            """
        )

        #
        # Fact table for aggregated team boxscores
        #
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_boxscores.fact_team_boxscores (
                team_id INT,
                season STRING,
                plus_minus_bucket STRING,
                total_minutes_played BIGINT,
                total_field_goals_made BIGINT,
                total_field_goals_attempted BIGINT,
                avg_field_goals_percentage DOUBLE,
                total_three_point_field_goals_made BIGINT,
                total_three_point_field_goals_attempted BIGINT,
                avg_three_point_field_goals_percentage DOUBLE,
                total_free_throws_made BIGINT,
                total_free_throws_attempted BIGINT,
                avg_free_throws_percentage DOUBLE,
                total_offensive_rebounds BIGINT,
                total_defensive_rebounds BIGINT,
                total_rebounds BIGINT,
                total_assists BIGINT,
                total_steals BIGINT,
                total_blocks BIGINT,
                total_turnovers BIGINT,
                total_personal_fouls BIGINT,
                total_points BIGINT,
                avg_plus_minus DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season, team_id)
            """
        )

        logger.info("Team boxscore tables created successfully.")
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
    Performs SCD Type 2 merges into NBA_team_boxscores.team_boxscores,
    then aggregates & writes to NBA_team_boxscores.fact_team_boxscores in normal batch mode.
    Demonstrates a CASE WHEN approach to bucketize plus_minus.
    """
    record_count = df.count()
    logger.info(f"[Batch-Job] Dataset has {record_count} records to process in this run.")

    if record_count == 0:
        logger.info("[Batch-Job] No records to merge or aggregate.")
        return

    #
    # 1) Handle SCD merges into team_boxscores
    #
    # NOTE: We removed `"game_date_param"` from the compare_columns list
    handle_scd_type_2(
        df=df,
        spark=spark,
        table_name="NBA_team_boxscores.team_boxscores",
        join_columns=["team_id", "game_id", "season"],
        compare_columns=[
            "team_name",
            "team_abbreviation",
            "game_date",
            "matchup",
            "wl",
            "minutes_played",
            "field_goals_made",
            "field_goals_attempted",
            "field_goals_percentage",
            "three_point_field_goals_made",
            "three_point_field_goals_attempted",
            "three_point_field_goals_percentage",
            "free_throws_made",
            "free_throws_attempted",
            "free_throws_percentage",
            "offensive_rebounds",
            "defensive_rebounds",
            "total_rebounds",
            "assists",
            "steals",
            "blocks",
            "turnovers",
            "personal_fouls",
            "points",
            "plus_minus",
            "video_available",
            "season_type"
            # "game_date_param"  <-- REMOVED
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
    # 3) Aggregate and write to fact_team_boxscores in batch
    #
    fact_aggregated = (
        df_buckets.groupBy("team_id", "season", "plus_minus_bucket")
        .agg(
            spark_sum(col("minutes_played").cast("long")).alias("total_minutes_played"),
            spark_sum(col("field_goals_made").cast("long")).alias("total_field_goals_made"),
            spark_sum(col("field_goals_attempted").cast("long")).alias("total_field_goals_attempted"),
            avg("field_goals_percentage").alias("avg_field_goals_percentage"),
            spark_sum(col("three_point_field_goals_made").cast("long")).alias("total_three_point_field_goals_made"),
            spark_sum(col("three_point_field_goals_attempted").cast("long")).alias("total_three_point_field_goals_attempted"),
            avg("three_point_field_goals_percentage").alias("avg_three_point_field_goals_percentage"),
            spark_sum(col("free_throws_made").cast("long")).alias("total_free_throws_made"),
            spark_sum(col("free_throws_attempted").cast("long")).alias("total_free_throws_attempted"),
            avg("free_throws_percentage").alias("avg_free_throws_percentage"),
            spark_sum(col("offensive_rebounds").cast("long")).alias("total_offensive_rebounds"),
            spark_sum(col("defensive_rebounds").cast("long")).alias("total_defensive_rebounds"),
            spark_sum(col("total_rebounds").cast("long")).alias("total_rebounds"),
            spark_sum(col("assists").cast("long")).alias("total_assists"),
            spark_sum(col("steals").cast("long")).alias("total_steals"),
            spark_sum(col("blocks").cast("long")).alias("total_blocks"),
            spark_sum(col("turnovers").cast("long")).alias("total_turnovers"),
            spark_sum(col("personal_fouls").cast("long")).alias("total_personal_fouls"),
            spark_sum(col("points").cast("long")).alias("total_points"),
            avg("plus_minus").alias("avg_plus_minus"),
        )
    )

    fact_aggregated.write.format("iceberg") \
        .mode("append") \
        .save("spark_catalog.NBA_team_boxscores.fact_team_boxscores")

    logger.info("[Batch-Job] Wrote aggregated data to fact_team_boxscores.")

def create_cumulative_tables(spark: SparkSession) -> None:
    """
    (Optional) Creates additional cumulative Iceberg tables for any further
    slicing/dicing of the team boxscore data. Adjust as needed or remove if not required.
    """
    try:
        # Drop if needed
        spark.sql("DROP TABLE IF EXISTS NBA_team_boxscores.current_season_team_totals")
        spark.sql("DROP TABLE IF EXISTS NBA_team_boxscores.all_season_team_totals")

        #
        # Example: current season team totals
        #
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_boxscores.current_season_team_totals (
                season STRING,
                team_name STRING,
                total_points BIGINT,
                total_field_goals_made BIGINT,
                total_field_goals_attempted BIGINT
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        #
        # Example: all season team totals
        #
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS NBA_team_boxscores.all_season_team_totals (
                team_name STRING,
                total_points BIGINT,
                total_field_goals_made BIGINT,
                total_field_goals_attempted BIGINT
            )
            USING ICEBERG
            PARTITIONED BY (team_name)
            """
        )

        logger.info("Cumulative team tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating cumulative tables: {e}", exc_info=True)
        sys.exit(1)

def update_cumulative_tables(spark: SparkSession) -> None:
    """
    Example function to update the cumulative tables (current season, all seasons)
    with the latest data from team_boxscores.
    """
    try:
        # Example of how you might define "current_season"
        current_season_row = spark.sql(
            """
            SELECT MAX(CAST(SUBSTRING(season, 1, 4) AS INT)) AS current_season
            FROM NBA_team_boxscores.team_boxscores
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
                SUM(field_goals_made) AS total_field_goals_made,
                SUM(field_goals_attempted) AS total_field_goals_attempted
            FROM NBA_team_boxscores.team_boxscores
            WHERE CAST(SUBSTRING(season,1,4) AS INT) = {current_season}
              AND is_current = true
            GROUP BY season, team_name
            """
        )
        current_season_totals.write.format("iceberg") \
            .mode("overwrite") \
            .save("spark_catalog.NBA_team_boxscores.current_season_team_totals")

        #
        # all_season_team_totals
        #
        all_season_totals = spark.sql(
            """
            SELECT
                team_name,
                SUM(points) AS total_points,
                SUM(field_goals_made) AS total_field_goals_made,
                SUM(field_goals_attempted) AS total_field_goals_attempted
            FROM NBA_team_boxscores.team_boxscores
            WHERE is_current = true
            GROUP BY team_name
            """
        )
        all_season_totals.write.format("iceberg") \
            .mode("overwrite") \
            .save("spark_catalog.NBA_team_boxscores.all_season_team_totals")

        logger.info("Cumulative team tables updated successfully.")
    except Exception as e:
        logger.error(f"Error updating cumulative tables: {e}", exc_info=True)
        sys.exit(1)

def read_kafka_batch_and_process(spark: SparkSession) -> None:
    """
    Reads data from Kafka in a single batch (using startingOffsets=earliest, endingOffsets=latest),
    then merges into team_boxscores + writes aggregated results to fact_team_boxscores.
    """
    try:
        # 1) Define schema matching the *original* Kafka JSON fields from the example.
        schema = StructType([
            StructField("SEASON_ID", StringType(), True),
            StructField("TEAM_ID", IntegerType(), True),
            StructField("TEAM_ABBREVIATION", StringType(), True),
            StructField("TEAM_NAME", StringType(), True),
            StructField("GAME_ID", StringType(), True),
            StructField("GAME_DATE", StringType(), True),  # string in the source
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
            StructField("PLUS_MINUS", IntegerType(), True),
            StructField("VIDEO_AVAILABLE", IntegerType(), True),
            StructField("Season", StringType(), True),
            StructField("SeasonType", StringType(), True),
            StructField("DateFrom", StringType(), True),
            StructField("DateTo", StringType(), True),
        ])

        # 2) Do a single batch read from Kafka (topic: NBA_team_boxscores)
        kafka_batch_df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", ",".join(BOOTSTRAP_SERVERS))
            .option("subscribe", "NBA_team_boxscores")
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

        # 4) Rename columns from uppercase/abbrev to fully spelled-out, lowercase + underscores
        renamed_df = (
            parsed_df
            .withColumnRenamed("SEASON_ID", "season_id")
            .withColumnRenamed("TEAM_ID", "team_id")
            .withColumnRenamed("TEAM_ABBREVIATION", "team_abbreviation")
            .withColumnRenamed("TEAM_NAME", "team_name")
            .withColumnRenamed("GAME_ID", "game_id")
            .withColumnRenamed("GAME_DATE", "game_date")  # still string, we'll cast next
            .withColumnRenamed("MATCHUP", "matchup")
            .withColumnRenamed("WL", "wl")
            .withColumnRenamed("MIN", "minutes_played")
            .withColumnRenamed("FGM", "field_goals_made")
            .withColumnRenamed("FGA", "field_goals_attempted")
            .withColumnRenamed("FG_PCT", "field_goals_percentage")
            .withColumnRenamed("FG3M", "three_point_field_goals_made")
            .withColumnRenamed("FG3A", "three_point_field_goals_attempted")
            .withColumnRenamed("FG3_PCT", "three_point_field_goals_percentage")
            .withColumnRenamed("FTM", "free_throws_made")
            .withColumnRenamed("FTA", "free_throws_attempted")
            .withColumnRenamed("FT_PCT", "free_throws_percentage")
            .withColumnRenamed("OREB", "offensive_rebounds")
            .withColumnRenamed("DREB", "defensive_rebounds")
            .withColumnRenamed("REB", "total_rebounds")
            .withColumnRenamed("AST", "assists")
            .withColumnRenamed("STL", "steals")
            .withColumnRenamed("BLK", "blocks")
            .withColumnRenamed("TOV", "turnovers")
            .withColumnRenamed("PF", "personal_fouls")
            .withColumnRenamed("PTS", "points")
            .withColumnRenamed("PLUS_MINUS", "plus_minus")
            .withColumnRenamed("VIDEO_AVAILABLE", "video_available")
            .withColumnRenamed("Season", "season")
            .withColumnRenamed("SeasonType", "season_type")
            # Example: we might keep date_from/date_to or rename them
            .withColumnRenamed("DateFrom", "game_date_param_from")
            .withColumnRenamed("DateTo", "game_date_param_to")
        )

        # --------------------------------------------------------------------
        # FIX: Cast game_date from STRING -> DATE to match the Iceberg schema
        # --------------------------------------------------------------------
        casted_df = renamed_df.withColumn("game_date", to_date(col("game_date"), "yyyy-MM-dd"))

        # 5) Repartition to reduce shuffle overhead (optional tweak)
        repartitioned_df = casted_df.repartition(25)

        #
        # (Optional) Join with dim_teams to illustrate dimension usage
        #
        dim_teams_df = spark.read.table("NBA_team_boxscores.dim_teams")  # hypothetical dimension
        joined_df = repartitioned_df.join(dim_teams_df.hint("merge"), on="team_id", how="left")

        # Keep only the original measure columns from the Kafka data
        final_df = joined_df.select(repartitioned_df["*"])

        # Now run your SCD merges + aggregator in batch
        scd_and_aggregate_batch(final_df, spark)

        logger.info("Batch read from Kafka (team boxscores) + SCD merges + aggregator completed.")

    except Exception as e:
        logger.error(f"Error in batch processing from Kafka: {e}", exc_info=True)
        sys.exit(1)

def main() -> None:
    """
    Main function to execute the pipeline in batch mode for NBA team boxscores.
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
        logger.info("Batch pipeline executed successfully for NBA team boxscores.")
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
