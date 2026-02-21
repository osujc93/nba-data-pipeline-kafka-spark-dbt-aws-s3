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
            SparkSession.builder.appName("NBA_Regular_Season_Standings_Batch")
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

        spark.sql("CREATE DATABASE IF NOT EXISTS nba_db")
        logger.info("Spark connection (batch mode) created successfully.")

        # Example: setting shuffle partitions
        spark.conf.set("spark.sql.shuffle.partitions", 25)
        # Example: disabling adaptive execution if you want consistent partitioning
        spark.conf.set("spark.sql.adaptive.enabled", "false")

        return spark
    except Exception as e:
        logger.error(f"Error creating Spark connection: {e}", exc_info=True)
        sys.exit(1)

def create_tables(spark: SparkSession) -> None:
    """
    Creates necessary Iceberg tables in the Hive metastore for NBA regular season standings.
    Adjusted to use fully spelled-out, lowercase column names with underscores.
    """
    try:
        # Drop old tables if they exist, so the new schema is correct
        for table_name in [
            "nba_db.nba_regular_season_standings",
            "nba_db.fact_nba_regular_season_standings",
            "nba_db.dim_teams",
        ]:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

        # Main SCD table
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS nba_db.nba_regular_season_standings (
                league_id STRING,
                season_id STRING,
                team_id INT,
                team_city STRING,
                team_name STRING,
                team_slug STRING,
                conference STRING,
                conference_record STRING,
                playoff_rank INT,
                clinch_indicator STRING,
                division STRING,
                division_record STRING,
                division_rank INT,
                wins INT,
                losses INT,
                win_percentage DOUBLE,
                league_rank INT,
                overall_record STRING,
                home_record STRING,
                road_record STRING,
                last_10_record STRING,
                overtime_record STRING,
                conference_games_back DOUBLE,
                division_games_back DOUBLE,
                points_pg DOUBLE,
                opp_points_pg DOUBLE,
                diff_points_pg DOUBLE,
                season STRING,
                section STRING,
                record_start_date DATE,
                record_end_date DATE,
                is_current BOOLEAN,
                hash_val STRING
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        # Dim table for teams (SCD type 2 approach if needed)
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS nba_db.dim_teams (
                team_id INT,
                team_name STRING,
                team_city STRING,
                team_slug STRING,
                record_start_date DATE,
                record_end_date DATE,
                is_current BOOLEAN,
                hash_val STRING
            )
            USING ICEBERG
            PARTITIONED BY (team_id)
            """
        )

        # Fact table for aggregated results
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS nba_db.fact_nba_regular_season_standings (
                team_id INT,
                season STRING,
                playoff_bucket STRING,
                total_wins BIGINT,
                total_losses BIGINT,
                avg_win_percentage DOUBLE,
                avg_points_pg DOUBLE,
                avg_opp_points_pg DOUBLE,
                avg_diff_points_pg DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season, team_id)
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
    Performs SCD Type 2 merges into nba_regular_season_standings,
    then aggregates & writes to fact_nba_regular_season_standings in normal batch mode.
    Demonstrates a simple "playoff bucket" approach (similar to the net_rating buckets logic).
    """
    record_count = df.count()
    logger.info(f"[Batch-Job] Dataset has {record_count} records to process in this run.")

    if record_count == 0:
        logger.info("[Batch-Job] No records to merge or aggregate.")
        return

    #
    # 1) Handle SCD merges into nba_regular_season_standings
    #
    handle_scd_type_2(
        df=df,
        spark=spark,
        table_name="nba_db.nba_regular_season_standings",
        join_columns=["team_id", "season"],  # Example join on team_id + season
        compare_columns=[
            "league_id",
            "season_id",
            "team_city",
            "team_name",
            "team_slug",
            "conference",
            "conference_record",
            "playoff_rank",
            "clinch_indicator",
            "division",
            "division_record",
            "division_rank",
            "wins",
            "losses",
            "win_percentage",
            "league_rank",
            "overall_record",
            "home_record",
            "road_record",
            "last_10_record",
            "overtime_record",
            "conference_games_back",
            "division_games_back",
            "points_pg",
            "opp_points_pg",
            "diff_points_pg",
            "section",
        ],
    )

    #
    # 2) Simple "playoff bucket" classification before aggregation
    #
    #   For example:
    #   <= 6  => "guaranteed_playoffs"
    #   <= 10 => "play_in_range"
    #   else  => "non_playoff"
    #
    df_buckets = df.withColumn(
        "playoff_bucket",
        when(col("playoff_rank") <= 6, "guaranteed_playoffs")
        .when((col("playoff_rank") > 6) & (col("playoff_rank") <= 10), "play_in_range")
        .otherwise("non_playoff")
    )

    #
    # 3) Aggregate and write to fact_nba_regular_season_standings in batch
    #
    fact_aggregated = (
        df_buckets.groupBy("team_id", "season", "playoff_bucket")
        .agg(
            spark_sum(col("wins").cast("long")).alias("total_wins"),
            spark_sum(col("losses").cast("long")).alias("total_losses"),
            avg("win_percentage").alias("avg_win_percentage"),
            avg("points_pg").alias("avg_points_pg"),
            avg("opp_points_pg").alias("avg_opp_points_pg"),
            avg("diff_points_pg").alias("avg_diff_points_pg"),
        )
    )

    fact_aggregated.write.format("iceberg") \
        .mode("append") \
        .save("spark_catalog.nba_db.fact_nba_regular_season_standings")

    logger.info("[Batch-Job] Wrote aggregated data to fact_nba_regular_season_standings.")

def create_cumulative_tables(spark: SparkSession) -> None:
    """
    Creates additional cumulative Iceberg tables (examples) for the new standardized columns.
    Adjust or omit as desired.
    """
    try:
        spark.sql("DROP TABLE IF EXISTS nba_db.current_season_team_standings_totals")
        spark.sql("DROP TABLE IF EXISTS nba_db.current_season_team_standings_averages")
        spark.sql("DROP TABLE IF EXISTS nba_db.all_season_team_standings_totals")
        spark.sql("DROP TABLE IF EXISTS nba_db.all_season_team_standings_averages")

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS nba_db.current_season_team_standings_totals (
                season STRING,
                team_name STRING,
                total_wins BIGINT,
                total_losses BIGINT
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS nba_db.current_season_team_standings_averages (
                season STRING,
                team_name STRING,
                avg_win_percentage DOUBLE,
                avg_points_pg DOUBLE,
                avg_opp_points_pg DOUBLE,
                avg_diff_points_pg DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (season)
            """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS nba_db.all_season_team_standings_totals (
                team_name STRING,
                total_wins BIGINT,
                total_losses BIGINT
            )
            USING ICEBERG
            PARTITIONED BY (team_name)
            """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS nba_db.all_season_team_standings_averages (
                team_name STRING,
                avg_win_percentage DOUBLE,
                avg_points_pg DOUBLE,
                avg_opp_points_pg DOUBLE,
                avg_diff_points_pg DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (team_name)
            """
        )

        logger.info("Cumulative tables created successfully.")
    except Exception as e:
        logger.error(
            f"Error creating cumulative tables: {e}",
            exc_info=True,
        )
        sys.exit(1)

def update_cumulative_tables(spark: SparkSession) -> None:
    """
    Example function to update the cumulative tables with the latest data from
    nba_regular_season_standings. Adjust column references as needed.
    """
    try:
        # Example of how you might define "current_season"
        current_season_row = spark.sql(
            """
            SELECT MAX(CAST(SUBSTRING(season, 1, 4) AS INT)) AS current_season
            FROM nba_db.nba_regular_season_standings
            """
        ).collect()[0]

        current_season = current_season_row["current_season"]
        if current_season is None:
            logger.warning("No current season found. Skipping cumulative tables update.")
            return

        # current_season_team_standings_totals
        current_season_totals = spark.sql(
            f"""
            SELECT
                season AS season,
                team_name,
                SUM(wins) AS total_wins,
                SUM(losses) AS total_losses
            FROM nba_db.nba_regular_season_standings
            WHERE CAST(SUBSTRING(season,1,4) AS INT) = {current_season}
            GROUP BY season, team_name
            """
        )
        current_season_totals.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.nba_db.current_season_team_standings_totals"
        )

        # current_season_team_standings_averages
        current_season_averages = spark.sql(
            f"""
            SELECT
                season AS season,
                team_name,
                AVG(win_percentage) AS avg_win_percentage,
                AVG(points_pg) AS avg_points_pg,
                AVG(opp_points_pg) AS avg_opp_points_pg,
                AVG(diff_points_pg) AS avg_diff_points_pg
            FROM nba_db.nba_regular_season_standings
            WHERE CAST(SUBSTRING(season,1,4) AS INT) = {current_season}
            GROUP BY season, team_name
            """
        )
        current_season_averages.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.nba_db.current_season_team_standings_averages"
        )

        # all_season_team_standings_totals
        all_season_totals = spark.sql(
            """
            SELECT
                team_name,
                SUM(wins) AS total_wins,
                SUM(losses) AS total_losses
            FROM nba_db.nba_regular_season_standings
            GROUP BY team_name
            """
        )
        all_season_totals.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.nba_db.all_season_team_standings_totals"
        )

        # all_season_team_standings_averages
        all_season_averages = spark.sql(
            """
            SELECT
                team_name,
                AVG(win_percentage) AS avg_win_percentage,
                AVG(points_pg) AS avg_points_pg,
                AVG(opp_points_pg) AS avg_opp_points_pg,
                AVG(diff_points_pg) AS avg_diff_points_pg
            FROM nba_db.nba_regular_season_standings
            GROUP BY team_name
            """
        )
        all_season_averages.write.format("iceberg").mode("overwrite").save(
            "spark_catalog.nba_db.all_season_team_standings_averages"
        )

        logger.info("Cumulative tables updated successfully.")
    except Exception as e:
        logger.error(f"Error updating cumulative tables: {e}", exc_info=True)
        sys.exit(1)

def read_kafka_batch_and_process(spark: SparkSession) -> None:
    """
    Reads data from Kafka in a single batch (using startingOffsets=earliest, endingOffsets=latest),
    then merges into nba_regular_season_standings + writes aggregated results to fact_nba_regular_season_standings.
    Adjusted for the "NBA_regular_season_standings" topic and columns.
    """
    try:
        # 1) Define schema matching the *original* Kafka JSON fields (from the example).
        #    Adjust as needed for the actual fields. We'll capture many from the sample.
        schema = StructType([
            StructField("LeagueID", StringType(), True),
            StructField("SeasonID", StringType(), True),
            StructField("TeamID", IntegerType(), True),
            StructField("TeamCity", StringType(), True),
            StructField("TeamName", StringType(), True),
            StructField("TeamSlug", StringType(), True),
            StructField("Conference", StringType(), True),
            StructField("ConferenceRecord", StringType(), True),
            StructField("PlayoffRank", IntegerType(), True),
            StructField("ClinchIndicator", StringType(), True),
            StructField("Division", StringType(), True),
            StructField("DivisionRecord", StringType(), True),
            StructField("DivisionRank", IntegerType(), True),
            StructField("WINS", IntegerType(), True),
            StructField("LOSSES", IntegerType(), True),
            StructField("WinPCT", DoubleType(), True),
            StructField("LeagueRank", IntegerType(), True),
            StructField("Record", StringType(), True),
            StructField("HOME", StringType(), True),
            StructField("ROAD", StringType(), True),
            StructField("L10", StringType(), True),
            StructField("OT", StringType(), True),
            StructField("ConferenceGamesBack", DoubleType(), True),
            StructField("DivisionGamesBack", DoubleType(), True),
            StructField("PointsPG", DoubleType(), True),
            StructField("OppPointsPG", DoubleType(), True),
            StructField("DiffPointsPG", DoubleType(), True),
            StructField("Season", StringType(), True),
            StructField("Section", StringType(), True),
        ])

        # 2) Do a single batch read from Kafka
        kafka_batch_df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", ",".join(BOOTSTRAP_SERVERS))
            .option("subscribe", "NBA_regular_season_standings")   # Adjust topic name
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

        # 4) Rename columns from uppercase / partial to spelled-out lowercase w/ underscores
        renamed_df = (
            parsed_df
            .withColumnRenamed("LeagueID", "league_id")
            .withColumnRenamed("SeasonID", "season_id")
            .withColumnRenamed("TeamID", "team_id")
            .withColumnRenamed("TeamCity", "team_city")
            .withColumnRenamed("TeamName", "team_name")
            .withColumnRenamed("TeamSlug", "team_slug")
            .withColumnRenamed("Conference", "conference")
            .withColumnRenamed("ConferenceRecord", "conference_record")
            .withColumnRenamed("PlayoffRank", "playoff_rank")
            .withColumnRenamed("ClinchIndicator", "clinch_indicator")
            .withColumnRenamed("Division", "division")
            .withColumnRenamed("DivisionRecord", "division_record")
            .withColumnRenamed("DivisionRank", "division_rank")
            .withColumnRenamed("WINS", "wins")
            .withColumnRenamed("LOSSES", "losses")
            .withColumnRenamed("WinPCT", "win_percentage")
            .withColumnRenamed("LeagueRank", "league_rank")
            .withColumnRenamed("Record", "overall_record")
            .withColumnRenamed("HOME", "home_record")
            .withColumnRenamed("ROAD", "road_record")
            .withColumnRenamed("L10", "last_10_record")
            .withColumnRenamed("OT", "overtime_record")
            .withColumnRenamed("ConferenceGamesBack", "conference_games_back")
            .withColumnRenamed("DivisionGamesBack", "division_games_back")
            .withColumnRenamed("PointsPG", "points_pg")
            .withColumnRenamed("OppPointsPG", "opp_points_pg")
            .withColumnRenamed("DiffPointsPG", "diff_points_pg")
            .withColumnRenamed("Season", "season")
            .withColumnRenamed("Section", "section")
        )

        # 5) Repartition to reduce shuffle overhead
        repartitioned_df = renamed_df.repartition(25)

        # (Optional) Example dimension table usage:
        #   We assume a dim_teams table might exist with SCD data about each team.
        #   For illustration, we do a left join on team_id.
        #   Then we keep the main columns from the Kafka data.
        try:
            dim_teams_df = spark.read.table("nba_db.dim_teams")
            joined_df = repartitioned_df.join(dim_teams_df.hint("merge"), on="team_id", how="left")
            final_df = joined_df.select(repartitioned_df["*"])  # keep only the original measure columns
        except:
            # If dim_teams doesn't exist or is empty, just use the renamed data
            final_df = repartitioned_df

        # Now run your SCD merges + aggregator in batch
        scd_and_aggregate_batch(final_df, spark)

        logger.info("Batch read from Kafka + SCD merges + aggregator completed.")

    except Exception as e:
        logger.error(f"Error in batch processing from Kafka: {e}", exc_info=True)
        sys.exit(1)

def main() -> None:
    """
    Main function to execute the pipeline in batch mode for NBA regular season standings.
    """
    try:
        spark = create_spark_connection()
        create_tables(spark)
        create_cumulative_tables(spark)

        # Single run of reading from Kafka + merging + writing aggregator
        read_kafka_batch_and_process(spark)

        # Example: you can optionally update cumulative tables
        update_cumulative_tables(spark)

        spark.stop()
        logger.info("Batch pipeline executed successfully.")
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
