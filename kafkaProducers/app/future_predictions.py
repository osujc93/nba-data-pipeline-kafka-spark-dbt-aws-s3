import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_add, current_date, year, month, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType
from sklearn.ensemble import RandomForestRegressor, AdaBoostRegressor, StackingRegressor
from sklearn.linear import Lasso, LinearRegression
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor
from catboost import CatBoostRegressor
from sklearn.model_selection import train_test_split, cross_val_score, ShuffleSplit, TimeSeriesSplit
from sklearn.metrics import mean_squared_error, mean_absolute_error, mean_absolute_percentage_error
from bayes_opt import BayesianOptimization
import logging
import mlflow
import mlflow.sklearn
import mlflow.xgboost
import mlflow.lightgbm
import mlflow.catboost
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='prediction.log', filemode='w')
logger = logging.getLogger(__name__)

# Define new metrics
def picp(y_true, y_pred):
    return np.mean((y_true >= y_pred[:, 0]) & (y_true <= y_pred[:, 1]))

def pinaw(y_true, y_pred):
    return np.mean(y_pred[:, 1] - y_pred[:, 0]) / (np.max(y_true) - np.min(y_true))

def winkler_score(y_true, y_pred, alpha=0.05):
    lower_bound = y_pred[:, 0]
    upper_bound = y_pred[:, 1]
    interval_width = upper_bound - lower_bound
    coverage = (y_true >= lower_bound) & (y_true <= upper_bound)
    score = interval_width + (2 / alpha) * ((lower_bound - y_true) * (y_true < lower_bound) + (y_true - upper_bound) * (y_true > upper_bound))
    return np.mean(score)

def create_spark_connection():
    try:
        azure_account_name = os.getenv('AZURE_ACCOUNT_NAME')
        azure_account_key = os.getenv('AZURE_ACCOUNT_KEY')
        azure_container = os.getenv('AZURE_CONTAINER')
        
        spark = SparkSession.builder \
            .appName('BoxscoreInfo') \
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                    "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.2,"
                    "org.apache.hadoop:hadoop-azure:3.3.4,"
                    "org.apache.hadoop:hadoop-azure-datalake:3.3.4") \
            .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem") \
            .config(f"spark.hadoop.fs.azure.account.key.{azure_account_name}.blob.core.windows.net", azure_account_key) \
            .config("spark.sql.warehouse.dir", f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/warehouse") \
            .enableHiveSupport() \
            .getOrCreate()
        
        spark.sql("CREATE DATABASE IF NOT EXISTS mlb_db")
        
        logger.info("Spark connection created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark connection: {e}", exc_info=True)
        raise

def create_future_tables(spark, azure_container, azure_account_name):
    try:
        # Future game results table
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.future_game_results (
            away_team_id INT,
            away_team_name STRING,
            away_team_season INT,
            away_team_teamCode STRING,
            away_team_abbreviation STRING,
            away_team_teamName STRING,
            away_team_locationName STRING,
            away_team_firstYearOfPlay INT,
            away_team_shortName STRING,
            away_team_franchiseName STRING,
            away_team_clubName STRING,
            away_team_active BOOLEAN,
            away_team_venue_id INT,
            away_team_venue_name STRING,
            away_team_league_id INT,
            away_team_league_name STRING,
            away_team_division_id INT,
            away_team_division_name STRING,
            away_team_sport_id INT,
            away_team_sport_name STRING,
            away_team_away_score INT,
            away_team_away_isWinner BOOLEAN,
            away_team_away_leagueRecord_wins INT,
            away_team_away_leagueRecord_losses INT,
            away_team_away_leagueRecord_pct DOUBLE,
            home_team_id INT,
            home_team_name STRING,
            home_team_season INT,
            home_team_teamCode STRING,
            home_team_abbreviation STRING,
            home_team_teamName STRING,
            home_team_locationName STRING,
            home_team_firstYearOfPlay INT,
            home_team_shortName STRING,
            home_team_franchiseName STRING,
            home_team_clubName STRING,
            home_team_active BOOLEAN,
            home_team_venue_id INT,
            home_team_venue_name STRING,
            home_team_league_id INT,
            home_team_league_name STRING,
            home_team_division_id INT,
            home_team_division_name STRING,
            home_team_sport_id INT,
            home_team_sport_name STRING,
            home_team_home_score INT,
            home_team_home_isWinner BOOLEAN,
            home_team_home_leagueRecord_wins INT,
            home_team_home_leagueRecord_losses INT,
            home_team_home_leagueRecord_pct DOUBLE,
            game_id INT,
            season INT,
            month INT,
            day INT,
            away_pitcher_id INT,
            away_pitcher_fullName STRING,
            away_pitcher_firstName STRING,
            away_pitcher_lastName STRING,
            away_pitcher_primaryNumber STRING,
            away_pitcher_birthDate STRING,
            away_pitcher_currentAge INT,
            away_pitcher_birthCity STRING,
            away_pitcher_birthStateProvince STRING,
            away_pitcher_birthCountry STRING,
            away_pitcher_height STRING,
            away_pitcher_weight INT,
            away_pitcher_active BOOLEAN,
            away_pitcher_useName STRING,
            away_pitcher_useLastName STRING,
            away_pitcher_middleName STRING,
            away_pitcher_boxscoreName STRING,
            away_pitcher_nickName STRING,
            away_pitcher_gender STRING,
            away_pitcher_isPlayer BOOLEAN,
            away_pitcher_isVerified BOOLEAN,
            away_pitcher_draftYear INT,
            away_pitcher_pronunciation STRING,
            away_pitcher_lastPlayedDate STRING,
            away_pitcher_stats STRING,
            away_pitcher_mlbDebutDate STRING,
            away_pitcher_nameFirstLast STRING,
            away_pitcher_nameSlug STRING,
            away_pitcher_firstLastName STRING,
            away_pitcher_lastFirstName STRING,
            away_pitcher_lastInitName STRING,
            away_pitcher_initLastName STRING,
            away_pitcher_fullFMLName STRING,
            away_pitcher_fullLFMName STRING,
            away_pitcher_strikeZoneTop DOUBLE,
            away_pitcher_strikeZoneBottom DOUBLE,
            away_pitcher_primaryPosition_code STRING,
            away_pitcher_primaryPosition_name STRING,
            away_pitcher_primaryPosition_type STRING,
            away_pitcher_primaryPosition_abbreviation STRING,
            away_pitcher_batSide_code STRING,
            away_pitcher_batSide_description STRING,
            away_pitcher_pitchHand_code STRING,
            away_pitcher_pitchHand_description STRING,
            home_pitcher_id INT,
            home_pitcher_fullName STRING,
            home_pitcher_firstName STRING,
            home_pitcher_lastName STRING,
            home_pitcher_primaryNumber STRING,
            home_pitcher_birthDate STRING,
            home_pitcher_currentAge INT,
            home_pitcher_birthCity STRING,
            home_pitcher_birthStateProvince STRING,
            home_pitcher_birthCountry STRING,
            home_pitcher_height STRING,
            home_pitcher_weight INT,
            home_pitcher_active BOOLEAN,
            home_pitcher_useName STRING,
            home_pitcher_useLastName STRING,
            home_pitcher_middleName STRING,
            home_pitcher_boxscoreName STRING,
            home_pitcher_gender STRING,
            home_pitcher_isPlayer BOOLEAN,
            home_pitcher_isVerified BOOLEAN,
            home_pitcher_draftYear INT,
            home_pitcher_pronunciation STRING,
            home_pitcher_lastPlayedDate STRING,
            home_pitcher_stats STRING,
            home_pitcher_mlbDebutDate STRING,
            home_pitcher_nameFirstLast STRING,
            home_pitcher_nameSlug STRING,
            home_pitcher_firstLastName STRING,
            home_pitcher_lastFirstName STRING,
            home_pitcher_lastInitName STRING,
            home_pitcher_initLastName STRING,
            home_pitcher_fullFMLName STRING,
            home_pitcher_fullLFMName STRING,
            home_pitcher_strikeZoneTop DOUBLE,
            home_pitcher_strikeZoneBottom DOUBLE,
            home_pitcher_primaryPosition_code STRING,
            home_pitcher_primaryPosition_name STRING,
            home_pitcher_primaryPosition_type STRING,
            home_pitcher_primaryPosition_abbreviation STRING,
            home_pitcher_batSide_code STRING,
            home_pitcher_batSide_description STRING,
            home_pitcher_pitchHand_code STRING,
            home_pitcher_pitchHand_description STRING,
            away_pitcher_deathDate STRING,
            away_pitcher_deathCity STRING,
            away_pitcher_deathStateProvince STRING,
            away_pitcher_deathCountry STRING,
            away_pitcher_nameMatrilineal STRING,
            home_pitcher_nickName STRING,
            home_pitcher_pronunciation STRING,
            home_pitcher_nameMatrilineal STRING,
            home_pitcher_deathDate STRING,
            home_pitcher_deathCity STRING,
            home_pitcher_deathStateProvince STRING,
            home_pitcher_deathCountry STRING,
            home_pitcher_nameTitle STRING,
            home_pitcher_nameSuffix STRING,
            away_pitcher_nameTitle STRING,
            away_pitcher_nameSuffix STRING
        ) STORED BY ICEBERG
        PARTITIONED BY (season, month, day)
        FROM spark_catalog.default.game_results
        WHERE officialDate >= date_add(current_date(), 1)                  
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_game_results'
        """)

        # Dimension Tables
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.future_dim_teams (
            team_id INT,
            team_name STRING,
            location_name STRING,
            league_name STRING,
            division_name STRING,
            sport_name STRING,
            first_year_of_play INT,
            active BOOLEAN,
            record_start_date DATE,
            record_end_date DATE,
            is_current BOOLEAN
        ) STORED BY ICEBERG
        PARTITIONED BY (team_id)
        FROM spark_catalog.default.dim_teams
        WHERE officialDate >= date_add(current_date(), 1)                  
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_dim_teams'
        """)

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.future_dim_pitchers (
            pitcher_id INT,
            full_name STRING,
            birth_date STRING,
            height STRING,
            weight INT,
            active BOOLEAN,
            primary_position_code STRING,
            primary_position_name STRING,
            bat_side_code STRING,
            bat_side_description STRING,
            pitch_hand_code STRING,
            pitch_hand_description STRING,
            record_start_date DATE,
            record_end_date DATE,
            is_current BOOLEAN
        ) STORED BY ICEBERG
        PARTITIONED BY (pitcher_id)
        FROM spark_catalog.default.dim_pitchers
        WHERE officialDate >= date_add(current_date(), 1)                  
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_dim_pitchers'
        """)

        # Fact Tables
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.future_fact_game_results (
            game_id INT,
            season INT,
            month INT,
            day INT,
            home_team_id INT,
            away_team_id INT,
            home_score INT,
            away_score INT,
            home_is_winner BOOLEAN,
            away_is_winner BOOLEAN,
            home_pitcher_id INT,
            away_pitcher_id INT
        ) STORED BY ICEBERG
        PARTITIONED BY (season, month, day)
        FROM spark_catalog.default.fact_game_results
        WHERE officialDate >= date_add(current_date(), 1)                  
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_fact_game_results'
        """)

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.future_fact_season_totals (
            season INT,
            team_id INT,
            total_away_score INT,
            total_home_score INT
        ) STORED BY ICEBERG
        PARTITIONED BY (season, team_id)
        FROM spark_catalog.default.fact_season_totals
        WHERE officialDate >= date_add(current_date(), 1)                  
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_fact_season_totals'
        """)

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.future_fact_season_averages (
            season INT,
            team_id INT,
            avg_away_score DOUBLE,
            avg_home_score DOUBLE
        ) STORED BY ICEBERG
        PARTITIONED BY (season, team_id)
        FROM spark_catalog.default.fact_season_averages
        WHERE officialDate >= date_add(current_date(), 1)                  
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_fact_season_averages'
        """)

        # Additional Fact Tables
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.future_fact_game_by_matchup (
            home_team_id INT,
            away_team_id INT,
            game_id INT,
            season INT,
            month INT,
            day INT,
            home_score INT,
            away_score INT,
            home_is_winner BOOLEAN,
            away_is_winner BOOLEAN
        ) STORED BY ICEBERG
        PARTITIONED BY (season, month, day, home_team_id, away_team_id)
        FROM spark_catalog.default.fact_game_by_matchup
        WHERE officialDate >= date_add(current_date(), 1)                  
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_fact_game_by_matchup'
        """)

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.future_fact_game_by_home_team (
            home_team_id INT,
            game_id INT,
            season INT,
            month INT,
            day INT,
            home_score INT,
            home_is_winner BOOLEAN
        ) STORED BY ICEBERG
        PARTITIONED BY (season, month, day, home_team_id)
        FROM spark_catalog.default.fact_game_by_home_team
        WHERE officialDate >= date_add(current_date(), 1)                  
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_fact_game_by_home_team'
        """)

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.future_fact_game_by_away_team (
            away_team_id INT,
            game_id INT,
            season INT,
            month INT,
            day INT,
            away_score INT,
            away_is_winner BOOLEAN
        ) STORED BY ICEBERG
        PARTITIONED BY (season, month, day, away_team_id)
        FROM spark_catalog.default.fact_game_by_away_team
        WHERE officialDate >= date_add(current_date(), 1)                     
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_fact_game_by_away_team'
        """)

        logger.info("Future tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating future tables: {e}", exc_info=True)
        raise

def handle_scd_type_2(df, spark, table_name, join_columns, compare_columns):
    try:
        df = df.withColumn('record_start_date', current_date())
        df = df.withColumn('record_end_date', lit(None).cast('date'))
        df = df.withColumn('is_current', lit(True))

        updates_df = df.selectExpr("*")
        updates_df.createOrReplaceTempView("updates_df")

        join_condition = " AND ".join([f"t.{col} = u.{col}" for col in join_columns])
        compare_condition = " OR ".join([f"t.{col} <> u.{col}" for col in compare_columns])

        merge_query = f"""
        MERGE INTO {table_name} AS t
        USING updates_df AS u
        ON {join_condition} AND t.is_current = true
        WHEN MATCHED AND ({compare_condition}) THEN
          UPDATE SET t.is_current = false, t.record_end_date = current_date()
        WHEN NOT MATCHED THEN
          INSERT (
            {", ".join(df.columns)}
          )
          VALUES (
            {", ".join([f"u.{col}" for col in df.columns])}
          )
        """

        spark.sql(merge_query)
        logger.info("SCD Type 2 handling completed successfully.")
    except Exception as e:
        logger.error(f"Error handling SCD Type 2: {e}", exc_info=True)
        raise

def read_and_write_stream(spark, azure_container, azure_account_name):
    try:
        schema = StructType([
            StructField("away_team_id", IntegerType(), True),
            StructField("away_team_name", StringType(), True),
            StructField("home_team_id", IntegerType(), True),
            StructField("home_team_name", StringType(), True),
            StructField("game_id", IntegerType(), True),
            StructField("season", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("day", IntegerType(), True),
            StructField("away_team_away_score", IntegerType(), True),
            StructField("home_team_home_score", IntegerType(), True),
            StructField("home_team_home_isWinner", BooleanType(), True),
            StructField("away_team_away_isWinner", BooleanType(), True),
            StructField("home_pitcher_id", IntegerType(), True),
            StructField("away_pitcher_id", IntegerType(), True),
            StructField("officialDate", StringType(), True)
        ])

        kafka_df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'kafka1:9092,kafka2:9093,kafka3:9094') \
            .option('subscribe', 'future_game_results') \
            .option('startingOffsets', 'earliest') \
            .load()

        game_results_df = kafka_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")).select("data.*")

        game_results_df = game_results_df \
            .filter(col("officialDate") >= date_add(current_date(), 1)) \
            .withColumn("month", col("month").cast(IntegerType())) \
            .withColumn("day", col("day").cast(IntegerType()))

        handle_scd_type_2(game_results_df, spark, "mlb_db.future_fact_game_results",
                          join_columns=["game_id", "season"],
                          compare_columns=["away_team_name", "home_team_name", "away_pitcher_id", "home_pitcher_id"])

        season_totals = game_results_df.groupBy("season", "away_team_id", "home_team_id") \
            .agg(spark_sum("away_team_away_score").alias("total_away_score"),
                 spark_sum("home_team_home_score").alias("total_home_score"))

        season_averages = game_results_df.groupBy("season", "away_team_id", "home_team_id") \
            .agg(avg("away_team_away_score").alias("avg_away_score"),
                 avg("home_team_home_score").alias("avg_home_score"))

        # Write the aggregated data to Iceberg tables
        season_totals.writeStream \
            .format("iceberg") \
            .option("path", f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_fact_season_totals") \
            .option("checkpointLocation", "/tmp/checkpoints/future_season_totals") \
            .outputMode("complete") \
            .start()

        season_averages.writeStream \
            .format("iceberg") \
            .option("path", f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_fact_season_averages") \
            .option("checkpointLocation", "/tmp/checkpoints/future_season_averages") \
            .outputMode("complete") \
            .start()

        # Additional Fact Tables
        game_by_matchup = game_results_df.groupBy("home_team_id", "away_team_id", "season", "month", "day") \
            .agg(spark_sum("home_team_home_score").alias("home_score"),
                 spark_sum("away_team_away_score").alias("away_score"),
                 max("home_team_home_isWinner").alias("home_is_winner"),
                 max("away_team_away_isWinner").alias("away_is_winner"))

        game_by_matchup.writeStream \
            .format("iceberg") \
            .option("path", f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_fact_game_by_matchup") \
            .option("checkpointLocation", "/tmp/checkpoints/future_fact_game_by_matchup") \
            .outputMode("complete") \
            .start()

        game_by_home_team = game_results_df.groupBy("home_team_id", "season", "month", "day") \
            .agg(spark_sum("home_team_home_score").alias("home_score"),
                 max("home_team_home_isWinner").alias("home_is_winner"))

        game_by_home_team.writeStream \
            .format("iceberg") \
            .option("path", f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_fact_game_by_home_team") \
            .option("checkpointLocation", "/tmp/checkpoints/future_fact_game_by_home_team") \
            .outputMode("complete") \
            .start()

        game_by_away_team = game_results_df.groupBy("away_team_id", "season", "month", "day") \
            .agg(spark_sum("away_team_away_score").alias("away_score"),
                 max("away_team_away_isWinner").alias("away_is_winner"))

        game_by_away_team.writeStream \
            .format("iceberg") \
            .option("path", f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_fact_game_by_away_team") \
            .option("checkpointLocation", "/tmp/checkpoints/future_fact_game_by_away_team") \
            .outputMode("complete") \
            .start()

        logger.info("Streaming read and write for future dates started successfully.")
    except Exception as e:
        logger.error(f"Error in read and write stream for future dates: {e}", exc_info=True)
        raise

def create_cumulative_tables(spark, azure_container, azure_account_name):
    try:
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.future_current_season_totals (
            season INT,
            team_name STRING,
            total_away_score INT,
            total_home_score INT
        ) STORED BY ICEBERG
        PARTITIONED BY (season)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_current_season_totals'
        """)
        
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.future_current_season_averages (
            season INT,
            team_name STRING,
            avg_away_score DOUBLE,
            avg_home_score DOUBLE
        ) STORED BY ICEBERG
        PARTITIONED BY (season)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_current_season_averages'
        """)
        
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.future_all_season_totals (
            team_name STRING,
            total_away_score INT,
            total_home_score INT
        ) STORED BY ICEBERG
        PARTITIONED BY (team_name)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_all_season_totals'
        """)
        
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.future_all_season_averages (
            team_name STRING,
            avg_away_score DOUBLE,
            avg_home_score DOUBLE
        ) STORED BY ICEBERG
        PARTITIONED BY (team_name)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_all_season_averages'
        """)
        
        logger.info("Cumulative future season tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating cumulative future season tables: {e}", exc_info=True)
        raise

def update_cumulative_tables(spark, azure_container, azure_account_name):
    try:
        current_season = spark.sql("SELECT MAX(season) AS current_season FROM mlb_db.future_fact_game_results").collect()[0]["current_season"]
        
        current_season_totals = spark.sql(f"""
        SELECT 
            season,
            away_team_name AS team_name,
            SUM(away_team_away_score) AS total_away_score,
            SUM(home_team_home_score) AS total_home_score
        FROM mlb_db.future_fact_game_results
        WHERE season = {current_season}
        GROUP BY season, away_team_name
        """)
        
        current_season_averages = spark.sql(f"""
        SELECT 
            season,
            away_team_name AS team_name,
            AVG(away_team_away_score) AS avg_away_score,
            AVG(home_team_home_score) AS avg_home_score
        FROM mlb_db.future_fact_game_results
        WHERE season = {current_season}
        GROUP BY season, away_team_name
        """)
        
        all_season_totals = spark.sql(f"""
        SELECT 
            away_team_name AS team_name,
            SUM(away_team_away_score) AS total_away_score,
            SUM(home_team_home_score) AS total_home_score
        FROM mlb_db.future_fact_game_results
        GROUP BY away_team_name
        """)
        
        all_season_averages = spark.sql(f"""
        SELECT 
            away_team_name AS team_name,
            AVG(away_team_away_score) AS avg_away_score,
            AVG(home_team_home_score) AS avg_home_score
        FROM mlb_db.future_fact_game_results
        GROUP BY away_team_name
        """)
        
        current_season_totals.write.format("iceberg") \
            .mode("overwrite") \
            .save(f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_current_season_totals")
        
        current_season_averages.write.format("iceberg") \
            .mode("overwrite") \
            .save(f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_current_season_averages")
        
        all_season_totals.write.format("iceberg") \
            .mode("overwrite") \
            .save(f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_all_season_totals")
        
        all_season_averages.write.format("iceberg") \
            .mode("overwrite") \
            .save(f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/future_all_season_averages")
        
        logger.info("Cumulative future season tables updated successfully.")
    except Exception as e:
        logger.error(f"Error updating cumulative future season tables: {e}", exc_info=True)
        raise

def eda_and_prepare_data(spark):
    try:
        df = spark.read.format("iceberg").load(f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_boxscore")
        pdf = df.toPandas()

        # Exploratory Data Analysis (EDA)
        print("Summary Statistics:")
        print(pdf.describe())

        print("\nMissing Values:")
        print(pdf.isnull().sum())

        print("\nData Types:")
        print(pdf.dtypes)

        # Visualizations
        plt.figure(figsize=(12, 6))
        sns.heatmap(pdf.corr(), annot=True, cmap='coolwarm')
        plt.title('Correlation Heatmap')
        plt.show()

        # Preprocessing
        pdf = pd.get_dummies(pdf, columns=['home_team', 'away_team'], drop_first=True)

        # Handle missing values
        pdf.fillna(0, inplace=True)

        # Separate features and target columns
        target_columns = ['away_team_away_score', 'home_team_home_score']  # Example target columns
        features = [col for col in pdf.columns if col not in target_columns + ['game_id', 'game_date']]

        # Split data into training and testing sets
        train_df, test_df = train_test_split(pdf, test_size=0.2, random_state=42, shuffle=False)

        return train_df, test_df, features, target_columns
    except Exception as e:
        logger.error(f"Error in EDA and data preparation: {e}", exc_info=True)
        raise

def optimize_and_train_model(model, X_train, y_train, param_space, n_iter=20):
    def objective(**params):
        model.set_params(**params)
        model.fit(X_train, y_train)
        pred = model.predict(X_train)
        return -mean_squared_error(y_train, pred)
    
    optimizer = BayesianOptimization(f=objective, pbounds=param_space, random_state=42, verbose=2)
    optimizer.maximize(init_points=5, n_iter=n_iter)
    
    best_params_list = sorted(optimizer.res, key=lambda x: x['target'], reverse=True)[:5]
    return [params['params'] for params in best_params_list]

def cross_val_evaluate(model, X, y, splits=10):
    tscv = TimeSeriesSplit(n_splits=splits)
    scores = cross_val_score(model, X, y, cv=tscv, scoring='neg_mean_squared_error')
    return np.mean(np.abs(scores))

def cross_val_mc_evaluate(model, X, y, splits=10, test_size=0.2):
    mc_cv = ShuffleSplit(n_splits=splits, test_size=test_size, random_state=42)
    scores = cross_val_score(model, X, y, cv=mc_cv, scoring='neg_mean_squared_error')
    return np.mean(np.abs(scores))

def evaluate_best_params(model_class, param_list, X_train, y_train, X_test, y_test):
    best_param = None
    best_cv_score = float('inf')
    
    for params in param_list:
        model = model_class(**params)
        model.fit(X_train, y_train)
        tscv_score = cross_val_evaluate(model, X_train, y_train)
        mc_cv_score = cross_val_mc_evaluate(model, X_train, y_train)
        avg_cv_score = (tscv_score + mc_cv_score) / 2
        
        if avg_cv_score < best_cv_score:
            best_cv_score = avg_cv_score
            best_param = params
    
    return best_param

def train_models(train_df, test_df, features, target_columns):
    try:
        mlflow.set_tracking_uri("http://mlflow:5000")
        models = {}
        for target in target_columns:
            X_train = train_df[features]
            y_train = train_df[target]
            X_test = test_df[features]
            y_test = test_df[target]
            
            rf_model = RandomForestRegressor(n_estimators=100, random_state=42)
            rf_model.fit(X_train, y_train)
            feature_importances = rf_model.feature_importances_
            important_features_rf = [features[i] for i in range(len(features)) if feature_importances[i] > 0.01]
            X_train_rf = X_train[important_features_rf]
            X_test_rf = X_test[important_features_rf]
            
            lasso_model = Lasso(alpha=0.1)
            lasso_model.fit(X_train, y_train)
            lasso_coef = lasso_model.coef_
            important_features_lasso = [features[i] for i in range(len(features)) if lasso_coef[i] != 0]
            X_train_lasso = X_train[important_features_lasso]
            X_test_lasso = X_test[important_features_lasso]
            
            xgb_param_space = {
                'n_estimators': (50, 300),
                'max_depth': (3, 15),
                'learning_rate': (0.01, 0.3),
                'subsample': (0.5, 1.0),
                'colsample_bytree': (0.5, 1.0)
            }
            lgb_param_space = {
                'num_leaves': (20, 70),
                'max_depth': (3, 15),
                'learning_rate': (0.01, 0.3),
                'n_estimators': (50, 300),
                'min_child_samples': (5, 50)
            }
            ada_param_space = {
                'n_estimators': (50, 300),
                'learning_rate': (0.01, 0.3)
            }
            lr_param_space = {
                'fit_intercept': [True, False]
            }
            
            xgb_best_params_list = optimize_and_train_model(XGBRegressor(objective='reg:squarederror', random_state=42), 
                                                            X_train_rf, y_train, xgb_param_space)
            lgb_best_params_list = optimize_and_train_model(LGBMRegressor(random_state=42), 
                                                            X_train_rf, y_train, lgb_param_space)
            ada_best_params_list = optimize_and_train_model(AdaBoostRegressor(random_state=42), 
                                                            X_train_rf, y_train, ada_param_space)
            lr_best_params_list = optimize_and_train_model(LinearRegression(), X_train_lasso, y_train, lr_param_space)
            
            xgb_best_params = evaluate_best_params(XGBRegressor, xgb_best_params_list, X_train_rf, y_train, X_test_rf, y_test)
            lgb_best_params = evaluate_best_params(LGBMRegressor, lgb_best_params_list, X_train_rf, y_train, X_test_rf, y_test)
            ada_best_params = evaluate_best_params(AdaBoostRegressor, ada_best_params_list, X_train_rf, y_train, X_test_rf, y_test)
            lr_best_params = evaluate_best_params(LinearRegression, lr_best_params_list, X_train_lasso, y_train, X_test_lasso, y_test)
            
            xgb_model = XGBRegressor(**xgb_best_params)
            xgb_model.fit(X_train_rf, y_train)
            xgb_pred = xgb_model.predict(X_test_rf)
            xgb_mse = mean_squared_error(y_test, xgb_pred)
            logger.info(f"XGBoost MSE for {target}: {xgb_mse}")
            
            lgb_model = LGBMRegressor(**lgb_best_params)
            lgb_model.fit(X_train_rf, y_train)
            lgb_pred = lgb_model.predict(X_test_rf)
            lgb_mse = mean_squared_error(y_test, lgb_pred)
            logger.info(f"LightGBM MSE for {target}: {lgb_mse}")
            
            ada_model = AdaBoostRegressor(**ada_best_params)
            ada_model.fit(X_train_rf, y_train)
            ada_pred = ada_model.predict(X_test_rf)
            ada_mse = mean_squared_error(y_test, ada_pred)
            logger.info(f"AdaBoost MSE for {target}: {ada_mse}")
            
            lr_model = LinearRegression(**lr_best_params)
            lr_model.fit(X_train_lasso, y_train)
            lr_pred = lr_model.predict(X_test_lasso)
            lr_mse = mean_squared_error(y_test, lr_pred)
            logger.info(f"Linear Regression MSE for {target}: {lr_mse}")

            # Log RandomForest and Lasso models in MLflow
            rf_pred = rf_model.predict(X_test_rf)
            rf_mse = mean_squared_error(y_test, rf_pred)
            logger.info(f"RandomForest MSE for {target}: {rf_mse}")

            lasso_pred = lasso_model.predict(X_test_lasso)
            lasso_mse = mean_squared_error(y_test, lasso_pred)
            logger.info(f"Lasso MSE for {target}: {lasso_mse}")

            catboost_param_space = {
                'iterations': (50, 300),
                'depth': (3, 15),
                'learning_rate': (0.01, 0.3),
                'l2_leaf_reg': (1, 10)
            }
            
            catboost_best_params_list = optimize_and_train_model(CatBoostRegressor(random_seed=42, silent=True), 
                                                                 X_train, y_train, catboost_param_space)
            
            best_catboost_params = evaluate_best_params(CatBoostRegressor, catboost_best_params_list, X_train, y_train, X_test, y_test)
            
            estimators = [
                ('xgb', xgb_model),
                ('lgb', lgb_model),
                ('ada', ada_model),
                ('lr', lr_model)
            ]

            # Define the stacking model with QR-Stacking
            final_estimator = QRStackingRegressor(estimators=estimators, final_estimator=CatBoostRegressor(**best_catboost_params, random_seed=42, silent=True))
            tpe_optimizer = TPEOptimizer(final_estimator, param_space)  # Define your param_space for the final estimator
            
            tpe_optimizer.fit(X_train, y_train)
            final_estimator = tpe_optimizer.best_estimator_

            ensemble_model = StackingRegressor(estimators=estimators, final_estimator=final_estimator)
            ensemble_model.fit(X_train, y_train)
            ensemble_pred = ensemble_model.predict(X_test)
            ensemble_mse = mean_squared_error(y_test, ensemble_pred)
            logger.info(f"Stacking Ensemble MSE for {target}: {ensemble_mse}")

            # Monte-Carlo CV
            mc_cv = ShuffleSplit(n_splits=10, test_size=0.2, random_state=42)
            mc_cv_scores = cross_val_score(ensemble_model, X, y, cv=mc_cv, scoring='neg_mean_squared_error')
            avg_mc_cv_score = np.mean(np.abs(mc_cv_scores))
            logger.info(f"Monte-Carlo Cross-Validation MSE for {target}: {avg_mc_cv_score}")

            # Time Series Split CV
            tscv = TimeSeriesSplit(n_splits=10)
            tscv_scores = cross_val_score(ensemble_model, X, y, cv=tscv, scoring='neg_mean_squared_error')
            avg_tscv_score = np.mean(np.abs(tscv_scores))
            logger.info(f"Time Series Split Cross-Validation MSE for {target}: {avg_tscv_score}")

            # Additional metrics for QR-Stacking
            y_pred_intervals = final_estimator.predict_intervals(X_test)  
            picp_score = picp(y_test, y_pred_intervals)
            pinaw_score = pinaw(y_test, y_pred_intervals)
            wc_score = winkler_score(y_test, y_pred_intervals)

            logger.info(f"PICP for {target}: {picp_score}")
            logger.info(f"PINAW for {target}: {pinaw_score}")
            logger.info(f"Winkler Score for {target}: {wc_score}")

            models[target] = {
                'XGBoost': xgb_model,
                'LightGBM': lgb_model,
                'AdaBoost': ada_model,
                'LinearRegression': lr_model,
                'RandomForest': rf_model,
                'Lasso': lasso_model,
                'CatBoost': CatBoostRegressor(**best_catboost_params, random_seed=42, silent=True),
                'Ensemble': ensemble_model
            }
            
            with mlflow.start_run(run_name=f"{target}_model_training"):
                mlflow.log_metric("XGBoost_MSE", xgb_mse)
                mlflow.log_metric("LightGBM_MSE", lgb_mse)
                mlflow.log_metric("AdaBoost_MSE", ada_mse)
                mlflow.log_metric("LinearRegression_MSE", lr_mse)
                mlflow.log_metric("RandomForest_MSE", rf_mse)
                mlflow.log_metric("Lasso_MSE", lasso_mse)
                mlflow.log_metric("Ensemble_MSE", ensemble_mse)
                mlflow.log_metric("MonteCarlo_CV_MSE", avg_mc_cv_score)
                mlflow.log_metric("TimeSeriesSplit_CV_MSE", avg_tscv_score)
                mlflow.log_metric("PICP", picp_score)
                mlflow.log_metric("PINAW", pinaw_score)
                mlflow.log_metric("Winkler_Score", wc_score)
                
                mlflow.xgboost.log_model(xgb_model, f"XGBoost_{target}")
                mlflow.lightgbm.log_model(lgb_model, f"LightGBM_{target}")
                mlflow.sklearn.log_model(ada_model, f"AdaBoost_{target}")
                mlflow.sklearn.log_model(lr_model, f"LinearRegression_{target}")
                mlflow.sklearn.log_model(rf_model, f"RandomForest_{target}")
                mlflow.sklearn.log_model(lasso_model, f"Lasso_{target}")
                mlflow.catboost.log_model(models[target]['CatBoost'], f"CatBoost_{target}")
                mlflow.sklearn.log_model(ensemble_model, f"Ensemble_{target}")
                
                best_model_name = 'Ensemble'
                best_model = models[target][best_model_name]
                mlflow.log_param("Best_Model", best_model_name)
                mlflow.sklearn.log_model(best_model, f"Best_Model_{target}")
                
                logger.info(f"Best model for {target} saved: {best_model_name}")

        return models
    except Exception as e:
        logger.error(f"Error training models: {e}", exc_info=True)
        raise

def predict_future(spark, models, features):
    try:
        mlflow.set_tracking_uri("http://mlflow:5000")

        future_df = spark.read.format("iceberg").load(f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/dim_future_combined_stats")
        future_pdf = future_df.toPandas()
        
        if 'home_team' in future_pdf.columns:
            future_pdf = pd.get_dummies(future_pdf, columns=['home_team'])
        if 'away_team' in future_pdf.columns:
            future_pdf = pd.get_dummies(future_pdf, columns=['away_team'])
        
        all_columns = set(features + list(future_pdf.columns))
        missing_columns = list(set(all_columns) - set(future_pdf.columns))
        for col in missing_columns:
            future_pdf[col] = 0
        
        X_future = future_pdf[features]
        
        predictions = {}
        for target in models.keys():
            model_uri = f"models:/Best_Model_{target}/1"
            best_model = mlflow.pyfunc.load_model(model_uri)
            predictions[target] = best_model.predict(X_future)
        
        for target, prediction in predictions.items():
            future_pdf[target] = prediction
        
        predictions_df = spark.createDataFrame(future_pdf)
        predictions_df.write.format("iceberg").mode("overwrite").save(f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/dim_future_combined_stats")

        logger.info("Predictions saved successfully.")
    except Exception as e:
        logger.error(f"Error in predict_future function: {e}", exc_info=True)
        raise

def main():
    try:
        spark = create_spark_connection()
        azure_container = os.getenv('AZURE_CONTAINER')
        azure_account_name = os.getenv('AZURE_ACCOUNT_NAME')
        
        create_future_tables(spark, azure_container, azure_account_name)
        create_cumulative_tables(spark, azure_container, azure_account_name)
        
        train_df, test_df, features, target_columns = eda_and_prepare_data(spark)
        models = train_models(train_df, test_df, features, target_columns)
        predict_future(spark, models, features)
        
        update_cumulative_tables(spark, azure_container, azure_account_name)
        
        read_and_write_stream(spark, azure_container, azure_account_name)
        
        spark.stop()
        logger.info("Main function executed successfully.")
    except Exception as e:
        logger.error(f"Error in main function: {e}", exc_info=True)

if __name__ == "__main__":
    main()