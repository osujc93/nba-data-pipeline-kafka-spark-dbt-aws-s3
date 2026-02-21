"""
This script is designed to ETL processes of MLB boxscore data into Iceberg tables using Azure Data Lake Storage.

Next, it sets up a Spark session with the necessary configurations, including packages for Kafka, Iceberg, and Azure, and configurations for connecting to the Azure Blob Storage. Spark is used for scalable and efficient data transformations and queries.

The script then creates and manages various tables in a database called 'mlb_db', designed to store different aspects of the MLB boxscore data, including team and player statistics, aggregated totals, and averages. The tables are partitioned by relevant columns to improve query performance and manage large datasets efficiently. Iceberg, a high-performance format for analytic tables, is used for storing these tables.

After creating the tables, the script migrates existing data into the combined tables by reading from source tables, transforming the data, and inserting it into the target tables, ensuring all historical data is available in the new table structures.

The script also handles Slowly Changing Dimensions (SCD) Type 2, crucial for tracking changes in data over time, such as player transfers or team name changes. It updates records to mark them as no longer current and inserts new records to reflect the latest data.

For dynamic data handling, the script includes functions for dynamically extracting and creating columns, particularly useful for handling player statistics with changing attributes.

The core of the script involves reading data from Kafka streams. Kafka is used for real-time data streaming, and the script processes each batch of data, transforming it as needed, and writing the processed data to the appropriate tables in the database.

Finally, the script includes a function to upload the processed data to Azure Blob Storage, ensuring scalable and accessible storage for further analysis and use.

Throughout the script, logging is used to provide insights into the process flow and help with debugging and monitoring. Exception handling ensures errors are caught and logged, allowing for robust and reliable data processing.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, year, month, dayofmonth, current_date, lit, explode, sum as spark_sum, avg, to_date, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
import logging
import re
import os
from azure.storage.blob import BlobServiceClient
import io

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='app.log', filemode='w')
logger = logging.getLogger(__name__)

azure_blob_service_client = BlobServiceClient.from_connection_string(
    f"DefaultEndpointsProtocol=https;AccountName={os.getenv('AZURE_ACCOUNT_NAME')};AccountKey={os.getenv('AZURE_ACCOUNT_KEY')};EndpointSuffix=core.windows.net"
)

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
                    "org.apache.hadoop:hadoop-azure:3.4.0,"
                    "org.apache.hadoop:hadoop-azure-datalake:3.4.0") \
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

def create_combined_tables(spark, azure_container, azure_account_name):
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS mlb_db.dim_team_boxscores (
                game_id STRING,
                season INT,
                month INT,
                day INT,
                game_time STRING,
                team_id INT,
                team_name STRING,
                flyOuts INT,
                groundOuts INT,
                airOuts INT,
                runs INT,
                doubles INT,
                triples INT,
                homeRuns INT,
                strikeOuts INT,
                baseOnBalls INT,
                intentionalWalks INT,
                hits INT,
                hitByPitch INT,
                avg DOUBLE,
                atBats INT,
                obp DOUBLE,
                slg DOUBLE,
                ops DOUBLE,
                caughtStealing INT,
                stolenBases INT,
                stolenBasePercentage DOUBLE,
                groundIntoDoublePlay INT,
                groundIntoTriplePlay INT,
                plateAppearances INT,
                totalBases INT,
                rbi INT,
                leftOnBase INT,
                sacBunts INT,
                sacFlies INT,
                catchersInterference INT,
                pickoffs INT,
                atBatsPerHomeRun DOUBLE,
                popOuts INT,
                lineOuts INT,
                team_type STRING
            ) STORED BY ICEBERG
            PARTITIONED BY (season, month, day, game_id, team_id)
            LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/dim_team_boxscores'
        """)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS mlb_db.dim_player_boxscores (
                game_id STRING,
                season INT,
                month INT,
                day INT,
                game_time STRING,
                player_id INT,
                player_name STRING,
                team_id INT,
                team_name STRING,
                batting_order INT,
                batting_summary STRING,
                batting_gamesPlayed INT,
                batting_flyOuts INT,
                batting_groundOuts INT,
                batting_airOuts INT,
                batting_runs INT,
                batting_doubles INT,
                batting_triples INT,
                batting_homeRuns INT,
                batting_strikeOuts INT,
                batting_baseOnBalls INT,
                batting_intentionalWalks INT,
                batting_hits INT,
                batting_hitByPitch INT,
                batting_avg DOUBLE,
                batting_atBats INT,
                batting_obp DOUBLE,
                batting_slg DOUBLE,
                batting_ops DOUBLE,
                batting_caughtStealing INT,
                batting_stolenBases INT,
                batting_stolenBasePercentage DOUBLE,
                batting_groundIntoDoublePlay INT,
                batting_groundIntoTriplePlay INT,
                batting_plateAppearances INT,
                batting_totalBases INT,
                batting_rbi INT,
                batting_leftOnBase INT,
                batting_sacBunts INT,
                batting_sacFlies INT,
                batting_catchersInterference INT,
                batting_pickoffs INT,
                batting_atBatsPerHomeRun DOUBLE,
                batting_popOuts INT,
                batting_lineOuts INT,
                fielding_gamesStarted INT,
                fielding_caughtStealing INT,
                fielding_stolenBases INT,
                fielding_stolenBasePercentage DOUBLE,
                fielding_assists INT,
                fielding_putOuts INT,
                fielding_errors INT,
                fielding_chances INT,
                fielding_fielding DOUBLE,
                fielding_passedBall INT,
                fielding_pickoffs INT,
                player_type STRING
            ) STORED BY ICEBERG
            PARTITIONED BY (season, month, day, game_id, team_id)
            LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/dim_player_boxscores'
        """)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS mlb_db.aggregated_team_season_boxscore_totals (
                season INT,
                game_id STRING,
                month INT,
                day INT,
                game_time STRING,
                team_id INT,
                total_flyOuts INT,
                total_groundOuts INT,
                total_airOuts INT,
                total_runs INT,
                total_doubles INT,
                total_triples INT,
                total_homeRuns INT,
                total_strikeOuts INT,
                total_baseOnBalls INT,
                total_intentionalWalks INT,
                total_hits INT,
                total_hitByPitch INT,
                total_avg DOUBLE,
                total_atBats INT,
                total_obp DOUBLE,
                total_slg DOUBLE,
                total_ops DOUBLE,
                total_caughtStealing INT,
                total_stolenBases INT,
                total_stolenBasePercentage DOUBLE,
                total_groundIntoDoublePlay INT,
                total_groundIntoTriplePlay INT,
                total_plateAppearances INT,
                total_totalBases INT,
                total_rbi INT,
                total_leftOnBase INT,
                total_sacBunts INT,
                total_sacFlies INT,
                total_catchersInterference INT,
                total_pickoffs INT,
                total_atBatsPerHomeRun DOUBLE,
                total_popOuts INT,
                total_lineOuts INT,
                total_gamesPlayed INT,
                team_type STRING
            ) STORED BY ICEBERG
            PARTITIONED BY (season, month, day, game_id, team_id)
            LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/aggregated_team_season_boxscore_totals'
        """)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS mlb_db.aggregated_player_season_boxscore_totals (
                season INT,
                game_id STRING,
                month INT,
                day INT,
                game_time STRING,
                team_id INT,
                player_id INT,
                total_batting_gamesPlayed INT,
                total_batting_flyOuts INT,
                total_batting_groundOuts INT,
                total_batting_airOuts INT,
                total_batting_runs INT,
                total_batting_doubles INT,
                total_batting_triples INT,
                total_batting_homeRuns INT,
                total_batting_strikeOuts INT,
                total_batting_baseOnBalls INT,
                total_batting_intentionalWalks INT,
                total_batting_hits INT,
                total_batting_hitByPitch INT,
                total_batting_avg DOUBLE,
                total_batting_atBats INT,
                total_batting_obp DOUBLE,
                total_batting_slg DOUBLE,
                total_batting_ops DOUBLE,
                total_batting_caughtStealing INT,
                total_batting_stolenBases INT,
                total_batting_stolenBasePercentage DOUBLE,
                total_batting_groundIntoDoublePlay INT,
                total_batting_groundIntoTriplePlay INT,
                total_batting_plateAppearances INT,
                total_batting_totalBases INT,
                total_batting_rbi INT,
                total_batting_leftOnBase INT,
                total_batting_sacBunts INT,
                total_batting_sacFlies INT,
                total_batting_catchersInterference INT,
                total_batting_pickoffs INT,
                total_batting_atBatsPerHomeRun DOUBLE,
                total_batting_popOuts INT,
                total_batting_lineOuts INT,
                total_fielding_gamesStarted INT,
                total_fielding_caughtStealing INT,
                total_fielding_stolenBases INT,
                total_fielding_stolenBasePercentage DOUBLE,
                total_fielding_assists INT,
                total_fielding_putOuts INT,
                total_fielding_errors INT,
                total_fielding_chances INT,
                total_fielding_fielding DOUBLE,
                total_fielding_passedBall INT,
                total_fielding_pickoffs INT,
                player_type STRING
            ) STORED BY ICEBERG
            PARTITIONED BY (season, month, day, game_id, team_id, player_id)
            LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/aggregated_player_season_boxscore_totals'
        """)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS mlb_db.aggregated_team_season_boxscore_averages (
                season INT,
                game_id STRING,
                month INT,
                day INT,
                game_time STRING,
                team_id INT,
                avg_flyOuts DOUBLE,
                avg_groundOuts DOUBLE,
                avg_airOuts DOUBLE,
                avg_runs DOUBLE,
                avg_doubles DOUBLE,
                avg_triples DOUBLE,
                avg_homeRuns DOUBLE,
                avg_strikeOuts DOUBLE,
                avg_baseOnBalls DOUBLE,
                avg_intentionalWalks DOUBLE,
                avg_hits DOUBLE,
                avg_hitByPitch DOUBLE,
                avg_avg DOUBLE,
                avg_atBats DOUBLE,
                avg_obp DOUBLE,
                avg_slg DOUBLE,
                avg_ops DOUBLE,
                avg_caughtStealing DOUBLE,
                avg_stolenBases DOUBLE,
                avg_stolenBasePercentage DOUBLE,
                avg_groundIntoDoublePlay DOUBLE,
                avg_groundIntoTriplePlay DOUBLE,
                avg_plateAppearances DOUBLE,
                avg_totalBases DOUBLE,
                avg_rbi DOUBLE,
                avg_leftOnBase DOUBLE,
                avg_sacBunts DOUBLE,
                avg_sacFlies DOUBLE,
                avg_catchersInterference DOUBLE,
                avg_pickoffs DOUBLE,
                avg_atBatsPerHomeRun DOUBLE,
                avg_popOuts DOUBLE,
                avg_lineOuts DOUBLE,
                avg_gamesPlayed DOUBLE,
                team_type STRING
            ) STORED BY ICEBERG
            PARTITIONED BY (season, month, day, game_id, team_id)
            LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/aggregated_team_season_boxscore_averages'
        """)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS mlb_db.aggregated_player_season_boxscore_averages (
                season INT,
                game_id STRING,
                month INT,
                day INT,
                game_time STRING,
                team_id INT,
                player_id INT,
                avg_batting_gamesPlayed DOUBLE,
                avg_batting_flyOuts DOUBLE,
                avg_batting_groundOuts DOUBLE,
                avg_batting_airOuts DOUBLE,
                avg_batting_runs DOUBLE,
                avg_batting_doubles DOUBLE,
                avg_batting_triples DOUBLE,
                avg_batting_homeRuns DOUBLE,
                avg_batting_strikeOuts DOUBLE,
                avg_batting_baseOnBalls DOUBLE,
                avg_batting_intentionalWalks DOUBLE,
                avg_batting_hits DOUBLE,
                avg_batting_hitByPitch DOUBLE,
                avg_batting_avg DOUBLE,
                avg_batting_atBats DOUBLE,
                avg_batting_obp DOUBLE,
                avg_batting_slg DOUBLE,
                avg_batting_ops DOUBLE,
                avg_batting_caughtStealing DOUBLE,
                avg_batting_stolenBases DOUBLE,
                avg_batting_stolenBasePercentage DOUBLE,
                avg_batting_groundIntoDoublePlay DOUBLE,
                avg_batting_groundIntoTriplePlay DOUBLE,
                avg_batting_plateAppearances DOUBLE,
                avg_batting_totalBases DOUBLE,
                avg_batting_rbi DOUBLE,
                avg_batting_leftOnBase DOUBLE,
                avg_batting_sacBunts DOUBLE,
                avg_batting_sacFlies DOUBLE,
                avg_batting_catchersInterference DOUBLE,
                avg_batting_pickoffs DOUBLE,
                avg_batting_atBatsPerHomeRun DOUBLE,
                avg_batting_popOuts DOUBLE,
                avg_batting_lineOuts DOUBLE,
                avg_fielding_gamesStarted DOUBLE,
                avg_fielding_caughtStealing DOUBLE,
                avg_fielding_stolenBases DOUBLE,
                avg_fielding_stolenBasePercentage DOUBLE,
                avg_fielding_assists DOUBLE,
                avg_fielding_putOuts DOUBLE,
                avg_fielding_errors DOUBLE,
                avg_fielding_chances DOUBLE,
                avg_fielding_fielding DOUBLE,
                avg_fielding_passedBall DOUBLE,
                avg_fielding_pickoffs DOUBLE,
                player_type STRING
            ) STORED BY ICEBERG
            PARTITIONED BY (season, month, day, game_id, team_id, player_id)
            LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/aggregated_player_season_boxscore_averages'
        """)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS mlb_db.cumulative_team_season_boxscore_totals (
                season INT,
                game_id STRING,
                month INT,
                day INT,
                game_time STRING,
                team_id INT,
                total_flyOuts INT,
                total_groundOuts INT,
                total_airOuts INT,
                total_runs INT,
                total_doubles INT,
                total_triples INT,
                total_homeRuns INT,
                total_strikeOuts INT,
                total_baseOnBalls INT,
                total_intentionalWalks INT,
                total_hits INT,
                total_hitByPitch INT,
                total_avg DOUBLE,
                total_atBats INT,
                total_obp DOUBLE,
                total_slg DOUBLE,
                total_ops DOUBLE,
                total_caughtStealing INT,
                total_stolenBases INT,
                total_stolenBasePercentage DOUBLE,
                total_groundIntoDoublePlay INT,
                total_groundIntoTriplePlay INT,
                total_plateAppearances INT,
                total_totalBases INT,
                total_rbi INT,
                total_leftOnBase INT,
                total_sacBunts INT,
                total_sacFlies INT,
                total_catchersInterference INT,
                total_pickoffs INT,
                total_atBatsPerHomeRun DOUBLE,
                total_popOuts INT,
                total_lineOuts INT,
                total_gamesPlayed INT,
                team_type STRING
            ) STORED BY ICEBERG
            PARTITIONED BY (season, month, day, game_id, team_id)
            LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/cumulative_team_season_boxscore_totals'
        """)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS mlb_db.cumulative_player_season_boxscore_totals (
                season INT,
                game_id STRING,
                month INT,
                day INT,
                game_time STRING,
                team_id INT,
                player_id INT,
                total_batting_gamesPlayed INT,
                total_batting_flyOuts INT,
                total_batting_groundOuts INT,
                total_batting_airOuts INT,
                total_batting_runs INT,
                total_batting_doubles INT,
                total_batting_triples INT,
                total_batting_homeRuns INT,
                total_batting_strikeOuts INT,
                total_batting_baseOnBalls INT,
                total_batting_intentionalWalks INT,
                total_batting_hits INT,
                total_batting_hitByPitch INT,
                total_batting_avg DOUBLE,
                total_batting_atBats INT,
                total_batting_obp DOUBLE,
                total_batting_slg DOUBLE,
                total_batting_ops DOUBLE,
                total_batting_caughtStealing INT,
                total_batting_stolenBases INT,
                total_batting_stolenBasePercentage DOUBLE,
                total_batting_groundIntoDoublePlay INT,
                total_batting_groundIntoTriplePlay INT,
                total_batting_plateAppearances INT,
                total_batting_totalBases INT,
                total_batting_rbi INT,
                total_batting_leftOnBase INT,
                total_batting_sacBunts INT,
                total_batting_sacFlies INT,
                total_batting_catchersInterference INT,
                total_batting_pickoffs INT,
                total_batting_atBatsPerHomeRun DOUBLE,
                total_batting_popOuts INT,
                total_batting_lineOuts INT,
                total_fielding_gamesStarted INT,
                total_fielding_caughtStealing INT,
                total_fielding_stolenBases INT,
                total_fielding_stolenBasePercentage DOUBLE,
                total_fielding_assists INT,
                total_fielding_putOuts INT,
                total_fielding_errors INT,
                total_fielding_chances INT,
                total_fielding_fielding DOUBLE,
                total_fielding_passedBall INT,
                total_fielding_pickoffs INT,
                player_type STRING
            ) STORED BY ICEBERG
            PARTITIONED BY (season, month, day, game_id, team_id, player_id)
            LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/cumulative_player_season_boxscore_totals'
        """)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS mlb_db.cumulative_team_season_boxscore_averages (
                season INT,
                game_id STRING,
                month INT,
                day INT,
                game_time STRING,
                team_id INT,
                avg_flyOuts DOUBLE,
                avg_groundOuts DOUBLE,
                avg_airOuts DOUBLE,
                avg_runs DOUBLE,
                avg_doubles DOUBLE,
                avg_triples DOUBLE,
                avg_homeRuns DOUBLE,
                avg_strikeOuts DOUBLE,
                avg_baseOnBalls DOUBLE,
                avg_intentionalWalks DOUBLE,
                avg_hits DOUBLE,
                avg_hitByPitch DOUBLE,
                avg_avg DOUBLE,
                avg_atBats DOUBLE,
                avg_obp DOUBLE,
                avg_slg DOUBLE,
                avg_ops DOUBLE,
                avg_caughtStealing DOUBLE,
                avg_stolenBases DOUBLE,
                avg_stolenBasePercentage DOUBLE,
                avg_groundIntoDoublePlay DOUBLE,
                avg_groundIntoTriplePlay DOUBLE,
                avg_plateAppearances DOUBLE,
                avg_totalBases DOUBLE,
                avg_rbi DOUBLE,
                avg_leftOnBase DOUBLE,
                avg_sacBunts DOUBLE,
                avg_sacFlies DOUBLE,
                avg_catchersInterference DOUBLE,
                avg_pickoffs DOUBLE,
                avg_atBatsPerHomeRun DOUBLE,
                avg_popOuts DOUBLE,
                avg_lineOuts DOUBLE,
                avg_gamesPlayed DOUBLE,
                team_type STRING
            ) STORED BY ICEBERG
            PARTITIONED BY (season, month, day, game_id, team_id)
            LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/cumulative_team_season_boxscore_averages'
        """)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS mlb_db.cumulative_player_season_boxscore_averages (
                season INT,
                game_id STRING,
                month INT,
                day INT,
                game_time STRING,
                team_id INT,
                player_id INT,
                avg_batting_gamesPlayed DOUBLE,
                avg_batting_flyOuts DOUBLE,
                avg_batting_groundOuts DOUBLE,
                avg_batting_airOuts DOUBLE,
                avg_batting_runs DOUBLE,
                avg_batting_doubles DOUBLE,
                avg_batting_triples DOUBLE,
                avg_batting_homeRuns DOUBLE,
                avg_batting_strikeOuts DOUBLE,
                avg_batting_baseOnBalls DOUBLE,
                avg_batting_intentionalWalks DOUBLE,
                avg_batting_hits DOUBLE,
                avg_batting_hitByPitch DOUBLE,
                avg_batting_avg DOUBLE,
                avg_batting_atBats DOUBLE,
                avg_batting_obp DOUBLE,
                avg_batting_slg DOUBLE,
                avg_batting_ops DOUBLE,
                avg_batting_caughtStealing DOUBLE,
                avg_batting_stolenBases DOUBLE,
                avg_batting_stolenBasePercentage DOUBLE,
                avg_batting_groundIntoDoublePlay DOUBLE,
                avg_batting_groundIntoTriplePlay DOUBLE,
                avg_batting_plateAppearances DOUBLE,
                avg_batting_totalBases DOUBLE,
                avg_batting_rbi DOUBLE,
                avg_batting_leftOnBase DOUBLE,
                avg_batting_sacBunts DOUBLE,
                avg_batting_sacFlies DOUBLE,
                avg_batting_catchersInterference DOUBLE,
                avg_batting_pickoffs DOUBLE,
                avg_batting_atBatsPerHomeRun DOUBLE,
                avg_batting_popOuts DOUBLE,
                avg_batting_lineOuts DOUBLE,
                avg_fielding_gamesStarted DOUBLE,
                avg_fielding_caughtStealing DOUBLE,
                avg_fielding_stolenBases DOUBLE,
                avg_fielding_stolenBasePercentage DOUBLE,
                avg_fielding_assists DOUBLE,
                avg_fielding_putOuts DOUBLE,
                avg_fielding_errors DOUBLE,
                avg_fielding_chances DOUBLE,
                avg_fielding_fielding DOUBLE,
                avg_fielding_passedBall DOUBLE,
                avg_fielding_pickoffs DOUBLE,
                player_type STRING
            ) STORED BY ICEBERG
            PARTITIONED BY (season, month, day, game_id, team_id, player_id)
            LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/cumulative_player_season_boxscore_averages'
        """)

        logger.info("Combined tables created successfully!")
    except Exception as e:
        logger.error(f"Error creating combined tables: {e}", exc_info=True)
        raise

def migrate_data_to_combined_tables(spark):
    try:
        spark.sql("""
            INSERT INTO mlb_db.dim_team_boxscores
            SELECT game_id, season, month, day, game_time, *, 'home' as team_type FROM mlb_db.dim_home_team_boxscores
            UNION ALL
            SELECT game_id, season, month, day, game_time, *, 'away' as team_type FROM mlb_db.dim_away_team_boxscores
        """)

        spark.sql("""
            INSERT INTO mlb_db.dim_player_boxscores
            SELECT game_id, season, month, day, game_time, *, 'home' as player_type FROM mlb_db.dim_home_player_boxscores
            UNION ALL
            SELECT game_id, season, month, day, game_time, *, 'away' as player_type FROM mlb_db.dim_away_player_boxscores
        """)

        spark.sql("""
            INSERT INTO mlb_db.aggregated_team_season_boxscore_totals
            SELECT game_id, season, month, day, game_time, *, 'home' as team_type FROM mlb_db.aggregated_home_team_season_boxscore_totals
            UNION ALL
            SELECT game_id, season, month, day, game_time, *, 'away' as team_type FROM mlb_db.aggregated_away_team_season_boxscore_totals
        """)

        spark.sql("""
            INSERT INTO mlb_db.aggregated_player_season_boxscore_totals
            SELECT game_id, season, month, day, game_time, *, 'home' as player_type FROM mlb_db.aggregated_home_player_season_boxscore_totals
            UNION ALL
            SELECT game_id, season, month, day, game_time, *, 'away' as player_type FROM mlb_db.aggregated_away_player_season_boxscore_totals
        """)

        spark.sql("""
            INSERT INTO mlb_db.aggregated_team_season_boxscore_averages
            SELECT game_id, season, month, day, game_time, *, 'home' as team_type FROM mlb_db.aggregated_home_team_season_boxscore_averages
            UNION ALL
            SELECT game_id, season, month, day, game_time, *, 'away' as team_type FROM mlb_db.aggregated_away_team_season_boxscore_averages
        """)

        spark.sql("""
            INSERT INTO mlb_db.aggregated_player_season_boxscore_averages
            SELECT game_id, season, month, day, game_time, *, 'home' as player_type FROM mlb_db.aggregated_home_player_season_boxscore_averages
            UNION ALL
            SELECT game_id, season, month, day, game_time, *, 'away' as player_type FROM mlb_db.aggregated_away_player_season_boxscore_averages
        """)

        spark.sql("""
            INSERT INTO mlb_db.cumulative_team_season_boxscore_totals
            SELECT game_id, season, month, day, game_time, *, 'home' as team_type FROM mlb_db.cumulative_home_team_season_boxscore_totals
            UNION ALL
            SELECT game_id, season, month, day, game_time, *, 'away' as team_type FROM mlb_db.cumulative_away_team_season_boxscore_totals
        """)

        spark.sql("""
            INSERT INTO mlb_db.cumulative_player_season_boxscore_totals
            SELECT game_id, season, month, day, game_time, *, 'home' as player_type FROM mlb_db.cumulative_home_player_season_boxscore_totals
            UNION ALL
            SELECT game_id, season, month, day, game_time, *, 'away' as player_type FROM mlb_db.cumulative_away_player_season_boxscore_totals
        """)

        spark.sql("""
            INSERT INTO mlb_db.cumulative_team_season_boxscore_averages
            SELECT game_id, season, month, day, game_time, *, 'home' as team_type FROM mlb_db.cumulative_home_team_season_boxscore_averages
            UNION ALL
            SELECT game_id, season, month, day, game_time, *, 'away' as team_type FROM mlb_db.cumulative_away_team_season_boxscore_averages
        """)

        spark.sql("""
            INSERT INTO mlb_db.cumulative_player_season_boxscore_averages
            SELECT game_id, season, month, day, game_time, *, 'home' as player_type FROM mlb_db.cumulative_home_player_season_boxscore_averages
            UNION ALL
            SELECT game_id, season, month, day, game_time, *, 'away' as player_type FROM mlb_db.cumulative_away_player_season_boxscore_averages
        """)

        logger.info("Data migration to combined tables completed successfully!")
    except Exception as e:
        logger.error(f"Error migrating data to combined tables: {e}", exc_info=True)
        raise

def handle_scd_type_2(spark, df, table_name, join_columns, compare_columns):
    try:
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number

        window_spec = Window.partitionBy(*join_columns).orderBy(df.record_start_date.desc())

        df = df.withColumn('row_num', row_number().over(window_spec)).filter(col('row_num') == 1).drop('row_num')
        df = df.withColumn('record_start_date', current_date())
        df = df.withColumn('record_end_date', lit(None).cast('date'))
        df = df.withColumn('is_current', lit(True))

        updates_df = df.selectExpr("*")
        updates_df.createOrReplaceTempView("updates_df")

        join_condition = " AND ".join([f"t.{col} = u.{col}" for col in join_columns])
        compare_condition = " OR ".join([f"t.{col} <> u.{col}" for col in compare_columns])

        merge_query = f"""
        MERGE INTO {table_name} t
        USING updates_df u
        ON {join_condition} AND t.is_current = true
        WHEN MATCHED AND ({compare_condition}) THEN
            UPDATE SET t.is_current = false, t.record_end_date = current_date()
        WHEN NOT MATCHED THEN
            INSERT ({", ".join(df.columns)})
            VALUES ({", ".join([f"u.{col}" for col in df.columns])})
        """

        spark.sql(merge_query)
        logger.info("SCD Type 2 handling completed successfully.")
    except Exception as e:
        logger.error(f"Error handling SCD Type 2: {e}", exc_info=True)
        raise

def extract_dynamic_columns(columns):
    player_ids = list(set(re.findall(r'(home|away)_players_(ID\d+)', " ".join(columns))))
    return player_ids

def create_dynamic_fields(player_ids):
    dynamic_fields = []
    for player_id in player_ids:
        for prefix in ["away", "home"]:
            dynamic_fields.extend([
                StructField(f"{prefix}_players_{player_id}_person_id", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_person_fullName", StringType(), True),
                StructField(f"{prefix}_players_{player_id}_person_link", StringType(), True),
                StructField(f"{prefix}_players_{player_id}_jerseyNumber", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_position_code", StringType(), True),
                StructField(f"{prefix}_players_{player_id}_position_name", StringType(), True),
                StructField(f"{prefix}_players_{player_id}_position_type", StringType(), True),
                StructField(f"{prefix}_players_{player_id}_position_abbreviation", StringType(), True),
                StructField(f"{prefix}_players_{player_id}_status_code", StringType(), True),
                StructField(f"{prefix}_players_{player_id}_status_description", StringType(), True),
                StructField(f"{prefix}_players_{player_id}_parentTeamId", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_battingOrder", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_summary", StringType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_gamesPlayed", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_flyOuts", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_groundOuts", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_airOuts", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_runs", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_doubles", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_triples", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_homeRuns", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_strikeOuts", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_baseOnBalls", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_intentionalWalks", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_hits", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_hitByPitch", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_atBats", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_caughtStealing", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_stolenBases", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_stolenBasePercentage", DoubleType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_groundIntoDoublePlay", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_groundIntoTriplePlay", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_plateAppearances", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_totalBases", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_rbi", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_leftOnBase", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_sacBunts", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_sacFlies", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_catchersInterference", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_pickoffs", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_atBatsPerHomeRun", DoubleType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_popOuts", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_batting_lineOuts", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_fielding_gamesStarted", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_fielding_caughtStealing", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_fielding_stolenBases", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_fielding_stolenBasePercentage", DoubleType(), True),
                StructField(f"{prefix}_players_{player_id}_fielding_assists", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_fielding_putOuts", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_fielding_errors", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_fielding_chances", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_fielding_fielding", DoubleType(), True),
                StructField(f"{prefix}_players_{player_id}_fielding_passedBall", IntegerType(), True),
                StructField(f"{prefix}_players_{player_id}_gameStatus_isCurrentBatter", BooleanType(), True),
                StructField(f"{prefix}_players_{player_id}_gameStatus_isCurrentPitcher", BooleanType(), True),
                StructField(f"{prefix}_players_{player_id}_gameStatus_isOnBench", BooleanType(), True),
                StructField(f"{prefix}_players_{player_id}_gameStatus_isSubstitute", BooleanType(), True),
            ])
    return dynamic_fields

def read_and_write_stream(spark):
    try:
        kafka_df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'kafka1:9092,kafka2:9093,kafka3:9094') \
            .option('subscribe', 'boxscore_info') \
            .option('startingOffsets', 'earliest') \
            .option('checkpointLocation', '/tmp/checkpoints/boxscore_info') \
            .load()

        base_schema = StructType([
            StructField("game_id", StringType(), True),
            StructField("season", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("day", IntegerType(), True),
            StructField("game_time", StringType(), True)
        ])

        def add_team_stats_fields(prefix, schema):
            team_schema = [
                StructField(f"{prefix}_team_allStarStatus", StringType(), True),
                StructField(f"{prefix}_team_id", IntegerType(), True),
                StructField(f"{prefix}_team_name", StringType(), True),
                StructField(f"{prefix}_team_link", StringType(), True),
                StructField(f"{prefix}_teamStats_batting_flyOuts", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_groundOuts", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_airOuts", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_runs", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_doubles", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_triples", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_homeRuns", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_strikeOuts", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_baseOnBalls", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_intentionalWalks", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_hits", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_hitByPitch", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_avg", DoubleType(), True),
                StructField(f"{prefix}_teamStats_batting_atBats", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_obp", DoubleType(), True),
                StructField(f"{prefix}_teamStats_batting_slg", DoubleType(), True),
                StructField(f"{prefix}_teamStats_batting_ops", DoubleType(), True),
                StructField(f"{prefix}_teamStats_batting_caughtStealing", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_stolenBases", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_stolenBasePercentage", DoubleType(), True),
                StructField(f"{prefix}_teamStats_batting_groundIntoDoublePlay", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_groundIntoTriplePlay", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_plateAppearances", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_totalBases", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_rbi", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_leftOnBase", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_sacBunts", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_sacFlies", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_catchersInterference", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_pickoffs", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_atBatsPerHomeRun", DoubleType(), True),
                StructField(f"{prefix}_teamStats_batting_popOuts", IntegerType(), True),
                StructField(f"{prefix}_teamStats_batting_lineOuts", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_flyOuts", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_groundOuts", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_airOuts", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_runs", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_doubles", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_triples", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_homeRuns", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_strikeOuts", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_baseOnBalls", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_intentionalWalks", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_hits", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_hitByPitch", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_atBats", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_obp", DoubleType(), True),
                StructField(f"{prefix}_teamStats_pitching_caughtStealing", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_stolenBases", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_stolenBasePercentage", DoubleType(), True),
                StructField(f"{prefix}_teamStats_pitching_numberOfPitches", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_era", DoubleType(), True),
                StructField(f"{prefix}_teamStats_pitching_inningsPitched", DoubleType(), True),
                StructField(f"{prefix}_teamStats_pitching_saveOpportunities", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_earnedRuns", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_whip", DoubleType(), True),
                StructField(f"{prefix}_teamStats_pitching_battersFaced", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_outs", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_completeGames", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_shutouts", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_pitchesThrown", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_balls", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_strikes", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_strikePercentage", DoubleType(), True),
                StructField(f"{prefix}_teamStats_pitching_hitBatsmen", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_balks", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_wildPitches", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_pickoffs", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_groundOutsToAirouts", DoubleType(), True),
                StructField(f"{prefix}_teamStats_pitching_rbi", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_pitchesPerInning", DoubleType(), True),
                StructField(f"{prefix}_teamStats_pitching_runsScoredPer9", DoubleType(), True),
                StructField(f"{prefix}_teamStats_pitching_homeRunsPer9", DoubleType(), True),
                StructField(f"{prefix}_teamStats_pitching_inheritedRunners", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_inheritedRunnersScored", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_catchersInterference", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_sacBunts", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_sacFlies", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_passedBall", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_popOuts", IntegerType(), True),
                StructField(f"{prefix}_teamStats_pitching_lineOuts", IntegerType(), True),
                StructField(f"{prefix}_teamStats_fielding_caughtStealing", IntegerType(), True),
                StructField(f"{prefix}_teamStats_fielding_stolenBases", IntegerType(), True),
                StructField(f"{prefix}_teamStats_fielding_stolenBasePercentage", DoubleType(), True),
                StructField(f"{prefix}_teamStats_fielding_assists", IntegerType(), True),
                StructField(f"{prefix}_teamStats_fielding_putOuts", IntegerType(), True),
                StructField(f"{prefix}_teamStats_fielding_errors", IntegerType(), True),
                StructField(f"{prefix}_teamStats_fielding_chances", IntegerType(), True),
                StructField(f"{prefix}_teamStats_fielding_passedBall", IntegerType(), True),
                StructField(f"{prefix}_teamStats_fielding_pickoffs", IntegerType(), True)
            ]
            return StructType(schema.fields + team_schema)

        for prefix in ["away", "home"]:
            base_schema = add_team_stats_fields(prefix, base_schema)

        def add_dynamic_columns(df, base_schema):
            current_columns = df.columns
            dynamic_fields = []
            for col_name in current_columns:
                if col_name.startswith("home_") or col_name.startswith("away_"):
                    field = StructField(col_name, StringType(), True)
                    dynamic_fields.append(field)
            new_schema = StructType(base_schema.fields + dynamic_fields)
            return new_schema

        current_schema = base_schema

        def process_batch(batch_df, batch_id):
            nonlocal current_schema

            new_schema = add_dynamic_columns(batch_df, current_schema)
            current_schema = new_schema

            parsed_df = batch_df.selectExpr("CAST(value AS STRING)") \
                .select(from_json(col("value"), new_schema).alias("data")).select("data.*")

            columns_to_select = [col(c).alias(re.sub(r'^(home_|away_)', '', c)) for c in parsed_df.columns]
            cleaned_df = parsed_df.select(columns_to_select)

            current_year = year(current_date())
            current_season_df = cleaned_df.filter(col("season") == current_year)

            def handle_team_stats(df, table_prefix, team_type):
                aggregated_totals_df = df.groupBy("team_id", "season", "month", "day", "game_id").agg(
                    spark_sum("runs").alias("total_runs"),
                    spark_sum("hits").alias("total_hits"),
                    spark_sum("homeRuns").alias("total_homeRuns"),
                    spark_sum("rbi").alias("total_rbi"),
                    spark_sum("strikeOuts").alias("total_strikeOuts"),
                    spark_sum("baseOnBalls").alias("total_baseOnBalls"),
                    avg("avg").alias("total_avg")
                ).select(
                    col("team_id"), col("season"), col("month"), col("day"), col("game_id"),
                    col("total_runs"), col("total_hits"), col("total_homeRuns"),
                    col("total_rbi"), col("total_strikeOuts"), col("total_baseOnBalls"),
                    col("total_avg")
                ).withColumn("team_type", lit(team_type))

                handle_scd_type_2(
                    spark, aggregated_totals_df, f"mlb_db.{table_prefix}_season_totals",
                    ["team_id", "season", "month", "day", "game_id"],
                    ["total_runs", "total_hits", "total_homeRuns", "total_rbi", "total_strikeOuts", "total_baseOnBalls", "total_avg"]
                )

                cumulative_totals_df = df.groupBy("team_id", "season", "game_id").agg(
                    spark_sum("runs").alias("cumulative_runs"),
                    spark_sum("hits").alias("cumulative_hits"),
                    spark_sum("homeRuns").alias("cumulative_homeRuns"),
                    spark_sum("rbi").alias("cumulative_rbi"),
                    spark_sum("strikeOuts").alias("cumulative_strikeOuts"),
                    spark_sum("baseOnBalls").alias("cumulative_baseOnBalls"),
                    avg("avg").alias("cumulative_avg")
                ).select(
                    col("team_id"), col("season"), col("game_id"),
                    col("cumulative_runs"), col("cumulative_hits"), col("cumulative_homeRuns"),
                    col("cumulative_rbi"), col("cumulative_strikeOuts"), col("cumulative_baseOnBalls"),
                    col("cumulative_avg")
                ).withColumn("team_type", lit(team_type))

                handle_scd_type_2(
                    spark, cumulative_totals_df, f"mlb_db.cumulative_{table_prefix}_season_totals",
                    ["team_id", "season", "game_id"],
                    ["cumulative_runs", "cumulative_hits", "cumulative_homeRuns", "cumulative_rbi", "cumulative_strikeOuts", "cumulative_baseOnBalls", "cumulative_avg"]
                )

            home_team_df = current_season_df.select(
                col("game_id"),
                col("season"),
                col("month"),
                col("day"),
                col("game_time"),
                col("home_team_id").alias("team_id"),
                col("home_team_name").alias("team_name"),
                col("home_teamStats_batting_runs").alias("runs"),
                col("home_teamStats_batting_hits").alias("hits"),
                col("home_teamStats_batting_homeRuns").alias("homeRuns"),
                col("home_teamStats_batting_rbi").alias("rbi"),
                col("home_teamStats_batting_strikeOuts").alias("strikeOuts"),
                col("home_teamStats_batting_baseOnBalls").alias("baseOnBalls"),
                col("home_teamStats_batting_avg").alias("avg")
            )
            handle_team_stats(home_team_df, "home_team", "home")

            away_team_df = current_season_df.select(
                col("game_id"),
                col("season"),
                col("month"),
                col("day"),
                col("game_time"),
                col("away_team_id").alias("team_id"),
                col("away_team_name").alias("team_name"),
                col("away_teamStats_batting_runs").alias("runs"),
                col("away_teamStats_batting_hits").alias("hits"),
                col("away_teamStats_batting_homeRuns").alias("homeRuns"),
                col("away_teamStats_batting_rbi").alias("rbi"),
                col("away_teamStats_batting_strikeOuts").alias("strikeOuts"),
                col("away_teamStats_batting_baseOnBalls").alias("baseOnBalls"),
                col("away_teamStats_batting_avg").alias("avg")
            )
            handle_team_stats(away_team_df, "away_team", "away")

            def handle_player_stats(df, table_prefix, player_type):
                aggregated_totals_df = df.groupBy("player_id", "season", "month", "day", "game_id").agg(
                    spark_sum("runs").alias("total_runs"),
                    spark_sum("hits").alias("total_hits"),
                    spark_sum("homeRuns").alias("total_homeRuns"),
                    spark_sum("rbi").alias("total_rbi"),
                    spark_sum("strikeOuts").alias("total_strikeOuts"),
                    spark_sum("baseOnBalls").alias("total_baseOnBalls"),
                    avg("avg").alias("total_avg")
                ).select(
                    col("player_id"), col("season"), col("month"), col("day"), col("game_id"),
                    col("total_runs"), col("total_hits"), col("total_homeRuns"),
                    col("total_rbi"), col("total_strikeOuts"), col("total_baseOnBalls"),
                    col("total_avg")
                ).withColumn("player_type", lit(player_type))

                handle_scd_type_2(
                    spark, aggregated_totals_df, f"mlb_db.{table_prefix}_season_totals",
                    ["player_id", "season", "month", "day", "game_id"],
                    ["total_runs", "total_hits", "total_homeRuns", "total_rbi", "total_strikeOuts", "total_baseOnBalls", "total_avg"]
                )

                cumulative_totals_df = df.groupBy("player_id", "season", "game_id").agg(
                    spark_sum("runs").alias("cumulative_runs"),
                    spark_sum("hits").alias("cumulative_hits"),
                    spark_sum("homeRuns").alias("cumulative_homeRuns"),
                    spark_sum("rbi").alias("cumulative_rbi"),
                    spark_sum("strikeOuts").alias("cumulative_strikeOuts"),
                    spark_sum("baseOnBalls").alias("cumulative_baseOnBalls"),
                    avg("avg").alias("cumulative_avg")
                ).select(
                    col("player_id"), col("season"), col("game_id"),
                    col("cumulative_runs"), col("cumulative_hits"), col("cumulative_homeRuns"),
                    col("cumulative_rbi"), col("cumulative_strikeOuts"), col("cumulative_baseOnBalls"),
                    col("cumulative_avg")
                ).withColumn("player_type", lit(player_type))

                handle_scd_type_2(
                    spark, cumulative_totals_df, f"mlb_db.cumulative_{table_prefix}_season_totals",
                    ["player_id", "season", "game_id"],
                    ["cumulative_runs", "cumulative_hits", "cumulative_homeRuns", "cumulative_rbi", "cumulative_strikeOuts", "cumulative_baseOnBalls", "cumulative_avg"]
                )

            home_players_df = current_season_df.selectExpr(
                "game_id", "season", "month", "day", "game_time", "home_team_id as team_id", "home_team_name as team_name",
                "explode(home_players) as player"
            ).select(
                col("game_id"),
                col("season"),
                col("month"),
                col("day"),
                col("game_time"),
                col("team_id"),
                col("team_name"),
                col("player.person.id").alias("player_id"),
                col("player.person.fullName").alias("player_name"),
                col("player.batting.runs").alias("runs"),
                col("player.batting.hits").alias("hits"),
                col("player.batting.homeRuns").alias("homeRuns"),
                col("player.batting.rbi").alias("rbi"),
                col("player.batting.strikeOuts").alias("strikeOuts"),
                col("player.batting.baseOnBalls").alias("baseOnBalls"),
                col("player.batting.avg").alias("avg")
            )
            handle_player_stats(home_players_df, "home_players", "home")

            away_players_df = current_season_df.selectExpr(
                "game_id", "season", "month", "day", "game_time", "away_team_id as team_id", "away_team_name as team_name",
                "explode(away_players) as player"
            ).select(
                col("game_id"),
                col("season"),
                col("month"),
                col("day"),
                col("game_time"),
                col("team_id"),
                col("team_name"),
                col("player.person.id").alias("player_id"),
                col("player.person.fullName").alias("player_name"),
                col("player.batting.runs").alias("runs"),
                col("player.batting.hits").alias("hits"),
                col("player.batting.homeRuns").alias("homeRuns"),
                col("player.batting.rbi").alias("rbi"),
                col("player.batting.strikeOuts").alias("strikeOuts"),
                col("player.batting.baseOnBalls").alias("baseOnBalls"),
                col("player.batting.avg").alias("avg")
            )
            handle_player_stats(away_players_df, "away_players", "away")

        query = kafka_df.selectExpr("CAST(value AS STRING)", "timestamp") \
            .select(from_json(col("value"), current_schema).alias("data"), "timestamp") \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(window("timestamp", "10 minutes", "5 minutes"), "data.*") \
            .count() \
            .writeStream \
            .foreachBatch(process_batch) \
            .option("checkpointLocation", "/tmp/checkpoints/boxscore_info") \
            .start()

        query.awaitTermination()
        
        logger.info("Streaming data from Kafka to Iceberg tables initiated successfully.")
    except Exception as e:
        logger.error(f"Error in read_and_write_stream function: {e}", exc_info=True)
        raise

def upload_to_storage(data_frame, storage_name, file_name):
    try:
        csv_data = data_frame.toPandas().to_csv(index=False).encode('utf-8')
        if len(csv_data) > 1000000000:
            raise ValueError("Data size is too large to upload.")
        azure_blob_client = azure_blob_service_client.get_blob_client(container=storage_name, blob=f"{file_name}.csv")
        azure_blob_client.upload_blob(io.BytesIO(csv_data), overwrite=True)
        logger.info(f"Data uploaded to Azure Blob Storage in container {storage_name} with file name {file_name}.csv")
    except Exception as e:
        logger.error(f"Failed to upload data to storage: {e}")

if __name__ == "__main__":
    spark = create_spark_connection()
    azure_container = os.getenv('AZURE_CONTAINER')
    azure_account_name = os.getenv('AZURE_ACCOUNT_NAME')
    create_combined_tables(spark, azure_container, azure_account_name)
    migrate_data_to_combined_tables(spark)
    read_and_write_stream(spark)
