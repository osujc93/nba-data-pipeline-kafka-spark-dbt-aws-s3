from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, year, month, dayofmonth, current_date, lit, sum as spark_sum, avg, window, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
import logging
import re
import os
from azure.storage.blob import BlobServiceClient
import io

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='app.log', filemode='w')
logger = logging.getLogger(__name__)

bootstrap_servers = ['kafka1:9092', 'kafka2:9093', 'kafka3:9094']

azure_blob_service_client = BlobServiceClient.from_connection_string(
    f"DefaultEndpointsProtocol=https;AccountName={os.getenv('AZURE_ACCOUNT_NAME')};AccountKey={os.getenv('AZURE_ACCOUNT_KEY')};EndpointSuffix=core.windows.net"
)

def create_spark_connection():
    try:
        azure_account_name = os.getenv('AZURE_ACCOUNT_NAME')
        azure_account_key = os.getenv('AZURE_ACCOUNT_KEY')
        azure_container = os.getenv('AZURE_CONTAINER')
        
        spark = SparkSession.builder \
            .appName('GameResultsInfo') \
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

def create_tables(spark, azure_container, azure_account_name):
    try:
        # Original game results table
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.game_results (
            away_team_allStarStatus STRING,
            away_team_id INT,
            away_team_name STRING,
            away_team_link STRING,
            away_team_season INT,
            away_team_teamCode STRING,
            away_team_fileCode STRING,
            away_team_abbreviation STRING,
            away_team_teamName STRING,
            away_team_locationName STRING,
            away_team_firstYearOfPlay INT,
            away_team_shortName STRING,
            away_team_franchiseName STRING,
            away_team_clubName STRING,
            away_team_active BOOLEAN,
            away_team_springLeague_id INT,
            away_team_springLeague_name STRING,
            away_team_springLeague_link STRING,
            away_team_springLeague_abbreviation STRING,
            away_team_venue_id INT,
            away_team_venue_name STRING,
            away_team_venue_link STRING,
            away_team_springVenue_id INT,
            away_team_springVenue_link STRING,
            away_team_league_id INT,
            away_team_league_name STRING,
            away_team_league_link STRING,
            away_team_division_id INT,
            away_team_division_name STRING,
            away_team_division_link STRING,
            away_team_sport_id INT,
            away_team_sport_link STRING,
            away_team_sport_name STRING,
            away_team_away_score INT,
            away_team_away_isWinner BOOLEAN,
            away_team_away_leagueRecord_wins INT,
            away_team_away_leagueRecord_losses INT,
            away_team_away_leagueRecord_pct DOUBLE,
            home_team_allStarStatus STRING,
            home_team_id INT,
            home_team_name STRING,
            home_team_link STRING,
            home_team_season INT,
            home_team_teamCode STRING,
            home_team_fileCode STRING,
            home_team_abbreviation STRING,
            home_team_teamName STRING,
            home_team_locationName STRING,
            home_team_firstYearOfPlay INT,
            home_team_shortName STRING,
            home_team_franchiseName STRING,
            home_team_clubName STRING,
            home_team_active BOOLEAN,
            home_team_springLeague_id INT,
            home_team_springLeague_name STRING,
            home_team_springLeague_link STRING,
            home_team_springLeague_abbreviation STRING,
            home_team_venue_id INT,
            home_team_venue_name STRING,
            home_team_venue_link STRING,
            home_team_springVenue_id INT,
            home_team_springVenue_link STRING,
            home_team_league_id INT,
            home_team_league_name STRING,
            home_team_league_link STRING,
            home_team_division_id INT,
            home_team_division_name STRING,
            home_team_division_link STRING,
            home_team_sport_id INT,
            home_team_sport_link STRING,
            home_team_sport_name STRING,
            home_team_home_score INT,
            home_team_home_isWinner BOOLEAN,
            home_team_home_leagueRecord_wins INT,
            home_team_home_leagueRecord_losses INT,
            home_team_home_leagueRecord_pct DOUBLE,
            gamePk INT,
            season INT,
            officialDate STRING,
            away_pitcher_id INT,
            away_pitcher_fullName STRING,
            away_pitcher_link STRING,
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
            home_pitcher_link STRING,
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
        ) USING iceberg
        PARTITIONED BY (season, month(officialDate), day(officialDate))
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/game_results'
        """)

        # Dimension Tables
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.dim_teams (
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
        ) USING iceberg
        PARTITIONED BY (team_id)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/dim_teams'
        """)

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.dim_pitchers (
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
        ) USING iceberg
        PARTITIONED BY (pitcher_id)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/dim_pitchers'
        """)

        # Fact Tables
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.fact_game_results (
            game_id INT,
            year INT,
            official_date DATE,
            home_team_id INT,
            away_team_id INT,
            home_score INT,
            away_score INT,
            home_is_winner BOOLEAN,
            away_is_winner BOOLEAN,
            home_pitcher_id INT,
            away_pitcher_id INT
        ) USING iceberg
        PARTITIONED BY (year, month(official_date), day(official_date))
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_game_results'
        """)

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.fact_season_totals (
            year INT,
            team_id INT,
            total_away_score INT,
            total_home_score INT
        ) USING iceberg
        PARTITIONED BY (year, team_id)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_season_totals'
        """)

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.fact_season_averages (
            year INT,
            team_id INT,
            avg_away_score DOUBLE,
            avg_home_score DOUBLE
        ) USING iceberg
        PARTITIONED BY (year, team_id)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_season_averages'
        """)

        # Additional Fact Tables
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.fact_game_by_matchup (
            home_team_id INT,
            away_team_id INT,
            game_id INT,
            year INT,
            home_score INT,
            away_score INT,
            home_is_winner BOOLEAN,
            away_is_winner BOOLEAN
        ) USING iceberg
        PARTITIONED BY (year, home_team_id, away_team_id)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_game_by_matchup'
        """)

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.fact_game_by_home_team (
            home_team_id INT,
            game_id INT,
            year INT,
            home_score INT,
            home_is_winner BOOLEAN
        ) USING iceberg
        PARTITIONED BY (year, home_team_id)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_game_by_home_team'
        """)

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.fact_game_by_away_team (
            away_team_id INT,
            game_id INT,
            year INT,
            away_score INT,
            away_is_winner BOOLEAN
        ) USING iceberg
        PARTITIONED BY (year, away_team_id)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_game_by_away_team'
        """)

        logger.info("Tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating tables: {e}", exc_info=True)
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

def read_and_write_stream(spark):
    try:
        schema = StructType([
            StructField("away_team_allStarStatus", StringType(), True),
            StructField("away_team_id", IntegerType(), True),
            StructField("away_team_name", StringType(), True),
            StructField("home_team_id", IntegerType(), True),
            StructField("home_team_name", StringType(), True),
            StructField("gamePk", IntegerType(), True),
            StructField("season", IntegerType(), True),
            StructField("officialDate", StringType(), True),
            StructField("away_team_away_score", IntegerType(), True),
            StructField("home_team_home_score", IntegerType(), True),
            StructField("home_team_home_isWinner", BooleanType(), True),
            StructField("away_team_away_isWinner", BooleanType(), True),
            StructField("home_pitcher_id", IntegerType(), True),
            StructField("away_pitcher_id", IntegerType(), True)
        ])

        kafka_df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'kafka1:9092,kafka2:9093,kafka3:9094') \
            .option('subscribe', 'game_results') \
            .option('startingOffsets', 'earliest') \
            .load()

        game_results_df = kafka_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")).select("data.*")

        game_results_df = game_results_df \
            .withColumnRenamed("gamePk", "game_id") \
            .withColumnRenamed("season", "year") \
            .withColumn("official_date", to_date("officialDate", "yyyy-MM-dd")) \
            .withColumn("month", month("official_date")) \
            .withColumn("day", dayofmonth("official_date"))

        handle_scd_type_2(game_results_df, spark, "mlb_db.fact_game_results",
                          join_columns=["game_id", "year"],
                          compare_columns=["away_team_name", "home_team_name", "away_pitcher_id", "home_pitcher_id"])

        season_totals = game_results_df.groupBy("year", "away_team_id", "home_team_id") \
            .agg(spark_sum("away_team_away_score").alias("total_away_score"),
                 spark_sum("home_team_home_score").alias("total_home_score"))

        season_averages = game_results_df.groupBy("year", "away_team_id", "home_team_id") \
            .agg(avg("away_team_away_score").alias("avg_away_score"),
                 avg("home_team_home_score").alias("avg_home_score"))

        # Write the aggregated data to Iceberg tables
        season_totals.writeStream \
            .format("iceberg") \
            .option("path", "abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_season_totals") \
            .option("checkpointLocation", "/tmp/checkpoints/season_totals") \
            .outputMode("complete") \
            .start()

        season_averages.writeStream \
            .format("iceberg") \
            .option("path", "abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_season_averages") \
            .option("checkpointLocation", "/tmp/checkpoints/season_averages") \
            .outputMode("complete") \
            .start()

        # Additional Fact Tables
        game_by_matchup = game_results_df.groupBy("home_team_id", "away_team_id", "year") \
            .agg(spark_sum("home_team_home_score").alias("home_score"),
                 spark_sum("away_team_away_score").alias("away_score"),
                 max("home_team_home_isWinner").alias("home_is_winner"),
                 max("away_team_away_isWinner").alias("away_is_winner"))

        game_by_matchup.writeStream \
            .format("iceberg") \
            .option("path", "abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_game_by_matchup") \
            .option("checkpointLocation", "/tmp/checkpoints/fact_game_by_matchup") \
            .outputMode("complete") \
            .start()

        game_by_home_team = game_results_df.groupBy("home_team_id", "year") \
            .agg(spark_sum("home_team_home_score").alias("home_score"),
                 max("home_team_home_isWinner").alias("home_is_winner"))

        game_by_home_team.writeStream \
            .format("iceberg") \
            .option("path", "abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_game_by_home_team") \
            .option("checkpointLocation", "/tmp/checkpoints/fact_game_by_home_team") \
            .outputMode("complete") \
            .start()

        game_by_away_team = game_results_df.groupBy("away_team_id", "year") \
            .agg(spark_sum("away_team_away_score").alias("away_score"),
                 max("away_team_away_isWinner").alias("away_is_winner"))

        game_by_away_team.writeStream \
            .format("iceberg") \
            .option("path", "abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/fact_game_by_away_team") \
            .option("checkpointLocation", "/tmp/checkpoints/fact_game_by_away_team") \
            .outputMode("complete") \
            .start()

        logger.info("Streaming read and write started successfully.")
    except Exception as e:
        logger.error(f"Error in read and write stream: {e}", exc_info=True)
        raise

def create_cumulative_tables(spark):
    try:
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.current_season_totals (
            season INT,
            team_name STRING,
            total_away_score INT,
            total_home_score INT
        ) USING iceberg
        PARTITIONED BY (season)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/current_season_totals'
        """)
        
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mlb_db.current_season_averages (
            season INT,
            team_name STRING,
            avg_away_score DOUBLE,
            avg_home_score DOUBLE
        ) USING iceberg
        PARTITIONED BY (season)
        LOCATION 'abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/current_season_averages'
        """)
        
        logger.info("Cumulative current season tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating cumulative current season tables: {e}", exc_info=True)
        raise

def update_cumulative_tables(spark):
    try:
        current_season = spark.sql("SELECT MAX(year) AS current_season FROM mlb_db.fact_game_results").collect()[0]["current_season"]
        
        current_season_totals = spark.sql(f"""
        SELECT 
            year AS season,
            away_team_name AS team_name,
            SUM(away_team_away_score) AS total_away_score,
            SUM(home_team_home_score) AS total_home_score
        FROM mlb_db.fact_game_results
        WHERE year = {current_season}
        GROUP BY year, away_team_name
        """)
        
        current_season_averages = spark.sql(f"""
        SELECT 
            year AS season,
            away_team_name AS team_name,
            AVG(away_team_away_score) AS avg_away_score,
            AVG(home_team_home_score) AS avg_home_score
        FROM mlb_db.fact_game_results
        WHERE year = {current_season}
        GROUP BY year, away_team_name
        """)
        
        current_season_totals.write.format("iceberg") \
            .mode("overwrite") \
            .save("abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/current_season_totals")
        
        current_season_averages.write.format("iceberg") \
            .mode("overwrite") \
            .save("abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/mlb_db/current_season_averages")
        
        logger.info("Cumulative current season tables updated successfully.")
    except Exception as e:
        logger.error(f"Error updating cumulative current season tables: {e}", exc_info=True)
        raise

def query_iceberg_tables(spark):
    try:
        tables = spark.sql("SHOW TABLES IN mlb_db").collect()
        logger.info("Tables in Iceberg catalog:")
        for table in tables:
            logger.info(f"Table: {table.tableName}")
            spark.sql(f"SELECT * FROM mlb_db.{table.tableName}").show()

        spark.sql("SELECT * FROM mlb_db.game_results LIMIT 10").show()

        logger.info("Queried Iceberg tables successfully.")
    except Exception as e:
        logger.error(f"Error querying Iceberg tables: {e}", exc_info=True)
        raise

def main():
    try:
        spark = create_spark_connection()
        create_tables(spark)
        create_cumulative_tables(spark)        
        
        read_and_write_stream(spark)
        update_cumulative_tables(spark)
        
        spark.stop()
        
        logging.info("Pipeline executed successfully.")
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)

if __name__ == "__main__":
    main()
