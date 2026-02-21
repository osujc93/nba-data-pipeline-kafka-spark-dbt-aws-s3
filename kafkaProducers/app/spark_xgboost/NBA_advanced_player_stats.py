# file: advanced_players_xgboost.py
# PEP 8 Compliant, includes explicit type hints for better readability and static type checking.

import logging
import sys
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    current_date,
    date_add,
    monotonically_increasing_id,
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, DateType
from pyspark.ml.feature import VectorAssembler
from xgboost.spark import SparkXGBRegressor  # XGBoost4J-Spark

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger: logging.Logger = logging.getLogger(__name__)


def create_spark_connection() -> SparkSession:
    """
    Creates and returns a SparkSession with the necessary configurations.
    """
    try:
        spark: SparkSession = (
            SparkSession.builder.appName("NBA_Advanced_Player_Stats_XGBoost")
            .enableHiveSupport()
            .getOrCreate()
        )
        spark.sql("CREATE DATABASE IF NOT EXISTS nba_db")
        logger.info("Spark connection created successfully for advanced players XGBoost.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark connection: {e}", exc_info=True)
        sys.exit(1)


def load_advanced_player_stats(spark: SparkSession) -> DataFrame:
    """
    Loads advanced player stats from the existing Iceberg table: nba_db.advanced_player_stats
    Uses multiple relevant columns, filtering only is_current = True.
    """
    try:
        # Columns of interest from your existing advanced_player_stats table
        columns_of_interest: List[str] = [
            "PLAYER_ID",
            "GP",
            "W",
            "L",
            "NET_RATING",
            "OFF_RATING",
            "DEF_RATING",
            "PIE",
            "PACE",
            "Month",
            "Season",
            "record_start_date",
        ]

        df: DataFrame = (
            spark.table("nba_db.advanced_player_stats")
            .filter(col("is_current") == True)
            .select(*columns_of_interest)
        )
        logger.info("Loaded advanced player stats from Iceberg.")
        return df
    except Exception as e:
        logger.error(f"Error loading advanced player stats: {e}", exc_info=True)
        sys.exit(1)


def train_and_predict(
    df: DataFrame,
    label_col: str = "OFF_RATING"
) -> DataFrame:
    """
    Performs feature engineering, trains XGBoost, and returns predictions.
    Label defaults to OFF_RATING.
    """
    try:
        # Convert columns to numeric where needed
        numeric_cols: List[str] = ["GP", "W", "L", "NET_RATING", "OFF_RATING", "DEF_RATING", "PIE", "PACE"]
        for col_name in numeric_cols:
            df = df.withColumn(col_name, col(col_name).cast(DoubleType()))

        df = df.withColumn("Month", col("Month").cast(IntegerType()))
        df = df.withColumn("Season", col("Season").cast(StringType()))
        df = df.withColumn("record_start_date", col("record_start_date").cast(DateType()))

        # Drop rows with null in critical columns
        df = df.na.drop(subset=numeric_cols)

        # Define features
        feature_cols: List[str] = ["GP", "W", "L", "NET_RATING", "DEF_RATING", "PIE", "PACE"]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        feature_df = assembler.transform(df).select("features", label_col, "Month", "Season", "record_start_date")

        # Split data
        train_df, test_df = feature_df.randomSplit([0.8, 0.2], seed=42)

        # XGBoost with advanced hyperparameters
        xgb_regressor = SparkXGBRegressor(
            numWorkers=2,
            numRound=50,
            maxDepth=6,
            eta=0.1,
            subsample=0.8,
            colsampleBytree=0.8,
            objective="reg:squarederror",
            seed=42
        ).setLabelCol(label_col).setFeaturesCol("features")

        model = xgb_regressor.fit(train_df)
        predictions: DataFrame = model.transform(test_df).withColumnRenamed("prediction", "predicted_OFF_RATING")

        logger.info("Training & prediction completed for advanced players XGBoost.")
        return predictions
    except Exception as e:
        logger.error(f"Error in train_and_predict: {e}", exc_info=True)
        sys.exit(1)


def write_future_predictions(
    spark: SparkSession,
    predictions: DataFrame,
    output_table: str = "nba_db.advanced_player_stats_future"
) -> None:
    """
    Writes predictions to a new Iceberg table, partitioned by future_date.
    Demonstrates a date_add approach for 'future_date'.
    """
    try:
        # Example: add 30 days to existing record_start_date
        future_df: DataFrame = predictions.withColumn(
            "future_date",
            date_add(col("record_start_date"), 30)
        )

        # Surrogate key
        future_df = future_df.withColumn("id", monotonically_increasing_id())

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {output_table} (
                id BIGINT,
                features ARRAY<DOUBLE>,
                OFF_RATING DOUBLE,
                predicted_OFF_RATING DOUBLE,
                Month INT,
                Season STRING,
                record_start_date DATE,
                future_date DATE
            )
            USING ICEBERG
            PARTITIONED BY (future_date)
        """)

        (
            future_df.select(
                "id",
                "features",
                "OFF_RATING",
                "predicted_OFF_RATING",
                "Month",
                "Season",
                "record_start_date",
                "future_date"
            )
            .write
            .format("iceberg")
            .mode("overwrite")
            .save(output_table)
        )

        logger.info(f"Future XGBoost predictions written to {output_table}.")
    except Exception as e:
        logger.error(f"Error in write_future_predictions: {e}", exc_info=True)
        sys.exit(1)


def main() -> None:
    spark = create_spark_connection()
    df_advanced: DataFrame = load_advanced_player_stats(spark)
    predictions: DataFrame = train_and_predict(df_advanced, label_col="OFF_RATING")
    write_future_predictions(spark, predictions, "nba_db.advanced_player_stats_future")
    spark.stop()
    logger.info("Advanced players XGBoost script completed successfully.")


if __name__ == "__main__":
    main()
