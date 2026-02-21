# file: season_standings_xgboost.py
# PEP 8 Compliant, includes explicit type hints.

import logging
import sys
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    date_add,
    monotonically_increasing_id
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, DateType
from pyspark.ml.feature import VectorAssembler
from xgboost.spark import SparkXGBRegressor

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
            SparkSession.builder.appName("NBA_Season_Standings_XGBoost")
            .enableHiveSupport()
            .getOrCreate()
        )
        spark.sql("CREATE DATABASE IF NOT EXISTS nba_db")
        logger.info("Spark connection created for season standings XGBoost.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark connection: {e}", exc_info=True)
        sys.exit(1)


def load_season_standings(spark: SparkSession) -> DataFrame:
    """
    Loads from the existing `nba_db.nba_overall` table for example,
    focusing on fields relevant to standings like WINS, LOSSES, WinPCT, PointsPG, etc.
    """
    try:
        columns_of_interest: List[str] = [
            "TeamID",
            "WINS",
            "LOSSES",
            "WinPCT",
            "PointsPG",
            "Month",
            "Season",
            "Section",
            "record_start_date"
        ]
        df: DataFrame = spark.table("nba_db.nba_overall").select(*columns_of_interest)
        logger.info("Loaded season standings from nba_overall.")
        return df
    except Exception as e:
        logger.error(f"Error loading season standings: {e}", exc_info=True)
        sys.exit(1)


def train_and_predict(
    df: DataFrame,
    label_col: str = "PointsPG"
) -> DataFrame:
    """
    Trains XGBoost to predict PointsPG from fields like WINS, LOSSES, WinPCT.
    """
    try:
        numeric_cols: List[str] = ["WINS", "LOSSES", "WinPCT", "PointsPG"]
        for c in numeric_cols:
            df = df.withColumn(c, col(c).cast(DoubleType()))

        df = df.withColumn("Month", col("Month").cast(IntegerType()))
        df = df.withColumn("Season", col("Season").cast(StringType()))
        df = df.withColumn("record_start_date", col("record_start_date").cast(DateType()))

        df = df.na.drop(subset=numeric_cols)

        feature_cols: List[str] = ["WINS", "LOSSES", "WinPCT"]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        feature_df = assembler.transform(df).select("features", label_col, "Month", "Season", "record_start_date")

        train_df, test_df = feature_df.randomSplit([0.8, 0.2], seed=42)

        xgb_regressor = SparkXGBRegressor(
            numWorkers=2,
            numRound=40,
            maxDepth=4,
            eta=0.1,
            subsample=0.8,
            colsampleBytree=0.8,
            objective="reg:squarederror",
            seed=42
        ).setLabelCol(label_col).setFeaturesCol("features")

        model = xgb_regressor.fit(train_df)
        predictions = model.transform(test_df).withColumnRenamed("prediction", "predicted_PointsPG")

        logger.info("Training & prediction completed for season standings XGBoost.")
        return predictions
    except Exception as e:
        logger.error(f"Error in train_and_predict: {e}", exc_info=True)
        sys.exit(1)


def write_future_predictions(
    spark: SparkSession,
    predictions: DataFrame,
    output_table: str = "nba_db.nba_overall_future"
) -> None:
    """
    Writes future predictions to a new table (partitioned by future_date).
    We add 20 days for demonstration.
    """
    try:
        future_df = predictions.withColumn("future_date", date_add(col("record_start_date"), 20))
        future_df = future_df.withColumn("id", monotonically_increasing_id())

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {output_table} (
                id BIGINT,
                features ARRAY<DOUBLE>,
                PointsPG DOUBLE,
                predicted_PointsPG DOUBLE,
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
                "PointsPG",
                "predicted_PointsPG",
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
    df_standings = load_season_standings(spark)
    predictions = train_and_predict(df_standings, label_col="PointsPG")
    write_future_predictions(spark, predictions, "nba_db.nba_overall_future")
    spark.stop()
    logger.info("Season standings XGBoost script completed successfully.")


if __name__ == "__main__":
    main()
