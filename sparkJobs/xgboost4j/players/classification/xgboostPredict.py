import sys
import psutil
import logging
import time

import numpy as np
import pandas as pd

from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DoubleType, IntegerType, DecimalType

import mlflow
import xgboost as xgb

from pyspark.ml.feature import VectorAssembler


class Predictor:
    """
    Generates predictions from the trained XGBoost classification model
    and writes them to an Iceberg table.
    """

    def __init__(self, logger: logging.Logger) -> None:
        """
        Initialize the Predictor with a logger.

        :param logger: Logger for logging messages.
        """
        self.logger = logger

    def generate_predictions_and_write_to_iceberg(
        self,
        spark: SparkSession,
        df: DataFrame,
        model_info: Dict[str, str],
        db_name: str = "iceberg_nba_player_boxscores_classification"
    ) -> None:
        """
        Generate predictions using the trained XGBoost classification model from MLflow
        and write them to an Iceberg table. Also maps 0 -> "L" and 1 -> "W".

        :param spark: The SparkSession in use.
        :param df: Spark DataFrame with features (same historical data for demonstration).
        :param model_info: Dict containing the run_id and model_uri.
        :param db_name: Destination database for saving Iceberg tables.
        """
        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()

        try:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            run_id = model_info["run_id"]
            model_uri = model_info["model_uri"]

            # Identify numeric columns, cast them to Double
            numeric_cols_raw = [
                f.name
                for f in df.schema.fields
                if isinstance(f.dataType, (DoubleType, IntegerType, DecimalType))
                and f.name not in ("win_loss_binary")
            ]
            for c in numeric_cols_raw:
                df = df.withColumn(c, col(c).cast(DoubleType()))

            numeric_cols = [
                f.name
                for f in df.schema.fields
                if (isinstance(f.dataType, DoubleType) or isinstance(f.dataType, IntegerType))
                and f.name not in ("win_loss_binary")
            ]
            if len(numeric_cols) == 0:
                self.logger.warning("No numeric features found. Skipping prediction step.")
                return

            # Ensure all numeric_cols exist; if not, fill with 0.0
            for c in numeric_cols:
                if c not in df.columns:
                    df = df.withColumn(c, lit(0.0))

            # Load the XGBoost model from MLflow (returns a native Booster)
            loaded_xgb_booster = mlflow.xgboost.load_model(model_uri)

            assembler = VectorAssembler(
                inputCols=numeric_cols,
                outputCol="features_assembled"
            )
            assembled_df = assembler.transform(df)

            pdf_inference = assembled_df.toPandas()
            if len(pdf_inference) == 0:
                self.logger.info("No rows to predict. Exiting.")
                return

            # Convert Spark vectors to NumPy arrays
            pdf_inference["features_assembled"] = pdf_inference["features_assembled"].apply(
                lambda v: v.toArray() if hasattr(v, "toArray") else np.array(v)
            )

            # Predict using XGBoost booster
            features_array = np.vstack(pdf_inference["features_assembled"].values)
            dtest = xgb.DMatrix(features_array)
            pred_proba = loaded_xgb_booster.predict(dtest)
            pred_class = (pred_proba >= 0.5).astype(int)

            def decode_pred(p: int) -> str:
                return "W" if p == 1 else "L"

            # Add predictions to DataFrame
            final_prediction_str = [decode_pred(p) for p in pred_class]
            pdf_inference["prediction"] = pred_class
            pdf_inference["probability"] = pred_proba
            pdf_inference["final_prediction"] = final_prediction_str
            pdf_inference["prediction_type"] = "historical_classification"

            # Drop the ndarray column to avoid serialization issues
            pdf_inference.drop(columns=["features_assembled"], inplace=True)

            # Convert back to Spark for writing
            pred_spark_df = spark.createDataFrame(pdf_inference)

            out_table = f"spark_catalog.{db_name}.xgboost_player_predictions_{run_id}"
            pred_spark_df.write.format("iceberg").mode("overwrite").saveAsTable(out_table)

            self.logger.info(
                "Wrote classification predictions to %s (run_id=%s).",
                out_table,
                run_id,
            )

        except Exception as e:
            self.logger.error(
                "Error generating/writing classification predictions: %s",
                str(e),
                exc_info=True
            )
            sys.exit(1)

        finally:
            end_cpu_time = time.process_time()
            end_cpu_percent = psutil.cpu_percent()
            self.logger.info(
                "CPU usage start: %s%%, end: %s%%. CPU time for "
                "generate_predictions_and_write_to_iceberg(): %.4f seconds",
                start_cpu_percent,
                end_cpu_percent,
                end_cpu_time - start_cpu_time,
            )
