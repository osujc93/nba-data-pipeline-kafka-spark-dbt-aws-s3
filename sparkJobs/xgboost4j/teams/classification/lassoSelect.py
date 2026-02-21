import sys
import logging

import numpy as np
import pandas as pd

from typing import Dict, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, desc
from pyspark.sql.types import DoubleType, IntegerType, DecimalType

from sklearn.linear_model import LassoCV
from sklearn.preprocessing import StandardScaler as SklearnStandardScaler

import mlflow


class LassoFeatureSelector:
    """
    Runs LassoCV locally to identify the best alpha for sparse feature selection.
    """

    def __init__(self, logger: logging.Logger) -> None:
        """
        Initialize the LassoFeatureSelector with a logger.

        :param logger: Logger for logging messages.
        """
        try:
            self.logger = logger
        except Exception as e:
            logging.error("Error in LassoFeatureSelector.__init__(): %s", str(e), exc_info=True)
            sys.exit(1)

    def run_lasso_feature_selection(
        self,
        spark: SparkSession,
        df: DataFrame,
        mlflow_experiment_name: str = "NBA_Lasso_Feature_Selection",
        # coefficient_strict_threshold: float = 1e-7
        coefficient_strict_threshold: float = 0.0
    ) -> Dict[str, List[str]]:
        """
        Run LassoCV locally to identify the best alpha for sparse feature selection.
        Returns a dictionary keyed by "table_target" with selected features.
        """
        try:
            numeric_cols = [
                f.name
                for f in df.schema.fields
                if isinstance(f.dataType, (DoubleType, IntegerType, DecimalType))
                and f.name not in ("win_loss_binary", "team_id")
            ]

            df = df.filter(col("win_loss_binary").isNotNull())

            for c in numeric_cols:
                df = df.withColumn(c, col(c).cast(DoubleType()))

            pdf = (
                df.orderBy(desc("game_date"))
                .select(*numeric_cols, "win_loss_binary")
                .dropna()
                .toPandas()
            )

            table_target_key = "scd_team_boxscores__win_loss_binary"
            if pdf.shape[0] < 20:
                self.logger.warning(
                    "Insufficient data to run LassoCV. Returning all numeric features."
                )
                return {table_target_key: numeric_cols}

            X_full = pdf[numeric_cols].values
            y_full = pdf["win_loss_binary"].values

            scaler = SklearnStandardScaler()
            if X_full.shape[1] < 1:
                self.logger.warning(
                    "No numeric columns found after decimal casting. Returning all numeric."
                )
                return {table_target_key: numeric_cols}

            X_scaled = scaler.fit_transform(X_full)

            mlflow.set_experiment(mlflow_experiment_name)
            top_features_dict: Dict[str, List[str]] = {}

            with mlflow.start_run(run_name="Lasso_Feature_Selection", nested=True):
                lasso_cv = LassoCV(
                    cv=8,
                    random_state=42,
                    n_jobs=-1,
                    max_iter=10000
                ).fit(X_scaled, y_full)

                best_alpha = lasso_cv.alpha_
                mlflow.log_param("lasso_best_alpha", best_alpha)

                coefs = lasso_cv.coef_

                for feat, coef_val in zip(numeric_cols, coefs):
                    self.logger.info("Lasso coefficient for feature '%s' = %.100g", feat, coef_val)
                    mlflow.log_metric(f"lasso_coef_{feat}", float(coef_val))

                nonzero_indices = np.where(np.abs(coefs) > coefficient_strict_threshold)[0]
                selected_features = [numeric_cols[i] for i in nonzero_indices]

                mlflow.log_param("strict_threshold", coefficient_strict_threshold)
                mlflow.log_param("num_selected_features", len(selected_features))
                mlflow.log_param("selected_features_list", str(selected_features))

                self.logger.info(
                    "[Lasso Feature Selection] Best alpha: %.6f, strict_threshold=%.4f, "
                    "Selected %d features: %s",
                    best_alpha,
                    coefficient_strict_threshold,
                    len(selected_features),
                    selected_features
                )

                top_features_dict[table_target_key] = selected_features

            return top_features_dict

        except Exception as e:
            self.logger.error("Error in run_lasso_feature_selection: %s", str(e), exc_info=True)
            sys.exit(1)
