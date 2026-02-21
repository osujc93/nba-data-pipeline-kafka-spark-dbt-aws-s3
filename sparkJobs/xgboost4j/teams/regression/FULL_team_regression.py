#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA_team_boxscores_regression.py

OVERVIEW:
---------
This script orchestrates an end-to-end pipeline for **regression** of numeric columns
(e.g. team_points, team_fga, etc.) in scd_team_boxscores using XGBoost. It closely mirrors
the classification pipeline but is now set up for continuous-target prediction.

Key points:
-----------
1) We treat each numeric target column separately, looping over them to build a
   separate XGBoost regressor.
2) We still apply the same advanced Spark feature engineering (including rolling means,
   wavelets, logs, etc.).
3) We run Lasso feature selection for each target to pick the top correlated features
   for that specific regression.
4) We run hyperopt (with SparkTrials) for XGBoost parameter tuning, then do a final
   SparkXGBRegressor fit with the best parameters.
5) We write out all predictions (for all targets) into one Iceberg table.

CHANGES FROM CLASSIFICATION SCRIPT:
-----------------------------------
- No "win_loss_binary" label creation.
- Uses "reg:squarederror" or "reg:squaredError" objective for XGBoost.
- Uses RMSE or MAE metrics (here we’ll use RMSE).
- The final predictions are written into columns named 'prediction_<target>' for each
  numeric target (e.g. 'prediction_team_points').

ADDED:
------
- Compute and log MSE, MAE, MAPE alongside RMSE in logger and MLflow.
- Explain TimeSeriesSplit + ShuffleSplit logic for hyperopt evaluation.
"""

import cProfile
import logging
import math
import sys
import threading
import time
from datetime import datetime

import psutil  # For CPU usage logging
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    asc,
    avg as spark_avg,
    col,
    count as spark_count,
    current_date,
    current_timestamp,
    date_add,
    desc,
    lit,
    monotonically_increasing_id,
    regexp_extract,
    row_number,
    sum as spark_sum,
    when,
    log,
    abs as spark_abs
)
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    DecimalType
)
from pyspark.sql.window import Window
# ADDED: import additional metrics
from sklearn.metrics import mean_squared_error, mean_absolute_error, mean_absolute_percentage_error
from sklearn.model_selection import ShuffleSplit, TimeSeriesSplit
from sklearn.preprocessing import StandardScaler as SklearnStandardScaler

import xgboost as xgb  # local xgboost for CV

# MLflow
import mlflow
import mlflow.xgboost
from mlflow.models.signature import infer_signature

# Hyperopt
from hyperopt import STATUS_OK, Trials, fmin, hp, tpe, SparkTrials

# For final training/predictions in Spark:
from xgboost.spark import SparkXGBRegressor
from pyspark.ml.feature import VectorAssembler

import pywt  # For Wavelet transforms
from pyspark.sql.functions import pandas_udf

# For Lasso feature selection
from sklearn.linear_model import LassoCV

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# List of columns you want to run regression on
REGRESSION_TARGET_COLS = [
    "team_points",
    "team_fga",
    "team_fgm",
    "team_fg3a",
    "team_fg3m",
    "team_fta",
    "team_ftm",
    "team_tov",
    "team_oreb",
    "team_dreb",
    "team_reb",
    "team_ast",
    "team_stl",
    "team_blk",
    "partial_possessions",
    "team_fg_pct",
    "team_fg3_pct",
    "team_ft_pct"
]


class NBATeamBoxscoreRegressionPipeline:
    """
    Pipeline class that orchestrates data loading, cleaning, feature engineering,
    and regression for various numeric columns in scd_team_boxscores.
    """

    def __init__(self) -> None:
        """
        Initialize the pipeline with a logger and a profiler.
        """
        self.logger = logging.getLogger(__name__)
        self.profiler = cProfile.Profile()

    def log_stats_periodically(self, interval: int = 120) -> None:
        """
        Log CPU profiling statistics every 'interval' seconds in a separate thread.
        """
        while True:
            time.sleep(interval)
            import io
            import pstats
            s = io.StringIO()
            ps = pstats.Stats(self.profiler, stream=s).sort_stats("cumulative")
            self.logger.info(
                "[Periodic CPU profiling stats - Last %s seconds]:\n%s",
                interval,
                s.getvalue(),
            )

    def create_spark_session(self) -> SparkSession:
        """
        Create and return a SparkSession configured for this pipeline.
        """
        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()
        try:
            spark = (
                SparkSession.builder.appName("XGBoost_NBA_Team_Regression")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024)
                .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
                .config("spark.sql.iceberg.target-file-size-bytes", "134217728")
                .enableHiveSupport()
                .getOrCreate()
            )

            self.logger.info("SparkSession created successfully.")
            return spark
        except Exception as e:
            self.logger.error("Failed to create SparkSession: %s", str(e), exc_info=True)
            sys.exit(1)
        finally:
            end_cpu_time = time.process_time()
            end_cpu_percent = psutil.cpu_percent()
            self.logger.info(
                "CPU usage start: %s%%, end: %s%%. CPU time for create_spark_session(): "
                "%.4f seconds",
                start_cpu_percent,
                end_cpu_percent,
                end_cpu_time - start_cpu_time,
            )

    def load_team_boxscores(self, spark: SparkSession) -> DataFrame:
        """
        Load the scd_team_boxscores table from Spark (Iceberg or Hive).
        """
        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()
        try:
            # Adjust to your actual table reference
            full_tbl_name = "iceberg_nba_player_boxscores.scd_team_boxscores"
            self.logger.info("Reading table: %s", full_tbl_name)
            df_tbl = spark.table(full_tbl_name)

            # Persist so we don't re-scan
            df_tbl = df_tbl.persist()
            _ = df_tbl.count()  # materialize
            self.logger.info("Loaded scd_team_boxscores table. Row count: %d", df_tbl.count())
            return df_tbl
        except Exception as e:
            self.logger.error("Error loading scd_team_boxscores: %s", str(e), exc_info=True)
            sys.exit(1)
        finally:
            end_cpu_time = time.process_time()
            end_cpu_percent = psutil.cpu_percent()
            self.logger.info(
                "CPU usage start: %s%%, end: %s%%. CPU time for load_team_boxscores(): "
                "%.4f seconds",
                start_cpu_percent,
                end_cpu_percent,
                end_cpu_time - start_cpu_time,
            )

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Clean the data by removing duplicates and filtering out invalid values.
        No binary label creation here (unlike classification).
        """
        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()
        try:
            df_dedup = df.dropDuplicates()

            # Basic cleaning if columns are present
            if "team_id" in df_dedup.columns:
                df_dedup = df_dedup.filter(col("team_id").isNotNull())

            self.logger.info("Data cleaning complete for regression pipeline.")
            return df_dedup
        except Exception as e:
            self.logger.error("Error in cleaning data: %s", str(e), exc_info=True)
            sys.exit(1)
        finally:
            end_cpu_time = time.process_time()
            end_cpu_percent = psutil.cpu_percent()
            self.logger.info(
                "CPU usage start: %s%%, end: %s%%. CPU time for clean_data(): "
                "%.4f seconds",
                start_cpu_percent,
                end_cpu_percent,
                end_cpu_time - start_cpu_time,
            )

    def spark_feature_engineering(self, df: DataFrame) -> DataFrame:
        """
        Perform advanced feature engineering using Spark DataFrame transformations
        on all numeric columns: absolute, squared, log, rolling mean, etc.
        Also includes Fourier/Wavelet transformations for each numeric column.
        """
        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()
        try:
            #
            # FIX #1: Treat DecimalType as numeric as well
            #
            numeric_cols = [
                f.name
                for f in df.schema.fields
                if (
                    isinstance(f.dataType, (DoubleType, IntegerType, DecimalType))
                    and f.name != "team_id"
                )
            ]

            # 1) Create columns: absolute, squared, log
            for c in numeric_cols:
                df = df.withColumn(f"{c}_abs", spark_abs(col(c).cast(DoubleType())))
                df = df.withColumn(f"{c}_squared", (col(c) * col(c)).cast(DoubleType()))
                df = df.withColumn(
                    f"{c}_log_protected",
                    when(col(c) <= 0, 1).otherwise(col(c).cast(DoubleType()))
                )
                df = df.withColumn(
                    f"{c}_log",
                    log(col(f"{c}_log_protected"))
                )

            # 2) Rolling mean example with a window of size 3, ordered by 'game_date' if it exists
            if "game_date" in df.columns:
                window_spec = (
                    Window.partitionBy("team_id")
                    .orderBy(col("game_date").asc())
                    .rowsBetween(-2, 0)
                )
                for c in numeric_cols:
                    df = df.withColumn(
                        f"{c}_ma_3",
                        spark_avg(col(c)).over(window_spec),
                    )
            else:
                # If no date, fallback
                for c in numeric_cols:
                    df = df.withColumn(f"{c}_ma_3", col(c))

            # 3) Clean up NaNs or infinities => fill numeric columns with 0
            numeric_cols_for_nullfill = [
                f.name
                for f in df.schema.fields
                if isinstance(f.dataType, (DoubleType, IntegerType, DecimalType))
                and f.name != "team_id"
            ]
            for c in numeric_cols_for_nullfill:
                df = df.withColumn(c, when(col(c).isNull(), 0).otherwise(col(c)))

            # ------------------------------------------------------------------------
            # 4) Fourier/Wavelet transformations (grouped by team_id, sorted by game_date)
            # ------------------------------------------------------------------------
            existing_fields = df.schema.fields
            extended_fields = list(existing_fields)

            for c in numeric_cols:
                for i in range(1, 4):
                    extended_fields.append(StructField(f"{c}_fft_{i}", DoubleType(), True))
                for i in range(1, 4):
                    extended_fields.append(StructField(f"{c}_wave_{i}", DoubleType(), True))

            extended_schema = StructType(extended_fields)

            def freq_domain_features(pdf: pd.DataFrame) -> pd.DataFrame:
                if "game_date" in pdf.columns:
                    pdf = pdf.sort_values("game_date")

                for col_name in numeric_cols:
                    # FIX #2: Force the column to float before rfft/wavedec
                    arr = pdf[col_name].astype(float).values

                    # FFT
                    freq = np.fft.rfft(arr)
                    freq_abs = np.abs(freq)
                    top_3_idx = freq_abs.argsort()[::-1][:3]
                    for i in range(1, 4):
                        if i <= len(top_3_idx):
                            pdf[f"{col_name}_fft_{i}"] = freq_abs[top_3_idx[i - 1]]
                        else:
                            pdf[f"{col_name}_fft_{i}"] = 0.0

                    # Wavelet
                    coeffs = pywt.wavedec(arr, 'db1', level=2)
                    c_concat = np.concatenate(coeffs)
                    c_abs = np.abs(c_concat)
                    top_3_idx_w = c_abs.argsort()[::-1][:3]
                    for i in range(1, 4):
                        if i <= len(top_3_idx_w):
                            pdf[f"{col_name}_wave_{i}"] = c_abs[top_3_idx_w[i - 1]]
                        else:
                            pdf[f"{col_name}_wave_{i}"] = 0.0

                return pdf

            if "team_id" in df.columns:
                df = df.groupBy("team_id").applyInPandas(freq_domain_features, schema=extended_schema)
            else:
                single_group_key = lit("dummy_group")
                df = df.groupBy(single_group_key).applyInPandas(freq_domain_features, schema=extended_schema).drop("dummy_group")

            self.logger.info("Spark-based feature engineering (Fourier/Wavelet) complete.")
            return df

        except Exception as e:
            self.logger.error("Error in spark-based feature engineering: %s", str(e), exc_info=True)
            sys.exit(1)
        finally:
            end_cpu_time = time.process_time()
            end_cpu_percent = psutil.cpu_percent()
            self.logger.info(
                "CPU usage start: %s%%, end: %s%%. CPU time for spark_feature_engineering(): "
                "%.4f seconds",
                start_cpu_percent,
                end_cpu_percent,
                end_cpu_time - start_cpu_time,
            )

    def run_lasso_feature_selection(
        self,
        spark: SparkSession,
        df: DataFrame,
        target_column: str,
        mlflow_experiment_name: str = "NBA_Lasso_Feature_Selection_Regression",
        coefficient_strict_threshold: float = 0.0
    ) -> list[str]:
        """
        Run LassoCV locally (for a single target_column) to identify the best alpha for
        sparse feature selection, returning a list of selected features above a threshold.

        Steps:
        1) Identify numeric columns (excluding team_id and the target_column).
        2) Collect to Pandas, scale features.
        3) Fit LassoCV to find best alpha.
        4) Identify non-zero-coeff (above threshold) features.
        5) Log results to MLflow.
        6) Return the selected feature list.
        """
        #
        # FIX #1: Also treat DecimalType as numeric in Lasso selection
        #
        numeric_cols = [
            f.name
            for f in df.schema.fields
            if isinstance(f.dataType, (DoubleType, IntegerType, DecimalType))
            and f.name not in ("team_id", target_column)
        ]

        # Filter rows that have a target
        df = df.filter(col(target_column).isNotNull())

        # Collect a subset to Pandas
        pdf = df.select(*numeric_cols, target_column).dropna().limit(600000).toPandas()
        if pdf.shape[0] < 20:
            self.logger.warning(
                "Insufficient data to run LassoCV for %s. Returning all numeric features.",
                target_column
            )
            return numeric_cols

        X_full = pdf[numeric_cols].values
        y_full = pdf[target_column].values

        # Check if we ended up with zero columns:
        if X_full.shape[1] == 0:
            self.logger.warning(
                "No numeric feature columns found for target '%s'. Returning empty list.",
                target_column
            )
            return []

        # Scale
        scaler = SklearnStandardScaler()
        X_scaled = scaler.fit_transform(X_full)

        mlflow.set_experiment(mlflow_experiment_name)

        with mlflow.start_run(run_name=f"Lasso_Feature_Selection_{target_column}", nested=True):
            lasso_cv = LassoCV(cv=5, random_state=42, n_jobs=-1, max_iter=10000)
            lasso_cv.fit(X_scaled, y_full)

            best_alpha = lasso_cv.alpha_
            mlflow.log_param("target_column", target_column)
            mlflow.log_param("lasso_best_alpha", best_alpha)

            # Identify non-zero-coeff features, apply threshold
            coefs = lasso_cv.coef_
            nonzero_indices = np.where(np.abs(coefs) > coefficient_strict_threshold)[0]
            selected_features = [numeric_cols[i] for i in nonzero_indices]

            mlflow.log_param("strict_threshold", coefficient_strict_threshold)
            mlflow.log_param("num_selected_features", len(selected_features))
            mlflow.log_param("selected_features_list", str(selected_features))

            self.logger.info(
                "[Lasso for %s] Best alpha: %.6f, threshold=%.4f, selected %d features: %s",
                target_column, best_alpha, coefficient_strict_threshold, len(selected_features), selected_features
            )

        return selected_features

    def xgboost_training_with_hyperopt(
        self,
        spark: SparkSession,
        df: DataFrame,
        target_column: str,
        mlflow_experiment_name: str = "XGBoost_NBA_Team_Regression",
        max_evals: int = 66
    ) -> dict[str, str]:
        """
        Train an XGBoost regressor for one target_column using Hyperopt with SparkTrials.
        1) We do local xgboost cross-validation in the objective function.
        2) Then we train a final SparkXGBRegressor with the best hyperparams (checkpointing).
        Returns a dict with run_id and model_uri.

        ADDED:
        ------
        - MSE, MAE, and MAPE computations/logging in local CV and final model.
        - Explanation:
          We use 11 folds from TimeSeriesSplit plus 6 folds from ShuffleSplit, which
          total 17 folds. The average of the fold results is used by hyperopt to
          pick the best hyperparameters.
        """
        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()

        try:
            mlflow.set_experiment(mlflow_experiment_name)

            df = df.filter(col(target_column).isNotNull())

            # Identify numeric features (excluding the target)
            feature_cols = [
                f.name
                for f in df.schema.fields
                if isinstance(f.dataType, (DoubleType, IntegerType, DecimalType))
                and f.name != target_column
            ]
            if len(feature_cols) < 1:
                self.logger.warning("No numeric columns found for regression features. Target=%s", target_column)
                return {}

            # Convert to Pandas for local cross-validation
            pdf = df.select(*feature_cols, target_column).dropna().limit(600000).toPandas()
            if len(pdf) < 10:
                self.logger.warning("Not enough data for regression on %s; skipping.", target_column)
                return {}

            X_full = pdf[feature_cols].values
            y_full = pdf[target_column].values

            # If no columns left, bail out
            if X_full.shape[1] == 0:
                self.logger.warning(
                    "After numeric filtering, 0 columns remain for target '%s'. Skipping.",
                    target_column
                )
                return {}

            # Scale
            scaler = SklearnStandardScaler()
            X_scaled = scaler.fit_transform(X_full)

            # Cross-validation splits:
            tscv = TimeSeriesSplit(n_splits=11)
            random_splits = ShuffleSplit(n_splits=6, test_size=0.3, random_state=42)

            def local_cv_score(params: dict[str, float]) -> float:
                """
                Local XGBoost cross-validation for regression. Minimizes RMSE.
                We also compute MSE, MAE, MAPE for logging but the objective is RMSE.
                """
                with mlflow.start_run(nested=True):
                    xgb_params = {
                        "eta": float(params["learning_rate"]),
                        "max_depth": int(params["max_depth"]),
                        "subsample": float(params["subsample"]),
                        "colsample_bytree": float(params["colsample_bytree"]),
                        "alpha": float(params["reg_alpha"]),
                        "lambda": float(params["reg_lambda"]),
                        "min_child_weight": float(params["min_child_weight"]),
                        "gamma": float(params["gamma"]),
                        "objective": "reg:squarederror",
                        "eval_metric": "rmse",
                        "verbosity": 0,
                    }
                    num_round = int(params["num_boost_round"])

                    # Log hyperparams
                    for k, v in xgb_params.items():
                        mlflow.log_param(k, v)
                    mlflow.log_param("num_boost_round", num_round)
                    mlflow.log_param("target_column", target_column)

                    rmse_scores = []
                    mse_scores = []
                    mae_scores = []
                    mape_scores = []

                    fold_index = 0

                    # ---- TimeSeriesSplit folds (5) ----
                    for train_idx, test_idx in tscv.split(X_scaled):
                        dtrain = xgb.DMatrix(X_scaled[train_idx], label=y_full[train_idx])
                        dtest = xgb.DMatrix(X_scaled[test_idx], label=y_full[test_idx])

                        booster = xgb.train(
                            xgb_params,
                            dtrain,
                            num_boost_round=num_round,
                            evals=[(dtest, "test")],
                            early_stopping_rounds=10,
                            verbose_eval=False,
                        )
                        preds = booster.predict(dtest)

                        # Compute metrics
                        mse_val = mean_squared_error(y_full[test_idx], preds)
                        rmse_val = math.sqrt(mse_val)
                        mae_val = mean_absolute_error(y_full[test_idx], preds)  # ADDED
                        # Avoid MAPE if y contains zeros
                        if np.any(y_full[test_idx] == 0):
                            # fallback to 0 if zero in ground truth
                            mape_val = float('inf')
                        else:
                            mape_val = mean_absolute_percentage_error(y_full[test_idx], preds)

                        # Collect
                        mse_scores.append(mse_val)
                        rmse_scores.append(rmse_val)
                        mae_scores.append(mae_val)
                        mape_scores.append(mape_val)

                        # Log per-fold metrics
                        mlflow.log_metric("fold_mse", mse_val, step=fold_index)
                        mlflow.log_metric("fold_rmse", rmse_val, step=fold_index)
                        mlflow.log_metric("fold_mae", mae_val, step=fold_index)
                        mlflow.log_metric("fold_mape", mape_val, step=fold_index)

                        fold_index += 1

                    # ---- ShuffleSplit folds (3) ----
                    for train_idx, test_idx in random_splits.split(X_scaled):
                        dtrain = xgb.DMatrix(X_scaled[train_idx], label=y_full[train_idx])
                        dtest = xgb.DMatrix(X_scaled[test_idx], label=y_full[test_idx])

                        booster = xgb.train(
                            xgb_params,
                            dtrain,
                            num_boost_round=num_round,
                            evals=[(dtest, "test")],
                            early_stopping_rounds=10,
                            verbose_eval=False,
                        )
                        preds = booster.predict(dtest)

                        # Compute metrics
                        mse_val = mean_squared_error(y_full[test_idx], preds)
                        rmse_val = math.sqrt(mse_val)
                        mae_val = mean_absolute_error(y_full[test_idx], preds)  # ADDED
                        if np.any(y_full[test_idx] == 0):
                            mape_val = float('inf')
                        else:
                            mape_val = mean_absolute_percentage_error(y_full[test_idx], preds)

                        # Collect
                        mse_scores.append(mse_val)
                        rmse_scores.append(rmse_val)
                        mae_scores.append(mae_val)
                        mape_scores.append(mape_val)

                        # Log per-fold metrics
                        mlflow.log_metric("fold_mse", mse_val, step=fold_index)
                        mlflow.log_metric("fold_rmse", rmse_val, step=fold_index)
                        mlflow.log_metric("fold_mae", mae_val, step=fold_index)
                        mlflow.log_metric("fold_mape", mape_val, step=fold_index)

                        fold_index += 1

                    # Compute averages across all folds (8 total)
                    mean_rmse = float(np.mean(rmse_scores))
                    mean_mse = float(np.mean(mse_scores))   # ADDED
                    mean_mae = float(np.mean(mae_scores))   # ADDED
                    mean_mape = float(np.mean(mape_scores)) # ADDED

                    # Log aggregated CV metrics
                    mlflow.log_metric("avg_cv_rmse", mean_rmse)
                    mlflow.log_metric("avg_cv_mse", mean_mse)   # ADDED
                    mlflow.log_metric("avg_cv_mae", mean_mae)   # ADDED
                    mlflow.log_metric("avg_cv_mape", mean_mape) # ADDED

                    # Hyperopt tries to MINIMIZE the returned value: we'll minimize RMSE
                    return mean_rmse

            search_space = {
                "learning_rate": hp.loguniform("learning_rate", np.log(0.001), np.log(0.3)),
                "max_depth": hp.quniform("max_depth", 2, 10, 1),
                "subsample": hp.uniform("subsample", 0.5, 1.0),
                "colsample_bytree": hp.uniform("colsample_bytree", 0.5, 1.0),
                "reg_alpha": hp.loguniform("reg_alpha", np.log(1e-6), np.log(10)),
                "reg_lambda": hp.loguniform("reg_lambda", np.log(1e-6), np.log(10)),
                "min_child_weight": hp.quniform("min_child_weight", 1, 10, 1),
                "gamma": hp.uniform("gamma", 0.0, 5.0),
                "num_boost_round": hp.quniform("num_boost_round", 50, 300, 10),
            }

            def objective(params):
                loss = local_cv_score(params)
                return {"loss": loss, "status": STATUS_OK}

            spark_trials = SparkTrials(parallelism=2)

            with mlflow.start_run(run_name=f"XGBoost_{target_column}") as run:
                mlflow.log_param("num_rows", len(pdf))
                mlflow.log_param("num_features", len(feature_cols))
                mlflow.log_param("cv_method", "TimeSeriesSplit(11) + ShuffleSplit(6)")
                mlflow.log_param("target_column", target_column)

                best = fmin(
                    fn=objective,
                    space=search_space,
                    algo=tpe.suggest,
                    max_evals=max_evals,
                    trials=spark_trials,
                )

                best_params = {
                    "learning_rate": float(best["learning_rate"]),
                    "max_depth": int(best["max_depth"]),
                    "subsample": float(best["subsample"]),
                    "colsample_bytree": float(best["colsample_bytree"]),
                    "reg_alpha": float(best["reg_alpha"]),
                    "reg_lambda": float(best["reg_lambda"]),
                    "min_child_weight": float(best["min_child_weight"]),
                    "gamma": float(best["gamma"]),
                    "num_boost_round": int(best["num_boost_round"]),
                }
                for k, v in best_params.items():
                    mlflow.log_param(f"best_{k}", v)

                # Final training with SparkXGBRegressor
                final_pdf = pd.DataFrame(X_scaled, columns=feature_cols)
                final_pdf[target_column] = y_full
                spark_final = spark.createDataFrame(final_pdf)

                assembler = VectorAssembler(
                    inputCols=feature_cols,
                    outputCol="features",
                )
                final_as = assembler.transform(spark_final)

                xgb_final = SparkXGBRegressor(
                    label_col=target_column,
                    features_col="features",
                    evalMetric="rmse",
                    useExternalMemory=True,
                    numWorkers=2,
                    maxDepth=best_params["max_depth"],
                    eta=best_params["learning_rate"],
                    subsample=best_params["subsample"],
                    colsampleBytree=best_params["colsample_bytree"],
                    regAlpha=best_params["reg_alpha"],
                    regLambda=best_params["reg_lambda"],
                    minChildWeight=best_params["min_child_weight"],
                    gamma=best_params["gamma"],
                    numRound=best_params["num_boost_round"],
                    checkpointPath="/tmp/xgb_final_checkpoints_reg",
                    checkpointInterval=1,
                    numEarlyStoppingRounds=10
                )

                final_model = xgb_final.fit(final_as)

                # Evaluate final model
                pred_sdf = final_model.transform(final_as).select("prediction", target_column)
                pred_results = pred_sdf.collect()
                preds = [r["prediction"] for r in pred_results]
                y_true = [r[target_column] for r in pred_results]

                # ADDED: Compute final MSE, RMSE, MAE, MAPE
                final_mse = mean_squared_error(y_true, preds)
                final_rmse = math.sqrt(final_mse)
                final_mae = mean_absolute_error(y_true, preds)
                if any(val == 0 for val in y_true):
                    final_mape = float('inf')
                else:
                    final_mape = mean_absolute_percentage_error(y_true, preds)

                # Log final metrics
                mlflow.log_metric("final_rmse", final_rmse)
                mlflow.log_metric("final_mse", final_mse)    # ADDED
                mlflow.log_metric("final_mae", final_mae)    # ADDED
                mlflow.log_metric("final_mape", final_mape)  # ADDED

                self.logger.info(
                    "[Final Model for %s] RMSE=%.4f, MSE=%.4f, MAE=%.4f, MAPE=%.4f",
                    target_column, final_rmse, final_mse, final_mae, final_mape
                )

                model_artifact_path = f"xgboost_{target_column}"

                # Log the model to MLflow
                input_example = final_pdf.iloc[:5]  # small example
                signature = infer_signature(
                    input_example.drop(target_column, axis=1),
                    input_example[target_column]
                )
                booster = final_model.get_booster()
                booster.set_param({"save_json": True})

                # THE CRITICAL FIX BELOW:
                # "xgb_model=booster" instead of "booster=booster"
                mlflow.xgboost.log_model(
                    xgb_model=booster,  # <-- FIX HERE
                    artifact_path=model_artifact_path,
                    input_example=input_example,
                    signature=signature
                )

                run_id = run.info.run_id
                model_uri = f"runs:/{run_id}/{model_artifact_path}"

                self.logger.info(
                    "Completed regression training for %s. Model URI: %s", target_column, model_uri
                )

                return {"run_id": run_id, "model_uri": model_uri}

        finally:
            end_cpu_time = time.process_time()
            end_cpu_percent = psutil.cpu_percent()
            self.logger.info(
                "CPU usage start: %s%%, end: %s%%. CPU time for "
                "xgboost_training_with_hyperopt(%s): %.4f seconds",
                start_cpu_percent,
                end_cpu_percent,
                target_column,
                end_cpu_time - start_cpu_time,
            )

    def generate_predictions_and_write_to_iceberg(
        self,
        spark: SparkSession,
        df_full_features: DataFrame,
        model_info_dict: dict[str, dict[str, str]],
        db_name: str = "iceberg_nba_player_boxscores_regression"
    ) -> None:
        """
        Generate regression predictions for each target column using the trained XGBoost
        models from MLflow, and write them all to an Iceberg table.

        :param df_full_features: Spark DataFrame with all feature columns.
        :param model_info_dict:
            A dict keyed by target_column -> { "run_id":..., "model_uri":... }
        :param db_name: iceberg db to write to
        """
        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()
        try:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

            # Identify numeric feature cols
            numeric_feature_cols = [
                f.name
                for f in df_full_features.schema.fields
                if isinstance(f.dataType, (DoubleType, IntegerType, DecimalType))
                and f.name not in REGRESSION_TARGET_COLS  # exclude the targets themselves
            ]

            # Ensure those columns exist
            for c in numeric_feature_cols:
                if c not in df_full_features.columns:
                    df_full_features = df_full_features.withColumn(c, lit(0.0))

            # Prepare a single Pandas DataFrame so we can add columns for each target's prediction
            assembler = VectorAssembler(
                inputCols=numeric_feature_cols,
                outputCol="features_assembled"
            )
            assembled_df = assembler.transform(df_full_features)
            pdf_inference = assembled_df.toPandas()
            if len(pdf_inference) == 0:
                self.logger.info("No rows to predict. Exiting.")
                return

            pdf_inference["features_assembled"] = pdf_inference["features_assembled"].apply(
                lambda v: v.toArray() if hasattr(v, "toArray") else np.array(v)
            )

            # For each target column, load the model, predict, store in pdf_inference
            for target_col, info in model_info_dict.items():
                model_uri = info["model_uri"]
                self.logger.info("Loading XGBoost model from MLflow for target=%s, URI=%s", target_col, model_uri)
                loaded_booster = mlflow.xgboost.load_model(model_uri)

                features_array = np.vstack(pdf_inference["features_assembled"].values)
                dtest = xgb.DMatrix(features_array)
                pred_vals = loaded_booster.predict(dtest)

                # Create a new column for predictions
                pred_col_name = f"prediction_{target_col}"
                pdf_inference[pred_col_name] = pred_vals

            # Drop the ndarray column
            pdf_inference.drop(columns=["features_assembled"], inplace=True)

            # Create a Spark DataFrame and write to Iceberg
            pred_spark_df = spark.createDataFrame(pdf_inference)

            run_ids_concat = "_".join([info["run_id"] for info in model_info_dict.values()])
            out_table = f"spark_catalog.{db_name}.xgboost_team_regression_predictions_{run_ids_concat}"
            pred_spark_df.write.format("iceberg").mode("overwrite").saveAsTable(out_table)

            self.logger.info(
                "Wrote regression predictions for all targets to %s. Combined run_ids: %s",
                out_table,
                run_ids_concat
            )
        except Exception as e:
            self.logger.error("Error generating/writing regression predictions: %s", str(e), exc_info=True)
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

    def main(self) -> None:
        """
        Main entry point for the NBA Team Boxscore Regression Pipeline.
        We build/score separate regressors for each column in REGRESSION_TARGET_COLS.
        """
        start_cpu_time_main = time.process_time()
        start_cpu_percent_main = psutil.cpu_percent()
        import pstats
        import io

        try:
            self.profiler.enable()
            stats_thread = threading.Thread(
                target=self.log_stats_periodically,
                args=(),
                daemon=True,
            )
            stats_thread.start()

            spark = self.create_spark_session()

            # 1) Load scd_team_boxscores
            df_team = self.load_team_boxscores(spark)

            # 2) Clean
            cleaned_df = self.clean_data(df_team)

            # 3) Feature engineering
            fe_df = self.spark_feature_engineering(cleaned_df).persist()

            # We'll store model metadata for each target column in a dict
            #   Key: target_col, Value: { "run_id":..., "model_uri":... }
            model_info_dict = {}

            # 4) For each target column, do Lasso + XGBoost
            for target_col in REGRESSION_TARGET_COLS:
                self.logger.info("=== Processing target column: %s ===", target_col)

                # 4a) Lasso
                selected_features = self.run_lasso_feature_selection(
                    spark,
                    fe_df,
                    target_column=target_col,
                    mlflow_experiment_name="NBA_Lasso_Feature_Selection_Regression",
                    coefficient_strict_threshold=0.0
                )
                # Possibly also keep the target_col
                selected_cols_for_reg = selected_features + [target_col]
                sub_df = fe_df.select(*selected_cols_for_reg)

                # 4b) XGBoost Regressor w/ Hyperopt
                model_info = self.xgboost_training_with_hyperopt(
                    spark,
                    sub_df,
                    target_column=target_col,
                    mlflow_experiment_name="XGBoost_NBA_Team_Regression",
                    max_evals=66
                )
                if model_info:
                    model_info_dict[target_col] = model_info
                else:
                    self.logger.warning("No model info returned for target=%s", target_col)

            # 5) Generate predictions for all target columns, write to table
            if len(model_info_dict) > 0:
                self.generate_predictions_and_write_to_iceberg(
                    spark,
                    fe_df,
                    model_info_dict=model_info_dict,
                    db_name="iceberg_nba_player_boxscores_regression",
                )

            spark.stop()
            self.profiler.disable()

            s_final = io.StringIO()
            p_final = pstats.Stats(self.profiler, stream=s_final).sort_stats("cumulative")
            p_final.print_stats()
            self.logger.info("[Final CPU profiling stats]:\n%s", s_final.getvalue())

            self.logger.info("Done executing main pipeline for NBA Team Regression.")

        except Exception as ex:
            self.logger.error("Unexpected error in main(): %s", str(ex), exc_info=True)
            sys.exit(1)
        finally:
            end_cpu_time_main = time.process_time()
            end_cpu_percent_main = psutil.cpu_percent()
            self.logger.info(
                "CPU usage start: %s%%, end: %s%%. CPU time for entire main() block: "
                "%.4f seconds",
                start_cpu_percent_main,
                end_cpu_percent_main,
                end_cpu_time_main - start_cpu_time_main,
            )


if __name__ == "__main__":
    pipeline = NBATeamBoxscoreRegressionPipeline()
    pipeline.main()
