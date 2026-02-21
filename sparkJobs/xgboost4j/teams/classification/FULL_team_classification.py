
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA_team_boxscores_classification.py

OVERVIEW:
---------
This script orchestrates an end-to-end pipeline for classification of a team's
win_loss outcome (W or L) based on numeric columns in scd_team_boxscores.

Key changes requested:
1) Convert 0 -> "L" and 1 -> "W" using IndexToString or direct mapping.
2) Add checkpointing for XGBoost training (checkpointPath & checkpointInterval).
3) Exclude 'team_id' from numeric features (not used as a numeric predictor).
4) Lower Lasso strict threshold from 0.01 to 0.001.
5) Increase max_evals or refine hyperparameter ranges in the hyperopt search.
6) Use more cross-validation splits and ensure all training and cross validations are logged.

CHANGES TO FIX THE SPARK CONTEXT PICKLING ERROR:
------------------------------------------------
- We do local XGBoost cross-validation for hyperopt. No references to Spark
  inside the Hyperopt objective function.
- After finding best params, we then do final training with SparkXGBClassifier.

FIX FOR THE ERROR:
------------------
- In generate_predictions_and_write_to_iceberg(), after assembling features
  and collecting to Pandas, remove or convert the 'features_assembled' column
  (which is ndarray/object) before createDataFrame(). Otherwise, Spark cannot
  infer the schema for that field.

***Additionally, exclude team_id from wavelet transforms to avoid "Missing: team_id_fft_3."***
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
    DecimalType,
)
from pyspark.sql.window import Window
from sklearn.metrics import (
    accuracy_score,
    roc_auc_score,
    f1_score,
    confusion_matrix
)
from sklearn.model_selection import ShuffleSplit, TimeSeriesSplit
from sklearn.preprocessing import StandardScaler as SklearnStandardScaler

import xgboost as xgb

# MLflow
import mlflow
import mlflow.xgboost
from mlflow.models.signature import infer_signature

# Hyperopt
from hyperopt import STATUS_OK, Trials, fmin, hp, tpe, SparkTrials

# For final training/predictions in Spark:
from xgboost.spark import SparkXGBClassifier
from pyspark.ml.feature import VectorAssembler

import pywt  # For Wavelet transforms
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, DoubleType

from sklearn.linear_model import LassoCV

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class NBATeamBoxscoreClassificationPipeline:
    """
    Pipeline class that orchestrates data loading, cleaning, feature engineering,
    and classification of the team's win/loss outcome from scd_team_boxscores.
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
                SparkSession.builder.appName("XGBoost_NBA_Team_Classification")
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

    def clean_and_partition_data(self, df: DataFrame) -> DataFrame:
        """
        Clean the data by removing duplicates and filtering out invalid values.
        Also create a binary label for 'win_loss': 1 if W, else 0.
        """
        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()
        try:
            df_dedup = df.dropDuplicates()

            # Basic cleaning if columns are present
            if "team_id" in df_dedup.columns:
                df_dedup = df_dedup.filter(col("team_id").isNotNull())

            # Create a new binary label column => 1 if W, else 0
            df_dedup = df_dedup.withColumn(
                "win_loss_binary",
                when(col("win_loss") == "W", lit(1)).otherwise(lit(0))
            )

            self.logger.info("Data cleaning complete, 'win_loss_binary' created.")
            return df_dedup
        except Exception as e:
            self.logger.error("Error in cleaning data: %s", str(e), exc_info=True)
            sys.exit(1)
        finally:
            end_cpu_time = time.process_time()
            end_cpu_percent = psutil.cpu_percent()
            self.logger.info(
                "CPU usage start: %s%%, end: %s%%. CPU time for clean_and_partition_data(): "
                "%.4f seconds",
                start_cpu_percent,
                end_cpu_percent,
                end_cpu_time - start_cpu_time,
            )

    def spark_feature_engineering(self, df: DataFrame) -> DataFrame:
        """
        Perform advanced feature engineering using Spark DataFrame transformations
        on all numeric columns: absolute, squared, log, rolling mean, etc.
        **Now also includes Fourier/Wavelet transformations for each numeric column.**

        --> FIX: we exclude "team_id" from numeric_cols to avoid schema mismatches
            like "Missing: team_id_fft_3."
        --> Also treat DecimalType columns as numeric, casting them to Double so
            we don't end up with zero features.
        """
        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()
        try:
            # Identify numeric columns (excluding the label AND excluding team_id).
            # We include DoubleType, IntegerType, and DecimalType, then cast decimals to double.
            numeric_cols_raw = [
                f.name
                for f in df.schema.fields
                if isinstance(f.dataType, (DoubleType, IntegerType, DecimalType))
                and f.name not in ("win_loss_binary", "team_id")
            ]

            # Cast any Decimal columns to double so they're fully numeric.
            # We'll just do a pass of col(c).cast("double").
            for c in numeric_cols_raw:
                df = df.withColumn(c, col(c).cast(DoubleType()))

            # Re-check numeric columns after cast
            numeric_cols = [
                f.name
                for f in df.schema.fields
                if (isinstance(f.dataType, DoubleType) or isinstance(f.dataType, IntegerType))
                and f.name not in ("win_loss_binary", "team_id")
            ]

            # 1) Create columns: absolute, squared, log
            for c in numeric_cols:
                df = df.withColumn(f"{c}_abs", spark_abs(col(c).cast(DoubleType())))
                df = df.withColumn(f"{c}_squared", (col(c) * col(c)).cast(DoubleType()))
                # Protect log transform: if <= 0, set to 1 for safety
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
                # If no date, skip or do a trivial fallback
                for c in numeric_cols:
                    df = df.withColumn(f"{c}_ma_3", col(c))

            # 3) Clean up NaNs or infinities => fill numeric columns with 0
            numeric_cols_for_nullfill = [
                f.name
                for f in df.schema.fields
                if isinstance(f.dataType, (DoubleType, IntegerType))
                and f.name != "win_loss_binary"
            ]
            for c in numeric_cols_for_nullfill:
                df = df.withColumn(c, when(col(c).isNull(), 0).otherwise(col(c)))

            # ------------------------------------------------------------------------
            # 4) Fourier/Wavelet transformations (grouped by team_id, sorted by game_date)
            #    This uses a Pandas UDF GROUPED MAP approach for demonstration.
            # ------------------------------------------------------------------------
            existing_fields = df.schema.fields
            # We'll add 6 new columns per numeric col: 3 for FFT, 3 for Wavelet
            extended_fields = list(existing_fields)

            for c in numeric_cols:
                for i in range(1, 4):
                    extended_fields.append(StructField(f"{c}_fft_{i}", DoubleType(), True))
                for i in range(1, 4):
                    extended_fields.append(StructField(f"{c}_wave_{i}", DoubleType(), True))

            extended_schema = StructType(extended_fields)

            def freq_domain_features(pdf: pd.DataFrame) -> pd.DataFrame:
                # Sort by game_date if present, to keep chronological order
                if "game_date" in pdf.columns:
                    pdf = pdf.sort_values("game_date")

                for c in numeric_cols:
                    # Convert to numpy array
                    arr = pdf[c].values

                    # FFT
                    freq = np.fft.rfft(arr)       # rfft for real-valued signals
                    freq_abs = np.abs(freq)
                    top_3_idx = freq_abs.argsort()[::-1][:3]  # indices of largest 3 amplitudes
                    # Always create 3 columns, filling with 0 if fewer than 3
                    for i in range(1, 4):
                        if i <= len(top_3_idx):
                            pdf[f"{c}_fft_{i}"] = freq_abs[top_3_idx[i-1]]
                        else:
                            pdf[f"{c}_fft_{i}"] = 0.0

                    # Wavelet
                    coeffs = pywt.wavedec(arr, 'db1', level=2)  # Example wavelet + level
                    c_concat = np.concatenate(coeffs)
                    c_abs = np.abs(c_concat)
                    top_3_idx_w = c_abs.argsort()[::-1][:3]
                    for i in range(1, 4):
                        if i <= len(top_3_idx_w):
                            pdf[f"{c}_wave_{i}"] = c_abs[top_3_idx_w[i-1]]
                        else:
                            pdf[f"{c}_wave_{i}"] = 0.0

                return pdf

            # If team_id or any grouping key does not exist, skip the transform
            if "team_id" in df.columns:
                df = df.groupBy("team_id").applyInPandas(freq_domain_features, schema=extended_schema)
            else:
                single_group_key = lit("dummy_group")
                df = df.groupBy(single_group_key).applyInPandas(freq_domain_features, schema=extended_schema).drop("dummy_group")

            self.logger.info("Spark-based feature engineering (including Fourier/Wavelet) complete.")
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
        mlflow_experiment_name: str = "NBA_Lasso_Feature_Selection",
        coefficient_strict_threshold: float = 0.00000001
    ) -> dict:
        """
        Run LassoCV locally to identify the best alpha for sparse feature selection.
        Returns a dictionary keyed by "table_target" with a list of features that
        have non-zero (and above the strict threshold) coefficients.

        Steps:
        1) Extract numeric columns (excluding the label win_loss_binary and 'team_id').
        2) Collect to Pandas, scale features.
        3) Fit LassoCV to find the best alpha.
        4) Identify non-zero-coefficient (above threshold) features.
        5) Log best alpha and selected features to MLflow.
        6) Return { "scd_team_boxscores__win_loss_binary": [...list of top features...] }
        """
        # Identify numeric features (exclude label and team_id), including DecimalType
        numeric_cols = [
            f.name
            for f in df.schema.fields
            if isinstance(f.dataType, (DoubleType, IntegerType, DecimalType))
            and f.name not in ("win_loss_binary", "team_id")
        ]

        # Filter rows that have a label
        df = df.filter(col("win_loss_binary").isNotNull())

        # Convert decimals to double just in case
        for c in numeric_cols:
            df = df.withColumn(c, col(c).cast(DoubleType()))

        pdf = (
            df.orderBy(desc("game_date"))
              .select(*numeric_cols, "win_loss_binary")
              .dropna()
              .limit(600000)
              .toPandas()
        )
        # --------------------------------------------------------------------------------

        if pdf.shape[0] < 20:
            self.logger.warning("Insufficient data to run LassoCV. Returning all numeric features.")
            table_target_key = "scd_team_boxscores__win_loss_binary"
            return {table_target_key: numeric_cols}

        X_full = pdf[numeric_cols].values
        y_full = pdf["win_loss_binary"].values

        # Scale
        scaler = SklearnStandardScaler()
        if X_full.shape[1] < 1:
            self.logger.warning("No numeric columns found after decimal casting. Returning all numeric.")
            table_target_key = "scd_team_boxscores__win_loss_binary"
            return {table_target_key: numeric_cols}

        X_scaled = scaler.fit_transform(X_full)

        # MLflow experiment
        mlflow.set_experiment(mlflow_experiment_name)
        table_target_key = "scd_team_boxscores__win_loss_binary"
        top_features_dict = {}

        with mlflow.start_run(run_name="Lasso_Feature_Selection", nested=True):
            lasso_cv = LassoCV(
                cv=5,
                random_state=42,
                n_jobs=-1,
                max_iter=10000
            ).fit(X_scaled, y_full)

            best_alpha = lasso_cv.alpha_
            mlflow.log_param("lasso_best_alpha", best_alpha)

            # Identify non-zero-coeff features, also apply a stricter threshold
            coefs = lasso_cv.coef_
            nonzero_indices = np.where(np.abs(coefs) > coefficient_strict_threshold)[0]
            selected_features = [numeric_cols[i] for i in nonzero_indices]

            # Log # of selected features and the actual names
            mlflow.log_param("strict_threshold", coefficient_strict_threshold)
            mlflow.log_param("num_selected_features", len(selected_features))
            mlflow.log_param("selected_features_list", str(selected_features))

            self.logger.info(
                "[Lasso Feature Selection] Best alpha: %.6f, strict_threshold=%.4f, "
                "Selected %d features: %s",
                best_alpha, coefficient_strict_threshold, len(selected_features), selected_features
            )

            # Save to dictionary
            top_features_dict[table_target_key] = selected_features

        return top_features_dict

    def xgboost_training_with_hyperopt(
        self,
        spark: SparkSession,
        df: DataFrame,
        mlflow_experiment_name: str = "XGBoost_NBA_Team_Classification",
        max_evals: int = 25  # increased from 15 to 25
    ) -> dict[str, str]:
        """
        Train an XGBoost classifier (binary: W vs L) using Hyperopt with SparkTrials.
        1) We do local xgboost cross-validation in the objective function (no Spark references).
        2) Then we train a final SparkXGBClassifier with the best hyperparams.

        Includes checkpointing for the final SparkXGBClassifier:
          - setCheckpointPath("/tmp/xgb_final_checkpoints")
          - setCheckpointInterval(1)
        """
        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()

        try:
            mlflow.set_experiment(mlflow_experiment_name)

            # Filter any rows missing the 'win_loss_binary' label
            df = df.filter(col("win_loss_binary").isNotNull())

            # Collect numeric columns (excluding the label), including DecimalType
            numeric_cols_raw = [
                f.name
                for f in df.schema.fields
                if isinstance(f.dataType, (DoubleType, IntegerType, DecimalType))
                and f.name not in ("win_loss_binary")
            ]

            # Cast decimals to double
            for c in numeric_cols_raw:
                df = df.withColumn(c, col(c).cast(DoubleType()))

            # After casting, re-check which columns are effectively numeric
            numeric_cols = [
                f.name
                for f in df.schema.fields
                if (isinstance(f.dataType, DoubleType) or isinstance(f.dataType, IntegerType))
                and f.name not in ("win_loss_binary")
            ]

            if len(numeric_cols) < 1:
                self.logger.warning("No numeric columns found for classification features.")
                return {}

            # ------------------------------------------------------------------------------
            # CHANGED: Ensure we get the 200k MOST RECENT rows by ordering by game_date DESC.
            # ------------------------------------------------------------------------------
            pdf = (
                df.orderBy(desc("game_date"))                  
                  .select(*numeric_cols, "win_loss_binary")
                  .dropna()
                  .limit(600000)
                  .toPandas()
            )
            # ------------------------------------------------------------------------------

            if len(pdf) < 10:
                self.logger.warning("Not enough data for classification; skipping.")
                return {}

            X_full = pdf[numeric_cols].values
            y_full = pdf["win_loss_binary"].values
            # Scale features
            scaler = SklearnStandardScaler()
            X_scaled = scaler.fit_transform(X_full)

            # Cross-validation splits (increased from 3 to 5, 2 to 3)
            tscv = TimeSeriesSplit(n_splits=5)
            random_splits = ShuffleSplit(n_splits=3, test_size=0.2, random_state=42)

            def local_cv_score(params: dict[str, float]) -> float:
                """
                Local XGBoost cross-validation, no Spark references.
                We combine multiple splits for a robust AUC measure.
                Returns (1 - mean AUC) so Hyperopt can minimize it.

                We also log fold AUC to MLflow in a nested run so each hyperopt trial
                has its own set of metrics.
                """
                # We'll do a nested MLflow run for each hyperopt trial:
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
                        "objective": "binary:logistic",
                        "eval_metric": "auc",
                        "verbosity": 0,  # keep quiet
                    }
                    num_round = int(params["num_boost_round"])

                    # Log these hyperparams
                    for k, v in xgb_params.items():
                        mlflow.log_param(k, v)
                    mlflow.log_param("num_boost_round", num_round)

                    auc_scores = []
                    fold_index = 0

                    # 1) TimeSeriesSplit folds
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
                        preds_proba = booster.predict(dtest)
                        # Only compute AUC if there's >1 class in y
                        if len(set(y_full[test_idx])) > 1:
                            fold_auc = roc_auc_score(y_full[test_idx], preds_proba)
                            auc_scores.append(fold_auc)
                            mlflow.log_metric("fold_auc", fold_auc, step=fold_index)
                        fold_index += 1

                    # 2) Random splits
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
                        preds_proba = booster.predict(dtest)
                        if len(set(y_full[test_idx])) > 1:
                            fold_auc = roc_auc_score(y_full[test_idx], preds_proba)
                            auc_scores.append(fold_auc)
                            mlflow.log_metric("fold_auc", fold_auc, step=fold_index)
                        fold_index += 1

                    if len(auc_scores) == 0:
                        mlflow.log_metric("avg_cv_auc", 0.5)
                        return 1.0

                    mean_auc = float(np.mean(auc_scores))
                    mlflow.log_metric("avg_cv_auc", mean_auc)

                    # Return 1 - AUC so Hyperopt can minimize it
                    return 1.0 - mean_auc

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

            with mlflow.start_run() as run:
                mlflow.log_param("num_rows", len(pdf))
                mlflow.log_param("num_features", len(numeric_cols))
                mlflow.log_param("cv_method", "TimeSeriesSplit(5) + ShuffleSplit(3)")

                # Hyperopt
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
                # Log best params
                for k, v in best_params.items():
                    mlflow.log_param(f"best_{k}", v)

                # ----- FINAL MODEL TRAINING WITH SparkXGBClassifier -----
                final_pdf = pd.DataFrame(X_scaled, columns=numeric_cols)
                final_pdf["win_loss_binary"] = y_full
                spark_final = spark.createDataFrame(final_pdf)

                assembler = VectorAssembler(
                    inputCols=numeric_cols,
                    outputCol="features",
                )
                final_as = assembler.transform(spark_final)

                xgb_final = SparkXGBClassifier(
                    label_col="win_loss_binary",
                    features_col="features",
                    evalMetric="auc",
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
                    checkpointPath="/tmp/xgb_final_checkpoints",
                    checkpointInterval=1,
                    numEarlyStoppingRounds=10
                )

                # Fit final model
                final_model = xgb_final.fit(final_as)

                # Evaluate final model
                pred_sdf = final_model.transform(final_as).select("probability", "prediction", "win_loss_binary")
                pred_results = pred_sdf.collect()
                preds = [r["prediction"] for r in pred_results]
                probs = [r["probability"][1] for r in pred_results]
                y_true = [r["win_loss_binary"] for r in pred_results]

                if len(set(y_true)) > 1:
                    final_auc = roc_auc_score(y_true, probs)
                    final_acc = accuracy_score(y_true, preds)
                    f1_val = f1_score(y_true, preds, average="binary")
                    cm = confusion_matrix(y_true, preds)

                    mlflow.log_metric("final_auc", final_auc)
                    mlflow.log_metric("final_accuracy", final_acc)
                    mlflow.log_metric("final_f1", f1_val)

                    self.logger.info(
                        "[Final Model on entire dataset] AUC=%.4f, Accuracy=%.4f, F1=%.4f, ConfusionMatrix=%s",
                        final_auc, final_acc, f1_val, cm.tolist()
                    )

                model_artifact_path = "xgboost_team_win_loss"

                # === ADDED: Provide an input_example and a signature to remove the MLflow warning ===
                input_example = final_pdf.iloc[:5]  # just 5 rows for example
                signature = infer_signature(input_example.drop("win_loss_binary", axis=1), input_example["win_loss_binary"])

                booster = final_model.get_booster()
                booster.set_param({"save_json": True})

                # FIX: Pass xgb_model instead of booster to log_model()
                mlflow.xgboost.log_model(
                    xgb_model=booster,  # <-- FIX HERE
                    artifact_path=model_artifact_path,
                    input_example=input_example,
                    signature=signature
                )

                run_id = run.info.run_id
                model_uri = f"runs:/{run_id}/{model_artifact_path}"
                self.logger.info(
                    "Completed classification training. Model URI: %s", model_uri
                )

                return {"run_id": run_id, "model_uri": model_uri}

        finally:
            end_cpu_time = time.process_time()
            end_cpu_percent = psutil.cpu_percent()
            self.logger.info(
                "CPU usage start: %s%%, end: %s%%. CPU time for "
                "xgboost_training_with_hyperopt(): %.4f seconds",
                start_cpu_percent,
                end_cpu_percent,
                end_cpu_time - start_cpu_time,
            )

    def generate_predictions_and_write_to_iceberg(
        self,
        spark: SparkSession,
        df: DataFrame,
        model_info: dict[str, str],
        db_name: str = "iceberg_nba_player_boxscores_classification",
    ) -> None:
        """
        Generate predictions (same historical data for demonstration) using the
        trained XGBoost classification model from MLflow, and write them to an Iceberg table.

        Also uses direct mapping logic to convert 0 => "L" and 1 => "W".
        (We no longer attempt loaded_xgb_model.transform() because that
        object is a Booster, not a Spark model.)

        FIX for "ValueError: setting an array element with a sequence." / "Cannot infer type":
         - Use a unique assembler outputCol (e.g. 'features_assembled')
         - Convert or drop the 'features_assembled' column (ndarray)
         - Then spark.createDataFrame() will succeed
        """
        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()
        try:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            run_id = model_info["run_id"]
            model_uri = model_info["model_uri"]

            # Identify numeric features
            numeric_cols_raw = [
                f.name
                for f in df.schema.fields
                if isinstance(f.dataType, (DoubleType, IntegerType, DecimalType))
                and f.name not in ("win_loss_binary")
            ]

            # Cast decimals to double
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

            # Ensure all numeric_cols exist
            for c in numeric_cols:
                if c not in df.columns:
                    df = df.withColumn(c, lit(0.0))

            # 1) Load the XGBoost model from MLflow (returns a native Booster)
            loaded_xgb_booster = mlflow.xgboost.load_model(model_uri)

            # 2) Vector-assemble in Spark with a unique output column name
            assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features_assembled")
            assembled_df = assembler.transform(df)

            # Convert to Pandas
            pdf_inference = assembled_df.toPandas()
            if len(pdf_inference) == 0:
                self.logger.info("No rows to predict. Exiting.")
                return

            # Convert each Spark DenseVector to a NumPy array
            pdf_inference["features_assembled"] = pdf_inference["features_assembled"].apply(
                lambda v: v.toArray() if hasattr(v, "toArray") else np.array(v)
            )

            # 3) Local inference with the loaded booster
            features_array = np.vstack(pdf_inference["features_assembled"].values)
            dtest = xgb.DMatrix(features_array)
            pred_proba = loaded_xgb_booster.predict(dtest)
            pred_class = (pred_proba >= 0.5).astype(int)

            def decode_pred(p):
                return "W" if p == 1 else "L"

            final_prediction_str = [decode_pred(p) for p in pred_class]

            # 4) Attach predictions
            pdf_inference["prediction"] = pred_class
            pdf_inference["probability"] = pred_proba
            pdf_inference["final_prediction"] = final_prediction_str
            pdf_inference["prediction_type"] = "historical_classification"

            # Drop the ndarray column
            pdf_inference.drop(columns=["features_assembled"], inplace=True)

            # 5) Rebuild Spark DF, write to Iceberg
            pred_spark_df = spark.createDataFrame(pdf_inference)

            out_table = f"spark_catalog.{db_name}.xgboost_team_predictions_{run_id}"
            pred_spark_df.write.format("iceberg").mode("overwrite").saveAsTable(out_table)

            self.logger.info(
                "Wrote classification predictions to %s (run_id=%s).",
                out_table,
                run_id,
            )
        except Exception as e:
            self.logger.error(
                "Error generating/writing classification predictions: %s", str(e), exc_info=True
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

    def main(self) -> None:
        """
        Main entry point for the NBA Team Boxscore Classification Pipeline.
        Orchestrates the entire process end-to-end.
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

            # 2) Clean & partition
            cleaned_df = self.clean_and_partition_data(df_team)

            # 3) Feature engineering (now includes Fourier/Wavelet)
            fe_df = self.spark_feature_engineering(cleaned_df).persist()

            # 4) LassoCV for feature selection
            lasso_dict = self.run_lasso_feature_selection(
                spark,
                fe_df,
                mlflow_experiment_name="NBA_Lasso_Feature_Selection",
                coefficient_strict_threshold=0.00000001
            )
            table_target_key = "scd_team_boxscores__win_loss_binary"
            if table_target_key in lasso_dict and len(lasso_dict[table_target_key]) > 0:
                selected_feats = lasso_dict[table_target_key]
                fe_df = fe_df.select(*selected_feats, "win_loss_binary")
                self.logger.info("Reduced dataset to Lasso-selected features: %d columns.", len(selected_feats))
            else:
                self.logger.info("No Lasso-filtered features found. Using all numeric features.")

            # 5) XGBoost => Bayesian optimization + cross-validation
            model_info = self.xgboost_training_with_hyperopt(
                spark,
                fe_df,
                mlflow_experiment_name="XGBoost_NBA_Team_Classification",
                max_evals=50  # increased from 15
            )

            # 6) Generate predictions
            if model_info:
                self.generate_predictions_and_write_to_iceberg(
                    spark,
                    fe_df,
                    model_info=model_info,
                    db_name="iceberg_nba_player_boxscores_classification",
                )

            spark.stop()
            self.profiler.disable()

            s_final = io.StringIO()
            p_final = pstats.Stats(self.profiler, stream=s_final).sort_stats("cumulative")
            p_final.print_stats()
            self.logger.info("[Final CPU profiling stats]:\n%s", s_final.getvalue())

            self.logger.info("Done executing main pipeline for NBA Team Classification.")

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
    pipeline = NBATeamBoxscoreClassificationPipeline()
    pipeline.main()
