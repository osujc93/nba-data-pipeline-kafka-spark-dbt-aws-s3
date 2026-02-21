#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA_player_boxscores.py

OVERVIEW:
---------
This script orchestrates a complete end-to-end pipeline for NBA player box-score data.
It reads raw historical data from Iceberg tables, cleans and de-duplicates it, performs
extensive feature engineering, selects the most relevant features via Lasso regression,
and then trains optimized XGBoost models (using Bayesian hyperparameter search) to
generate both historical and future predictions. The script also illustrates how
continual learning stages could be integrated, as well as robust sampling techniques
to handle large or imbalanced datasets. All code is contained here without omissions.

MAJOR STEPS & DETAILS:
----------------------

1) **Data Loading from Iceberg**
   - Connects to an Apache Spark environment and accesses multiple Iceberg tables
     within the "NBA_player_boxscores" database (e.g., player_alltime,
     player_boxscores_advanced_stats, player_currentseason,
     scd_player_boxscores).
   - Combines all relevant tables into one large Spark DataFrame with columns
     such as player stats, team information, and additional metadata.
   - Enriches the DataFrame by adding a source_table column to keep track of each
     record’s origin.

2) **Data Cleaning & Partitioning**
   - Drops duplicate rows and filters out invalid or null values (for instance,
     negative points).
   - Ensures partition-aware processing by referencing key columns (team_id,
     player_id, etc.).
   - In practice, this results in a curated and de-duplicated Spark DataFrame that is
     ready for further processing steps.

3) **Advanced Feature Engineering**
   - Formerly used Pandas transformations, but now implemented via Spark DataFrame
     operations, UDFs, and Window functions to avoid forcing data onto a single driver.
   - Generates a multitude of new features for each numeric column:
     - **Absolute, Squared**
     - **Log**
     - **Rolling Mean** using a Spark window (example).
     - **Fourier / Wavelet Transforms** using a Spark UDF for demonstration (can be
       expensive in distributed).
   - Cleans up any infinities or NaNs so the resulting data is well-defined for
     machine learning models.

4) **Lasso Regression (LassoCV) for Feature Selection**
   - Still uses scikit-learn’s LassoCV, but we only collect a *sample* to Pandas
     (instead of the entire dataset) to reduce overhead.
   - Identifies an optimal alpha that yields the most relevant subset of features.
   - Logs each run’s parameters, metrics, and model artifacts to MLflow.
   - Yields a dictionary mapping each (table, target) to a list of features that
     have nonzero Lasso coefficients (i.e., the “best features” for that target).

5) **XGBoost Model Training with Bayesian Optimization**
   - Uses SparkXGBRegressor plus Hyperopt with SparkTrials for distributed
     hyperparameter search.
   - Employs both time-series and Monte Carlo splits to get robust metrics.
   - Logs results to MLflow, returning a map of final trained model metadata.

6) **Generating Predictions (Historical & Future)**
   - Loads the trained XGBoost models from MLflow and applies them to the dataset
     to produce predicted values.
   - Stores predictions in a new Iceberg table, ensuring each run is versioned
     without overwriting prior results.

7) **Single-Script, No Omissions**
   - All the code is contained in this file with no hidden partial scripts.
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

# --------- We removed heavy usage of Pandas for the entire dataset; only sampling now.
import pandas as pd
import pywt
import numpy.fft as npfft

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
    abs as spark_abs,
    udf,
    collect_list
)
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    ArrayType
)
from pyspark.sql.window import Window
from sklearn.linear_model import Lasso, LassoCV
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import ShuffleSplit, TimeSeriesSplit
from sklearn.preprocessing import StandardScaler as SklearnStandardScaler
from xgboost.spark import SparkXGBRegressor

import mlflow
import mlflow.sklearn
import mlflow.xgboost
from hyperopt import STATUS_OK, Trials, fmin, hp, tpe, SparkTrials

from pyspark.ml.feature import VectorAssembler


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class NBAPlayerBoxscorePipeline:
    """
    NBA Player Boxscore Pipeline class that orchestrates data loading,
    cleaning, feature engineering, Lasso feature selection, XGBoost
    model training with Bayesian optimization, and prediction generation.
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
                SparkSession.builder.appName("XGBoost_NBA_Predictions")
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

    def load_all_iceberg_tables(self, spark: SparkSession) -> DataFrame:
        """
        Load all relevant tables from the Iceberg 'nba_player_boxscores' schema
        and union them into a single Spark DataFrame.
        """
        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()
        try:
            table_names = ["scd_player_boxscores"]

            all_dfs = []
            for tbl_name in table_names:
                full_tbl_name = f"iceberg_nba_player_boxscores.{tbl_name}"
                self.logger.info("Reading table: %s", full_tbl_name)
                df_tbl = spark.table(full_tbl_name)
                df_tbl = df_tbl.withColumn("source_table", lit(tbl_name))
                all_dfs.append(df_tbl)

            if not all_dfs:
                self.logger.warning("No tables found in iceberg_nba_player_boxscores!")
                schema = StructType([StructField("dummy", StringType(), True)])
                return spark.createDataFrame([], schema)

            big_df = all_dfs[0]
            for dfx in all_dfs[1:]:
                big_df = big_df.unionByName(dfx, allowMissingColumns=True)

            # Break lineage by persisting and materializing the unioned DataFrame
            big_df = big_df.persist()
            _ = big_df.count()  # Force materialization

            total_count = big_df.count()
            self.logger.info(
                "Successfully loaded %d tables into one big DataFrame. Row count: %d",
                len(table_names),
                total_count,
            )
            return big_df
        except Exception as e:
            self.logger.error("Error loading tables: %s", str(e), exc_info=True)
            sys.exit(1)
        finally:
            end_cpu_time = time.process_time()
            end_cpu_percent = psutil.cpu_percent()
            self.logger.info(
                "CPU usage start: %s%%, end: %s%%. CPU time for load_all_iceberg_tables(): "
                "%.4f seconds",
                start_cpu_percent,
                end_cpu_percent,
                end_cpu_time - start_cpu_time,
            )

    def clean_and_partition_data(self, df: DataFrame) -> DataFrame:
        """
        Clean the data by removing duplicates, filtering out invalid values,
        and preparing partitions if necessary.
        """
        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()
        try:
            df_dedup = df.dropDuplicates()

            # Example cleaning
            if "player_id" in df_dedup.columns:
                df_dedup = df_dedup.filter(col("player_id").isNotNull())
            if "team_id" in df_dedup.columns:
                df_dedup = df_dedup.filter(col("team_id").isNotNull())

            if "points" in df_dedup.columns:
                df_dedup = df_dedup.filter(col("points") >= 0)

            self.logger.info("Data cleaning complete.")
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

    # ------------------------- NEW WAVELET+FFT UDF HELPER ---------------------------
    def _wavelet_fft_transform(self, values: list[float]) -> list[float]:
        """
        Given a list of numeric values (e.g., from a rolling window),
        compute a simple wavelet transform (db1, level=2) and a basic FFT
        to capture a few repeating/cyclical features.

        Returns an array of length 4, for example:
          [mean_of_detail_coeff_level2, mean_of_detail_coeff_level1,
           top_fft_magnitude_1,         top_fft_magnitude_2]
        so we can easily expand them into columns.
        """
        # If not enough data, return zeroes
        if len(values) < 2:
            return [0.0, 0.0, 0.0, 0.0]

        # Wavelet decomposition (level=2 with 'db1')
        try:
            coeffs = pywt.wavedec(values, 'db1', level=2)
            # Typically: coeffs = [cA2, cD2, cD1]
            cA2, cD2, cD1 = coeffs
            wave_d2_mean = float(np.mean(cD2)) if len(cD2) > 0 else 0.0
            wave_d1_mean = float(np.mean(cD1)) if len(cD1) > 0 else 0.0
        except:
            # Fallback
            wave_d2_mean = 0.0
            wave_d1_mean = 0.0

        # FFT (we'll just pick the top 2 magnitudes)
        try:
            fft_vals = npfft.rfft(values)
            fft_mags = np.abs(fft_vals)
            if len(fft_mags) < 2:
                return [wave_d2_mean, wave_d1_mean, 0.0, 0.0]
            # Get indices of top 2
            top2_idx = np.argsort(fft_mags)[-2:]
            fft_mag1 = float(fft_mags[top2_idx[-1]])
            fft_mag2 = float(fft_mags[top2_idx[-2]])
        except:
            fft_mag1 = 0.0
            fft_mag2 = 0.0

        return [wave_d2_mean, wave_d1_mean, fft_mag1, fft_mag2]
    # --------------------------------------------------------------------------------

    def spark_feature_engineering(self, df: DataFrame) -> DataFrame:
        """
        Perform advanced feature engineering using Spark DataFrame transformations.
        For demonstration: absolute value, squared, log, rolling mean, plus
        wavelet/FFT transforms for each numeric column (via a UDF).
        """
        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()
        try:
            # 1) Identify numeric columns
            numeric_cols = [
                f.name
                for f in df.schema.fields
                if isinstance(f.dataType, (DoubleType, IntegerType))
            ]

            # 2) Create columns: absolute, squared, log
            for c in numeric_cols:
                df = df.withColumn(f"{c}_abs", spark_abs(col(c).cast(DoubleType())))
                df = df.withColumn(f"{c}_squared", (col(c) * col(c)).cast(DoubleType()))
                df = df.withColumn(
                    f"{c}_log",
                    when(col(c) <= 0, 0).otherwise(col(c).cast(DoubleType()))
                )
                df = df.withColumn(f"{c}_log", col(f"{c}_log") + lit(1.0))
                df = df.withColumn(
                    f"{c}_log",
                    when(col(f"{c}_log") > 0, col(f"{c}_log")).otherwise(lit(1.0))
                )
                df = df.withColumn(f"{c}_log", log(col(f"{c}_log")))

            # 3) Rolling mean example with a window of size 3, partitioned by player_id
            # and ordered by a 'game_date' column if it exists.
            if "game_date" in df.columns:
                window_spec_3 = (
                    Window.partitionBy("player_id").orderBy(col("game_date").asc())
                    .rowsBetween(-2, 0)
                )
                for c in numeric_cols:
                    df = df.withColumn(
                        f"{c}_ma_3",
                        spark_avg(col(c)).over(window_spec_3),
                    )
            else:
                # If no date, just do a partition-based rolling or skip
                for c in numeric_cols:
                    df = df.withColumn(f"{c}_ma_3", col(c))  # trivial fallback

            # --------- 4) Fourier / Wavelet transforms (windowed)  ---------------
            # We'll use a 7-row rolling window. For each numeric column, we compute:
            #   wave_d2_mean, wave_d1_mean, fft_mag1, fft_mag2
            # using our _wavelet_fft_transform UDF.
            wave_fft_udf = udf(self._wavelet_fft_transform, ArrayType(DoubleType()))

            # We'll define a window of size 7 behind each row
            if "game_date" in df.columns:
                window_spec_7 = (
                    Window.partitionBy("player_id")
                    .orderBy(col("game_date").asc())
                    .rowsBetween(-6, 0)
                )
            else:
                window_spec_7 = (
                    Window.partitionBy("player_id")
                    .orderBy("player_id")  # fallback ordering
                    .rowsBetween(-6, 0)
                )

            for c in numeric_cols:
                # create an array of the last 7 values for that column
                df = df.withColumn(
                    f"{c}_wave_fft_arr",
                    wave_fft_udf(collect_list(col(c)).over(window_spec_7))
                )
                # expand them into separate columns
                df = df.withColumn(
                    f"{c}_wave_d2_mean", col(f"{c}_wave_fft_arr").getItem(0)
                )
                df = df.withColumn(
                    f"{c}_wave_d1_mean", col(f"{c}_wave_fft_arr").getItem(1)
                )
                df = df.withColumn(
                    f"{c}_fft_mag1", col(f"{c}_wave_fft_arr").getItem(2)
                )
                df = df.withColumn(
                    f"{c}_fft_mag2", col(f"{c}_wave_fft_arr").getItem(3)
                )
                # Remove the intermediate array column
                df = df.drop(f"{c}_wave_fft_arr")
            # ---------------------------------------------------------------------

            # 5) Clean up NaNs or infinities => cast to double and replace
            for c in df.columns:
                df = df.withColumn(c, when(col(c).isNull(), 0).otherwise(col(c)))

            self.logger.info("Spark-based feature engineering complete.")
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

    def run_lasso_for_each_table_and_column(
        self,
        spark: SparkSession,
        df: DataFrame,
        mlflow_experiment_name: str = "Lasso_Regression_NBA",
    ) -> dict[str, list[str]]:
        """
        For each table and each numeric target column, run LassoCV on a *sample* of data
        (instead of the entire dataset) to identify the best alpha. Then record which
        features have nonzero coefficients. Returns a dictionary keyed by "table_target"
        with a list of top features.
        """
        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()
        try:
            if "source_table" not in df.columns:
                df = df.withColumn("source_table", lit("unified_table"))

            # We'll collect only up to e.g. 100k rows per table to Pandas for Lasso
            SAMPLE_LIMIT = 100000

            # Gather numeric columns (exclude ID or other columns from being "targets")
            numeric_cols = [
                f.name
                for f in df.schema.fields
                if isinstance(f.dataType, (DoubleType, IntegerType))
            ]
            exclude_as_target = {"player_id", "team_id"}
            possible_numeric_targets = [c for c in numeric_cols if c not in exclude_as_target]

            results: dict[str, list[str]] = {}
            mlflow.set_experiment(mlflow_experiment_name)

            # Distinct table names
            table_names = [row[0] for row in df.select("source_table").distinct().collect()]

            for table_name in table_names:
                self.logger.info("Running Lasso for table: %s", table_name)
                subset_df = df.filter(col("source_table") == table_name)

                # If too few rows, skip
                count_subset = subset_df.count()
                if count_subset < 10:
                    self.logger.warning("Skipping table %s, not enough rows for Lasso.", table_name)
                    continue

                # Sample to local Pandas
                limit_df = subset_df.limit(SAMPLE_LIMIT)
                pdf = limit_df.toPandas().dropna()

                all_numeric_cols = pdf.select_dtypes(include=[np.number]).columns.tolist()
                for tgt_col in possible_numeric_targets:
                    if tgt_col not in all_numeric_cols:
                        continue

                    X_cols = [c for c in all_numeric_cols if c != tgt_col]
                    if len(X_cols) < 1 or len(pdf) < 10:
                        continue

                    X = pdf[X_cols].values
                    y = pdf[tgt_col].values

                    if len(np.unique(y)) < 2:
                        self.logger.warning(
                            "Skipping target %s for table %s (no variance).",
                            tgt_col, table_name
                        )
                        continue

                    scaler = SklearnStandardScaler()
                    X_scaled = scaler.fit_transform(X)

                    with mlflow.start_run(nested=True):
                        mlflow.log_param("table_name", table_name)
                        mlflow.log_param("target_column", tgt_col)
                        mlflow.log_param("data_rows_sampled", len(X))

                        alphas = np.logspace(-6, 1, 50)
                        lasso_cv = LassoCV(
                            alphas=alphas,
                            cv=5,
                            random_state=42,
                            max_iter=500000,
                            tol=1e-7,
                        )
                        lasso_cv.fit(X_scaled, y)
                        best_alpha = lasso_cv.alpha_
                        mlflow.log_param("best_alpha", best_alpha)

                        lasso = Lasso(alpha=best_alpha, max_iter=500000, tol=1e-7)
                        lasso.fit(X_scaled, y)

                        coefs = lasso.coef_
                        coef_df = pd.DataFrame({"feature": X_cols, "coefficient": coefs})
                        coef_df["abs_coef"] = coef_df["coefficient"].abs()
                        coef_df = coef_df[coef_df["abs_coef"] > 1e-9]
                        coef_df.sort_values("abs_coef", ascending=False, inplace=True)
                        selected_features = coef_df["feature"].tolist()

                        key = f"{table_name}_{tgt_col}"
                        results[key] = selected_features

                        preds = lasso.predict(X_scaled)
                        mse_val = mean_squared_error(y, preds)
                        mae_val = mean_absolute_error(y, preds)
                        r2_val = r2_score(y, preds)

                        mlflow.log_metric("MSE", mse_val)
                        mlflow.log_metric("MAE", mae_val)
                        mlflow.log_metric("R2", r2_val)

                        mlflow.sklearn.log_model(lasso, f"lasso_model_{table_name}_{tgt_col}")

                        self.logger.info(
                            "[LassoCV] table=%s, target=%s, alpha=%f, selected_features=%d",
                            table_name, tgt_col, best_alpha, len(selected_features)
                        )

            self.logger.info("All Lasso regressions completed.")
            return results
        finally:
            end_cpu_time = time.process_time()
            end_cpu_percent = psutil.cpu_percent()
            self.logger.info(
                "CPU usage start: %s%%, end: %s%%. CPU time for "
                "run_lasso_for_each_table_and_column(): %.4f seconds",
                start_cpu_percent,
                end_cpu_percent,
                end_cpu_time - start_cpu_time,
            )

    def xgboost_training_with_hyperopt(
        self,
        spark: SparkSession,
        df: DataFrame,
        best_features_dict: dict[str, list[str]],
        mlflow_experiment_name: str = "XGBoost_NBA_Bayesian_Optimization",
        max_evals: int = 20,
    ) -> dict[str, dict]:
        """
        Train XGBoost models using Hyperopt with SparkTrials for each table
        and target column. Returns a map of model metadata keyed by table+target.
        """
        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()
        try:
            if "source_table" not in df.columns:
                df = df.withColumn("source_table", lit("unified_table"))

            # Identify numeric columns
            numeric_cols = [
                f.name
                for f in df.schema.fields
                if isinstance(f.dataType, (DoubleType, IntegerType))
            ]
            exclude_as_target = {"player_id", "team_id", "source_table"}
            possible_targets = [c for c in numeric_cols if c not in exclude_as_target]

            table_names = [row[0] for row in df.select("source_table").distinct().collect()]

            mlflow.set_experiment(mlflow_experiment_name)
            trained_models_map: dict[str, dict] = {}

            for table_name in table_names:
                subset_df = df.filter(col("source_table") == table_name)
                rowcount = subset_df.count()
                if rowcount < 50:
                    self.logger.info(
                        "Skipping table %s (only %d rows) for XGBoost.", table_name, rowcount
                    )
                    continue

                pdf_limited = subset_df.limit(200000).toPandas().dropna()
                if len(pdf_limited) < 50:
                    continue

                for tgt_col in possible_targets:
                    key = f"{table_name}_{tgt_col}"
                    if key not in best_features_dict:
                        continue

                    use_features = [f for f in best_features_dict[key] if f in pdf_limited.columns]
                    if len(use_features) < 1:
                        continue

                    X_full = pdf_limited[use_features].values
                    y_full = pdf_limited[tgt_col].values
                    if len(X_full) < 30:
                        continue

                    scaler = SklearnStandardScaler()
                    X_scaled = scaler.fit_transform(X_full)

                    from sklearn.metrics import mean_squared_error
                    tscv = TimeSeriesSplit(n_splits=3)
                    mc_splits = ShuffleSplit(n_splits=3, test_size=0.2, random_state=42)

                    def combined_cv_score(params: dict[str, float]) -> float:
                        import numpy as np
                        rmse_scores = []
                        assembler = VectorAssembler(
                            inputCols=[
                                "col_%d" % i for i in range(len(use_features))
                            ],
                            outputCol="features",
                        )

                        # We do TSCV folds
                        for train_idx, test_idx in tscv.split(X_scaled):
                            X_train, X_test = X_scaled[train_idx], X_scaled[test_idx]
                            y_train, y_test = y_full[train_idx], y_full[test_idx]

                            train_pdf = pd.DataFrame(
                                X_train,
                                columns=["col_%d" % i for i in range(len(use_features))],
                            )
                            train_pdf[tgt_col] = y_train

                            test_pdf = pd.DataFrame(
                                X_test,
                                columns=["col_%d" % i for i in range(len(use_features))],
                            )
                            test_pdf[tgt_col] = y_test

                            spark_train = spark.createDataFrame(train_pdf)
                            spark_test = spark.createDataFrame(test_pdf)

                            train_as = assembler.transform(spark_train)
                            test_as = assembler.transform(spark_test)

                            xgbRegressor = (
                                SparkXGBRegressor()
                                .setLabelCol(tgt_col)
                                .setFeaturesCol("features")
                                .setObjective("reg:squarederror")
                                .setEvalMetric("rmse")
                                .setNumWorkers(2)
                                .setMaxDepth(int(params["max_depth"]))
                                .setEta(float(params["learning_rate"]))
                                .setSubsample(float(params["subsample"]))
                                .setColsampleBytree(float(params["colsample_bytree"]))
                                .setRegAlpha(float(params["reg_alpha"]))
                                .setRegLambda(float(params["reg_lambda"]))
                                .setMinChildWeight(float(params["min_child_weight"]))
                                .setGamma(float(params["gamma"]))
                                .setNumRound(int(params["num_boost_round"]))
                                .setCheckpointPath(f"/tmp/chkpts_{table_name}_{tgt_col}")
                                .setCheckpointInterval(5)  # checkpoint every 5 rounds
                            )
                            xgbRegressor.setEvalDataset(test_as)
                            xgbRegressor.setNumEarlyStoppingRounds(10)

                            model = xgbRegressor.fit(train_as)

                            preds_df = model.transform(test_as).select("prediction", tgt_col).collect()
                            preds = [row["prediction"] for row in preds_df]
                            y_true = [row[tgt_col] for row in preds_df]

                            rmse = float(np.sqrt(mean_squared_error(y_true, preds)))
                            rmse_scores.append(rmse)

                        # MC random splits
                        for train_idx, test_idx in mc_splits.split(X_scaled):
                            X_train, X_test = X_scaled[train_idx], X_scaled[test_idx]
                            y_train, y_test = y_full[train_idx], y_full[test_idx]

                            train_pdf = pd.DataFrame(
                                X_train,
                                columns=["col_%d" % i for i in range(len(use_features))],
                            )
                            train_pdf[tgt_col] = y_train

                            test_pdf = pd.DataFrame(
                                X_test,
                                columns=["col_%d" % i for i in range(len(use_features))],
                            )
                            test_pdf[tgt_col] = y_test

                            spark_train = spark.createDataFrame(train_pdf)
                            spark_test = spark.createDataFrame(test_pdf)

                            train_as = assembler.transform(spark_train)
                            test_as = assembler.transform(spark_test)

                            xgbRegressor = (
                                SparkXGBRegressor()
                                .setLabelCol(tgt_col)
                                .setFeaturesCol("features")
                                .setObjective("reg:squarederror")
                                .setEvalMetric("rmse")
                                .setNumWorkers(2)
                                .setMaxDepth(int(params["max_depth"]))
                                .setEta(float(params["learning_rate"]))
                                .setSubsample(float(params["subsample"]))
                                .setColsampleBytree(float(params["colsample_bytree"]))
                                .setRegAlpha(float(params["reg_alpha"]))
                                .setRegLambda(float(params["reg_lambda"]))
                                .setMinChildWeight(float(params["min_child_weight"]))
                                .setGamma(float(params["gamma"]))
                                .setNumRound(int(params["num_boost_round"]))
                                .setCheckpointPath(f"/tmp/chkpts_{table_name}_{tgt_col}")
                                .setCheckpointInterval(5)  # checkpoint every 5 rounds
                            )
                            xgbRegressor.setEvalDataset(test_as)
                            xgbRegressor.setNumEarlyStoppingRounds(10)

                            model = xgbRegressor.fit(train_as)

                            preds_df = model.transform(test_as).select("prediction", tgt_col).collect()
                            preds = [row["prediction"] for row in preds_df]
                            y_true = [row[tgt_col] for row in preds_df]

                            rmse = float(np.sqrt(mean_squared_error(y_true, preds)))
                            rmse_scores.append(rmse)

                        return float(np.mean(rmse_scores))

                    def objective(space: dict[str, float]) -> dict:
                        param_dict = {
                            "learning_rate": space["learning_rate"],
                            "max_depth": int(space["max_depth"]),
                            "subsample": space["subsample"],
                            "colsample_bytree": space["colsample_bytree"],
                            "reg_alpha": space["reg_alpha"],
                            "reg_lambda": space["reg_lambda"],
                            "min_child_weight": space["min_child_weight"],
                            "gamma": space["gamma"],
                            "num_boost_round": int(space["num_boost_round"]),
                        }
                        rmse_val = combined_cv_score(param_dict)
                        return {"loss": rmse_val, "status": STATUS_OK}

                    search_space = {
                        "learning_rate": hp.loguniform("learning_rate", np.log(0.001), np.log(0.3)),
                        "max_depth": hp.quniform("max_depth", 2, 10, 1),
                        "subsample": hp.uniform("subsample", 0.5, 1.0),
                        "colsample_bytree": hp.uniform("colsample_bytree", 0.5, 1.0),
                        "reg_alpha": hp.loguniform("reg_alpha", np.log(1e-6), np.log(10)),
                        "reg_lambda": hp.loguniform("reg_lambda", np.log(1e-6), np.log(10)),
                        "min_child_weight": hp.quniform("min_child_weight", 1, 10, 1),
                        "gamma": hp.uniform("gamma", 0.0, 5.0),
                        "num_boost_round": hp.quniform("num_boost_round", 50, 400, 10),
                    }

                    spark_trials = SparkTrials(parallelism=2)

                    with mlflow.start_run(nested=True):
                        mlflow.log_param("table_name", table_name)
                        mlflow.log_param("target_column", tgt_col)
                        mlflow.log_param("num_rows_sampled", len(pdf_limited))
                        mlflow.log_param("num_features", len(use_features))

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

                        from pyspark.ml.feature import VectorAssembler
                        from sklearn.preprocessing import StandardScaler
                        final_pdf = pd.DataFrame(
                            X_scaled,
                            columns=["col_%d" % i for i in range(len(use_features))],
                        )
                        final_pdf[tgt_col] = y_full

                        spark_final = spark.createDataFrame(final_pdf)
                        assembler = VectorAssembler(
                            inputCols=["col_%d" % i for i in range(len(use_features))],
                            outputCol="features",
                        )
                        final_as = assembler.transform(spark_final)

                        final_regressor = (
                            SparkXGBRegressor()
                            .setLabelCol(tgt_col)
                            .setFeaturesCol("features")
                            .setObjective("reg:squarederror")
                            .setEvalMetric("rmse")
                            .setNumWorkers(2)
                            .setMaxDepth(best_params["max_depth"])
                            .setEta(best_params["learning_rate"])
                            .setSubsample(best_params["subsample"])
                            .setColsampleBytree(best_params["colsample_bytree"])
                            .setRegAlpha(best_params["reg_alpha"])
                            .setRegLambda(best_params["reg_lambda"])
                            .setMinChildWeight(best_params["min_child_weight"])
                            .setGamma(best_params["gamma"])
                            .setNumRound(best_params["num_boost_round"])
                            .setCheckpointPath(f"/tmp/chkpts_{table_name}_{tgt_col}")
                            .setCheckpointInterval(5)  # checkpoint every 5 rounds
                        )
                        final_regressor.setEvalDataset(final_as)
                        final_regressor.setNumEarlyStoppingRounds(10)

                        final_model = final_regressor.fit(final_as)

                        full_preds_df = final_model.transform(final_as).select("prediction", tgt_col).collect()
                        final_preds = [row["prediction"] for row in full_preds_df]
                        final_y = [row[tgt_col] for row in full_preds_df]

                        final_rmse_val = float(np.sqrt(mean_squared_error(final_y, final_preds)))
                        final_mae_val = float(mean_absolute_error(final_y, final_preds))
                        final_r2_val = float(r2_score(final_y, final_preds))

                        mlflow.log_metric("final_RMSE", final_rmse_val)
                        mlflow.log_metric("final_MAE", final_mae_val)
                        mlflow.log_metric("final_R2", final_r2_val)

                        for hp_name, hp_val in best_params.items():
                            mlflow.log_param(f"best_{hp_name}", hp_val)

                        model_artifact_path = f"xgboost_model_{table_name}_{tgt_col}"
                        mlflow.xgboost.log_model(final_model.nativeBooster, model_artifact_path)

                        run_id = mlflow.active_run().info.run_id
                        model_uri = f"runs:/{run_id}/{model_artifact_path}"

                        trained_models_map[key] = {
                            "table_name": table_name,
                            "target_col": tgt_col,
                            "features": use_features,
                            "run_id": run_id,
                            "model_uri": model_uri,
                        }

                        self.logger.info(
                            "[XGBoost] Completed hyperopt for table=%s, target=%s. "
                            "Best params=%s, RMSE=%f",
                            table_name,
                            tgt_col,
                            best_params,
                            final_rmse_val,
                        )

            return trained_models_map
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
        best_features_dict: dict[str, list[str]],
        trained_models_map: dict[str, dict],
        future_or_historical: str = "historical",
        db_name: str = "iceberg_nba_player_boxscores_xgboost",
    ) -> None:
        """
        Generate predictions using trained XGBoost models loaded from MLflow,
        then write the predictions to an Iceberg table in the specified DB.
        """
        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()
        try:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

            for key, model_info in trained_models_map.items():
                table_name = model_info["table_name"]
                target_col = model_info["target_col"]
                run_id = model_info["run_id"]
                model_uri = model_info["model_uri"]
                features = model_info["features"]

                subset_df = df.filter(col("source_table") == table_name)

                if subset_df.count() == 0:
                    continue

                for f in features:
                    if f not in subset_df.columns:
                        subset_df = subset_df.withColumn(f, lit(0.0))

                loaded_xgb_model = mlflow.xgboost.load_model(model_uri)

                assembler = VectorAssembler(
                    inputCols=features,
                    outputCol="features",
                )
                assembled_df = assembler.transform(subset_df)

                pred_sdf = loaded_xgb_model.transform(assembled_df)
                pred_sdf = pred_sdf.withColumn(
                    f"prediction_{target_col}", col("prediction")
                ).drop("prediction")

                pred_sdf = pred_sdf.withColumn("prediction_type", lit(future_or_historical))

                out_table = f"spark_catalog.{db_name}.xgboost_player_predictions_{run_id}"
                pred_sdf.write.format("iceberg").mode("append").save(out_table)

                self.logger.info(
                    "Wrote predictions for table=%s, target=%s into %s.",
                    table_name,
                    target_col,
                    out_table,
                )
        except Exception as e:
            self.logger.error(
                "Error generating/writing predictions: %s", str(e), exc_info=True
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

    def stratified_reservoir_sample_data(
        self,
        df: DataFrame,
        strata_cols: list[str],
        reservoir_size: int = 100,
        seed: int = 42,
    ) -> DataFrame:
        """
        Perform stratified reservoir sampling on the given DataFrame by the
        specified columns, returning up to 'reservoir_size' rows per group.
        """
        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()
        try:
            def reservoir_sampling(pdf: pd.DataFrame, k: int, random_seed: int) -> pd.DataFrame:
                if len(pdf) <= k:
                    return pdf
                return pdf.sample(n=k, random_state=random_seed)

            schema = df.schema

            def sample_group(pdf: pd.DataFrame) -> pd.DataFrame:
                return reservoir_sampling(pdf, reservoir_size, seed)

            sampled_df = df.groupBy(strata_cols).applyInPandas(sample_group, schema=schema)
            return sampled_df
        except Exception as e:
            self.logger.error("Error in stratified sampling: %s", str(e), exc_info=True)
            sys.exit(1)
        finally:
            end_cpu_time = time.process_time()
            end_cpu_percent = psutil.cpu_percent()
            self.logger.info(
                "CPU usage start: %s%%, end: %s%%. CPU time for "
                "stratified_reservoir_sample_data(): %.4f seconds",
                start_cpu_percent,
                end_cpu_percent,
                end_cpu_time - start_cpu_time,
            )

    def main(self) -> None:
        """
        Main entry point for the NBA Player Boxscore Pipeline.
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

            # 1) Load all tables
            big_df = self.load_all_iceberg_tables(spark)

            # 2) Clean & partition
            cleaned_df = self.clean_and_partition_data(big_df)

            # 3) Stratified reservoir sampling demonstration
            strata_columns = ["team_id", "source_table"]
            sampled_df = self.stratified_reservoir_sample_data(
                df=cleaned_df,
                strata_cols=strata_columns,
                reservoir_size=100,
                seed=42,
            )
            self.logger.info(
                "Stratified reservoir sample completed. Final count = %d",
                sampled_df.count(),
            )

            # 4) Feature engineering (now done via Spark, not Pandas)
            fe_df = self.spark_feature_engineering(sampled_df)

            # 4b) Cache/Persist the engineered DataFrame so we don't re-read
            fe_df = fe_df.persist()

            # 5) Lasso => best features (use sample-based approach)
            lasso_best_features = self.run_lasso_for_each_table_and_column(
                spark,
                fe_df,
                mlflow_experiment_name="Lasso_Regression_NBA",
            )

            # 6) XGBoost => Bayesian optimization + cross-validation
            trained_models_map = self.xgboost_training_with_hyperopt(
                spark,
                fe_df,
                best_features_dict=lasso_best_features,
                mlflow_experiment_name="XGBoost_NBA_Bayesian_Optimization",
                max_evals=15,
            )

            # 7) Generate predictions (only historical, existing data)
            self.generate_predictions_and_write_to_iceberg(
                spark,
                fe_df,
                lasso_best_features,
                trained_models_map,
                future_or_historical="historical",
                db_name="iceberg_nba_player_boxscores_xgboost",
            )

            spark.stop()
            self.profiler.disable()

            s_final = io.StringIO()
            p_final = pstats.Stats(self.profiler, stream=s_final).sort_stats("cumulative")
            p_final.print_stats()
            self.logger.info("[Final CPU profiling stats]:\n%s", s_final.getvalue())

            self.logger.info("Done executing main pipeline for XGBoost NBA predictions.")

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
    pipeline = NBAPlayerBoxscorePipeline()
    pipeline.main()
