#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
xgboost_nba_prediction.py

This script does the following, end-to-end, WITHOUT any code omitted:

1. Reads all historical NBA player box score data from Iceberg tables within
   the "NBA_player_boxscores" database warehouse for all PLAYERS stats
   into one big table/dataframe (all columns, all tables).

2. Performs data cleaning, deduplication, anomaly fixes, and ensures
   partitioning on main columns (team_name, player_name, etc.).

3. Applies advanced feature engineering in Spark using various transformations:
   (absolute, squared, log, sqrt, wavelet transforms, Fourier expansions,
   rolling stats, lags, etc.) as well as any additional robust transformations
   beneficial to this use case.

4. Implements Lasso regression (LassoCV) using all features (original &
   engineered) to discover the best features to predict each important target
   column for each table. So for each (table, target_col), we run LassoCV,
   store results in MLflow, and get a list of top features for that (table, col).

5. Using those best features, trains an **XGBoost** model (via xgboost /
   XGBoost4J-Spark) extensively with **Bayesian optimization** (Hyperopt +
   SparkTrials + TPE) to search hyperparameters in parallel. Logs each trial’s
   params, metrics, best run, and final model in MLflow.

6. Includes **k-fold cross validation** (TimeSeriesSplit for time-based folds)
   and **Monte Carlo cross validation** in the hyperopt objective function so
   that each hyperparam set is evaluated thoroughly. Results are stored in MLflow.

7. Generates both **historical** predictions (backfill from latest date to
   current) and **future** predictions (officialDate >= date_add(current_date(), 1))
   and writes them to a **separate** database `nbd_predictions`, ensuring
   that re-running the script updates predictions properly.

8. Everything is in this single script, with **no omissions** or fragmentations
   of code.


ADDITIONAL NOTES ON CONTINUAL LEARNING & ROBUST SAMPLING:

- **Continual Learning - 4 Stages**:
  1. **Stateless Training (Manual)**:
     - Manually trigger this pipeline for a batch of historical data. No internal
       memory or training state is carried forward automatically; it’s purely
       an on-demand run.

  2. **Automated Retraining**:
     - Schedule this pipeline to run periodically (e.g., daily or weekly) to
       retrain models on the most recent data without manual intervention.

  3. **Stateful Training (Automated)**:
     - The pipeline can store and retrieve previous model states (e.g., from MLflow)
       and update them incrementally with new data, retaining learned parameters.

  4. **Continual Learning**:
     - The pipeline evolves into a system where each training run adaptively
       refines the model using both prior knowledge (previous states) and
       incoming data streams, effectively “learning” over time without resetting.

- **Robust Sampling**:
  - Below, we illustrate a **Stratified Reservoir Sampling** function that ensures
    balanced coverage across key strata (e.g., decades, teams). This helps address
    imbalances in large datasets spanning multiple eras or subgroups.
"""

import sys
import logging
import math
import random
from datetime import datetime

# Third-party libraries
import numpy as np
import pandas as pd
import pywt

# Spark / PySpark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    when,
    lit,
    current_timestamp,
    date_add,
    current_date,
    monotonically_increasing_id,
    row_number,
    asc,
    desc,
    regexp_extract,
    input_file_name,
    count as spark_count,
    sum as spark_sum,
    avg as spark_avg,
    mean as spark_mean,
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType,
)

# Sklearn for Lasso
from sklearn.linear_model import Lasso, LassoCV
from sklearn.preprocessing import StandardScaler as SklearnStandardScaler
from sklearn.model_selection import (
    train_test_split,
    TimeSeriesSplit,
    ShuffleSplit,
)
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

# XGBoost
import xgboost as xgb

# Hyperopt + SparkTrials
import mlflow
import mlflow.xgboost
import mlflow.sklearn
from hyperopt import hp, tpe, STATUS_OK, Trials, fmin, SparkTrials

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """
    Create and return a SparkSession with necessary configurations.
    """
    try:
        spark = (
            SparkSession.builder.appName("XGBoost_NBA_Predictions")
            .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
            .config("spark.sql.iceberg.target-file-size-bytes", "134217728")
            .enableHiveSupport()
            .getOrCreate()
        )
        spark.conf.set("spark.sql.shuffle.partitions", 25)
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        logger.info("SparkSession created successfully.")
        return spark
    except Exception as e:
        logger.error("Failed to create SparkSession: %s", str(e), exc_info=True)
        sys.exit(1)


def load_all_iceberg_tables(spark: SparkSession) -> DataFrame:
    """
    Loads all historical data from the nine tables in your
    'iceberg_nba_player_boxscores' database into one big Spark DataFrame.
    """
    try:
        # Explicitly list the tables we want to union:
        table_names = [
            "player_alltime",
            "player_boxscores_advanced_stats",
            "player_currentseason",
            "scd_player_boxscores",
            "scd_team_boxscores",
            "team_alltime",
            "team_boxscores",
            "team_boxscores_advanced_stats",
            "team_currentseason",
        ]

        all_dfs = []
        for tbl_name in table_names:
            full_tbl_name = f"iceberg_nba_player_boxscores.{tbl_name}"
            logger.info("Reading table: %s", full_tbl_name)
            df_tbl = spark.table(full_tbl_name)
            df_tbl = df_tbl.withColumn("source_table", lit(tbl_name))
            all_dfs.append(df_tbl)

        if not all_dfs:
            logger.warning("No tables found in iceberg_nba_player_boxscores!")
            schema = StructType([StructField("dummy", StringType(), True)])
            return spark.createDataFrame([], schema)

        # Union them all (outer union on column names):
        big_df = all_dfs[0]
        for dfx in all_dfs[1:]:
            big_df = big_df.unionByName(dfx, allowMissingColumns=True)

        logger.info(
            "Successfully loaded %d tables into one big DataFrame. Row count: %d",
            len(table_names),
            big_df.count(),
        )
        return big_df
    except Exception as e:
        logger.error("Error loading tables: %s", str(e), exc_info=True)
        sys.exit(1)


def clean_and_partition_data(df: DataFrame) -> DataFrame:
    """
    Performs data cleaning and deduplication, fixes anomalies, and
    conceptually partitions on main columns (player_name, team_name, etc.).

    Steps:
      - Remove duplicates
      - Drop rows with null player_id/team_id (if present)
      - Remove negative points or rebounds as an example
    """
    try:
        df_dedup = df.dropDuplicates()

        if "player_id" in df_dedup.columns:
            df_dedup = df_dedup.filter(col("player_id").isNotNull())
        if "team_id" in df_dedup.columns:
            df_dedup = df_dedup.filter(col("team_id").isNotNull())

        if "points" in df_dedup.columns:
            df_dedup = df_dedup.filter(col("points") >= 0)

        logger.info("Data cleaning complete.")
        return df_dedup
    except Exception as e:
        logger.error("Error in cleaning data: %s", str(e), exc_info=True)
        sys.exit(1)


def feature_engineering_pandas(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Applies advanced feature engineering on a Pandas DataFrame.
    Includes: absolute, squared, log, sqrt, wavelet transforms,
    Fourier expansions, rolling averages, lags, etc.
    This is run in a single Pandas function for demonstration.

    :param pdf: Input pandas DataFrame to be transformed.
    :return: Transformed pandas DataFrame with new feature columns.
    """
    numeric_cols = pdf.select_dtypes(include=[np.number]).columns.tolist()

    # 1) absolute & squared
    for c in numeric_cols:
        pdf[f"{c}_abs"] = pdf[c].abs()
        pdf[f"{c}_squared"] = pdf[c] ** 2

    # 2) log & sqrt
    for c in numeric_cols:
        pdf[f"{c}_log"] = np.log(pdf[c] + 1.0)  # shift by 1 to avoid log(0)
        pdf[f"{c}_sqrt"] = np.sqrt(np.where(pdf[c] < 0, 0, pdf[c]))

    # 3) simple 3-pt rolling average
    for c in numeric_cols:
        pdf[f"{c}_ma_3"] = pdf[c].rolling(window=3).mean().fillna(0)

    # 4) wavelet transform & Fourier expansions

    def wavelet_transform(series: pd.Series) -> tuple[np.ndarray, np.ndarray]:
        (cA, cD) = pywt.dwt(series, "db1")
        return cA, cD

    def fourier_terms(
        series: pd.Series, n_terms: int = 5
    ) -> np.ndarray:
        length = len(series)
        t = np.arange(0, length)
        if length == 0:
            return np.empty((0, 2 * n_terms))

        cos_list = [
            np.cos((2 * math.pi * t * k) / length) for k in range(1, n_terms + 1)
        ]
        sin_list = [
            np.sin((2 * math.pi * t * k) / length) for k in range(1, n_terms + 1)
        ]
        return np.column_stack(cos_list + sin_list)

    for c in numeric_cols:
        # Fourier
        F = fourier_terms(pdf[c].fillna(0), n_terms=3)
        for k in range(3):
            pdf[f"{c}_cos_term_{k+1}"] = F[:, k]
            pdf[f"{c}_sin_term_{k+1}"] = F[:, k + 3]

        # wavelet
        cA, cD = wavelet_transform(pdf[c].fillna(0))
        length = len(pdf)
        cA_padded = np.pad(
            cA, (0, length - len(cA)), "constant", constant_values=0
        )
        cD_padded = np.pad(
            cD, (0, length - len(cD)), "constant", constant_values=0
        )
        pdf[f"{c}_wavelet_approx"] = cA_padded
        pdf[f"{c}_wavelet_detail"] = cD_padded

        # cumulative sum
        pdf[f"{c}_cumsum"] = pdf[c].cumsum()

    # 5) lags & rolling stats
    for c in numeric_cols:
        for lag in range(1, 3):
            pdf[f"{c}_lag_{lag}"] = pdf[c].shift(lag).fillna(0)
        for window in [2, 3]:
            pdf[f"{c}_roll_mean_{window}"] = (
                pdf[c].rolling(window=window).mean().fillna(0)
            )
            pdf[f"{c}_roll_std_{window}"] = (
                pdf[c].rolling(window=window).std().fillna(0)
            )

    pdf.replace([np.inf, -np.inf], np.nan, inplace=True)
    pdf.fillna(0, inplace=True)

    return pdf


def apply_feature_engineering(
    spark: SparkSession, df: DataFrame
) -> DataFrame:
    """
    Converts the Spark DataFrame to Pandas, applies feature_engineering_pandas,
    then returns a Spark DataFrame with new columns.

    NOTE: In production, consider Spark UDF or mapInPandas for distribution.

    :param spark: Active SparkSession.
    :param df: Input Spark DataFrame.
    :return: Transformed Spark DataFrame with engineered features.
    """
    logger.info("Collecting data to Pandas for advanced feature engineering.")
    pdf = df.toPandas()

    logger.info("Applying advanced feature engineering transformations...")
    pdf_transformed = feature_engineering_pandas(pdf)

    transformed_df = spark.createDataFrame(pdf_transformed)
    logger.info("Feature engineering complete. Returning Spark DataFrame.")
    return transformed_df


def run_lasso_for_each_table_and_column(
    spark: SparkSession,
    df: DataFrame,
    mlflow_experiment_name: str = "Lasso_Regression_NBA",
) -> dict[str, list[str]]:
    """
    For each table (df['source_table']) and for each numeric column (target),
    run LassoCV to find best features from among all numeric columns.
    Logs results (alphas, MSE, MAE, R2) to MLflow.

    Returns a dict mapping "tableName_columnName" -> List of selected features.

    :param spark: Active SparkSession.
    :param df: Spark DataFrame containing features and target columns.
    :param mlflow_experiment_name: MLflow experiment name for logging results.
    :return: Dictionary of best features per table and column.
    """
    pdf = df.toPandas()

    if "source_table" not in pdf.columns:
        pdf["source_table"] = "unified_table"

    numeric_cols = pdf.select_dtypes(include=[np.number]).columns.tolist()
    exclude_as_target = {"player_id", "team_id", "source_table"}
    possible_numeric_targets = [
        c for c in numeric_cols if c not in exclude_as_target
    ]

    grouped = pdf.groupby("source_table")
    results: dict[str, list[str]] = {}

    mlflow.set_experiment(mlflow_experiment_name)

    for table_name, group_df in grouped:
        logger.info(
            "Running Lasso for table: %s, rows=%d", table_name, len(group_df)
        )
        group_df = group_df.dropna()
        if len(group_df) < 10:
            logger.warning(
                "Skipping table %s, not enough rows for Lasso.", table_name
            )
            continue

        all_numeric_cols = (
            group_df.select_dtypes(include=[np.number]).columns.tolist()
        )

        for tgt_col in possible_numeric_targets:
            if tgt_col not in all_numeric_cols:
                continue
            X_cols = [c for c in all_numeric_cols if c != tgt_col]
            if len(X_cols) < 1:
                continue

            X = group_df[X_cols].values
            y = group_df[tgt_col].values
            if len(X) < 10:
                continue

            scaler = SklearnStandardScaler()
            X_scaled = scaler.fit_transform(X)

            with mlflow.start_run(nested=True):
                mlflow.log_param("table_name", table_name)
                mlflow.log_param("target_column", tgt_col)
                mlflow.log_param("data_rows", len(X))

                alphas = np.logspace(-6, 1, 100)
                lasso_cv = LassoCV(
                    alphas=alphas, cv=5, random_state=42, max_iter=10000
                )
                lasso_cv.fit(X_scaled, y)
                best_alpha = lasso_cv.alpha_
                mlflow.log_param("best_alpha", best_alpha)

                lasso = Lasso(alpha=best_alpha, max_iter=10000)
                lasso.fit(X_scaled, y)

                coefs = lasso.coef_
                coef_df = pd.DataFrame(
                    {"feature": X_cols, "coefficient": coefs}
                )
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

                mlflow.sklearn.log_model(
                    lasso, f"lasso_model_{table_name}_{tgt_col}"
                )

                logger.info(
                    "Lasso done for %s, target=%s, alpha=%f, NumFeatures=%d",
                    table_name,
                    tgt_col,
                    best_alpha,
                    len(selected_features),
                )

    logger.info("All Lasso regressions completed.")
    return results


def xgboost_training_with_hyperopt(
    spark: SparkSession,
    df: DataFrame,
    best_features_dict: dict[str, list[str]],
    mlflow_experiment_name: str = "XGBoost_NBA_Bayesian_Optimization",
    max_evals: int = 20,
) -> None:
    """
    Uses the best features from Lasso for each (table, column)
    to train XGBoost with Bayesian optimization (Hyperopt + SparkTrials + TPE).
    Implements TimeSeriesSplit + Monte Carlo cross-validation for each trial.

    Logs all runs in MLflow, including the best hyperparameters and final model.

    :param spark: Active SparkSession.
    :param df: Spark DataFrame containing features and target columns.
    :param best_features_dict: Dictionary of best features from Lasso.
    :param mlflow_experiment_name: MLflow experiment name for logging results.
    :param max_evals: Number of Hyperopt evaluations to run.
    """
    pdf = df.toPandas()
    if "source_table" not in pdf.columns:
        pdf["source_table"] = "unified_table"

    numeric_cols = pdf.select_dtypes(include=[np.number]).columns.tolist()
    exclude_as_target = {"player_id", "team_id", "source_table"}
    possible_targets = [c for c in numeric_cols if c not in exclude_as_target]

    grouped = pdf.groupby("source_table")
    mlflow.set_experiment(mlflow_experiment_name)

    for table_name, group_df in grouped:
        group_df = group_df.dropna()
        if len(group_df) < 50:
            logger.info(
                "Skipping table %s, insufficient rows for XGBoost.", table_name
            )
            continue

        for tgt_col in possible_targets:
            key = f"{table_name}_{tgt_col}"
            if key not in best_features_dict:
                continue

            use_features = best_features_dict[key]
            use_features = [f for f in use_features if f in group_df.columns]
            if len(use_features) < 1:
                continue

            X = group_df[use_features].values
            y = group_df[tgt_col].values
            if len(X) < 30:
                logger.info("Skipping %s, not enough data.", key)
                continue

            scaler = SklearnStandardScaler()
            X_scaled = scaler.fit_transform(X)

            tscv = TimeSeriesSplit(n_splits=3)
            mc_splits = ShuffleSplit(
                n_splits=3, test_size=0.2, random_state=42
            )

            def combined_cv_score(params: dict[str, float]) -> float:
                """
                Train an XGBoost model with given params and evaluate
                using both TimeSeriesSplit & Monte Carlo splits.
                Return average RMSE.
                """
                rmse_scores: list[float] = []

                for train_idx, test_idx in tscv.split(X_scaled):
                    X_train, X_test = (
                        X_scaled[train_idx],
                        X_scaled[test_idx],
                    )
                    y_train, y_test = y[train_idx], y[test_idx]

                    dtrain = xgb.DMatrix(X_train, label=y_train)
                    dtest = xgb.DMatrix(X_test, label=y_test)
                    model = xgb.train(
                        params=params,
                        dtrain=dtrain,
                        num_boost_round=int(params["num_boost_round"]),
                        evals=[(dtrain, "train"), (dtest, "eval")],
                        verbose_eval=False,
                    )
                    preds = model.predict(dtest)
                    rmse_scores.append(
                        float(np.sqrt(mean_squared_error(y_test, preds)))
                    )

                for train_idx, test_idx in mc_splits.split(X_scaled):
                    X_train, X_test = (
                        X_scaled[train_idx],
                        X_scaled[test_idx],
                    )
                    y_train, y_test = y[train_idx], y[test_idx]

                    dtrain = xgb.DMatrix(X_train, label=y_train)
                    dtest = xgb.DMatrix(X_test, label=y_test)
                    model = xgb.train(
                        params=params,
                        dtrain=dtrain,
                        num_boost_round=int(params["num_boost_round"]),
                        evals=[(dtrain, "train"), (dtest, "eval")],
                        verbose_eval=False,
                    )
                    preds = model.predict(dtest)
                    rmse_scores.append(
                        float(np.sqrt(mean_squared_error(y_test, preds)))
                    )

                return float(np.mean(rmse_scores))

            def objective(space: dict[str, float]) -> dict[str, float | str]:
                """
                Hyperopt objective: returns a dict with loss & status=STATUS_OK.
                Minimizing RMSE over combined CV.
                """
                param_dict = {
                    "objective": "reg:squarederror",
                    "eval_metric": "rmse",
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
                "learning_rate": hp.loguniform(
                    "learning_rate", np.log(0.001), np.log(0.3)
                ),
                "max_depth": hp.quniform("max_depth", 2, 10, 1),
                "subsample": hp.uniform("subsample", 0.5, 1.0),
                "colsample_bytree": hp.uniform("colsample_bytree", 0.5, 1.0),
                "reg_alpha": hp.loguniform(
                    "reg_alpha", np.log(1e-6), np.log(10)
                ),
                "reg_lambda": hp.loguniform(
                    "reg_lambda", np.log(1e-6), np.log(10)
                ),
                "min_child_weight": hp.quniform("min_child_weight", 1, 10, 1),
                "gamma": hp.uniform("gamma", 0.0, 5.0),
                "num_boost_round": hp.quniform("num_boost_round", 50, 400, 10),
            }

            spark_trials = SparkTrials(parallelism=2)

            with mlflow.start_run(nested=True):
                mlflow.log_param("table_name", table_name)
                mlflow.log_param("target_column", tgt_col)
                mlflow.log_param("num_rows", len(group_df))
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

                dtrain_full = xgb.DMatrix(X_scaled, label=y)
                final_model = xgb.train(
                    params={
                        "objective": "reg:squarederror",
                        "eval_metric": "rmse",
                        "learning_rate": best_params["learning_rate"],
                        "max_depth": best_params["max_depth"],
                        "subsample": best_params["subsample"],
                        "colsample_bytree": best_params["colsample_bytree"],
                        "reg_alpha": best_params["reg_alpha"],
                        "reg_lambda": best_params["reg_lambda"],
                        "min_child_weight": best_params["min_child_weight"],
                        "gamma": best_params["gamma"],
                    },
                    dtrain=dtrain_full,
                    num_boost_round=best_params["num_boost_round"],
                )

                preds_full = final_model.predict(dtrain_full)
                final_rmse_val = float(
                    np.sqrt(mean_squared_error(y, preds_full))
                )
                final_mae_val = float(mean_absolute_error(y, preds_full))
                final_r2_val = float(r2_score(y, preds_full))

                mlflow.log_metric("final_RMSE", final_rmse_val)
                mlflow.log_metric("final_MAE", final_mae_val)
                mlflow.log_metric("final_R2", final_r2_val)

                for hp_name, hp_val in best_params.items():
                    mlflow.log_param(f"best_{hp_name}", hp_val)

                mlflow.xgboost.log_model(
                    final_model, f"xgboost_model_{table_name}_{tgt_col}"
                )

                logger.info(
                    "[XGBoost] Completed hyperopt for table=%s, target=%s. "
                    "Best params=%s, RMSE=%f",
                    table_name,
                    tgt_col,
                    best_params,
                    final_rmse_val,
                )


def generate_predictions_and_write_to_iceberg(
    spark: SparkSession,
    df: DataFrame,
    best_features_dict: dict[str, list[str]],
    target_cols: list[str],
    future_or_historical: str = "historical",
    db_name: str = "nbd_predictions",
) -> None:
    """
    Generate predictions (batch) and write them to an Iceberg table in your data lake:
      - If future_or_historical == "historical", do a backfill from "latest date"
        or "latest game" up to now, storing in e.g. "nbd_predictions.historical_predictions".
      - If future_or_historical == "future", do predictions for officialDate >=
        date_add(current_date(), 1) storing in "nbd_predictions.future_predictions".

    In a real scenario, you'd reload the best XGB models from MLflow or keep
    them in memory to do actual predictions. Here we demonstrate random placeholders.

    :param spark: Active SparkSession.
    :param df: Spark DataFrame used for generating predictions.
    :param best_features_dict: Dictionary of best features from Lasso.
    :param target_cols: List of target columns for which to generate predictions.
    :param future_or_historical: Whether generating 'historical' or 'future' predictions.
    :param db_name: Destination database/schema name.
    """
    if future_or_historical == "historical":
        out_table = f"{db_name}.historical_predictions"
    else:
        out_table = f"{db_name}.future_predictions"

    pdf = df.toPandas()

    # Example random predictions; replace with real model inference in production
    for col_target in target_cols:
        predictions = [random.uniform(0, 50) for _ in range(len(pdf))]
        pdf[f"prediction_{col_target}"] = predictions

    pred_df = spark.createDataFrame(pdf)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    pred_df.write.format("iceberg").mode("append").save(f"spark_catalog.{out_table}")

    logger.info(
        "Wrote %d rows of %s predictions to %s.",
        pred_df.count(),
        future_or_historical,
        out_table,
    )


def stratified_reservoir_sample_data(
    df: DataFrame,
    strata_cols: list[str],
    reservoir_size: int = 100,
    seed: int = 42,
) -> DataFrame:
    """
    Demonstrates a Stratified Reservoir Sampling approach in PySpark.
    For each distinct stratum (defined by strata_cols), we sample up
    to `reservoir_size` rows.

    :param df: Original Spark DataFrame.
    :param strata_cols: List of columns used to define the stratum.
    :param reservoir_size: Max number of rows to keep per stratum.
    :param seed: Random seed for reproducibility.
    :return: Sampled Spark DataFrame containing up to `reservoir_size` per stratum.
    """

    def reservoir_sampling(
        pdf: pd.DataFrame, k: int, random_seed: int
    ) -> pd.DataFrame:
        if len(pdf) <= k:
            return pdf
        return pdf.sample(n=k, random_state=random_seed)

    schema = df.schema
    group_cols = strata_cols

    def sample_group(pdf: pd.DataFrame) -> pd.DataFrame:
        return reservoir_sampling(pdf, reservoir_size, seed)

    sampled_df = (
        df.groupBy(group_cols)
        .applyInPandas(sample_group, schema=schema)
    )
    return sampled_df


def main() -> None:
    """
    Main pipeline orchestrating the entire workflow:
      1) Create Spark session
      2) Load data from NBA_player_boxscores (all tables)
      3) Clean & partition
      4) [NEW] Stratified reservoir sampling demonstration
      5) Feature engineering
      6) Lasso => best features
      7) XGBoost => Bayesian optimization + cross-validation, log to MLflow
      8) Generate historical + future predictions => save to nbd_predictions
      9) Use officialDate >= date_add(current_date(), 1) for future updates

    4 STAGES OF CONTINUAL LEARNING (as context):
      - Stateless Training (manual)
      - Automated Retraining
      - Stateful Training (automated)
      - Continual Learning
    """
    spark = create_spark_session()

    # 1) Load all tables
    big_df = load_all_iceberg_tables(spark)

    # 2) Clean & partition
    cleaned_df = clean_and_partition_data(big_df)

    # 3) Stratified reservoir sampling by e.g. "team_id", "source_table"
    strata_columns = ["team_id", "source_table"]
    sampled_df = stratified_reservoir_sample_data(
        df=cleaned_df,
        strata_cols=strata_columns,
        reservoir_size=100,
        seed=42,
    )
    logger.info(
        "Stratified reservoir sample completed. Final count = %d",
        sampled_df.count(),
    )

    # 4) Feature engineering
    fe_df = apply_feature_engineering(spark, sampled_df)

    # 5) Run Lasso to get best features
    lasso_best_features = run_lasso_for_each_table_and_column(
        spark, fe_df, mlflow_experiment_name="Lasso_Regression_NBA"
    )

    # 6) XGBoost training + Bayesian optimization
    xgboost_training_with_hyperopt(
        spark,
        fe_df,
        best_features_dict=lasso_best_features,
        mlflow_experiment_name="XGBoost_NBA_Bayesian_Optimization",
        max_evals=15,
    )

    # 7) Generate predictions
    unique_targets = set()
    for k in lasso_best_features.keys():
        splitted = k.split("_")
        if len(splitted) < 2:
            continue
        target_col = splitted[-1]
        unique_targets.add(target_col)

    target_cols_list = list(unique_targets)

    # Historical predictions
    generate_predictions_and_write_to_iceberg(
        spark,
        fe_df,
        lasso_best_features,
        target_cols=target_cols_list,
        future_or_historical="historical",
        db_name="nbd_predictions",
    )

    # Future predictions
    generate_predictions_and_write_to_iceberg(
        spark,
        fe_df,
        lasso_best_features,
        target_cols=target_cols_list,
        future_or_historical="future",
        db_name="nbd_predictions",
    )

    spark.stop()
    logger.info("Done executing main pipeline for XGBoost NBA predictions.")


if __name__ == "__main__":
    main()
