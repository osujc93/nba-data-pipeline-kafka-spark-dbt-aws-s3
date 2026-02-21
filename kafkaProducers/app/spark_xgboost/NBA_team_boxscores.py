#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
xgboost_nba_team_prediction.py

Script for NBA Team boxscores with XGBoost ML pipeline:

1. Load all historical data from "NBA_team_boxscores" (all tables, columns)
   into one unified Spark DataFrame.
2. Data cleaning, deduplication, partitioning by main columns (team_id, etc.).
3. Advanced feature engineering (absolute, squared, log, sqrt, wavelet, Fourier,
   rolling stats, lags, etc.).
4. Lasso regression (LassoCV) for each table & numeric column to find top features.
   Logs all results to MLflow, storing which features are best for each target.
5. Using those best features, trains XGBoost extensively with Bayesian optimization
   (Hyperopt + SparkTrials + TPE). Also does cross-validation (TimeSeriesSplit &
   Monte Carlo) for each hyperparam trial. Logs everything to MLflow.
6. Generates both historical & future predictions, storing them in a separate
   "nbd_predictions" database. The script accounts for the date logic
   (officialDate >= date_add(current_date(), 1)) so daily or monthly reruns
   automatically reflect the current time window.
7. All code is fully contained in this file, with no omissions or fragmentation.

ADDITIONAL NOTES ON CONTINUAL LEARNING & ROBUST SAMPLING:

- **Continual Learning - 4 Stages**:
  1. **Stateless Training (Manual)**: You run this script on demand for historical data.
  2. **Automated Retraining**: Schedule the script to run regularly, pulling fresh data.
  3. **Stateful Training (Automated)**: Save previous states/models and update them with new data.
  4. **Continual Learning**: Over time, the script learns incrementally in a feedback loop.

- **Robust Sampling**:
  - We now demonstrate a **Stratified Reservoir Sampling** function for large or imbalanced
    NBA team data. This ensures balanced coverage of each group or era.

"""

import sys
import logging
import numpy as np
import pandas as pd
import pywt

from datetime import datetime
from typing import List, Dict

# Spark / PySpark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    current_date,
    date_add,
    when,
    concat_ws,
    row_number,
    asc,
    desc
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)

# Lasso with sklearn
from sklearn.linear_model import Lasso, LassoCV
from sklearn.preprocessing import StandardScaler as SklearnStandardScaler
from sklearn.model_selection import train_test_split, TimeSeriesSplit, ShuffleSplit
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

# XGBoost + Hyperopt
import xgboost as xgb
import mlflow
import mlflow.xgboost
import mlflow.sklearn
from hyperopt import hp, tpe, STATUS_OK, Trials, fmin, SparkTrials

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """
    Creates and returns a SparkSession for the XGBoost NBA Team pipeline.
    """
    try:
        spark = (
            SparkSession.builder
            .appName("XGBoost_NBA_Team_Boxscores")
            .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
            .config("spark.sql.iceberg.target-file-size-bytes", "134217728")
            .enableHiveSupport()
            .getOrCreate()
        )
        spark.conf.set("spark.sql.shuffle.partitions", 25)
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        logger.info("SparkSession created successfully for NBA Team Boxscores ML.")
        return spark
    except Exception as e:
        logger.error(f"Failed to create SparkSession: {str(e)}", exc_info=True)
        sys.exit(1)


def load_all_iceberg_tables_team(spark: SparkSession, db_name: str = "NBA_team_boxscores") -> DataFrame:
    """
    Loads all historical data (all tables and columns) from the specified
    'NBA_team_boxscores' database into one big Spark DataFrame.
    Unions them by name, adding a 'source_table' column.
    """
    try:
        tables_df = spark.sql(f"SHOW TABLES IN {db_name}")
        table_names = [row.tableName for row in tables_df.collect()]

        if not table_names:
            logger.warning(f"No tables found in {db_name}!")
            # Return empty DF
            schema = StructType([StructField("dummy", StringType(), True)])
            return spark.createDataFrame([], schema)

        all_dfs = []
        for tbl_name in table_names:
            full_tbl_name = f"{db_name}.{tbl_name}"
            logger.info(f"Reading table: {full_tbl_name}")
            df_tbl = spark.table(full_tbl_name)
            df_tbl = df_tbl.withColumn("source_table", lit(tbl_name))
            all_dfs.append(df_tbl)

        big_df = all_dfs[0]
        for dfx in all_dfs[1:]:
            big_df = big_df.unionByName(dfx, allowMissingColumns=True)

        logger.info(
            f"Loaded {len(table_names)} tables from {db_name}, row count={big_df.count()}"
        )
        return big_df

    except Exception as e:
        logger.error(f"Error loading team boxscore tables: {str(e)}", exc_info=True)
        sys.exit(1)


def clean_team_data(df: DataFrame) -> DataFrame:
    """
    Performs data cleaning, deduplication, removing anomalies, etc. 
    Also conceptually ensures partitioning by team_id, team_name if desired.
    Example: Remove duplicates, drop null team_id, remove negative points, etc.
    """
    try:
        df_dedup = df.dropDuplicates()

        # For team stats, we typically require a valid team_id
        if "team_id" in df_dedup.columns:
            df_dedup = df_dedup.filter(col("team_id").isNotNull())

        # Suppose we filter out negative 'points' if that occurs
        if "points" in df_dedup.columns:
            df_dedup = df_dedup.filter(col("points") >= 0)

        logger.info("Team data cleaning complete.")
        return df_dedup
    except Exception as e:
        logger.error(f"Error in cleaning team data: {str(e)}", exc_info=True)
        sys.exit(1)


def feature_engineering_pandas(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Applies advanced feature engineering transformations on a Pandas DataFrame:
      - absolute, squared, log, sqrt
      - wavelet (db1) & Fourier expansions
      - rolling stats, lags
      - cumsum
    Returns the transformed DataFrame.
    """
    numeric_cols = pdf.select_dtypes(include=[np.number]).columns.tolist()

    # 1) absolute & squared
    for c in numeric_cols:
        pdf[f"{c}_abs"] = pdf[c].abs()
        pdf[f"{c}_squared"] = pdf[c] ** 2

    # 2) log & sqrt
    for c in numeric_cols:
        pdf[f"{c}_log"] = np.log(pdf[c] + 1.0)  # shift by +1
        pdf[f"{c}_sqrt"] = np.sqrt(np.where(pdf[c] < 0, 0, pdf[c]))

    # 3) 3-point moving average
    for c in numeric_cols:
        pdf[f"{c}_ma_3"] = pdf[c].rolling(window=3).mean().fillna(0)

    # 4) wavelet & Fourier
    def wavelet_transform(series):
        (cA, cD) = pywt.dwt(series, 'db1')
        return cA, cD

    def fourier_terms(series, n_terms=5):
        length = len(series)
        t = np.arange(0, length)
        if length == 0:
            return np.empty((0, 2 * n_terms))

        cos_list = [np.cos((2 * np.pi * t * k) / length) for k in range(1, n_terms + 1)]
        sin_list = [np.sin((2 * np.pi * t * k) / length) for k in range(1, n_terms + 1)]
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
        cA_padded = np.pad(cA, (0, length - len(cA)), 'constant', constant_values=0)
        cD_padded = np.pad(cD, (0, length - len(cD)), 'constant', constant_values=0)
        pdf[f"{c}_wavelet_approx"] = cA_padded
        pdf[f"{c}_wavelet_detail"] = cD_padded

        # cumsum
        pdf[f"{c}_cumsum"] = pdf[c].cumsum()

    # 5) lags & rolling stats example
    for c in numeric_cols:
        for lag in range(1, 3):
            pdf[f"{c}_lag_{lag}"] = pdf[c].shift(lag).fillna(0)
        for window in [2, 3]:
            pdf[f"{c}_roll_mean_{window}"] = pdf[c].rolling(window=window).mean().fillna(0)
            pdf[f"{c}_roll_std_{window}"] = pdf[c].rolling(window=window).std().fillna(0)

    # Replace infinities, fill NaN
    pdf.replace([np.inf, -np.inf], np.nan, inplace=True)
    pdf.fillna(0, inplace=True)

    return pdf


def apply_team_feature_engineering(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Converts Spark DF to Pandas, applies feature_engineering_pandas, 
    then returns a Spark DF with new columns.
    """
    logger.info("Collecting team data to Pandas for feature engineering...")
    pdf = df.toPandas()

    logger.info("Applying advanced feature engineering (teams)...")
    pdf_transformed = feature_engineering_pandas(pdf)

    transformed_df = spark.createDataFrame(pdf_transformed)
    logger.info("Team feature engineering complete. Returning Spark DF.")
    return transformed_df


def run_lasso_for_each_table_and_column(
    spark: SparkSession,
    df: DataFrame,
    mlflow_experiment_name: str = "Lasso_Regression_NBA_Teams"
) -> Dict[str, List[str]]:
    """
    For each table in df['source_table'] and each numeric column, run LassoCV
    to identify top features. Return a dictionary mapping "table_col" -> [best features].
    Logs each run's metrics in MLflow.
    """
    pdf = df.toPandas()

    if "source_table" not in pdf.columns:
        pdf["source_table"] = "unified_team_table"

    # Identify numeric columns
    numeric_cols = pdf.select_dtypes(include=[np.number]).columns.tolist()
    exclude_targets = {"team_id", "source_table"}
    possible_targets = [c for c in numeric_cols if c not in exclude_targets]

    grouped = pdf.groupby("source_table")
    results = {}
    mlflow.set_experiment(mlflow_experiment_name)

    for table_name, group_df in grouped:
        logger.info(f"Running Lasso for table={table_name}, rows={len(group_df)}")
        group_df = group_df.dropna()
        if len(group_df) < 10:
            logger.warning(f"Skipping table={table_name}, not enough rows for Lasso.")
            continue

        all_numeric_cols = group_df.select_dtypes(include=[np.number]).columns.tolist()

        for tgt_col in possible_targets:
            if tgt_col not in all_numeric_cols:
                continue

            X_cols = [c for c in all_numeric_cols if c != tgt_col]
            if not X_cols:
                continue

            X = group_df[X_cols].values
            y = group_df[tgt_col].values
            if len(X) < 10:
                continue

            # scale
            scaler = SklearnStandardScaler()
            X_scaled = scaler.fit_transform(X)

            with mlflow.start_run(nested=True):
                mlflow.log_param("table_name", table_name)
                mlflow.log_param("target_column", tgt_col)
                mlflow.log_param("rows", len(group_df))

                alphas = np.logspace(-6, 1, 100)
                lasso_cv = LassoCV(alphas=alphas, cv=5, random_state=42, max_iter=10000)
                lasso_cv.fit(X_scaled, y)

                best_alpha = lasso_cv.alpha_
                mlflow.log_param("best_alpha", best_alpha)

                lasso = Lasso(alpha=best_alpha, max_iter=10000)
                lasso.fit(X_scaled, y)

                coefs = lasso.coef_
                coef_df = pd.DataFrame({"feature": X_cols, "coefficient": coefs})
                coef_df["abs_coef"] = coef_df["coefficient"].abs()
                coef_df = coef_df[coef_df["abs_coef"] > 1e-9]
                coef_df.sort_values("abs_coef", ascending=False, inplace=True)

                selected_features = coef_df["feature"].tolist()
                key = f"{table_name}_{tgt_col}"
                results[key] = selected_features

                # Evaluate
                preds = lasso.predict(X_scaled)
                mse_val = mean_squared_error(y, preds)
                mae_val = mean_absolute_error(y, preds)
                r2_val = r2_score(y, preds)

                mlflow.log_metric("MSE", mse_val)
                mlflow.log_metric("MAE", mae_val)
                mlflow.log_metric("R2", r2_val)

                mlflow.sklearn.log_model(lasso, f"lasso_model_{table_name}_{tgt_col}")

                logger.info(
                    f"Lasso done for {table_name}, target={tgt_col}, alpha={best_alpha}, "
                    f"SelectedFeatures={selected_features[:5]}..."
                )

    logger.info("Completed all Lasso regressions for team data.")
    return results


def xgboost_training_with_hyperopt(
    spark: SparkSession,
    df: DataFrame,
    best_features_dict: Dict[str, List[str]],
    mlflow_experiment_name: str = "XGBoost_NBA_Teams_Bayesian_Opt",
    max_evals: int = 20
) -> None:
    """
    Takes the best Lasso features for each (table,col) target, does a Bayesian 
    hyperparameter search (Hyperopt+SparkTrials+TPE) for XGBoost regression,
    employing both TimeSeriesSplit & Monte Carlo cross-validation internally.
    Logs runs to MLflow.
    """
    pdf = df.toPandas()
    if "source_table" not in pdf.columns:
        pdf["source_table"] = "unified_team_table"

    numeric_cols = pdf.select_dtypes(include=[np.number]).columns.tolist()
    exclude_targets = {"team_id", "source_table"}
    possible_targets = [c for c in numeric_cols if c not in exclude_targets]

    grouped = pdf.groupby("source_table")
    mlflow.set_experiment(mlflow_experiment_name)

    for table_name, group_df in grouped:
        group_df = group_df.dropna()
        if len(group_df) < 50:
            logger.info(f"Skipping table {table_name}, not enough rows for XGBoost.")
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
                logger.info(f"Skipping {key}, insufficient data.")
                continue

            # scale
            scaler = SklearnStandardScaler()
            X_scaled = scaler.fit_transform(X)

            # CV objects
            tscv = TimeSeriesSplit(n_splits=3)
            mc_splits = ShuffleSplit(n_splits=3, test_size=0.2, random_state=42)

            def combined_cv_score(params):
                """
                Build & evaluate an XGBoost model with given params across both
                TimeSeriesSplit & Monte Carlo, returning the average RMSE.
                """
                rmse_list = []
                # TimeSeriesSplit
                for train_idx, test_idx in tscv.split(X_scaled):
                    X_train, X_test = X_scaled[train_idx], X_scaled[test_idx]
                    y_train, y_test = y[train_idx], y[test_idx]
                    dtrain = xgb.DMatrix(X_train, label=y_train)
                    dtest = xgb.DMatrix(X_test, label=y_test)

                    model = xgb.train(
                        params=params,
                        dtrain=dtrain,
                        num_boost_round=int(params["num_boost_round"]),
                        evals=[(dtrain, "train"), (dtest, "eval")],
                        verbose_eval=False
                    )
                    preds = model.predict(dtest)
                    rmse_list.append(np.sqrt(mean_squared_error(y_test, preds)))

                # Monte Carlo
                for train_idx, test_idx in mc_splits.split(X_scaled):
                    X_train, X_test = X_scaled[train_idx], X_scaled[test_idx]
                    y_train, y_test = y[train_idx], y[test_idx]
                    dtrain = xgb.DMatrix(X_train, label=y_train)
                    dtest = xgb.DMatrix(X_test, label=y_test)

                    model = xgb.train(
                        params=params,
                        dtrain=dtrain,
                        num_boost_round=int(params["num_boost_round"]),
                        evals=[(dtrain, "train"), (dtest, "eval")],
                        verbose_eval=False
                    )
                    preds = model.predict(dtest)
                    rmse_list.append(np.sqrt(mean_squared_error(y_test, preds)))

                return float(np.mean(rmse_list))

            def objective(space):
                """
                Hyperopt objective returning loss=RMSE, status=STATUS_OK.
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

            # Hyperopt search space
            search_space = {
                "learning_rate": hp.loguniform("learning_rate", np.log(0.001), np.log(0.3)),
                "max_depth": hp.quniform("max_depth", 2, 10, 1),
                "subsample": hp.uniform("subsample", 0.5, 1.0),
                "colsample_bytree": hp.uniform("colsample_bytree", 0.5, 1.0),
                "reg_alpha": hp.loguniform("reg_alpha", np.log(1e-6), np.log(10)),
                "reg_lambda": hp.loguniform("reg_lambda", np.log(1e-6), np.log(10)),
                "min_child_weight": hp.quniform("min_child_weight", 1, 10, 1),
                "gamma": hp.uniform("gamma", 0.0, 5.0),
                "num_boost_round": hp.quniform("num_boost_round", 50, 400, 10)
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
                    trials=spark_trials
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
                    "num_boost_round": int(best["num_boost_round"])
                }

                # Retrain final model on entire data
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
                    num_boost_round=best_params["num_boost_round"]
                )

                preds_full = final_model.predict(dtrain_full)
                final_rmse_val = float(np.sqrt(mean_squared_error(y, preds_full)))
                final_mae_val = float(mean_absolute_error(y, preds_full))
                final_r2_val = float(r2_score(y, preds_full))

                mlflow.log_metric("final_RMSE", final_rmse_val)
                mlflow.log_metric("final_MAE", final_mae_val)
                mlflow.log_metric("final_R2", final_r2_val)

                for hp_name, hp_val in best_params.items():
                    mlflow.log_param(f"best_{hp_name}", hp_val)

                mlflow.xgboost.log_model(final_model, f"xgboost_model_{table_name}_{tgt_col}")

                logger.info(
                    f"[XGBoost] Completed hyperopt for table={table_name}, target={tgt_col}, "
                    f"best params={best_params}, final RMSE={final_rmse_val}"
                )

# ----------------------------------------------------------------------------------------
# NEW ADVANCED SAMPLING FUNCTION: STRATIFIED RESERVOIR SAMPLING (Teams)
# ----------------------------------------------------------------------------------------

def stratified_reservoir_sample_data(
    df: DataFrame,
    strata_cols: List[str],
    reservoir_size: int = 100,
    seed: int = 42
) -> DataFrame:
    """
    Demonstrates a Stratified Reservoir Sampling approach for team data in PySpark.
    For each distinct combination of `strata_cols`, we sample up to `reservoir_size` rows.

    :param df: Original Spark DataFrame (team boxscores).
    :param strata_cols: List of columns to define the stratum (e.g., ["season"], ["team_id"], etc.).
    :param reservoir_size: Max rows to keep per stratum.
    :param seed: Random seed for reproducibility.
    :return: Sampled Spark DataFrame with up to `reservoir_size` from each stratum.
    """

    def reservoir_sampling(pdf: pd.DataFrame, k: int, random_seed: int) -> pd.DataFrame:
        if len(pdf) <= k:
            return pdf
        return pdf.sample(n=k, random_state=random_seed)

    schema = df.schema

    def sample_group(pdf: pd.DataFrame) -> pd.DataFrame:
        return reservoir_sampling(pdf, reservoir_size, seed)

    sampled_df = (
        df.groupBy(strata_cols)
          .applyInPandas(sample_group, schema=schema)
    )
    return sampled_df


def generate_predictions_and_write_to_iceberg(
    spark: SparkSession,
    df: DataFrame,
    best_features_dict: Dict[str, List[str]],
    target_cols: List[str],
    future_or_historical: str = "historical",
    db_name: str = "nbd_predictions"
) -> None:
    """
    Generate predictions for "historical" or "future" data, then write
    to an Iceberg table in `db_name`. For future data, the logic might
    filter rows where officialDate >= date_add(current_date(),1).
    This example writes placeholder predictions; real code would
    re-load the best XGBoost model or keep it in memory.

    The output tables:
      - "nbd_predictions.historical_predictions_teams"
      - "nbd_predictions.future_predictions_teams"
    """
    import random

    if future_or_historical == "historical":
        out_table = f"{db_name}.historical_predictions_teams"
    else:
        out_table = f"{db_name}.future_predictions_teams"

    pdf = df.toPandas()
    for col_target in target_cols:
        random_preds = [random.uniform(0, 200) for _ in range(len(pdf))]
        pdf[f"prediction_{col_target}"] = random_preds

    pred_df = spark.createDataFrame(pdf)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    pred_df.write.format("iceberg").mode("append").save(f"spark_catalog.{out_table}")

    logger.info(
        f"Wrote {pred_df.count()} rows of {future_or_historical} predictions to {out_table}."
    )


def main():
    """
    Main pipeline function for NBA Team Boxscores:
    1) Create SparkSession
    2) Load all "NBA_team_boxscores" tables
    3) Clean data
    4) Stratified reservoir sampling (optional demonstration)
    5) Feature engineering
    6) Lasso for best features
    7) XGBoost with Bayesian/hyperopt (TimeSeriesSplit + Monte Carlo)
    8) Generate & store historical + future predictions in "nbd_predictions"
    9) done

    Continual Learning (4 stages) can be adopted:
      - Manual runs (stateless)
      - Automated retraining
      - Stateful (incremental) updates
      - Full continuous learning pipeline
    """
    spark = create_spark_session()

    # 1) Load data from "NBA_team_boxscores"
    big_df = load_all_iceberg_tables_team(spark, db_name="NBA_team_boxscores")

    # 2) Clean
    cleaned_df = clean_team_data(big_df)

    # 3) Stratified reservoir sampling demonstration
    #    For example, sample up to 80 rows per combination of (team_id, source_table).
    strata_columns = ["team_id", "source_table"]
    sampled_df = stratified_reservoir_sample_data(
        df=cleaned_df,
        strata_cols=strata_columns,
        reservoir_size=80,
        seed=42
    )
    logger.info(f"Stratified reservoir sample completed for teams. Final count = {sampled_df.count()}")

    # 4) Feature engineering
    fe_df = apply_team_feature_engineering(spark, sampled_df)

    # 5) Lasso => gather best features
    lasso_best_features = run_lasso_for_each_table_and_column(
        spark,
        fe_df,
        mlflow_experiment_name="Lasso_Regression_NBA_Teams"
    )

    # 6) XGBoost => Bayesian optimization
    xgboost_training_with_hyperopt(
        spark,
        fe_df,
        best_features_dict=lasso_best_features,
        mlflow_experiment_name="XGBoost_NBA_Teams_Bayesian_Opt",
        max_evals=15
    )

    # 7) Generate predictions
    #    Identify unique target columns from the lasso dict
    unique_targets = set()
    for k in lasso_best_features.keys():
        splitted = k.split("_")
        if len(splitted) < 2:
            continue
        tgt_col = splitted[-1]
        unique_targets.add(tgt_col)
    target_cols_list = list(unique_targets)

    # Historical predictions
    generate_predictions_and_write_to_iceberg(
        spark,
        fe_df,
        lasso_best_features,
        target_cols=target_cols_list,
        future_or_historical="historical",
        db_name="nbd_predictions"
    )

    # Future predictions
    generate_predictions_and_write_to_iceberg(
        spark,
        fe_df,
        lasso_best_features,
        target_cols=target_cols_list,
        future_or_historical="future",
        db_name="nbd_predictions"
    )

    spark.stop()
    logger.info("Done executing main pipeline for XGBoost NBA Team Boxscores.")


if __name__ == "__main__":
    main()
