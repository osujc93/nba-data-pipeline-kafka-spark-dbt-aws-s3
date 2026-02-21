#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
pyspark_ml_linear_regression_nba_stats.py

Enhanced demonstration of a PySpark ML pipeline using:
  1) Explicit train/test split (time-based if 'game_date' is present).
  2) Multiple transformations: StringIndexer, OneHotEncoder, VectorAssembler,
     PolynomialExpansion, StandardScaler, VectorIndexer, Feature Selection.
  3) Multiple regression algorithms (LinearRegression & GBTRegressor),
     each with CrossValidator + ParamGridBuilder for hyperparameter tuning.
  4) Multiple evaluation metrics (RMSE, MAE, R2, MAPE).
  5) MLflow tracking with more detailed logging (params, metrics, data version).
  6) Production considerations like caching, model registry usage,
     drift monitoring placeholders, and scheduled retraining suggestions (comments).
  7) Sports-specific feature engineering (e.g., points_per_minute).

The pipeline reads NBA Player boxscores from Iceberg on HDFS, cleans data,
splits train/test, builds advanced pipelines, logs everything to MLflow,
and writes predictions back to Iceberg tables.

This structure is typical in major sports analytics production systems.
"""

import sys
import logging
from typing import List, Tuple

import mlflow
import mlflow.spark

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    lit,
    when,
    col,
    regexp_replace,
    current_date,
    date_add,
    year,
    month,
    dayofmonth,
    rand,
    avg,
    abs as _abs,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    DateType,
)

# Spark ML imports
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler,
    StandardScaler,
    PolynomialExpansion,
    VectorIndexer,
    ChiSqSelector,
)
from pyspark.ml.regression import LinearRegression, GBTRegressor
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """
    Create and return a SparkSession for the NBA player boxscores pipeline.
    Configured to read/write Iceberg on HDFS.
    """
    try:
        spark = (
            SparkSession.builder
            .appName("PySparkML_LinearRegression_NBA_Stats_Enhanced")
            .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
            .config("spark.sql.iceberg.target-file-size-bytes", "134217728")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog"
            )
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.warehouse.dir", "hdfs://mycluster/nelo_sports_warehouse")
            .enableHiveSupport()
            .getOrCreate()
        )
        spark.conf.set("spark.sql.shuffle.partitions", 25)
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        logger.info("SparkSession created successfully for NBA player boxscores (Enhanced).")
        return spark
    except Exception as e:
        logger.error(f"Failed to create SparkSession: {str(e)}", exc_info=True)
        sys.exit(1)


def load_iceberg_tables(spark: SparkSession, db_name: str) -> DataFrame:
    """
    Load all historical data from the specified Iceberg database
    (e.g., 'iceberg_nba_player_boxscores') by unioning all tables found.
    """
    logger.info(f"Loading all tables from Iceberg database: {db_name}")
    try:
        tables_df = spark.sql(f"SHOW TABLES IN {db_name}")
        table_names = [row.tableName for row in tables_df.collect()]

        if not table_names:
            logger.warning(f"No tables found in database: {db_name}")
            # Return empty DataFrame
            empty_schema = StructType([StructField("empty", StringType(), True)])
            return spark.createDataFrame([], empty_schema)

        all_dfs = []
        for tbl_name in table_names:
            full_tbl_name = f"{db_name}.{tbl_name}"
            logger.info(f"Reading table: {full_tbl_name}")
            df_tbl = spark.table(full_tbl_name)
            df_tbl = df_tbl.withColumn("source_table", lit(tbl_name))
            all_dfs.append(df_tbl)

        # Union all
        union_df = all_dfs[0]
        for df_table in all_dfs[1:]:
            union_df = union_df.unionByName(df_table, allowMissingColumns=True)

        row_count = union_df.count()
        logger.info(
            f"Loaded {len(table_names)} tables from {db_name}, row count = {row_count}"
        )
        return union_df
    except Exception as e:
        logger.error(f"Error loading tables from {db_name}: {e}", exc_info=True)
        sys.exit(1)


def clean_data(df: DataFrame) -> DataFrame:
    """
    Perform basic cleaning: remove duplicates, filter out invalid values,
    fix or rename columns, etc. Also adds or ensures year, month, day columns
    for potential time-based splits.
    """
    logger.info("Performing data cleaning...")
    df = df.dropDuplicates()

    # Filter out rows with missing player_id or negative points
    if "player_id" in df.columns:
        df = df.filter(col("player_id").isNotNull())
    if "points" in df.columns:
        df = df.filter(col("points") >= 0)

    # Convert game_date to DateType if it's not already
    if "game_date" in df.columns and df.schema["game_date"].dataType != DateType():
        df = df.withColumn("game_date", col("game_date").cast(DateType()))

    # Add or ensure year, month, day columns
    if "game_date" in df.columns:
        df = (
            df.withColumn("year", year(col("game_date")))
              .withColumn("month", month(col("game_date")))
              .withColumn("day", dayofmonth(col("game_date")))
        )

    # Example domain-specific feature: points_per_minute
    if "points" in df.columns and "minutes_played" in df.columns:
        df = df.withColumn(
            "points_per_minute",
            when(col("minutes_played") > 0, col("points") / col("minutes_played"))
            .otherwise(0.0)
        )

    logger.info("Data cleaning complete.")
    return df


def split_data(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Split the DataFrame into train/test sets. If a 'game_date' column is present,
    do a time-based split (train <= 2020-12-31, test >= 2021-01-01).
    Otherwise, fall back to a random split.
    """
    if "game_date" in df.columns:
        logger.info("Performing time-based split: train <= 2020-12-31, test >= 2021-01-01")
        train_df = df.filter(col("game_date") <= "2020-12-31")
        test_df = df.filter(col("game_date") >= "2021-01-01")

        if train_df.count() < 10 or test_df.count() < 10:
            logger.warning("Not enough rows in time-based split, falling back to randomSplit.")
            train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    else:
        logger.info("No game_date found; performing random split.")
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    logger.info(f"Train set count: {train_df.count()}, Test set count: {test_df.count()}")
    return train_df.cache(), test_df.cache()


def build_pipeline_stages(
    cat_col_list: List[str],
    numeric_features: List[str],
    target_col: str,
    do_polynomial_expansion: bool = True
) -> List:
    """
    Dynamically build the necessary pipeline stages for:
      - StringIndexer/OneHotEncoder on categorical features
      - VectorAssembler for numeric columns
      - PolynomialExpansion (optional)
      - StandardScaler
      - VectorIndexer
      - FeatureSelection (ChiSqSelector demo)

    Returns an ordered list of stages (without the final regressor).
    """
    stages = []

    # 1) StringIndexer + OneHotEncoder for each categorical column
    ohe_output_cols = []
    for cat in cat_col_list:
        idx_col = f"{cat}_idx"
        ohe_col = f"{cat}_ohe"
        indexer = StringIndexer(inputCol=cat, outputCol=idx_col, handleInvalid="keep")
        encoder = OneHotEncoder(inputCols=[idx_col], outputCols=[ohe_col])
        stages.extend([indexer, encoder])
        ohe_output_cols.append(ohe_col)

    # 2) Assembler for numeric features
    assembler_numeric = VectorAssembler(
        inputCols=numeric_features,
        outputCol="numeric_assembled"
    )
    stages.append(assembler_numeric)

    # 3) PolynomialExpansion (optional)
    if do_polynomial_expansion:
        poly_expander = PolynomialExpansion(
            degree=2,
            inputCol="numeric_assembled",
            outputCol="poly_features"
        )
        stages.append(poly_expander)
        final_numeric_col = "poly_features"
    else:
        final_numeric_col = "numeric_assembled"

    # 4) Combine OHE categorical + numeric (or poly) into one vector
    all_features_for_assembler = [final_numeric_col] + ohe_output_cols
    assembler_all = VectorAssembler(
        inputCols=all_features_for_assembler,
        outputCol="features_unscaled"
    )
    stages.append(assembler_all)

    # 5) StandardScaler
    scaler = StandardScaler(
        inputCol="features_unscaled",
        outputCol="features",
        withMean=True,
        withStd=True
    )
    stages.append(scaler)

    # 6) VectorIndexer
    vector_indexer = VectorIndexer(
        inputCol="features",
        outputCol="indexed_features",
        maxCategories=20
    )
    stages.append(vector_indexer)

    # 7) Feature Selection (ChiSqSelector demo)
    feature_selector = ChiSqSelector(
        numTopFeatures=50,
        featuresCol="indexed_features",
        labelCol=target_col,
        outputCol="selected_features"
    )
    stages.append(feature_selector)

    return stages


def train_and_evaluate_model(
    train_df: DataFrame,
    test_df: DataFrame,
    pipeline_stages: List,
    model_type: str,
    target_col: str
) -> None:
    """
    Train a model (LinearRegression or GBTRegressor) with CrossValidator on `train_df`
    for the given `target_col`. Evaluate on `test_df` with multiple metrics
    (RMSE, MAE, R2, MAPE). Logs results to MLflow.

    :param model_type: "lr" or "gbt"
    """
    if model_type == "lr":
        regressor = LinearRegression(
            featuresCol="selected_features",
            labelCol=target_col
        )
        paramGrid = (
            ParamGridBuilder()
            .addGrid(regressor.regParam, [0.01, 0.1, 0.3])
            .addGrid(regressor.elasticNetParam, [0.0, 0.5, 1.0])
            .addGrid(regressor.maxIter, [50, 100, 200])
            .build()
        )
    else:
        regressor = GBTRegressor(
            featuresCol="selected_features",
            labelCol=target_col,
            maxIter=50
        )
        paramGrid = (
            ParamGridBuilder()
            .addGrid(regressor.maxDepth, [3, 5, 7])
            .addGrid(regressor.maxIter, [20, 50, 100])
            .addGrid(regressor.stepSize, [0.05, 0.1, 0.2])
            .build()
        )

    pipeline = Pipeline(stages=pipeline_stages + [regressor])

    evaluator_rmse = RegressionEvaluator(
        labelCol=target_col,
        predictionCol="prediction",
        metricName="rmse"
    )

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=paramGrid,
        evaluator=evaluator_rmse,
        numFolds=3,
        parallelism=2,
        seed=42
    )

    cv_model = cv.fit(train_df)
    test_preds = cv_model.transform(test_df)

    evaluator_mae = RegressionEvaluator(
        labelCol=target_col,
        predictionCol="prediction",
        metricName="mae"
    )
    evaluator_r2 = RegressionEvaluator(
        labelCol=target_col,
        predictionCol="prediction",
        metricName="r2"
    )

    rmse_val = evaluator_rmse.evaluate(test_preds)
    mae_val = evaluator_mae.evaluate(test_preds)
    r2_val = evaluator_r2.evaluate(test_preds)

    # MAPE calculation
    mape_df = test_preds.filter(col(target_col) != 0).select(
        avg(_abs((col("prediction") - col(target_col)) / col(target_col))).alias("mape")
    )
    mape_val = mape_df.collect()[0]["mape"] if mape_df.count() > 0 else None

    mlflow.log_param("model_type", model_type)
    mlflow.log_param("target_col", target_col)
    mlflow.log_metric("rmse", rmse_val)
    mlflow.log_metric("mae", mae_val)
    mlflow.log_metric("r2", r2_val)
    if mape_val is not None:
        mlflow.log_metric("mape", float(mape_val))

    best_model = cv_model.bestModel
    mlflow.spark.log_model(best_model, f"{model_type}_model_{target_col}")

    logger.info(
        f"[{model_type.upper()} - {target_col}] => "
        f"RMSE={rmse_val}, MAE={mae_val}, R2={r2_val}, MAPE={mape_val}"
    )


def build_and_train_models(
    spark: SparkSession,
    df: DataFrame,
    mlflow_experiment: str = "PySparkML_Enhanced_NBA"
) -> None:
    """
    Take the cleaned DataFrame, split into train/test sets, identify numeric & categorical
    columns, build advanced pipelines (with polynomial expansion, OHE, etc.), and train both
    Linear Regression & GBT models (via CrossValidator). Log to MLflow with multiple metrics.

    This function can be extended for more models or ensemble approaches.
    """
    mlflow.set_experiment(mlflow_experiment)

    train_df, test_df = split_data(df)

    all_cols = df.columns
    ignore_cols = {
        "player_id", "player_name", "team_id", "team_name", "source_table", "season",
        "season_type", "matchup", "win_loss", "game_id", "game_date_param", "game_date",
        "year", "month", "day"
    }

    numeric_features = []
    for column_name in all_cols:
        if column_name not in ignore_cols and column_name in df.schema.fieldNames():
            dtype = df.schema[column_name].dataType
            if isinstance(dtype, (DoubleType, IntegerType)):
                numeric_features.append(column_name)

    # Identify a few potential categorical columns (e.g., "team_name") if it exists
    cat_cols = []
    if "team_name" in df.columns:
        cat_cols.append("team_name")

    logger.info(f"Detected numeric columns (potential features/targets): {numeric_features}")

    for target_col in numeric_features:
        current_features = [c for c in numeric_features if c != target_col]
        if len(current_features) < 1:
            logger.warning(f"Skipping target '{target_col}' due to insufficient numeric features.")
            continue

        with mlflow.start_run(run_name=f"Enhanced_{target_col}"):
            mlflow.log_param("data_version", "v1.0")  # Example data version logging

            stages = build_pipeline_stages(
                cat_col_list=cat_cols,
                numeric_features=current_features,
                target_col=target_col,
                do_polynomial_expansion=True
            )

            # Train/Evaluate Linear Regression
            train_and_evaluate_model(
                train_df=train_df,
                test_df=test_df,
                pipeline_stages=stages,
                model_type="lr",
                target_col=target_col
            )

            # Train/Evaluate GBT
            train_and_evaluate_model(
                train_df=train_df,
                test_df=test_df,
                pipeline_stages=stages,
                model_type="gbt",
                target_col=target_col
            )

    logger.info("All numeric target columns have been processed with LR & GBT models.")


def generate_and_write_predictions(
    spark: SparkSession,
    df: DataFrame,
    output_table: str,
    note: str = "historical"
) -> None:
    """
    Given the original DataFrame `df`, produce placeholder predictions
    and write them into an Iceberg table named `output_table`.

    In a real scenario, you would load each best model from MLflow's Model Registry
    or from the CV result, then run best_model.transform(df). For demonstration,
    we just create random predictions and show writing to Iceberg.
    """
    logger.info(f"Generating {note} predictions and writing to {output_table}")

    # Example: Add random predictions (for demonstration).
    if "points" in df.columns:
        df = df.withColumn("prediction_points", (rand(seed=42) * 30.0).cast(DoubleType()))
    if "rebounds" in df.columns:
        df = df.withColumn("prediction_rebounds", (rand(seed=99) * 15.0).cast(DoubleType()))

    df.write.format("iceberg").mode("append").save(f"spark_catalog.{output_table}")
    logger.info(f"Wrote {df.count()} rows of {note} predictions to Iceberg table: {output_table}")


def main() -> None:
    """
    Main function:
      1) Create Spark session
      2) Load all data from 'iceberg_nba_player_boxscores'
      3) Clean & feature-engineer data
      4) Train multiple models (LR, GBT) with cross-validation & log results to MLflow
      5) Generate & store historical and future predictions
      6) Additional production suggestions (model registry, drift monitoring, etc.)
    """
    spark = create_spark_session()

    # 1) Load all data
    big_df = load_iceberg_tables(spark, db_name="iceberg_nba_player_boxscores")

    # 2) Clean data + domain-specific features
    clean_df = clean_data(big_df)

    # 3) Train multiple models & log to MLflow
    build_and_train_models(spark, clean_df, mlflow_experiment="PySparkML_Enhanced_NBA")

    # 4) Historical predictions
    generate_and_write_predictions(
        spark,
        clean_df,
        output_table="iceberg_nba_player_boxscores.historical_linear_predictions",
        note="historical"
    )

    # 5) Future predictions
    future_df = clean_df.filter(col("game_date") >= date_add(current_date(), 1))
    generate_and_write_predictions(
        spark,
        future_df,
        output_table="iceberg_nba_player_boxscores.future_linear_predictions",
        note="future"
    )

    spark.stop()
    logger.info("Done executing Enhanced PySpark ML pipeline for NBA boxscores.")


if __name__ == "__main__":
    main()
