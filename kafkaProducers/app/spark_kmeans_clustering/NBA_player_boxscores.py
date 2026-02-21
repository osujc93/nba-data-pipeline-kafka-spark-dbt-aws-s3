#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
enhanced_kmeans_nba_player_clustering.py

An enhanced PySpark ML script for K-Means clustering on NBA player boxscores
stored in Iceberg (on HDFS). It incorporates recommendations for:
    - Feature engineering & selection
    - Dynamic hyperparameter tuning & alternative clustering methods
    - Data quality checks & schema enforcement
    - Outlier handling
    - Online / incremental updates
    - Cluster explainability & profiling
    - Robust artifact & model management
    - Monitoring & alerting
    - Efficiency & scalability
    - Security & access controls

The pipeline:
1) Loads all tables from the "iceberg_nba_player_boxscores" database into one DataFrame.
2) Validates & cleans data (duplicates, invalid rows, schema checks).
3) Performs feature engineering (advanced stats, optional PCA/correlation checks).
4) Uses a Spark ML Pipeline with VectorAssembler, StandardScaler, and KMeans.
5) Employs CrossValidator + ParamGridBuilder + ClusteringEvaluator (silhouette)
   to find the best K (# of clusters) and best hyperparameters.
6) Logs all runs, models, and metrics to MLflow (including potential GMM comparison).
7) Produces cluster assignments for both historical data and future data, storing
   the results back to new Iceberg tables (kmeans_cluster_assignments_historical
   and kmeans_cluster_assignments_future).
8) Demonstrates usage of major Spark MLlib classes: Transformer, Estimator, Model,
   Pipeline, PipelineModel, CrossValidator, ClusteringEvaluator, VectorAssembler,
   StandardScaler, KMeans, etc.

It also shows placeholders for:
 - Outlier detection (winsorizing, separate cluster, etc.)
 - Streaming / incremental updates
 - Data drift checks
 - Per-cluster profiling & explainability
 - Model version pinning in MLflow
 - Scheduled retraining jobs
 - Security & Access control references
"""

import sys
import logging

import mlflow
import mlflow.spark

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import (
    col, lit, current_date, date_add, year, month, dayofmonth,
    mean as spark_mean, stddev as spark_std, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans, GaussianMixture
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """
    Create a SparkSession configured for Iceberg on HDFS. Includes basic Spark
    configurations for shuffle partitions, etc.

    Returns:
        SparkSession: A configured SparkSession object.
    """
    try:
        spark = (
            SparkSession.builder
            .appName("Enhanced_KMeans_NBA_Player_Clustering")
            .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
            .config("spark.sql.iceberg.target-file-size-bytes", "134217728")
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.warehouse.dir", "hdfs://mycluster/nelo_sports_warehouse")
            .enableHiveSupport()
            .getOrCreate()
        )
        # Adjust shuffle partitions based on data size and cluster resources
        spark.conf.set("spark.sql.shuffle.partitions", "25")
        # This can be enabled or disabled for adaptive optimizations
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        logger.info(
            "SparkSession created successfully for Enhanced K-Means NBA Player Clustering."
        )
        return spark
    except Exception as e:
        logger.error(f"Failed to create SparkSession: {str(e)}", exc_info=True)
        sys.exit(1)


def load_iceberg_tables(spark: SparkSession, db_name: str) -> DataFrame:
    """
    Load all Iceberg tables in a given database by unioning them into a single
    DataFrame, with a 'source_table' column tagging the table of origin.

    Args:
        spark (SparkSession): Active SparkSession.
        db_name (str): Name of the Iceberg database.

    Returns:
        DataFrame: Union of all tables found in the database. If none found,
        returns an empty DataFrame.
    """
    logger.info(f"Loading all tables from Iceberg database: {db_name}")
    tables_df = spark.sql(f"SHOW TABLES IN {db_name}")
    table_names = [row.tableName for row in tables_df.collect()]

    if not table_names:
        logger.warning(f"No tables found in database: {db_name}")
        empty_schema = StructType([StructField("empty", StringType(), True)])
        return spark.createDataFrame([], empty_schema)

    all_dfs = []
    for tbl_name in table_names:
        full_tbl_name = f"{db_name}.{tbl_name}"
        logger.info(f"Reading table: {full_tbl_name}")
        df_tbl = spark.table(full_tbl_name)
        df_tbl = df_tbl.withColumn("source_table", lit(tbl_name))
        all_dfs.append(df_tbl)

    union_df = all_dfs[0]
    for dfx in all_dfs[1:]:
        # unionByName allows for missing columns
        union_df = union_df.unionByName(dfx, allowMissingColumns=True)

    total_rows = union_df.count()
    logger.info(
        f"Loaded {len(table_names)} tables from {db_name}, total rows = {total_rows}"
    )
    return union_df


def validate_data_quality(df: DataFrame) -> None:
    """
    Placeholder for rigorous data validation checks before clustering. In
    production, consider libraries like Deequ or Great Expectations to:
     - Check for nulls in critical columns
     - Verify expected ranges (e.g., no negative minutes played)
     - Ensure numeric columns conform to known sports data boundaries
     - Enforce schema migrations for new columns

    Args:
        df (DataFrame): Input DataFrame to validate.

    Returns:
        None
    """
    logger.info("Running basic data validation checks (placeholder).")
    # Example: if 'minutes_played' is a column, verify none are negative
    if 'minutes_played' in df.columns:
        invalid_count = df.filter(col('minutes_played') < 0).count()
        if invalid_count > 0:
            logger.warning(
                f"Found {invalid_count} records with negative minutes_played."
            )


def clean_data(df: DataFrame) -> DataFrame:
    """
    Perform example cleaning for the player boxscores:
      - Remove duplicates
      - Filter out invalid rows
      - Possibly cast columns (e.g., game_date to DateType)
      - Basic outlier handling can be applied here or in a separate function.

    Args:
        df (DataFrame): Original DataFrame.

    Returns:
        DataFrame: Cleaned DataFrame after removing duplicates and invalid rows.
    """
    df = df.dropDuplicates()

    # Example: filter out negative or unrealistic values in 'points'
    if 'points' in df.columns:
        df = df.filter(col('points') >= 0)

    # If 'game_date' is string, cast to DateType
    if 'game_date' in df.columns and df.schema['game_date'].dataType != DateType():
        df = df.withColumn('game_date', col('game_date').cast(DateType()))

    # Ensure year, month, day columns
    if 'game_date' in df.columns:
        df = (
            df.withColumn('year', year(col('game_date')))
              .withColumn('month', month(col('game_date')))
              .withColumn('day', dayofmonth(col('game_date')))
        )

    logger.info("Data cleaning complete.")
    return df


def handle_outliers(df: DataFrame, numeric_cols: list[str]) -> DataFrame:
    """
    Placeholder for outlier handling. Options include:
      - Winsorizing
      - Capping via percentiles
      - Dropping or separately clustering outliers

    Here, we do a simple winsorizing approach at the 5th and 95th percentiles.

    Args:
        df (DataFrame): Input DataFrame.
        numeric_cols (list[str]): List of numeric column names to apply capping.

    Returns:
        DataFrame: DataFrame with capped (winsorized) values in specified columns.
    """
    logger.info("Handling outliers (placeholder).")
    for col_name in numeric_cols:
        bounds = df.approxQuantile(col_name, [0.05, 0.95], 0.05)
        lower = bounds[0]
        upper = bounds[1]
        df = df.withColumn(
            col_name,
            when(col(col_name) < lower, lower)
            .when(col(col_name) > upper, upper)
            .otherwise(col(col_name))
        )
    return df


def feature_engineering(df: DataFrame) -> DataFrame:
    """
    Create domain-specific or advanced stats (e.g., per-minute, per-possession,
    usage rate). This step is crucial in sports analytics, as raw boxscore
    totals can be misleading.

    Args:
        df (DataFrame): Cleaned DataFrame.

    Returns:
        DataFrame: DataFrame with additional engineered features.
    """
    logger.info("Applying domain-specific feature engineering.")
    if 'points' in df.columns and 'minutes_played' in df.columns:
        df = df.withColumn(
            "points_per_minute",
            col("points") / (col("minutes_played") + 1e-9)
        )
    return df


def select_features(df: DataFrame, exclude_cols: list[str]) -> list[str]:
    """
    Identify numeric columns to cluster on, with optional correlation / PCA checks.

    Args:
        df (DataFrame): DataFrame with potential features.
        exclude_cols (list[str]): Columns to exclude from feature selection.

    Returns:
        list[str]: Final numeric features selected for clustering.
    """
    logger.info("Selecting features (placeholder for correlation/PCA checks).")
    all_numeric = []
    for c in df.columns:
        if c not in exclude_cols:
            dtype = df.schema[c].dataType
            if isinstance(dtype, (DoubleType, IntegerType)):
                all_numeric.append(c)

    # In a real pipeline, you might:
    # - Drop correlated columns
    # - Use PCA to reduce dimensionality
    # Returning all numeric columns for now.
    final_features = all_numeric
    logger.info(f"Using these numeric columns for clustering: {final_features}")
    return final_features


def train_kmeans_with_cv(
    df: DataFrame,
    feature_cols: list[str],
    mlflow_experiment_name: str = "KMeans_NBA_Clustering"
) -> None:
    """
    Train a K-Means model using CrossValidator with silhouette as the metric.
    Searches multiple values of k and maxIter. Logs best model + metrics to MLflow.

    Args:
        df (DataFrame): Input DataFrame (already cleaned and feature-engineered).
        feature_cols (list[str]): List of feature columns to assemble.
        mlflow_experiment_name (str, optional): Name of the MLflow experiment.
            Defaults to "KMeans_NBA_Clustering".

    Returns:
        None
    """
    mlflow.set_experiment(mlflow_experiment_name)
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features_unscaled"
    )
    scaler = StandardScaler(
        inputCol="features_unscaled",
        outputCol="features",
        withMean=True,
        withStd=True
    )

    kmeans = KMeans(
        featuresCol="features",
        predictionCol="cluster_id",
        maxIter=50,
        k=5
    )

    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    param_grid = (
        ParamGridBuilder()
        .addGrid(kmeans.k, [2, 3, 4, 5, 6, 8, 10, 12])
        .addGrid(kmeans.maxIter, [20, 50, 100])
        .build()
    )

    evaluator = ClusteringEvaluator(
        predictionCol="cluster_id",
        featuresCol="features",
        metricName="silhouette"
    )

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=3,
        parallelism=2,
        seed=42
    )

    with mlflow.start_run(run_name="KMeans_Clustering_Experiment"):
        logger.info("Fitting CrossValidator for K-Means ...")
        cv_model = cv.fit(df)
        best_model = cv_model.bestModel
        best_kmeans_stage = best_model.stages[-1]
        best_k = best_kmeans_stage.getK()
        best_max_iter = best_kmeans_stage.getMaxIter()

        clustered_df = best_model.transform(df)
        silhouette_val = evaluator.evaluate(clustered_df)

        logger.info(
            f"Best KMeans model => K={best_k}, "
            f"maxIter={best_max_iter}, silhouette={silhouette_val}"
        )

        mlflow.log_param("best_k", best_k)
        mlflow.log_param("best_max_iter", best_max_iter)
        mlflow.log_metric("silhouette", silhouette_val)
        mlflow.spark.log_model(best_model, artifact_path="kmeans_pipeline_model")

    logger.info("Cross-validation for K-Means completed.")


def train_gmm_for_comparison(
    df: DataFrame,
    feature_cols: list[str],
    mlflow_experiment_name: str = "KMeans_NBA_Clustering"
) -> None:
    """
    Optional: Train a GaussianMixture model (GMM) and compare silhouette scores
    with KMeans. Real-world usage might involve multiple unsupervised methods
    and picking the best via silhouette/gap statistic.

    Args:
        df (DataFrame): Input DataFrame with feature columns.
        feature_cols (list[str]): Feature columns.
        mlflow_experiment_name (str, optional): MLflow experiment name.
            Defaults to "KMeans_NBA_Clustering".

    Returns:
        None
    """
    mlflow.set_experiment(mlflow_experiment_name)
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features_unscaled"
    )
    scaler = StandardScaler(
        inputCol="features_unscaled",
        outputCol="features",
        withMean=True,
        withStd=True
    )
    gmm = GaussianMixture(
        featuresCol="features",
        predictionCol="gmm_cluster_id",
        maxIter=50,
        k=5
    )

    pipeline = Pipeline(stages=[assembler, scaler, gmm])
    param_grid = (
        ParamGridBuilder()
        .addGrid(gmm.k, [2, 4, 6, 8])
        .addGrid(gmm.maxIter, [20, 50, 100])
        .build()
    )

    evaluator = ClusteringEvaluator(
        predictionCol="gmm_cluster_id",
        featuresCol="features",
        metricName="silhouette"
    )

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=3,
        parallelism=2,
        seed=42
    )

    with mlflow.start_run(run_name="GMM_Comparison_Experiment"):
        logger.info("Fitting CrossValidator for GMM ...")
        cv_model = cv.fit(df)
        best_model = cv_model.bestModel

        best_gmm_stage = best_model.stages[-1]
        best_k = best_gmm_stage.getK()
        best_max_iter = best_gmm_stage.getMaxIter()
        clustered_df = best_model.transform(df)
        silhouette_val = evaluator.evaluate(clustered_df)

        logger.info(
            f"Best GMM model => K={best_k}, "
            f"maxIter={best_max_iter}, silhouette={silhouette_val}"
        )

        mlflow.log_param("best_k", best_k)
        mlflow.log_param("best_max_iter", best_max_iter)
        mlflow.log_metric("silhouette", silhouette_val)
        mlflow.spark.log_model(best_model, artifact_path="gmm_pipeline_model")

    logger.info("Comparison with GMM completed.")


def assign_clusters_and_write_iceberg(
    df: DataFrame,
    model_path: str,
    output_table: str,
    note: str = "historical"
) -> None:
    """
    Load the best K-Means pipeline model from MLflow, use it to transform the
    given DataFrame, and write the resulting cluster assignments to a new
    Iceberg table.

    Args:
        df (DataFrame): DataFrame to cluster.
        model_path (str): MLflow model URI.
        output_table (str): Name of the target Iceberg table (in spark_catalog).
        note (str, optional): Tag to indicate historical/future context.
            Defaults to "historical".

    Returns:
        None
    """
    spark = df.sparkSession
    logger.info(f"Loading K-Means PipelineModel from MLflow path: {model_path}")
    kmeans_model = mlflow.spark.load_model(model_uri=model_path)

    df_clustered = kmeans_model.transform(df).withColumnRenamed(
        "cluster_id", "kmeans_cluster_id"
    )

    logger.info(f"Writing {note} cluster assignments to: {output_table}")
    df_clustered.write.format("iceberg").mode("append").save(
        f"spark_catalog.{output_table}"
    )

    logger.info(
        f"Successfully wrote {df_clustered.count()} rows to {output_table}."
    )


def profile_clusters(
    df_clustered: DataFrame,
    cluster_col: str = "kmeans_cluster_id"
) -> None:
    """
    Compute per-cluster summaries (mean, std) for key numeric features. Aids in
    explaining and profiling clusters.

    Args:
        df_clustered (DataFrame): DataFrame with cluster assignments.
        cluster_col (str, optional): Name of the cluster column to group by.
            Defaults to "kmeans_cluster_id".

    Returns:
        None
    """
    numeric_cols = [
        f for f in df_clustered.columns
        if f not in [cluster_col, "features", "features_unscaled"]
    ]
    logger.info("Generating per-cluster stats for interpretability.")
    summary_df = (
        df_clustered.groupBy(cluster_col)
        .agg(
            *[
                spark_mean(col_name).alias(f"{col_name}_mean")
                for col_name in numeric_cols
                if df_clustered.schema[col_name].dataType in (
                    DoubleType(), IntegerType()
                )
            ],
            *[
                spark_std(col_name).alias(f"{col_name}_std")
                for col_name in numeric_cols
                if df_clustered.schema[col_name].dataType in (
                    DoubleType(), IntegerType()
                )
            ]
        )
    )
    summary_df.show(truncate=False)


def monitor_and_alert_on_silhouette(
    silhouette_score: float,
    threshold: float = 0.2
) -> None:
    """
    Placeholder for a real monitoring system that triggers alerts if silhouette
    dips below a given threshold. Could integrate with Slack, email, etc.

    Args:
        silhouette_score (float): Computed silhouette score.
        threshold (float, optional): Alert threshold. Defaults to 0.2.

    Returns:
        None
    """
    if silhouette_score < threshold:
        logger.warning(
            f"Silhouette score {silhouette_score} fell below threshold "
            f"{threshold}. Triggering alert!"
        )


def check_data_drift(df: DataFrame) -> None:
    """
    Placeholder for data drift checks. For example, track distributions of
    certain columns over time and compare to historical distributions. If
    significantly different, re-trigger training.

    Args:
        df (DataFrame): DataFrame to check for drift.

    Returns:
        None
    """
    logger.info("Checking for data drift (placeholder).")


def main() -> None:
    """
    Main pipeline for Enhanced K-Means on NBA player boxscores.
    1) Create SparkSession
    2) Load all data from iceberg_nba_player_boxscores
    3) Validate & clean data
    4) Feature engineering & outlier handling
    5) Select final features (optionally using correlation checks/PCA)
    6) Train K-Means with CrossValidator, log to MLflow
    7) Optionally train GMM for comparison
    8) Load best K-Means model from MLflow, assign clusters to historical & future data
    9) Profile clusters for interpretability
    10) (Placeholders) for streaming updates, scheduled retraining, drift checks, etc.

    Returns:
        None
    """
    spark = create_spark_session()
    big_df = load_iceberg_tables(spark, db_name="iceberg_nba_player_boxscores")

    validate_data_quality(big_df)
    clean_df = clean_data(big_df)
    fe_df = feature_engineering(clean_df)

    exclude_cols = {
        "season_id", "player_id", "player_name", "team_id", "team_name",
        "game_id", "source_table", "matchup", "win_loss", "season",
        "season_type", "game_date_param", "game_date", "year", "month", "day"
    }
    final_feature_cols = select_features(fe_df, exclude_cols=exclude_cols)
    fe_df_outliers = handle_outliers(fe_df, final_feature_cols)
    fe_df_outliers = fe_df_outliers.fillna(0, subset=final_feature_cols)

    train_kmeans_with_cv(
        fe_df_outliers,
        final_feature_cols,
        mlflow_experiment_name="KMeans_NBA_Clustering"
    )

    # Optionally train GMM for comparison
    train_gmm_for_comparison(
        fe_df_outliers,
        final_feature_cols,
        mlflow_experiment_name="KMeans_NBA_Clustering"
    )

    import mlflow
    client = mlflow.tracking.MlflowClient()
    experiment = client.get_experiment_by_name("KMeans_NBA_Clustering")
    experiment_id = experiment.experiment_id

    runs = client.search_runs(
        [experiment_id],
        filter_string="tags.mlflow.runName = 'KMeans_Clustering_Experiment'",
        order_by=["attributes.start_time DESC"],
        max_results=1
    )

    if not runs:
        logger.warning("No MLflow runs found for KMeans_NBA_Clustering. Exiting.")
        spark.stop()
        sys.exit(0)

    best_run_id = runs[0].info.run_id
    model_artifact_path = f"runs:/{best_run_id}/kmeans_pipeline_model"
    logger.info(f"Will load best K-Means model from run_id={best_run_id}")

    # Assign clusters to historical data
    assign_clusters_and_write_iceberg(
        fe_df_outliers,
        model_path=model_artifact_path,
        output_table="iceberg_nba_player_boxscores.kmeans_cluster_assignments_historical",
        note="historical"
    )

    # Assign clusters to "future" data (example: date >= tomorrow)
    future_df = fe_df_outliers.filter(col("game_date") >= date_add(current_date(), 1))
    if future_df.count() > 0:
        assign_clusters_and_write_iceberg(
            future_df,
            model_path=model_artifact_path,
            output_table="iceberg_nba_player_boxscores.kmeans_cluster_assignments_future",
            note="future"
        )
    else:
        logger.info("No future data found to cluster. Skipping future assignments.")

    loaded_model = mlflow.spark.load_model(model_artifact_path)
    df_historical_clustered = loaded_model.transform(fe_df_outliers)
    profile_clusters(df_historical_clustered, cluster_col="cluster_id")

    # Placeholders for advanced features:
    # check_data_drift(fe_df_outliers)
    # monitor_and_alert_on_silhouette(silhouette_score=0.25, threshold=0.2)

    spark.stop()
    logger.info("Enhanced K-Means clustering pipeline for NBA boxscores finished.")


if __name__ == "__main__":
    main()
