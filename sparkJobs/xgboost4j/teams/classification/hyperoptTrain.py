import sys
import logging

import numpy as np
import pandas as pd

from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import desc, col
from pyspark.sql.types import DoubleType, IntegerType, DecimalType

import mlflow
import mlflow.xgboost
from mlflow.models.signature import infer_signature

import xgboost as xgb

from hyperopt import STATUS_OK, Trials, fmin, hp, tpe, SparkTrials
from sklearn.metrics import roc_auc_score, accuracy_score, f1_score, confusion_matrix
from sklearn.model_selection import ShuffleSplit, TimeSeriesSplit
from sklearn.preprocessing import StandardScaler as SklearnStandardScaler

from xgboost.spark import SparkXGBClassifier
from pyspark.ml.feature import VectorAssembler


class XGBoostTrainer:
    """
    Orchestrates training of an XGBoost classifier using Hyperopt for parameter search,
    then trains a final SparkXGBClassifier with the best parameters.
    """

    def __init__(self, logger: logging.Logger) -> None:
        """
        Initialize the XGBoostTrainer with a logger.

        :param logger: Logger for logging messages.
        """
        try:
            self.logger = logger
        except Exception as e:
            logger.error("Error in XGBoostTrainer.__init__(): %s", str(e), exc_info=True)
            sys.exit(1)

    def xgboost_training_with_hyperopt(
        self,
        spark: SparkSession,
        df: DataFrame,
        mlflow_experiment_name: str = "XGBoost_NBA_Team_Classification",
        max_evals: int = 66
    ) -> Dict[str, str]:
        """
        Train an XGBoost classifier using Hyperopt with SparkTrials.
        Returns dict with {"run_id": run_id, "model_uri": model_uri}.
        """
        try:
            mlflow.set_experiment(mlflow_experiment_name)

            # Filter rows missing 'win_loss_binary'
            df = df.filter(col("win_loss_binary").isNotNull())

            # Collect numeric columns (excluding label)
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
                if isinstance(f.dataType, DoubleType)
                and f.name not in ("win_loss_binary")
            ]

            if len(numeric_cols) < 1:
                self.logger.warning("No numeric columns found for classification features.")
                return {}

            # Bring data into Pandas
            pdf = (
                df.orderBy(desc("game_date"))
                .select(*numeric_cols, "win_loss_binary")
                .dropna()
                .toPandas()
            )

            if len(pdf) < 10:
                self.logger.warning("Not enough data for classification; skipping.")
                return {}

            # 1) Split off hold-out
            hold_out_ratio = 0.1
            hold_out_count = int(len(pdf) * hold_out_ratio)

            hold_out_pdf = pdf.iloc[:hold_out_count].copy()
            train_pdf = pdf.iloc[hold_out_count:].copy()

            if len(train_pdf) < 10:
                self.logger.warning("Not enough data left for training after hold-out split.")
                return {}

            # Prepare arrays
            X_full = train_pdf[numeric_cols].values
            y_full = train_pdf["win_loss_binary"].values

            # Scale features
            scaler = SklearnStandardScaler()
            X_scaled = scaler.fit_transform(X_full)

            X_hold_out = hold_out_pdf[numeric_cols].values
            y_hold_out = hold_out_pdf["win_loss_binary"].values
            X_hold_out_scaled = scaler.transform(X_hold_out)

            tscv = TimeSeriesSplit(n_splits=13)
            random_splits = ShuffleSplit(n_splits=11, test_size=0.2, random_state=42)

            def local_cv_score(params: Dict[str, float]) -> float:
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
                        "eval_metric": "auc",
                        "verbosity": 0,
                    }
                    num_round = int(params["num_boost_round"])

                    for k, v in xgb_params.items():
                        mlflow.log_param(k, v)
                    mlflow.log_param("num_boost_round", num_round)

                    auc_scores = []
                    fold_index = 0

                    # TimeSeriesSplit
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
                        if len(set(y_full[test_idx])) > 1:
                            fold_auc = roc_auc_score(y_full[test_idx], preds_proba)
                            auc_scores.append(fold_auc)
                            mlflow.log_metric("fold_auc", fold_auc, step=fold_index)
                        fold_index += 1

                    # Random splits
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

                    return 1.0 - mean_auc

            search_space = {
                "learning_rate": hp.loguniform("learning_rate", np.log(1e-9), np.log(1.0)),
                "max_depth": hp.quniform("max_depth", 1, 50, 1),
                "subsample": hp.uniform("subsample", 0.01, 1.0),
                "colsample_bytree": hp.uniform("colsample_bytree", 0.01, 1.0),
                "reg_alpha": hp.loguniform("reg_alpha", np.log(1e-9), np.log(100)),
                "reg_lambda": hp.loguniform("reg_lambda", np.log(1e-9), np.log(100)),
                "min_child_weight": hp.quniform("min_child_weight", 1, 50, 1),
                "gamma": hp.uniform("gamma", 0.0, 20.0),
                "num_boost_round": hp.quniform("num_boost_round", 50, 2000, 10),
            }

            def objective(params: Dict[str, float]) -> Dict[str, Any]:
                loss = local_cv_score(params)
                return {"loss": loss, "status": STATUS_OK}

            spark_trials = SparkTrials(parallelism=2)

            with mlflow.start_run() as run:
                mlflow.log_param("num_rows", len(train_pdf))
                mlflow.log_param("num_features", len(numeric_cols))
                mlflow.log_param("cv_method", "TimeSeriesSplit(13) + ShuffleSplit(11)")

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

                # Final SparkXGBClassifier training
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
                    use_external_memory=True,
                    max_depth=best_params["max_depth"],
                    eta=best_params["learning_rate"],
                    subsample=best_params["subsample"],
                    colsample_bytree=best_params["colsample_bytree"],
                    reg_alpha=best_params["reg_alpha"],
                    reg_lambda=best_params["reg_lambda"],
                    min_child_weight=best_params["min_child_weight"],
                    gamma=best_params["gamma"],
                    nround=best_params["num_boost_round"],
                    num_early_stopping_rounds=10,
                    checkpoint_path="/tmp/xgb_final_checkpoints",
                    checkpoint_interval=1,
                    num_workers=3
                )

                final_model = xgb_final.fit(final_as)

                # Evaluate final model on the training portion
                pred_sdf = final_model.transform(final_as).select(
                    "probability", "prediction", "win_loss_binary"
                )
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
                        "[Final Model on training set] AUC=%.4f, Accuracy=%.4f, "
                        "F1=%.4f, ConfusionMatrix=%s",
                        final_auc, final_acc, f1_val, cm.tolist()
                    )

                # Evaluate on hold-out
                if len(hold_out_pdf) > 0:
                    hold_out_final_pdf = pd.DataFrame(X_hold_out_scaled, columns=numeric_cols)
                    hold_out_final_pdf["win_loss_binary"] = y_hold_out
                    hold_out_sdf = spark.createDataFrame(hold_out_final_pdf)
                    hold_out_as = assembler.transform(hold_out_sdf)

                    pred_sdf_hold = final_model.transform(hold_out_as).select(
                        "probability", "prediction", "win_loss_binary"
                    )
                    pred_results_hold = pred_sdf_hold.collect()
                    preds_hold = [r["prediction"] for r in pred_results_hold]
                    probs_hold = [r["probability"][1] for r in pred_results_hold]
                    y_hold = [r["win_loss_binary"] for r in pred_results_hold]

                    if len(set(y_hold)) > 1:
                        hold_out_auc = roc_auc_score(y_hold, probs_hold)
                        hold_out_acc = accuracy_score(y_hold, preds_hold)
                        hold_out_f1_val = f1_score(y_hold, preds_hold, average="binary")
                        cm_hold = confusion_matrix(y_hold, preds_hold)

                        mlflow.log_metric("final_hold_out_auc", hold_out_auc)
                        mlflow.log_metric("final_hold_out_accuracy", hold_out_acc)
                        mlflow.log_metric("final_hold_out_f1", hold_out_f1_val)

                        self.logger.info(
                            "[Final Model on HOLD-OUT data] AUC=%.4f, Accuracy=%.4f, "
                            "F1=%.4f, ConfusionMatrix=%s",
                            hold_out_auc, hold_out_acc, hold_out_f1_val, cm_hold.tolist()
                        )

                model_artifact_path = "xgboost_team_win_loss"

                input_example = final_pdf[numeric_cols].iloc[:5]
                signature = infer_signature(
                    input_example,
                    final_pdf["win_loss_binary"].iloc[:5]
                )

                booster = final_model.get_booster()

                mlflow.xgboost.log_model(
                    xgb_model=booster,
                    artifact_path=model_artifact_path,
                    input_example=input_example,
                    signature=signature,
                    pip_requirements=[
                        "xgboost==1.7.5",
                        "scikit-learn==1.2.2",
                        "cloudpickle==2.2.1",
                        "pandas==1.5.3",
                        "numpy==1.23.5"
                    ],
                )

                run_id = run.info.run_id
                model_uri = f"runs:/{run_id}/{model_artifact_path}"
                self.logger.info(
                    "Completed classification training. Model URI: %s", model_uri
                )

                return {"run_id": run_id, "model_uri": model_uri}

        except Exception as e:
            self.logger.error("Error in xgboost_training_with_hyperopt: %s", str(e), exc_info=True)
            sys.exit(1)
