#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import logging
import threading

from sparkManager import SparkManager
from loadData import DataLoader
from cleanData import DataCleaner
from featureEngineer import FeatureEngineer
from lassoSelect import LassoFeatureSelector
from hyperoptTrain import XGBoostTrainer
from xgboostPredict import Predictor


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class NBATeamBoxscoreClassificationPipeline:
    """
    Pipeline class that orchestrates everything end-to-end.
    """

    def __init__(self) -> None:
        """
        Initialize the pipeline with a logger and the needed helper-class objects.
        """
        try:
            self.logger: logging.Logger = logging.getLogger(__name__)
            self.spark_manager = SparkManager(self.logger)
            self.data_loader = DataLoader(self.logger)
            self.data_cleaner = DataCleaner(self.logger)
            self.feature_engineer = FeatureEngineer(self.logger)
            self.lasso_selector = LassoFeatureSelector(self.logger)
            self.xgb_trainer = XGBoostTrainer(self.logger)
            self.predictor = Predictor(self.logger)

        except Exception as e:
            logging.error("Error in NBATeamBoxscoreClassificationPipeline.__init__(): %s", str(e), exc_info=True)
            sys.exit(1)

    def main(self) -> None:
        """
        Main entry point for the NBA Team Boxscore Classification Pipeline.
        Orchestrates the entire process end-to-end.
        """
        try:
            # 1) Create Spark session
            spark = self.spark_manager.create_spark_session()

            # 2) Load scd_team_boxscores
            df_team = self.data_loader.load_team_boxscores(spark)

            # 3) Clean & partition
            cleaned_df = self.data_cleaner.clean_and_partition_data(df_team)

            # 4) Feature engineering
            fe_df = self.feature_engineer.spark_feature_engineering(cleaned_df).persist()

            # 5) LassoCV for feature selection
            lasso_dict = self.lasso_selector.run_lasso_feature_selection(
                spark,
                fe_df,
                mlflow_experiment_name="NBA_Lasso_Feature_Selection",
                coefficient_strict_threshold=0.0
            )
            table_target_key = "scd_team_boxscores__win_loss_binary"

            if table_target_key in lasso_dict and len(lasso_dict[table_target_key]) > 0:
                selected_feats = lasso_dict[table_target_key]
                fe_df = fe_df.select(*selected_feats, "win_loss_binary")
                self.logger.info(
                    "Reduced dataset to Lasso-selected features: %d columns.",
                    len(selected_feats)
                )
            else:
                self.logger.info(
                    "No Lasso-filtered features found. Using all numeric features."
                )

            # 6) XGBoost => Bayesian optimization + cross-validation
            model_info = self.xgb_trainer.xgboost_training_with_hyperopt(
                spark,
                fe_df,
                mlflow_experiment_name="XGBoost_NBA_Team_Classification",
                max_evals=66
            )

            # 7) Generate predictions
            if model_info:
                self.predictor.generate_predictions_and_write_to_iceberg(
                    spark,
                    fe_df,
                    model_info=model_info,
                    db_name="iceberg_nba_player_boxscores_classification",
                )


            fe_df.unpersist()           # unpersist the feature-engineered DF
            df_team.unpersist()         # unpersist the loaded DF
            
            spark.stop()
            self.logger.info(
                "Done executing main pipeline for NBA Team Classification."
            )

        except Exception as ex:
            self.logger.error("Unexpected error in main(): %s", str(ex), exc_info=True)
            sys.exit(1)
