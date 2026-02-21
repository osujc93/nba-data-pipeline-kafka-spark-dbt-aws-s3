import sys
import time
import psutil
import cProfile
import pstats
import io
import logging
import threading

from typing import Dict

from pyspark.sql import SparkSession, DataFrame

# Our custom classes
from statsLogger import StatsLogger
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


class NBAPlayerBoxscoreClassificationPipeline:
    """
    Pipeline class that orchestrates data loading, cleaning, feature engineering,
    Lasso feature selection, XGBoost training, and final predictions for the
    NBA Player Boxscore Classification scenario.
    """

    def __init__(self) -> None:
        """
        Initialize the pipeline with a logger and a profiler, and
        instantiate all needed helper-class objects.
        """
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.profiler: cProfile.Profile = cProfile.Profile()

        self.stats_logger = StatsLogger(self.profiler, self.logger)
        self.spark_manager = SparkManager(self.logger)
        self.data_loader = DataLoader(self.logger)
        self.data_cleaner = DataCleaner(self.logger)
        self.feature_engineer = FeatureEngineer(self.logger)
        self.lasso_selector = LassoFeatureSelector(self.logger)
        self.xgb_trainer = XGBoostTrainer(self.logger)
        self.predictor = Predictor(self.logger)

    def main(self) -> None:
        """
        Main entry point for the NBA Player Boxscore Classification Pipeline.
        Orchestrates the entire process end-to-end.
        """
        start_cpu_time_main = time.process_time()
        start_cpu_percent_main = psutil.cpu_percent()

        try:
            self.profiler.enable()
            stats_thread = threading.Thread(
                target=self.stats_logger.log_stats_periodically,
                args=(120,),
                daemon=True,
            )
            stats_thread.start()

            # 1) Create Spark session
            spark = self.spark_manager.create_spark_session()

            # 2) Load scd_player_boxscores
            df_player = self.data_loader.load_player_boxscores(spark)

            # 3) Clean & partition
            cleaned_df = self.data_cleaner.clean_and_partition_data(df_player)

            # 4) Feature engineering (includes Fourier/Wavelet)
            fe_df = self.feature_engineer.spark_feature_engineering(cleaned_df).persist()

            # 5) LassoCV for feature selection
            lasso_dict = self.lasso_selector.run_lasso_feature_selection(
                spark,
                fe_df,
                mlflow_experiment_name="NBA_Lasso_Feature_Selection",
                coefficient_strict_threshold=0.00000001
            )
            table_target_key = "scd_player_boxscores__win_loss_binary"

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
                mlflow_experiment_name="XGBoost_NBA_Player_Classification",
                max_evals=55
            )

            # 7) Generate predictions
            if model_info:
                self.predictor.generate_predictions_and_write_to_iceberg(
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

            self.logger.info(
                "Done executing main pipeline for NBA Player Classification."
            )

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
