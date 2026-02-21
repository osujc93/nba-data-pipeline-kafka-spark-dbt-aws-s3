#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA_player_boxscores_classification.py

OVERVIEW:
---------
This is the main entry point for orchestrating an end-to-end pipeline to classify
an NBA team's win/loss outcomes. It uses multiple classes defined across separate
modules, each containing a specific portion of the logic (data loading, cleaning,
feature engineering, Lasso feature selection, XGBoost hyperparameter tuning,
predictions, etc.).

Key changes requested:
1) Convert 0 -> "L" and 1 -> "W" using direct mapping.
2) Add checkpointing for XGBoost training (checkpointPath & checkpointInterval).
3) Exclude 'team_id' from numeric features (not used as a numeric predictor).
4) Lower Lasso strict threshold from 0.01 to 0.001 (or smaller).
5) Increase max_evals or refine hyperparameter ranges in the hyperopt search.
6) Use more cross-validation splits and ensure all training and cross validations
   are logged.

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

Additionally, exclude 'team_id' from wavelet transforms to avoid
"Missing: team_id_fft_3."
"""

import sys
from pipelineWorkflow import NBAPlayerBoxscoreClassificationPipeline


def main() -> None:
    """
    Main function to kick off the classification pipeline.
    """
    pipeline = NBAPlayerBoxscoreClassificationPipeline()
    pipeline.main()


if __name__ == "__main__":
    # Let the pipeline do all the work.
    sys.exit(main())
