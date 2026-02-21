import sys
import logging

import numpy as np
import pandas as pd
import pywt

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    log,
    abs as spark_abs,
    when,
    lit,
    asc,
    avg as spark_avg,
    stddev as spark_stddev,
    lag as spark_lag
)
from pyspark.sql.types import DoubleType, DecimalType, StructType, StructField
from pyspark.sql.window import Window


class FeatureEngineer:
    """
    Performs advanced feature engineering on numeric columns.
    """

    def __init__(self, logger: logging.Logger) -> None:
        """
        Initialize the FeatureEngineer with a logger.

        :param logger: Logger for logging messages.
        """
        try:
            self.logger = logger
        except Exception as e:
            logger.error("Error in FeatureEngineer.__init__(): %s", str(e), exc_info=True)
            sys.exit(1)

    def spark_feature_engineering(self, df: DataFrame) -> DataFrame:
        """
        Perform advanced feature engineering using Spark DataFrame transformations
        on all numeric columns.
        """
        try:
            # ---------------------------------------------------------
            # 1) Identify & cast numeric columns (excluding label, team_id)
            # ---------------------------------------------------------
            numeric_cols_raw = [
                f.name
                for f in df.schema.fields
                if isinstance(f.dataType, (DoubleType, DecimalType))
                and f.name not in ("win_loss_binary", "team_id")
            ]

            # Cast DecimalType -> DoubleType
            for c in numeric_cols_raw:
                df = df.withColumn(c, col(c).cast(DoubleType()))

            # Re-check numeric columns after cast
            numeric_cols = [
                f.name
                for f in df.schema.fields
                if (isinstance(f.dataType, DoubleType))
                and f.name not in ("win_loss_binary", "team_id")
            ]

            # ---------------------------------------------------------
            # 2) Absolute, squared, log_protected, log
            # ---------------------------------------------------------
            for c in numeric_cols:
                df = df.withColumn(f"{c}_abs", spark_abs(col(c).cast(DoubleType())))
                df = df.withColumn(f"{c}_squared", (col(c) * col(c)).cast(DoubleType()))
                # Protect log transform
                df = df.withColumn(
                    f"{c}_log_protected",
                    when(col(c) <= 0, 1).otherwise(col(c).cast(DoubleType()))
                )
                df = df.withColumn(f"{c}_log", log(col(f"{c}_log_protected")))

            # ---------------------------------------------------------
            # 3) Rolling mean (window=3) & Rolling std dev (window=3)
            # ---------------------------------------------------------
            if "game_date" in df.columns:
                window_spec = (
                    Window.partitionBy("team_id")
                    .orderBy(col("game_date").asc())
                    .rowsBetween(-2, 0)
                )
                for c in numeric_cols:
                    df = df.withColumn(
                        f"{c}_ma_3",
                        spark_avg(col(c)).over(window_spec)
                    )
                    df = df.withColumn(
                        f"{c}_std_3",
                        spark_stddev(col(c)).over(window_spec)
                    )
            else:
                # If no game_date, fallback
                for c in numeric_cols:
                    df = df.withColumn(f"{c}_ma_3", col(c))
                    df = df.withColumn(f"{c}_std_3", col(c))

            # ---------------------------------------------------------
            # 5) Lag & difference features up to 6-game lags
            # ---------------------------------------------------------
            if "game_date" in df.columns:
                lag_window_spec = (
                    Window.partitionBy("team_id").orderBy(col("game_date").asc())
                )
                for c in numeric_cols:
                    for i in range(1, 7):
                        df = df.withColumn(f"{c}_lag_{i}", spark_lag(col(c), i).over(lag_window_spec))
                        df = df.withColumn(f"{c}_diff_{i}", col(c) - col(f"{c}_lag_{i}"))
            else:
                for c in numeric_cols:
                    for i in range(1, 7):
                        df = df.withColumn(f"{c}_lag_{i}", col(c))
                        df = df.withColumn(f"{c}_diff_{i}", col(c))

            # ---------------------------------------------------------
            # 6) Clean up NaNs or infinities => fill with 0
            # ---------------------------------------------------------
            numeric_cols_for_nullfill = [
                f.name
                for f in df.schema.fields
                if isinstance(f.dataType, DoubleType)
                and f.name != "win_loss_binary"
            ]
            for c in numeric_cols_for_nullfill:
                df = df.withColumn(
                    c,
                    when(
                        col(c).isNull() | F.isnan(col(c)) | F.isnull(col(c))
                        | (col(c) == float("inf")) | (col(c) == -float("inf")),
                        0
                    ).otherwise(col(c))
                )

            # ---------------------------------------------------------
            # 7) EMA, Fourier & Wavelet transforms with groupBy/applyInPandas
            # ---------------------------------------------------------
            existing_fields = df.schema.fields
            extended_fields = list(existing_fields)

            for c in numeric_cols:
                extended_fields.append(StructField(f"{c}_ema_3", DoubleType(), True))
                for i in range(1, 4):
                    extended_fields.append(StructField(f"{c}_fft_{i}", DoubleType(), True))
                for i in range(1, 4):
                    extended_fields.append(StructField(f"{c}_wave_{i}", DoubleType(), True))

            extended_schema = StructType(extended_fields)

            def advanced_transform_features(pdf: pd.DataFrame) -> pd.DataFrame:
                # Sort by game_date if present
                if "game_date" in pdf.columns:
                    pdf = pdf.sort_values("game_date")

                for col_name in numeric_cols:
                    arr = pdf[col_name].values

                    # Exponential Moving Average (span=3)
                    pdf[f"{col_name}_ema_3"] = pdf[col_name].ewm(span=3, adjust=False).mean()

                    # FFT transform (top-3)
                    freq = np.fft.rfft(arr)
                    freq_abs = np.abs(freq)
                    top_3_idx = freq_abs.argsort()[::-1][:3]
                    for i in range(1, 4):
                        if i <= len(top_3_idx):
                            pdf[f"{col_name}_fft_{i}"] = freq_abs[top_3_idx[i - 1]]
                        else:
                            pdf[f"{col_name}_fft_{i}"] = 0.0

                    # Wavelet transform (top-3)
                    coeffs = pywt.wavedec(arr, 'db1', level=2)
                    c_concat = np.concatenate(coeffs)
                    c_abs = np.abs(c_concat)
                    top_3_idx_w = c_abs.argsort()[::-1][:3]
                    for i in range(1, 4):
                        if i <= len(top_3_idx_w):
                            pdf[f"{col_name}_wave_{i}"] = c_abs[top_3_idx_w[i - 1]]
                        else:
                            pdf[f"{col_name}_wave_{i}"] = 0.0

                return pdf

            if "team_id" in df.columns:
                df = df.groupBy("team_id").applyInPandas(
                    advanced_transform_features,
                    schema=extended_schema
                )
            else:
                single_group_key = lit("dummy_group")
                df = df.groupBy(single_group_key).applyInPandas(
                    advanced_transform_features,
                    schema=extended_schema
                ).drop("dummy_group")

            # Null-fill pass for newly added columns
            new_cols_for_nullfill = [
                f.name
                for f in df.schema.fields
                if isinstance(f.dataType, DoubleType)
                and f.name not in numeric_cols_for_nullfill
            ]
            for c in new_cols_for_nullfill:
                df = df.withColumn(
                    c,
                    when(
                        col(c).isNull() | F.isnan(col(c)) | F.isnull(col(c))
                        | (col(c) == float("inf")) | (col(c) == -float("inf")),
                        0
                    ).otherwise(col(c))
                )

            self.logger.info(
                "Spark-based feature engineering (EMA, Rolling Std, up to 6-game Lag/Diff, FFT/Wavelet) complete."
            )
            return df

        except Exception as e:
            self.logger.error(
                "Error in spark-based feature engineering: %s",
                str(e),
                exc_info=True
            )
            sys.exit(1)
