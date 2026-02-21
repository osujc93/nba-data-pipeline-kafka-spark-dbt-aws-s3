import sys
import psutil
import logging
import time

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
    Performs advanced feature engineering on numeric columns, including absolute
    values, squared, log-protected transforms, moving averages, rolling std dev,
    lag/diff features (up to 6 lags), exponential moving average, and Fourier/Wavelet transformations.
    """

    def __init__(self, logger: logging.Logger) -> None:
        """
        Initialize the FeatureEngineer with a logger.

        :param logger: Logger for logging messages.
        """
        self.logger = logger

    def spark_feature_engineering(self, df: DataFrame) -> DataFrame:
        """
        Perform advanced feature engineering using Spark DataFrame transformations
        on all numeric columns, excluding the label ('win_loss_binary') and 'team_id'.
        
        Steps included:
        1) Type casting Decimal -> Double.
        2) Absolute, squared, log transforms.
        3) Rolling mean (window=3).
        4) Rolling std dev (window=3).
        5) Lag & difference features (up to 6-game lags).
        6) Fill NaN/inf -> 0.
        7) Exponential moving average (EMA), Fourier, and Wavelet transformations 
           via groupBy/applyInPandas, sorted by game_date.

        :param df: Input Spark DataFrame.
        :return: Spark DataFrame with additional engineered columns.
        """
        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()
        try:
            # ---------------------------------------------------------
            # 1) Identify & cast numeric columns (excluding label, team_id)
            # ---------------------------------------------------------
            numeric_cols_raw = [
                f.name
                for f in df.schema.fields
                if isinstance(f.dataType, (DoubleType, DecimalType))
                and f.name not in ("win_loss_binary", "team_id", "player_id")
            ]

            # Cast DecimalType -> DoubleType
            for c in numeric_cols_raw:
                df = df.withColumn(c, col(c).cast(DoubleType()))

            # Re-check numeric columns after cast
            numeric_cols = [
                f.name
                for f in df.schema.fields
                if (isinstance(f.dataType, DoubleType))
                and f.name not in ("win_loss_binary", "team_id", "player_id")
            ]

            # ---------------------------------------------------------
            # 2) Create columns: absolute, squared, log_protected, log
            # ---------------------------------------------------------
            for c in numeric_cols:
                df = df.withColumn(f"{c}_abs", spark_abs(col(c).cast(DoubleType())))
                df = df.withColumn(f"{c}_squared", (col(c) * col(c)).cast(DoubleType()))
                # Protect log transform: if <= 0, set to 1 for safety
                df = df.withColumn(
                    f"{c}_log_protected",
                    when(col(c) <= 0, 1).otherwise(col(c).cast(DoubleType()))
                )
                df = df.withColumn(f"{c}_log", log(col(f"{c}_log_protected")))

            # ---------------------------------------------------------
            # 3) Rolling mean (window=3) & 4) Rolling std dev (window=3)
            # ---------------------------------------------------------
            if "game_date" in df.columns:
                window_spec = (
                    Window.partitionBy("player_id")
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
                    Window.partitionBy("player_id").orderBy(col("game_date").asc())
                )
                for c in numeric_cols:
                    # We'll create c_lag_1...c_lag_6 and c_diff_1...c_diff_6
                    for i in range(1, 7):
                        df = df.withColumn(f"{c}_lag_{i}", spark_lag(col(c), i).over(lag_window_spec))
                        df = df.withColumn(f"{c}_diff_{i}", col(c) - col(f"{c}_lag_{i}"))
            else:
                # If no game_date, fallback to same column
                for c in numeric_cols:
                    for i in range(1, 7):
                        df = df.withColumn(f"{c}_lag_{i}", col(c))
                        df = df.withColumn(f"{c}_diff_{i}", col(c))

            # ---------------------------------------------------------
            # 6) Clean up NaNs or infinities => fill numeric columns with 0
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
            # 7) Exponential Moving Average (EMA), Fourier & Wavelet
            #    transformations with groupBy/applyInPandas.
            #
            #    We'll create new columns for:
            #      - c_ema_3 (EMA with span=3)
            #      - c_fft_1, c_fft_2, c_fft_3
            #      - c_wave_1, c_wave_2, c_wave_3
            # ---------------------------------------------------------
            existing_fields = df.schema.fields
            extended_fields = list(existing_fields)

            # For each original numeric column, define new columns in schema
            for c in numeric_cols:
                # Exponential Moving Average
                extended_fields.append(StructField(f"{c}_ema_3", DoubleType(), True))
                # FFT top-3
                for i in range(1, 4):
                    extended_fields.append(StructField(f"{c}_fft_{i}", DoubleType(), True))
                # Wavelet top-3
                for i in range(1, 4):
                    extended_fields.append(StructField(f"{c}_wave_{i}", DoubleType(), True))

            extended_schema = StructType(extended_fields)

            def advanced_transform_features(pdf: pd.DataFrame) -> pd.DataFrame:
                # Sort by game_date if present
                if "game_date" in pdf.columns:
                    pdf = pdf.sort_values("game_date")

                # For each numeric column, compute:
                # 1) Exponential Moving Average (span=3)
                # 2) FFT top 3 absolute coefficients
                # 3) Wavelet top 3 absolute coefficients
                for col_name in numeric_cols:
                    arr = pdf[col_name].values

                    # ---- Exponential Moving Average (window=3 → span=3) ----
                    pdf[f"{col_name}_ema_3"] = pdf[col_name].ewm(span=3, adjust=False).mean()

                    # ---- FFT transform (take top-3) ----
                    freq = np.fft.rfft(arr)  # rfft for real-valued signals
                    freq_abs = np.abs(freq)
                    top_3_idx = freq_abs.argsort()[::-1][:3]
                    for i in range(1, 4):
                        if i <= len(top_3_idx):
                            pdf[f"{col_name}_fft_{i}"] = freq_abs[top_3_idx[i - 1]]
                        else:
                            pdf[f"{col_name}_fft_{i}"] = 0.0

                    # ---- Wavelet transform (take top-3) ----
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

            # Group by player_id if present, otherwise dummy group
            if "player_id" in df.columns:
                df = df.groupBy("player_id").applyInPandas(
                    advanced_transform_features,
                    schema=extended_schema
                )
            else:
                single_group_key = lit("dummy_group")
                df = df.groupBy(single_group_key).applyInPandas(
                    advanced_transform_features,
                    schema=extended_schema
                ).drop("dummy_group")

            # One more null-fill pass for newly added columns:
            new_cols_for_nullfill = [
                f.name
                for f in df.schema.fields
                if isinstance(f.dataType, DoubleType)
                and f.name not in numeric_cols_for_nullfill  # newly created
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

        finally:
            end_cpu_time = time.process_time()
            end_cpu_percent = psutil.cpu_percent()
            self.logger.info(
                "CPU usage start: %s%%, end: %s%%. CPU time for "
                "spark_feature_engineering(): %.4f seconds",
                start_cpu_percent,
                end_cpu_percent,
                end_cpu_time - start_cpu_time,
            )
