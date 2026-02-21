import sys
import psutil
import logging
import time

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit


class DataCleaner:
    """
    Cleans the data by removing duplicates, filtering out invalid values, and
    creating a binary column for win/loss classification.
    """

    def __init__(self, logger: logging.Logger) -> None:
        """
        Initialize the DataCleaner with a logger.

        :param logger: Logger for logging messages.
        """
        self.logger = logger

    def clean_and_partition_data(self, df: DataFrame) -> DataFrame:
        """
        Clean the data by removing duplicates and filtering out invalid values.
        Also create a binary label for 'win_loss': 1 if W, else 0.

        :param df: Input Spark DataFrame.
        :return: Cleaned and prepared Spark DataFrame.
        """
        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()

        try:
            df_dedup = df.dropDuplicates()

            # Basic cleaning if columns are present
            if "team_id" in df_dedup.columns:
                df_dedup = df_dedup.filter(col("team_id").isNotNull())

            # Create a new binary label column => 1 if W, else 0
            df_dedup = df_dedup.withColumn(
                "win_loss_binary",
                when(col("win_loss") == "W", lit(1)).otherwise(lit(0))
            )

            self.logger.info(
                "Data cleaning complete, 'win_loss_binary' created."
            )
            return df_dedup

        except Exception as e:
            self.logger.error("Error in cleaning data: %s", str(e), exc_info=True)
            sys.exit(1)

        finally:
            end_cpu_time = time.process_time()
            end_cpu_percent = psutil.cpu_percent()
            self.logger.info(
                "CPU usage start: %s%%, end: %s%%. CPU time for "
                "clean_and_partition_data(): %.4f seconds",
                start_cpu_percent,
                end_cpu_percent,
                end_cpu_time - start_cpu_time,
            )
