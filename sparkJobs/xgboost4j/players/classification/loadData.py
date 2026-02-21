import sys
import psutil
import logging
import time

from pyspark.sql import SparkSession, DataFrame


class DataLoader:
    """
    Loads the scd_player_boxscores table (or any relevant table)
    from Spark (Iceberg or Hive).
    """

    def __init__(self, logger: logging.Logger) -> None:
        """
        Initialize the DataLoader with a logger.

        :param logger: Logger for logging messages.
        """
        self.logger = logger

    def load_player_boxscores(self, spark: SparkSession) -> DataFrame:
        """
        Load the scd_player_boxscores table from Spark (Iceberg or Hive).

        :param spark: The SparkSession to use.
        :return: A Spark DataFrame containing the loaded data.
        """
        import pyspark.sql.functions as F

        start_cpu_time = time.process_time()
        start_cpu_percent = psutil.cpu_percent()
        try:
            # Adjust to your actual table reference
            full_tbl_name = "iceberg_nba_player_boxscores.scd_player_boxscores"
            self.logger.info("Reading table: %s", full_tbl_name)

            df_tbl = spark.table(full_tbl_name)
            # Persist so we don't re-scan
            df_tbl = df_tbl.persist()
            _ = df_tbl.count()  # materialize

            self.logger.info(
                "Loaded scd_player_boxscores table. Row count: %d",
                df_tbl.count(),
            )
            return df_tbl

        except Exception as e:
            self.logger.error(
                "Error loading scd_player_boxscores: %s",
                str(e),
                exc_info=True,
            )
            sys.exit(1)

        finally:
            end_cpu_time = time.process_time()
            end_cpu_percent = psutil.cpu_percent()
            self.logger.info(
                "CPU usage start: %s%%, end: %s%%. CPU time for load_player_boxscores(): "
                "%.4f seconds",
                start_cpu_percent,
                end_cpu_percent,
                end_cpu_time - start_cpu_time,
            )
