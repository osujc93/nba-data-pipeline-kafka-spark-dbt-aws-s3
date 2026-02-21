import sys
import logging

from pyspark.sql import SparkSession, DataFrame


class DataLoader:
    """
    Loads the scd_team_boxscores table (or any relevant table)
    from Spark (Iceberg or Hive).
    """

    def __init__(self, logger: logging.Logger) -> None:
        """
        Initialize the DataLoader with a logger.

        :param logger: Logger for logging messages.
        """
        try:
            self.logger = logger
        except Exception as e:
            logging.error("Error in DataLoader.__init__(): %s", str(e), exc_info=True)
            sys.exit(1)

    def load_team_boxscores(self, spark: SparkSession) -> DataFrame:
        """
        Load the scd_team_boxscores table from Spark (Iceberg or Hive).

        :param spark: The SparkSession to use.
        :return: A Spark DataFrame containing the loaded data.
        """
        try:
            import pyspark.sql.functions as F

            full_tbl_name = "iceberg_nba_player_boxscores.scd_team_boxscores"
            self.logger.info("Reading table: %s", full_tbl_name)

            df_tbl = spark.table(full_tbl_name)
            df_tbl = df_tbl.persist()
            _ = df_tbl.count()  # materialize

            self.logger.info(
                "Loaded scd_team_boxscores table. Row count: %d",
                df_tbl.count(),
            )
            return df_tbl

        except Exception as e:
            self.logger.error(
                "Error loading scd_team_boxscores: %s",
                str(e),
                exc_info=True,
            )
            sys.exit(1)
