#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""

Orchestrates the overall batch ingestion steps:
  1) Creating the Spark session
  2) Creating/verifying the Iceberg table
  3) Reading from Kafka in offset chunks and appending to Iceberg
"""

import logging
import sys

from sparkSessionManager import SparkSessionManager
from icebergTableManager import IcebergTableManager
from batchProcessor import BatchProcessor

logger = logging.getLogger(__name__)

class FirstBatch:
    """
    Orchestrates the overall batch ingestion steps.
    """

    def __init__(self, bootstrap_servers: list[str]):
        try:
            self.session_manager = SparkSessionManager()
            self.spark = self.session_manager.create_spark_connection()
            self.iceberg_manager = IcebergTableManager(self.spark)
            self.kafka_processor = BatchProcessor(self.spark, bootstrap_servers)
        finally:
            pass

    def run(self):
        """
        Executes the end-to-end batch ingestion pipeline.
        """
        try:
            # Ensure the Iceberg table is created/verified
            self.iceberg_manager.create_main_boxscore_table_iceberg()

            # Now read from Kafka in a batch manner and append to Iceberg
            self.kafka_processor.read_kafka_batch_and_process()

            # Stop the Spark session
            self.spark.stop()
            logger.info("Batch ingestion pipeline executed successfully.")
        except Exception as e:
            logger.error("Error in main run(): %s", e, exc_info=True)
            sys.exit(1)
        finally:
            pass
