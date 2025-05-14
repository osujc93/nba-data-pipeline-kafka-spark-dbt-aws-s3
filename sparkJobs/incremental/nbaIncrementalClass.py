#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import sys

from sparkSessionManager import SparkSessionManager
from icebergTableManager import IcebergTableManager
from incrementalProcessor import IncrementalKafkaProcessor

logger = logging.getLogger(__name__)

class NBAIncrementalLoad:
    """
    Orchestrates the overall incremental ingestion steps:
    1) Create the Spark session
    2) Create/verify the Iceberg table
    3) Read incremental data from Kafka and append to Iceberg
    """

    def __init__(self):
        self.session_manager = SparkSessionManager()
        self.spark = self.session_manager.create_spark_connection()
        self.iceberg_manager = IcebergTableManager(self.spark)
        self.kafka_processor = IncrementalKafkaProcessor(self.spark)

    def run(self):
        """
        Executes the incremental ingestion pipeline.
        """
        try:
            # Ensure Iceberg table is created/verified
            self.iceberg_manager.create_main_boxscore_table_iceberg()

            # Read only new offsets from Kafka and process
            self.kafka_processor.read_kafka_incremental_and_process()

            # Stop Spark
            self.spark.stop()
            logger.info("[Incremental] Spark job completed successfully.")
        except Exception as e:
            logger.error("[Incremental] Error in run(): %s", e, exc_info=True)
            sys.exit(1)
