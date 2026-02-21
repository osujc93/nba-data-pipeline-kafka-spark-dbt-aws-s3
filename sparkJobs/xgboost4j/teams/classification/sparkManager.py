#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import sys
import os
import subprocess

from typing import Union
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class SparkManager:
    """
    Manages the creation of a SparkSession with the necessary configurations.
    """

    def __init__(self, app_name: str = "XGBoost_NBA_Team_Classification"):
        try:
            self.app_name = app_name
            self.spark: Union[SparkSession, None] = None
        except Exception as e:
            logger.error("Error in SparkManager.__init__(): %s", str(e), exc_info=True)
            sys.exit(1)

    def ensure_spark_events_prefix(self, bucket_name: str, prefix: str):
        """
        Checks if spark-events/ prefix exists in S3. If not, create it.
        """
        try:
            check_cmd = [
                "aws", "s3api", "head-object",
                "--bucket", bucket_name,
                "--key", prefix
            ]
            subprocess.run(check_cmd, check=True, capture_output=True)
            logger.info(f"S3 prefix '{prefix}' in '{bucket_name}' already exists.")
        except subprocess.CalledProcessError:
            logger.info(f"S3 prefix '{prefix}' not found. Creating it now...")
            create_cmd = [
                "aws", "s3api", "put-object",
                "--bucket", bucket_name,
                "--key", prefix
            ]
            try:
                subprocess.run(create_cmd, check=True)
                logger.info(f"Created S3 prefix: s3://{bucket_name}/{prefix}")
            except subprocess.CalledProcessError as e:
                logger.error("Failed to create the spark-events prefix in S3.")
                logger.error(e)
                raise

    def create_spark_session(self) -> SparkSession:
        """
        Create and return a SparkSession with the necessary configurations.
        """
        try:
            bucket_name = os.environ["S3_BUCKET"]

            self.ensure_spark_events_prefix(bucket_name, "spark-events/")

            self.spark = (
                SparkSession.builder.appName(self.app_name)
                .config("spark.sql.warehouse.dir", f"s3a://{bucket_name}/warehouse")
                .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.path.style.access", "false")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
                )
                .enableHiveSupport()
                .getOrCreate()
            )

            self.spark.sql("CREATE DATABASE IF NOT EXISTS iceberg_nba_player_boxscores_classification")

            logger.info("Spark connection created successfully, using AWS S3.")
            return self.spark

        except Exception as e:
            logger.error("Error creating Spark connection: %s", e, exc_info=True)
            sys.exit(1)
