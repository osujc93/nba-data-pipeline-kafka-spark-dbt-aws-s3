import sys
import logging
import datetime

from pyspark.sql import SparkSession

class CheckpointManager:
    """
    Manages Spark checkpoint directories in S3 (or local/HDFS),
    dynamically creating and cleaning them up.
    """

    def __init__(self, logger: logging.Logger) -> None:
        """
        :param logger: Logger for logging messages.
        """
        try:
            self.logger = logger
            self.checkpoint_dir = None
        except Exception as e:
            logging.error("Error in CheckpointManager.__init__(): %s", str(e), exc_info=True)
            sys.exit(1)

    def configure_checkpoint_dir(self, spark: SparkSession, s3_bucket_path: str = None) -> None:
        """
        Dynamically configures the Spark checkpoint directory.
        By default, uses an S3 path with a timestamp subfolder if none provided.

        :param spark: Active SparkSession.
        :param s3_bucket_path: Optional S3 base path (e.g. 's3a://my-bucket/checkpoints').
        """
        try:
            if not s3_bucket_path:
                # If you prefer a default, specify it here
                s3_bucket_path = "s3a://nelodatawarehouse93/sparkCheckpoints"

            # Append a timestamp to ensure uniqueness
            timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            self.checkpoint_dir = f"{s3_bucket_path}/pipeline_ckpt_{timestamp_str}/"

            self.logger.info("Configuring Spark checkpoint directory => %s", self.checkpoint_dir)
            spark.sparkContext.setCheckpointDir(self.checkpoint_dir)

        except Exception as e:
            self.logger.error("Error configuring checkpoint dir: %s", str(e), exc_info=True)
            sys.exit(1)

    def clean_up_checkpoint_dir(self, spark: SparkSession) -> None:
        """
        Removes the checkpoint directory after successful completion.
        """
        try:
            if not self.checkpoint_dir:
                self.logger.info("No checkpoint directory was set; skipping cleanup.")
                return

            self.logger.info("Cleaning up checkpoint directory => %s", self.checkpoint_dir)
            # Use the Hadoop FileSystem API to remove the directory recursively
            jvm = spark._jvm
            jsc = spark._jsc
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
            path = jvm.org.apache.hadoop.fs.Path(self.checkpoint_dir)
            if fs.exists(path):
                fs.delete(path, True)  # True => recursive delete
                self.logger.info("Checkpoint directory deleted: %s", self.checkpoint_dir)
            else:
                self.logger.info("Checkpoint directory does not exist, nothing to remove.")

        except Exception as e:
            # Not fatal, but log a warning
            self.logger.warning("Failed to remove checkpoint directory: %s", str(e), exc_info=True)
