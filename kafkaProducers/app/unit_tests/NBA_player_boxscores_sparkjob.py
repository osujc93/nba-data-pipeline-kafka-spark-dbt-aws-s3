# File: tests/test_spark_job.py
# -----------------------------------------------------------
# Example unit tests for the Spark job script that ingests
# NBA player boxscore data from Kafka into Iceberg.
# We assume the script is named `my_spark_script.py`.
# If it's actually `NBA_player_boxscores.py`, adjust imports accordingly.
# -----------------------------------------------------------

import sys
import pytest
from unittest.mock import patch, MagicMock, call
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)

# Import the functions from your script
import my_spark_script

from my_spark_script import (
    create_spark_connection,
    create_main_boxscore_table_iceberg,
    read_kafka_batch_and_process,
    main,
)


@pytest.fixture(scope="module")
def spark_mock_session():
    """
    A real SparkSession is large overhead in unit tests. 
    Here, we create a local Spark session if needed,
    or we can mock the entire builder chain if we want no real Spark at all.
    """
    # Option A: Provide a real local Spark for an integration-ish test:
    # spark = SparkSession.builder \
    #     .master("local[2]") \
    #     .appName("TestSparkJob") \
    #     .getOrCreate()
    # yield spark
    # spark.stop()

    # Option B: Just return a MagicMock that stands in for SparkSession
    # to test code paths without real Spark behind them.
    mock_spark = MagicMock(spec=SparkSession)
    yield mock_spark


# -----------------------------------------------------------
# TEST: create_spark_connection()
# -----------------------------------------------------------
@patch("my_spark_script.SparkSession")
def test_create_spark_connection_success(mock_spark_class):
    """
    Test that create_spark_connection sets up Spark and returns a session,
    logs success, and does not exit on error.
    """
    mock_builder = MagicMock()
    mock_session = MagicMock(spec=SparkSession)
    # We'll simulate the builder chain:
    mock_builder.config.return_value = mock_builder
    mock_builder.enableHiveSupport.return_value = mock_builder
    mock_builder.getOrCreate.return_value = mock_session

    # The builder property on SparkSession is also a MagicMock
    mock_spark_class.builder = mock_builder

    returned_spark = create_spark_connection()
    assert returned_spark == mock_session

    # We can check that the code tried to create DBs:
    mock_session.sql.assert_any_call("CREATE DATABASE IF NOT EXISTS iceberg_nba_player_boxscores")
    mock_session.sql.assert_any_call("CREATE DATABASE IF NOT EXISTS hive_nba_player_boxscores")


@patch("my_spark_script.SparkSession")
def test_create_spark_connection_failure(mock_spark_class):
    """
    If there's an exception during creation, we expect sys.exit(1).
    We'll patch sys.exit to track calls instead of killing the test process.
    """
    mock_spark_class.builder.getOrCreate.side_effect = Exception("Spark init error")

    with patch("sys.exit") as mock_exit:
        create_spark_connection()
        mock_exit.assert_called_once_with(1)


# -----------------------------------------------------------
# TEST: create_main_boxscore_table_iceberg()
# -----------------------------------------------------------
def test_create_main_boxscore_table_iceberg_success(spark_mock_session):
    """
    Ensure it runs the CREATE TABLE DDL on the spark session
    without raising an exception.
    """
    # The function calls spark.sql(...)
    create_main_boxscore_table_iceberg(spark_mock_session)

    # We expect at least one .sql call
    spark_mock_session.sql.assert_called()
    # If we want, we can check the last call had "CREATE TABLE IF NOT EXISTS" ...
    ddl_call = spark_mock_session.sql.call_args[0][0]
    assert "CREATE TABLE IF NOT EXISTS" in ddl_call
    assert "spark_catalog.iceberg_nba_player_boxscores.nba_player_boxscores" in ddl_call


def test_create_main_boxscore_table_iceberg_failure(spark_mock_session):
    """
    If spark.sql throws an exception, we expect sys.exit(1).
    """
    spark_mock_session.sql.side_effect = Exception("DDL error")

    with patch("sys.exit") as mock_exit:
        create_main_boxscore_table_iceberg(spark_mock_session)
        mock_exit.assert_called_once_with(1)


# -----------------------------------------------------------
# TEST: read_kafka_batch_and_process()
# -----------------------------------------------------------
@patch("my_spark_script.BOOTSTRAP_SERVERS", ["fake1:9092"])
def test_read_kafka_batch_and_process_empty(spark_mock_session):
    """
    If the Kafka DataFrame is empty, it logs and returns early.
    """
    # We'll simulate read.format(...).load() returning an empty DF
    empty_df = MagicMock()
    empty_df.rdd.isEmpty.return_value = True

    # Mock the entire chain for spark.read.format("kafka") ...
    read_mock = MagicMock()
    read_mock.option.return_value = read_mock
    read_mock.load.return_value = empty_df
    spark_mock_session.read.format.return_value = read_mock

    # Call the function
    read_kafka_batch_and_process(spark_mock_session)

    # We expect it to have tried to read from Kafka but found no records:
    read_mock.option.assert_any_call("subscribe", "NBA_player_boxscores")
    # No writes to iceberg
    spark_mock_session.write.format.assert_not_called()


@patch("my_spark_script.BOOTSTRAP_SERVERS", ["fake1:9092"])
def test_read_kafka_batch_and_process_nonempty(spark_mock_session):
    """
    If the Kafka DataFrame has data, we parse JSON, rename, cast, and write to Iceberg.
    """
    # We'll simulate a small DataFrame with 2 rows
    df_mock = MagicMock()
    df_mock.rdd.isEmpty.return_value = False

    # Then after .selectExpr("CAST(value AS STRING)") -> .select(...)
    # we eventually get another DF that is the "parsed_df".
    # We'll chain them with side_effect or return_value patterns.
    read_format_mock = MagicMock()
    read_format_mock.option.return_value = read_format_mock
    read_format_mock.load.return_value = df_mock
    spark_mock_session.read.format.return_value = read_format_mock

    # We'll also mock .select(from_json(...)...) -> final DF
    # so that final_df.write.format("iceberg") call can be tested.
    # For simplicity, we can have them all point to the same df_mock or additional mocks.
    parse_select_mock = MagicMock()
    parse_select_mock.withColumnRenamed.return_value = parse_select_mock
    parse_select_mock.replace.return_value = parse_select_mock
    parse_select_mock.withColumn.return_value = parse_select_mock
    parse_select_mock.write = MagicMock()

    # We'll simulate a chain of .select(...) calls returning parse_select_mock
    df_mock.selectExpr.return_value = df_mock
    df_mock.select.return_value = parse_select_mock

    # The final count call
    parse_select_mock.count.return_value = 2

    read_kafka_batch_and_process(spark_mock_session)

    # We expect a final .write.format("iceberg").mode("append").save(...)
    parse_select_mock.write.format.assert_called_once_with("iceberg")
    write_mode_mock = parse_select_mock.write.format.return_value
    write_mode_mock.mode.assert_called_once_with("append")
    save_mock = write_mode_mock.mode.return_value.save
    save_mock.assert_called_once()
    # Possibly check the actual table name
    assert "nba_player_boxscores" in save_mock.call_args[0][0]


def test_read_kafka_batch_and_process_exception(spark_mock_session):
    """
    If something fails in the body, we expect sys.exit(1).
    """
    with patch("sys.exit") as mock_exit:
        # Let's say the parse or the load throws an exception
        spark_mock_session.read.format.side_effect = Exception("Kafka read error")

        read_kafka_batch_and_process(spark_mock_session)
        mock_exit.assert_called_once_with(1)


# -----------------------------------------------------------
# TEST: main()
# -----------------------------------------------------------
@patch("my_spark_script.create_spark_connection")
@patch("my_spark_script.create_main_boxscore_table_iceberg")
@patch("my_spark_script.read_kafka_batch_and_process")
def test_main_success(
    mock_read_kafka,
    mock_create_table,
    mock_spark_conn
):
    """
    If all steps pass, we expect no sys.exit calls, 
    and a final log message for success.
    """
    mock_session = MagicMock()
    mock_spark_conn.return_value = mock_session

    with patch("sys.exit") as mock_exit:
        my_spark_script.main()
        mock_exit.assert_not_called()

    mock_create_table.assert_called_once_with(mock_session)
    mock_read_kafka.assert_called_once_with(mock_session)
    mock_session.stop.assert_called_once()


@patch("my_spark_script.create_spark_connection", side_effect=Exception("Oops"))
def test_main_failure(mock_spark_conn):
    """
    If main steps raise an exception, we expect sys.exit(1).
    """
    with patch("sys.exit") as mock_exit:
        my_spark_script.main()
        mock_exit.assert_called_once_with(1)

