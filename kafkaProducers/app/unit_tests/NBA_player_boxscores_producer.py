# File: tests/test_kafka_producer.py
# ---------------------------------------------------------
# EXAMPLE: Pytest-based unit tests for the Kafka producer script.
# You may adapt import paths, function names, or mocks as needed.
# ---------------------------------------------------------

import os
import io
import json
import time
import pytest
import requests
from unittest.mock import patch, MagicMock, call

# We assume your script is named `my_kafka_script`. 
# If it's actually `NBA_player_boxscores.py`, just adjust the import:
import my_kafka_script

from my_kafka_script import (
    create_producer,
    create_topic,
    fetch_and_send_boxscore_data,
    TokenBucketRateLimiter,
    LeagueGameLog,
    # Possibly import other items you want to test
)

from kafka.errors import TopicAlreadyExistsError, KafkaTimeoutError, NotLeaderForPartitionError


@pytest.fixture
def mock_producer_config():
    """
    Example fixture for Producer config. 
    If your config is dynamic, override accordingly.
    """
    return {
        'bootstrap_servers': ['fake1:9092', 'fake2:9093'],
        'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
        'key_serializer': lambda x: str(x).encode('utf-8'),
        'retries': 10,
    }


@pytest.fixture
def mock_kafka_admin():
    """
    Example fixture that mocks KafkaAdminClient usage.
    """
    with patch("my_kafka_script.KafkaAdminClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        yield mock_client


@pytest.fixture
def mock_kafka_producer():
    """
    Example fixture that mocks KafkaProducer usage.
    """
    with patch("my_kafka_script.KafkaProducer") as mock_prod_cls:
        mock_producer_inst = MagicMock()
        mock_prod_cls.return_value = mock_producer_inst
        yield mock_producer_inst


@pytest.fixture
def mock_requests_session():
    """
    Fixture to provide a mock requests.Session so we don't do real HTTP calls.
    """
    with patch("my_kafka_script.requests.Session") as session_cls:
        session_inst = MagicMock()
        session_cls.return_value = session_inst
        yield session_inst


# ---------------------------------------------------------
# TESTS: create_producer()
# ---------------------------------------------------------
def test_create_producer_success(mock_producer_config, mock_kafka_producer):
    """
    Test that create_producer() returns a producer if creation succeeds.
    """
    # Suppose create_producer uses PRODUCER_CONFIG internally,
    # or you might pass it as an argument. We'll assume it uses a global dict.

    producer = create_producer()
    assert producer is not None
    # The mock_kafka_producer fixture ensures the producer is a MagicMock.
    # We can verify calls if needed:
    # mock_kafka_producer.some_method.assert_called_once() # if relevant


def test_create_producer_failure(mock_producer_config):
    """
    Test that create_producer() logs an error if KafkaProducer raises an exception.
    """
    with patch("my_kafka_script.KafkaProducer", side_effect=Exception("Boom!")):
        with pytest.raises(Exception) as excinfo:
            _ = create_producer()
        assert "Boom!" in str(excinfo.value)


# ---------------------------------------------------------
# TESTS: create_topic()
# ---------------------------------------------------------
def test_create_topic_already_exists(mock_kafka_admin):
    """
    Test scenario where topic already exists and we handle it gracefully.
    """
    # Simulate existing topic
    mock_kafka_admin.list_topics.return_value = ["MyExistingTopic", "AnotherTopic"]

    my_kafka_script.create_topic(topic_name="MyExistingTopic")
    mock_kafka_admin.create_topics.assert_not_called()


def test_create_topic_newly_created(mock_kafka_admin):
    """
    Test scenario where topic does not exist, so we create it.
    """
    mock_kafka_admin.list_topics.return_value = ["SomeTopic"]

    create_topic(topic_name="NewTopic", num_partitions=2, replication_factor=1)

    # Expect create_topics to be called once
    mock_kafka_admin.create_topics.assert_called_once()
    args, kwargs = mock_kafka_admin.create_topics.call_args
    assert len(args[0]) == 1  # The list of NewTopic
    new_topic = args[0][0]
    assert new_topic.name == "NewTopic"
    assert new_topic.num_partitions == 2
    assert new_topic.replication_factor == 1


def test_create_topic_exception(mock_kafka_admin):
    """
    If something else fails, we log an error.
    """
    mock_kafka_admin.list_topics.side_effect = Exception("Some other error")

    # Just ensure no crash. Possibly check logs if needed.
    create_topic("AnotherTopic")


# ---------------------------------------------------------
# TESTS: fetch_and_send_boxscore_data()
# ---------------------------------------------------------
@pytest.fixture
def sample_leaguegamelog_data():
    """
    Return a sample JSON that mimics the NBA's leaguegamelog structure.
    """
    return {
        "resource": "leaguegamelog",
        "parameters": {},
        "resultSets": [
            {
                "name": "LeagueGameLog",
                "headers": ["GAME_ID", "GAME_DATE", "TEAM_ID"],
                "rowSet": [
                    ["12345", "2025-02-14", 1610612737],
                    ["12346", "2025-02-15", 1610612738],
                ]
            }
        ]
    }


def test_fetch_and_send_boxscore_data_ok(
    mock_kafka_producer,
    mock_requests_session,
    sample_leaguegamelog_data
):
    """
    End-to-end test of fetch_and_send_boxscore_data under normal conditions.
    Mocks out the network call + Kafka.
    """
    # Mock the Session.get call to return a successful response
    response_mock = MagicMock()
    response_mock.status_code = 200
    response_mock.json.return_value = sample_leaguegamelog_data
    mock_requests_session.get.return_value = response_mock

    topic = "TestTopic"
    season = "2024-25"
    season_type = "Regular%20Season"
    date_from = "2025-02-14"
    date_to = "2025-02-15"

    # Actually call the function
    max_game_date = fetch_and_send_boxscore_data(
        producer=mock_kafka_producer,
        topic=topic,
        season=season,
        season_type=season_type,
        date_from=date_from,
        date_to=date_to,
        session=mock_requests_session,
        retries=1,  # For speed
        timeout=5
    )

    assert max_game_date is not None
    assert max_game_date.strftime("%Y-%m-%d") == "2025-02-15"

    # Check that producer.send was called for each row
    assert mock_kafka_producer.send.call_count == 2
    calls = mock_kafka_producer.send.call_args_list
    # e.g. calls[0] => call('TestTopic', key=..., value=...)
    # check if the second argument has row data
    for i, c in enumerate(calls):
        _topic_arg, kwargs = c[0], c[1]
        # c[0] is the positional arguments: ('TestTopic',)
        # c[1] is the keyword arguments: { 'key': b'12345', 'value': {...} }
        # Usually 'key' is in kwargs, 'value' is in kwargs.
        # Let's ensure it's the correct topic:
        assert _topic_arg == "TestTopic"
        # or check the data if you want:
        # row_dict = kwargs["value"]
        # assert row_dict["GAME_ID"] in ("12345", "12346", ...)


def test_fetch_and_send_boxscore_data_http_error(mock_requests_session, mock_kafka_producer):
    """
    If the request fails entirely, we return None and log the failure.
    """
    # Simulate a 404 or general exception
    mock_requests_session.get.side_effect = requests.exceptions.RequestException("404 not found")

    result = fetch_and_send_boxscore_data(
        producer=mock_kafka_producer,
        topic="TestTopic",
        season="2024-25",
        season_type="Regular%20Season",
        date_from="2025-02-14",
        date_to="2025-02-15",
        session=mock_requests_session,
        retries=1,
        timeout=2
    )
    assert result is None
    assert mock_kafka_producer.send.call_count == 0  # no sends if request fails


def test_fetch_and_send_boxscore_data_validation_error(
    mock_requests_session,
    mock_kafka_producer
):
    """
    If the JSON doesn't match the Pydantic model, we return None.
    """
    # Return data missing 'resultSets' or some crucial key:
    bad_data = { "resource": "leaguegamelog", "parameters": {} }
    response_mock = MagicMock()
    response_mock.status_code = 200
    response_mock.json.return_value = bad_data
    mock_requests_session.get.return_value = response_mock

    result = fetch_and_send_boxscore_data(
        producer=mock_kafka_producer,
        topic="TestTopic",
        season="2024-25",
        season_type="Regular%20Season",
        date_from="2025-02-14",
        date_to="2025-02-15",
        session=mock_requests_session
    )
    assert result is None
    mock_kafka_producer.send.assert_not_called()


# ---------------------------------------------------------
# TESTS: TokenBucketRateLimiter
# ---------------------------------------------------------
def test_token_bucket_basic():
    """
    Ensure the token bucket limits calls if we quickly call acquire_token multiple times.
    """
    limiter = TokenBucketRateLimiter(tokens_per_interval=1, interval=0.5, max_tokens=2)

    # Initially max_tokens=2 -> we can acquire 2 tokens right away
    limiter.acquire_token()  # 1st
    limiter.acquire_token()  # 2nd

    # 3rd call should block until the interval refills a token
    start_time = time.time()
    limiter.acquire_token()  # 3rd
    end_time = time.time()

    # We expect at least ~0.5s delay before we got the 3rd token
    assert (end_time - start_time) >= 0.5


def test_token_bucket_refill():
    """
    Test that after waiting the interval, we can get more tokens.
    """
    limiter = TokenBucketRateLimiter(tokens_per_interval=2, interval=1.0, max_tokens=2)
    # Acquire both tokens
    limiter.acquire_token()
    limiter.acquire_token()

    # None left, so the next acquire will block unless we wait
    time.sleep(1.1)  # Wait for refill
    start_time = time.time()
    limiter.acquire_token()  # Should be immediate
    end_time = time.time()

    # We expect almost no wait time since we let it refill
    assert (end_time - start_time) < 0.2


# ---------------------------------------------------------
# You can add more tests for process_all_boxscore_combinations, 
# run_batch_layer, or finalize_batch_in_postgres 
# by mocking their dependencies (e.g. filesystem, psycopg2).
# ---------------------------------------------------------
