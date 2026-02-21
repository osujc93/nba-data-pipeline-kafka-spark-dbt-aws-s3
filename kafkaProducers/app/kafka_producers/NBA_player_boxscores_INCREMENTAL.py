#!/usr/bin/env python3
import argparse
import json
import logging
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Union, Dict
import cProfile
import pstats
import io
import threading
import psutil
import os
from datetime import datetime, timedelta

import psycopg2  # For reading/updating last ingestion date in Postgres
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.errors import NotLeaderForPartitionError, KafkaTimeoutError
from pydantic import BaseModel, ValidationError

# ------------------ NEW: Import our enhanced data cache ------------------
from NBA_data_cache_incremental import NBADataCacheIncremental

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ------------------ Kafka Configuration ------------------
BOOTSTRAP_SERVERS: List[str] = [
    '172.16.10.2:9092',
    '172.16.10.3:9093',
    '172.16.10.4:9094'
]

PRODUCER_CONFIG: Dict[str, Union[str, int]] = {
    'bootstrap_servers': BOOTSTRAP_SERVERS,
    'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
    'key_serializer': lambda x: str(x).encode('utf-8'),
    'retries': 10,
    'max_block_ms': 120000,
    'request_timeout_ms': 120000,
    'acks': 'all',
    'linger_ms': 6555,
    'batch_size': 5 * 1024 * 1024,
    'max_request_size': 20 * 1024 * 1024,
    'compression_type': 'gzip',
    'buffer_memory': 512 * 1024 * 1024,
    'max_in_flight_requests_per_connection': 5
}

# ------------------ Pydantic Models for LeagueGameLog ------------------
class BoxScoreResultSet(BaseModel):
    name: str
    headers: List[str]
    rowSet: List[List[Union[str, int, float, None]]]

class LeagueGameLog(BaseModel):
    resource: str
    parameters: dict
    resultSets: List[BoxScoreResultSet]

# ------------------ NBA Configuration ------------------
NBA_HEADERS: Dict[str, str] = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "connection": "keep-alive",
    "host": "stats.nba.com",
    "origin": "https://www.nba.com",
    "pragma": "no-cache",
    "referer": "https://www.nba.com/",
    "sec-ch-ua": "\"Google Chrome\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
}

# We won't iterate all seasons for daily increments. We'll do just "current" or last few seasons if needed.
# Or you can have logic to figure out what season the "last_ingestion_date" belongs to, etc.
# For example, let's keep it simple: we only increment from the last date stored in Postgres.
season_type_map: Dict[str, str] = {
    "Regular": "Regular%20Season",
    "PreSeason": "Pre%20Season",
    "Playoffs": "Playoffs",
    "All-Star": "All%20Star",
    "PlayIn": "PlayIn",
    "NBA Cup": "IST"
}

FAILURES_LOG = "failures_log_incremental.json"

# ------------------ Token-Bucket Rate Limiter ------------------
class TokenBucketRateLimiter:
    def __init__(self, tokens_per_interval: int = 1, interval: float = 7.0, max_tokens: int = 1):
        self.tokens_per_interval = tokens_per_interval
        self.interval = interval
        self.max_tokens = max_tokens
        self.available_tokens = max_tokens
        self.last_refill_time = time.monotonic()
        self.lock = threading.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill_time
        intervals_passed = int(elapsed // self.interval)
        if intervals_passed > 0:
            refill_amount = intervals_passed * self.tokens_per_interval
            self.available_tokens = min(self.available_tokens + refill_amount, self.max_tokens)
            self.last_refill_time += intervals_passed * self.interval

    def acquire_token(self) -> None:
        while True:
            with self.lock:
                self._refill()
                if self.available_tokens > 0:
                    self.available_tokens -= 1
                    return
            time.sleep(0.05)

PROXIES = {}
rate_limiter = TokenBucketRateLimiter(tokens_per_interval=1, interval=7.0, max_tokens=1)

# ------------------ Postgres Helpers for last ingestion date ------------------
def get_last_ingestion_date(
    pg_host: str = "postgres",
    pg_port: int = 5432,
    pg_db: str = "nelomlb",
    pg_user: str = "nelomlb",
    pg_password: str = "Pacmanbrooklyn19"
) -> datetime:
    """
    Returns the last ingestion date from the nba_player_boxscores_metadata table,
    or defaults to 1946-10-01 if the table is empty or doesn't exist yet.
    """
    default_date = datetime(1946, 10, 1)
    try:
        conn = psycopg2.connect(
            dbname=pg_db,
            user=pg_user,
            password=pg_password,
            host=pg_host,
            port=pg_port
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Ensure table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nba_player_boxscores_metadata (
                id SERIAL PRIMARY KEY,
                last_ingestion_date DATE NOT NULL,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Attempt to fetch the single row
        cur.execute("SELECT last_ingestion_date FROM nba_player_boxscores_metadata ORDER BY id DESC LIMIT 1;")
        row = cur.fetchone()
        if row and row[0]:
            return datetime.combine(row[0], datetime.min.time())
        else:
            return default_date

    except Exception as e:
        logger.error(f"Error fetching last ingestion date: {e}")
        return default_date
    finally:
        if 'conn' in locals():
            conn.close()

def update_last_ingestion_date(
    new_last_date: datetime,
    pg_host: str = "postgres",
    pg_port: int = 5432,
    pg_db: str = "nelomlb",
    pg_user: str = "nelomlb",
    pg_password: str = "Pacmanbrooklyn19"
) -> None:
    """
    Updates the nba_player_boxscores_metadata table with the new last ingestion date.
    """
    try:
        conn = psycopg2.connect(
            dbname=pg_db,
            user=pg_user,
            password=pg_password,
            host=pg_host,
            port=pg_port
        )
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO nba_player_boxscores_metadata (last_ingestion_date)
            VALUES (%s);
        """, (new_last_date.strftime("%Y-%m-%d"),))

        logger.info(f"Updated last_ingestion_date to {new_last_date.strftime('%Y-%m-%d')} in Postgres.")
    except Exception as e:
        logger.error(f"Error updating last ingestion date in Postgres: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

# ------------------ Kafka Utility ------------------
def create_topic(topic_name: str, num_partitions: int = 5, replication_factor: int = 2) -> None:
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id='NBA_player_boxscores_incremental'
        )
        topic_list = [NewTopic(name=topic_name,
                               num_partitions=num_partitions,
                               replication_factor=replication_factor)]
        existing_topics = admin_client.list_topics()

        if topic_name not in existing_topics:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logger.info(
                f"Topic '{topic_name}' created successfully with "
                f"{num_partitions} partitions, replication factor {replication_factor}."
            )
        else:
            logger.info(f"Topic '{topic_name}' already exists.")
    except TopicAlreadyExistsError:
        logger.info(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        logger.error(f"Failed to create Kafka topic: {e}")

def create_producer() -> KafkaProducer:
    try:
        producer = KafkaProducer(**PRODUCER_CONFIG)
        logger.info("Kafka Producer (incremental) created successfully.")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka Producer: {e}")
        raise

# ------------------ Boxscore fetcher ------------------
def build_boxscore_url(
    season_type: str,
    date_from: str,
    date_to: str
) -> str:
    """
    For incremental loads, we pass in date_from/date_to. We assume current season or
    we pass in the correct season type if needed. Using a large 'Counter' to retrieve everything.
    """
    base_url = "https://stats.nba.com/stats/leaguegamelog"
    return (
        f"{base_url}?Counter=999999"
        f"&DateFrom={date_from}"
        f"&DateTo={date_to}"
        f"&Direction=DESC"
        f"&ISTRound="
        f"&LeagueID=00"
        f"&PlayerOrTeam=P"
        f"&Season=2023-24"        # example: you can also map the year from the date if needed
        f"&SeasonType={season_type}"
        f"&Sorter=DATE"
    )

def log_failure(date_from: str, date_to: str, season_type: str) -> None:
    fail_entry = {
        "date_from": date_from,
        "date_to": date_to,
        "season_type": season_type
    }
    if not os.path.exists(FAILURES_LOG):
        with open(FAILURES_LOG, 'w') as f:
            json.dump([fail_entry], f, indent=2)
    else:
        with open(FAILURES_LOG, 'r') as f:
            data = json.load(f)
        data.append(fail_entry)
        with open(FAILURES_LOG, 'w') as f:
            json.dump(data, f, indent=2)

def fetch_and_send_boxscore_data(
    producer: KafkaProducer,
    topic: str,
    date_from: str,
    date_to: str,
    season_type: str,
    session: requests.Session,
    retries: int = 3,
    timeout: int = 240
) -> None:
    url = build_boxscore_url(
        season_type=season_type,
        date_from=date_from,
        date_to=date_to
    )
    cached_data = NBADataCacheIncremental.get(url)
    if cached_data is not None:
        logger.info(f"[CACHE-Incremental] Using cached data for {url}")
        data = cached_data
    else:
        data = None
        attempt = 0
        while attempt < retries:
            rate_limiter.acquire_token()
            try:
                response = session.get(url, headers=NBA_HEADERS, timeout=timeout)
                if response.status_code in [443, 503]:
                    logger.warning(f"Got HTTP {response.status_code}. Retrying.")
                    session.close()
                    session = requests.Session()
                    time.sleep(60)
                    attempt += 1
                    continue
                response.raise_for_status()
                data = response.json()
                NBADataCacheIncremental.set(url, data)
                break
            except requests.exceptions.RequestException as e:
                logger.error(f"Attempt {attempt+1} failed for {url} with error: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"All {retries} attempts failed for {url}.")
                    log_failure(date_from, date_to, season_type)
                    return
            attempt += 1

    if not data:
        logger.warning(f"No data returned for {url}")
        return

    # Validate JSON
    try:
        validated_data = LeagueGameLog(**data)
    except ValidationError as val_err:
        logger.error(
            f"[Incremental] Data validation error for date_from={date_from}, date_to={date_to}: {val_err}"
        )
        return

    for result_set in validated_data.resultSets:
        if result_set.name == "LeagueGameLog":
            for row in result_set.rowSet:
                row_dict = dict(zip(result_set.headers, row))
                row_dict["DateFrom"] = date_from
                row_dict["DateTo"] = date_to
                row_dict["SeasonType"] = season_type
                # If you want to store the actual date or additional metadata, do it here.

                key_str = str(row_dict.get("GAME_ID", "NO_GAMEID"))
                try:
                    producer.send(topic, key=key_str.encode('utf-8'), value=row_dict)
                except (NotLeaderForPartitionError, KafkaTimeoutError) as kafka_err:
                    logger.error(
                        f"[Incremental] Producer error for GAME_ID {key_str}: {kafka_err}"
                    )
                except Exception as e:
                    logger.error(
                        f"[Incremental] Failed to send message to Kafka for GAME_ID {key_str}: {e}"
                    )

    logger.info(
        f"[Incremental] Sent data to Kafka for date range {date_from} - {date_to}, season_type={season_type}"
    )

# ------------------ Incremental Orchestration ------------------
def run_incremental_load(producer: KafkaProducer, topic: str) -> None:
    """
    1) Grab the last_ingestion_date from Postgres.
    2) Build a date range from last_date+1 to today.
    3) For each season type (Regular, Playoffs, etc.), fetch data.
    4) Update last_ingestion_date in Postgres after successful ingestion.
    """
    last_date = get_last_ingestion_date()
    start_date = last_date + timedelta(days=1)
    end_date = datetime.today() - timedelta(days=0)  # up to "today", or "yesterday"
    if start_date > end_date:
        logger.info("[Incremental] No new dates to ingest.")
        return

    date_from_str = start_date.strftime("%Y-%m-%d")
    date_to_str = end_date.strftime("%Y-%m-%d")

    session = requests.Session()
    if PROXIES:
        session.proxies.update(PROXIES)

    # Just an example: We do each season type for the new date range
    for stype_key, stype_value in season_type_map.items():
        fetch_and_send_boxscore_data(
            producer=producer,
            topic=topic,
            date_from=date_from_str,
            date_to=date_to_str,
            season_type=stype_value,
            session=session
        )

    # If everything is successful, update last_ingestion_date
    update_last_ingestion_date(end_date)

# ------------------ Failures re-run if you want ------------------
def run_batch_layer_incremental(producer: KafkaProducer, topic: str) -> None:
    if not os.path.exists(FAILURES_LOG):
        logger.info("[Incremental] No failures log found.")
        return

    with open(FAILURES_LOG, 'r') as f:
        failures = json.load(f)

    # Clear the file
    open(FAILURES_LOG, 'w').close()

    session = requests.Session()
    if PROXIES:
        session.proxies.update(PROXIES)

    for fail_entry in failures:
        date_from = fail_entry['date_from']
        date_to = fail_entry['date_to']
        season_type = fail_entry['season_type']
        fetch_and_send_boxscore_data(
            producer=producer,
            topic=topic,
            date_from=date_from,
            date_to=date_to,
            season_type=season_type,
            session=session
        )

# ------------------ cProfile & Logging ------------------
profiler = cProfile.Profile()

def log_stats_periodically(prof: cProfile.Profile, interval: int = 120) -> None:
    while True:
        time.sleep(interval)
        s = io.StringIO()
        ps = pstats.Stats(prof, stream=s).sort_stats("cumulative")
        ps.print_stats(10)
        logger.info(f"[Incremental CPU stats - Last {interval} seconds]:\n{s.getvalue()}")

# ------------------ Main ------------------
def main(topic: str = "NBA_player_boxscores_incr") -> None:
    producer = None
    try:
        producer = create_producer()
        create_topic(topic, num_partitions=5, replication_factor=2)

        run_incremental_load(producer, topic)
        run_batch_layer_incremental(producer, topic)

    except Exception as e:
        logger.error(f"[Incremental] Unexpected error: {e}")
    finally:
        if producer:
            producer.close()

if __name__ == "__main__":
    profiler.enable()
    t = threading.Thread(target=log_stats_periodically, args=(profiler,), daemon=True)
    t.start()

    main()

    profiler.disable()
    s_final = io.StringIO()
    p_final = pstats.Stats(profiler, stream=s_final).sort_stats("cumulative")
    p_final.print_stats()
    logger.info(f"[Final Incremental CPU stats]:\n{s_final.getvalue()}")
