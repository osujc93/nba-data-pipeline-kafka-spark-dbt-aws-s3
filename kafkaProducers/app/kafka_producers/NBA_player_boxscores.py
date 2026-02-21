#!/usr/bin/env python3
"""
Kafka Producer Script for fetching NBA boxscore data and publishing it to a Kafka topic.

Fetches boxscore data from the NBA stats API in chunks, sends each row to Kafka, and
logs any failures for batch re-run. Utilizes Pydantic for data validation and PostgreSQL
for final metadata storage. Also stores all retrieved rows into a Postgres table.
"""

import argparse
import cProfile
import io
import json
import logging
import os
import pstats
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import psutil  # For CPU usage logging
import requests
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import (KafkaTimeoutError, NotLeaderForPartitionError,
                          TopicAlreadyExistsError)
from pydantic import BaseModel, ValidationError

import psycopg2  # For finalizing batch date and storing data

# ------------------ NEW: Import our NBADataCache ------------------
from NBA_data_cache import NBADataCache

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ------------------ Kafka Configuration ------------------
BOOTSTRAP_SERVERS: list[str] = [
    '172.16.10.2:9092',
    '172.16.10.3:9093',
    '172.16.10.4:9094'
]

PRODUCER_CONFIG: dict[str, Any] = {
    'bootstrap_servers': BOOTSTRAP_SERVERS,
    'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
    'key_serializer': lambda x: str(x).encode('utf-8'),
    'retries': 10,
    'max_block_ms': 120000,
    'request_timeout_ms': 120000,
    'acks': 'all',  # Wait for all ISR
    'linger_ms': 6555,
    'batch_size': 5 * 1024 * 1024,
    'max_request_size': 20 * 1024 * 1024,
    'compression_type': 'gzip',
    'buffer_memory': 512 * 1024 * 1024,
    'max_in_flight_requests_per_connection': 5
}


class BoxScoreResultSet(BaseModel):
    """
    Pydantic model defining an individual result set within the
    boxscore data returned by the NBA API.
    """
    name: str
    headers: list[str]
    rowSet: list[list[str | int | float | None]]


class LeagueGameLog(BaseModel):
    """
    Pydantic model defining the overall response structure
    for the NBA leaguegamelog endpoint.
    """
    resource: str
    parameters: dict[str, Any]
    resultSets: list[BoxScoreResultSet]


# ------------------ NBA Configuration ------------------
NBA_HEADERS: dict[str, str] = {
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

season_type_map: dict[str, str] = {
    "Regular": "Regular%20Season",
    "PreSeason": "Pre%20Season",
    "Playoffs": "Playoffs",
    "All-Star": "All%20Star",
    "PlayIn": "PlayIn",
    "NBA Cup": "IST"
}

# Define the seasons we want (1946-47 to 2024-25 as an example)
seasons: list[str] = [f"{year}-{str(year+1)[-2:]}" for year in range(1946, 2025)]

FAILURES_LOG = "failures_log.json"


@dataclass
class TokenBucketRateLimiter:
    """
    Simple Token-Bucket rate limiter:
    - 'tokens_per_interval' tokens are replenished every 'interval' seconds,
      up to 'max_tokens'.
    - Each .acquire_token() call consumes 1 token, blocking if necessary.
    """
    tokens_per_interval: int = 1
    interval: float = 7.0
    max_tokens: int = 1
    available_tokens: int = field(init=False)
    last_refill_time: float = field(init=False)
    lock: threading.Lock = field(init=False)

    def __post_init__(self) -> None:
        """
        Post-initialization to set up token bucket internal variables.
        """
        self.available_tokens = self.max_tokens
        self.last_refill_time = time.monotonic()
        self.lock = threading.Lock()

    def _refill(self) -> None:
        """
        Refill tokens based on elapsed time since last refill.
        """
        now = time.monotonic()
        elapsed = now - self.last_refill_time
        intervals_passed = int(elapsed // self.interval)
        if intervals_passed > 0:
            refill_amount = intervals_passed * self.tokens_per_interval
            self.available_tokens = min(
                self.available_tokens + refill_amount,
                self.max_tokens
            )
            self.last_refill_time += intervals_passed * self.interval

    def acquire_token(self) -> None:
        """
        Acquire a token, blocking if none are available.
        """
        while True:
            with self.lock:
                self._refill()
                if self.available_tokens > 0:
                    self.available_tokens -= 1
                    return
            time.sleep(0.05)


# ------------------ ADDED: Optional Proxies ------------------
PROXIES: dict[str, str] = {}

# Create a global rate limiter instance with these defaults
rate_limiter = TokenBucketRateLimiter(tokens_per_interval=1, interval=7.0, max_tokens=1)


def create_topic(topic_name: str, num_partitions: int = 25,
                 replication_factor: int = 2) -> None:
    """
    Create the specified Kafka topic if it does not already exist.
    """
    start_cpu_time: float = time.process_time()
    start_cpu_percent: float = psutil.cpu_percent()
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id='NBA_player_boxscores'
        )
        topic_list = [
            NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
        ]
        existing_topics = admin_client.list_topics()

        if topic_name not in existing_topics:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logger.info(
                f"Topic '{topic_name}' created successfully with "
                f"{num_partitions} partitions and replication factor "
                f"{replication_factor}."
            )
        else:
            logger.info(f"Topic '{topic_name}' already exists.")
    except TopicAlreadyExistsError:
        logger.info(f"Topic '{topic_name}' already exists.")
    except Exception as exc:
        logger.error(f"Failed to create Kafka topic: {exc}")
    finally:
        end_cpu_time: float = time.process_time()
        end_cpu_percent: float = psutil.cpu_percent()
        logger.info(
            "CPU usage start: %s%%, end: %s%%. CPU time for create_topic(): %.4f seconds",
            start_cpu_percent, end_cpu_percent,
            end_cpu_time - start_cpu_time
        )


def create_producer() -> KafkaProducer:
    """
    Create and return a KafkaProducer using the specified PRODUCER_CONFIG.
    """
    start_cpu_time: float = time.process_time()
    start_cpu_percent: float = psutil.cpu_percent()
    producer = None
    try:
        producer = KafkaProducer(**PRODUCER_CONFIG)
        logger.info("Kafka Producer created successfully.")
        return producer
    except Exception as exc:
        logger.error(f"Failed to create Kafka Producer: {exc}")
        raise
    finally:
        end_cpu_time: float = time.process_time()
        end_cpu_percent: float = psutil.cpu_percent()
        logger.info(
            "CPU usage start: %s%%, end: %s%%. CPU time for create_producer(): %.4f seconds",
            start_cpu_percent, end_cpu_percent,
            end_cpu_time - start_cpu_time
        )


def build_boxscore_url(
    season: str,
    season_type: str,
    date_from: str,
    date_to: str
) -> str:
    """
    Build the NBA boxscore URL with given parameters.

    Uses a large Counter value (999999) to ensure we get all rows.
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
        f"&Season={season}"
        f"&SeasonType={season_type}"
        f"&Sorter=DATE"
    )


def log_failure(season: str, season_type: str, date_from: str, date_to: str) -> None:
    """
    Append a failed combination to 'failures_log.json' so the batch layer can re-run it.
    """
    fail_entry = {
        "season": season,
        "season_type": season_type,
        "date_from": date_from,
        "date_to": date_to
    }
    if not os.path.exists(FAILURES_LOG):
        with open(FAILURES_LOG, 'w', encoding='utf-8') as file:
            json.dump([fail_entry], file, indent=2)
    else:
        with open(FAILURES_LOG, 'r', encoding='utf-8') as file:
            data = json.load(file)
        data.append(fail_entry)
        with open(FAILURES_LOG, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=2)


def store_boxscore_data_in_postgres(rows: list[dict]) -> None:
    """
    Stores a list of boxscore rows into Postgres in JSON form.
    Creates the table if it doesn't exist.
    """
    if not rows:
        return  # Nothing to store

    try:
        conn = psycopg2.connect(
            dbname="nelomlb",
            user="nelomlb",
            password="Pacmanbrooklyn19",
            host="postgres",
            port=5432
        )
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS nba_player_boxscores_data (
                    id SERIAL PRIMARY KEY,
                    data JSONB NOT NULL,
                    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Insert each row as JSON
            insert_query = """
                INSERT INTO nba_player_boxscores_data (data)
                VALUES (%s)
            """
            cur.executemany(
                insert_query,
                [(json.dumps(r),) for r in rows]
            )
        logger.info("Stored %d boxscore rows in Postgres.", len(rows))

    except Exception as e:
        logger.error("Error while storing boxscore data in Postgres: %s", e)
    finally:
        if 'conn' in locals():
            conn.close()


def fetch_and_send_boxscore_data(
    producer: KafkaProducer,
    topic: str,
    season: str,
    season_type: str,
    date_from: str,
    date_to: str,
    session: requests.Session,
    retries: int = 3,
    timeout: int = 240
) -> datetime | None:
    """
    Fetch boxscore data from the NBA API and send to Kafka, then store it in Postgres.

    :param producer: The KafkaProducer to use.
    :param topic: The Kafka topic to send messages to.
    :param season: The NBA season (e.g., '2020-21').
    :param season_type: The season type (e.g., 'Regular%20Season').
    :param date_from: The start date in YYYY-MM-DD format.
    :param date_to: The end date in YYYY-MM-DD format.
    :param session: A requests.Session object.
    :param retries: Number of retries if request fails.
    :param timeout: Request timeout in seconds.
    :return: The maximum GAME_DATE encountered (as a datetime) or None.
    """
    start_cpu_time: float = time.process_time()
    start_cpu_percent: float = psutil.cpu_percent()

    url = build_boxscore_url(season, season_type, date_from, date_to)
    max_game_date: datetime | None = None

    # Check the cache first
    cached_data = NBADataCache.get(url)
    if cached_data is not None:
        logger.info(f"[CACHE] Using cached data for {url}")
        data = cached_data
    else:
        data = None
        attempt = 0
        while attempt < retries:
            rate_limiter.acquire_token()
            try:
                response = session.get(url, headers=NBA_HEADERS, timeout=timeout)

                # Check for 443/503 (often means throttling or connection issues)
                if response.status_code in [443, 503]:
                    logger.warning(
                        "Got HTTP %s -> Throttled/connection issue. Closing session & retrying.",
                        response.status_code
                    )
                    session.close()
                    session = requests.Session()
                    time.sleep(120)
                    attempt += 1
                    continue

                response.raise_for_status()
                data = response.json()

                # Store in cache so future calls skip download
                NBADataCache.set(url, data)
                break

            except requests.exceptions.RequestException as exc:
                logger.error(
                    "Attempt %d failed for %s with error: %s",
                    attempt + 1, url, exc
                )

                if isinstance(exc, requests.exceptions.ConnectionError):
                    logger.error("Connection error encountered. Closing session & forcing a new one.")
                    session.close()
                    session = requests.Session()

                if attempt < retries - 1:
                    sleep_time = 2 ** attempt
                    logger.info("Retrying in %s seconds...", sleep_time)
                    time.sleep(sleep_time)
                else:
                    logger.error("All %d attempts failed for %s.", retries, url)
                    log_failure(season, season_type, date_from, date_to)
                    end_cpu_time = time.process_time()
                    end_cpu_percent = psutil.cpu_percent()
                    logger.info(
                        "CPU usage start: %s%%, end: %s%%. CPU time for "
                        "fetch_and_send_boxscore_data(%s, %s, %s-%s): %.4f seconds",
                        start_cpu_percent, end_cpu_percent, season, season_type,
                        date_from, date_to, end_cpu_time - start_cpu_time
                    )
                    return None
            attempt += 1

    if not data:
        logger.warning("No data returned for %s", url)
        end_cpu_time = time.process_time()
        end_cpu_percent = psutil.cpu_percent()
        logger.info(
            "CPU usage start: %s%%, end: %s%%. CPU time for "
            "fetch_and_send_boxscore_data(%s, %s, %s-%s): %.4f seconds",
            start_cpu_percent, end_cpu_percent, season, season_type, date_from,
            date_to, end_cpu_time - start_cpu_time
        )
        return None

    # Validate JSON with Pydantic
    try:
        validated_data = LeagueGameLog(**data)
    except ValidationError as val_err:
        logger.error(
            "Data validation error for %s, %s, %s-%s: %s",
            season, season_type, date_from, date_to, val_err
        )
        end_cpu_time = time.process_time()
        end_cpu_percent = psutil.cpu_percent()
        logger.info(
            "CPU usage start: %s%%, end: %s%%. CPU time (validation error) "
            "fetch_and_send_boxscore_data(%s, %s, %s-%s): %.4f seconds",
            start_cpu_percent, end_cpu_percent, season, season_type, date_from,
            date_to, end_cpu_time - start_cpu_time
        )
        return None

    # Collect rows to store in Postgres
    rows_for_postgres = []

    # Send each row to Kafka, track max GAME_DATE
    for result_set in validated_data.resultSets:
        if result_set.name == "LeagueGameLog":
            for row in result_set.rowSet:
                row_dict = dict(zip(result_set.headers, row))
                row_dict["Season"] = season
                row_dict["SeasonType"] = season_type
                row_dict["DateFrom"] = date_from
                row_dict["DateTo"] = date_to

                game_date_str = row_dict.get("GAME_DATE")
                if game_date_str:
                    try:
                        g_date = datetime.strptime(game_date_str, "%Y-%m-%d")
                        if (not max_game_date) or (g_date > max_game_date):
                            max_game_date = g_date
                    except ValueError:
                        pass

                key_str = str(row_dict.get("GAME_ID", "NO_GAMEID"))
                try:
                    producer.send(topic, key=key_str.encode('utf-8'), value=row_dict)
                except (NotLeaderForPartitionError, KafkaTimeoutError) as kafka_err:
                    logger.error("Producer error for GAME_ID %s: %s", key_str, kafka_err)
                except Exception as exc:
                    logger.error("Failed to send message to Kafka for GAME_ID %s: %s", key_str, exc)

                # Collect the row for Postgres insert
                rows_for_postgres.append(row_dict)

    # Once we're done sending, store all rows in Postgres
    store_boxscore_data_in_postgres(rows_for_postgres)

    logger.info(
        "Boxscore data for %s, %s, DateRange %s-%s sent to Kafka successfully.",
        season, season_type, date_from, date_to
    )

    end_cpu_time = time.process_time()
    end_cpu_percent = psutil.cpu_percent()
    logger.info(
        "CPU usage start: %s%%, end: %s%%. CPU time for fetch_and_send_boxscore_data"
        "(%s, %s, %s-%s): %.4f seconds",
        start_cpu_percent, end_cpu_percent, season, season_type,
        date_from, date_to, end_cpu_time - start_cpu_time
    )

    return max_game_date


def get_season_start_end_dates(season: str) -> tuple[datetime, datetime]:
    """
    Given a season string like '1946-47', return approximate start/end dates.
    """
    start_year = int(season[:4])
    end_year = start_year + 1

    approximate_start = datetime(start_year, 10, 1)
    approximate_end = datetime(end_year, 6, 30)

    return approximate_start, approximate_end


def daterange_chunked(
    start_date: datetime,
    end_date: datetime,
    chunk_days: int = 365
) -> tuple[datetime, datetime]:
    """
    Yield tuples of (chunk_start, chunk_end) in increments of 'chunk_days'.
    """
    chunk_start = start_date
    while chunk_start <= end_date:
        chunk_end = chunk_start + timedelta(days=chunk_days - 1)
        if chunk_end > end_date:
            chunk_end = end_date
        yield chunk_start, chunk_end
        chunk_start = chunk_end + timedelta(days=1)


def process_all_boxscore_combinations(
    producer: KafkaProducer,
    topic: str,
    max_workers: int = 1
) -> datetime:
    """
    Process all season/season_type combinations in a chunked manner and publish to Kafka.

    max_workers=1 ensures strictly blocking I/O: each request finishes fully
    before the next one begins. Returns the latest game date encountered.
    """
    start_cpu_time = time.process_time()
    start_cpu_percent = psutil.cpu_percent()

    tasks = []
    last_processed_date = datetime(1900, 1, 1)
    session = requests.Session()
    if PROXIES:
        session.proxies.update(PROXIES)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for season_str in seasons:
            for _, season_type_value in season_type_map.items():
                start_date, end_date = get_season_start_end_dates(season_str)
                for chunk_start, chunk_end in daterange_chunked(
                    start_date,
                    end_date,
                    chunk_days=365
                ):
                    future = executor.submit(
                        fetch_and_send_boxscore_data,
                        producer,
                        topic,
                        season_str,
                        season_type_value,
                        chunk_start.strftime("%Y-%m-%d"),
                        chunk_end.strftime("%Y-%m-%d"),
                        session
                    )
                    tasks.append(future)

        for future in as_completed(tasks):
            try:
                chunk_max_date = future.result()
                if chunk_max_date and chunk_max_date > last_processed_date:
                    last_processed_date = chunk_max_date
            except Exception as exc:
                logger.error("Error processing boxscore data: %s", exc)

    end_cpu_time = time.process_time()
    end_cpu_percent = psutil.cpu_percent()
    logger.info(
        "CPU usage start: %s%%, end: %s%%. CPU time for process_all_boxscore_combinations(): %.4f seconds",
        start_cpu_percent, end_cpu_percent,
        end_cpu_time - start_cpu_time
    )

    return last_processed_date


def run_batch_layer(producer: KafkaProducer, topic: str) -> None:
    """
    Re-run any failed combinations recorded in 'failures_log.json' (batch layer).
    """
    if not os.path.exists(FAILURES_LOG):
        logger.info("No failures_log.json found. No batch re-runs needed.")
        return

    logger.info("Starting batch layer re-run of missing data from failures_log.json...")
    with open(FAILURES_LOG, 'r', encoding='utf-8') as file:
        failures = json.load(file)

    # Clear the file so we don't repeatedly re-run the same fails in a loop
    open(FAILURES_LOG, 'w', encoding='utf-8').close()

    session = requests.Session()
    if PROXIES:
        session.proxies.update(PROXIES)

    for fail_entry in failures:
        season = fail_entry['season']
        season_type = fail_entry['season_type']
        date_from = fail_entry['date_from']
        date_to = fail_entry['date_to']

        fetch_and_send_boxscore_data(
            producer=producer,
            topic=topic,
            season=season,
            season_type=season_type,
            date_from=date_from,
            date_to=date_to,
            session=session,
            retries=3,
            timeout=240
        )

    logger.info("Batch layer re-run completed.")


def finalize_batch_in_postgres(final_ingested_date: datetime) -> None:
    """
    Creates (if needed) the nba_player_boxscores_metadata table in Postgres and
    inserts the final_ingested_date so incremental scripts pick up from there.
    """
    try:
        conn = psycopg2.connect(
            dbname="nelomlb",
            user="nelomlb",
            password="Pacmanbrooklyn19",
            host="postgres",
            port=5432
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nba_player_boxscores_metadata (
                id SERIAL PRIMARY KEY,
                last_ingestion_date DATE NOT NULL,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute(
            """
            INSERT INTO nba_player_boxscores_metadata (last_ingestion_date)
            VALUES (%s);
            """,
            (final_ingested_date.strftime("%Y-%m-%d"),)
        )
        logger.info(
            "[FINALIZE] Inserted final date %s into nba_player_boxscores_metadata.",
            final_ingested_date.strftime('%Y-%m-%d')
        )
    except Exception as exc:
        logger.error("[FINALIZE] Error inserting final ingestion date: %s", exc)
    finally:
        if 'conn' in locals():
            conn.close()


profiler = cProfile.Profile()


def log_stats_periodically(prof: cProfile.Profile, interval: int = 120) -> None:
    """
    Logs CPU profiling statistics every 'interval' seconds in a separate thread.
    """
    while True:
        time.sleep(interval)
        s = io.StringIO()
        ps = pstats.Stats(prof, stream=s).sort_stats("cumulative")
        ps.print_stats(10)
        logger.info("[Periodic CPU profiling stats - Last %s seconds]:\n%s", interval, s.getvalue())


def main(topic: str = "NBA_player_boxscores") -> None:
    """
    Main function to run speed layer + batch layer ingestion.
    """
    producer = None
    try:
        start_cpu_time_main = time.process_time()
        start_cpu_percent_main = psutil.cpu_percent()

        producer = create_producer()
        create_topic(topic, num_partitions=25, replication_factor=2)

        # 1) Speed Layer: fetch data + track the actual last game date
        final_date_ingested = process_all_boxscore_combinations(producer, topic, max_workers=1)

        # 2) Batch Layer: re-run any failures
        run_batch_layer(producer, topic)

        # 3) Finalize: Insert the actual final_date_ingested (max game_date) into Postgres
        finalize_batch_in_postgres(final_date_ingested)

        end_cpu_time_main = time.process_time()
        end_cpu_percent_main = psutil.cpu_percent()
        logger.info(
            "CPU usage start: %s%%, end: %s%%. CPU time for entire main block: %.4f seconds",
            start_cpu_percent_main, end_cpu_percent_main,
            end_cpu_time_main - start_cpu_time_main
        )
        logger.info("All NBA boxscore data has been collected and sent to Kafka (real-time + batch).")

    except Exception as exc:
        logger.error("Unexpected error occurred: %s", exc)
    finally:
        if producer:
            try:
                producer.close()
                logger.info("Kafka Producer closed successfully.")
            except Exception as close_exc:
                logger.error("Error closing Kafka Producer: %s", close_exc)


if __name__ == "__main__":
    profiler.enable()

    stats_thread = threading.Thread(target=log_stats_periodically, args=(profiler,), daemon=True)
    stats_thread.start()

    main()

    profiler.disable()
    s_final = io.StringIO()
    p_final = pstats.Stats(profiler, stream=s_final).sort_stats("cumulative")
    p_final.print_stats()
    logger.info("[Final CPU profiling stats]:\n%s", s_final.getvalue())
