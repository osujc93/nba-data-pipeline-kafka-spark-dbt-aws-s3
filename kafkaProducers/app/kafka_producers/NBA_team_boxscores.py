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
import psutil  # For CPU usage logging
import os
from datetime import datetime, timedelta

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.errors import NotLeaderForPartitionError, KafkaTimeoutError
from pydantic import BaseModel, ValidationError

# ------------------ NEW: Import the in-memory cache (same as in NBA_player_boxscores.py) ------------------
from NBA_data_cache import NBADataCache

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

season_type_map: Dict[str, str] = {
    "Regular": "Regular%20Season",
    "PreSeason": "Pre%20Season",
    "Playoffs": "Playoffs",
    "All-Star": "All%20Star",
    "PlayIn": "PlayIn",
    "NBA Cup": "IST"
}

# Example range of seasons
seasons: List[str] = [f"{year}-{str(year+1)[-2:]}" for year in range(1946, 2025)]

FAILURES_LOG = "failures_log.json"

# ------------------ Optional Proxies ------------------
PROXIES = {
    # "http": "http://YOUR_PROXY:PORT",
    # "https": "http://YOUR_PROXY:PORT",
}

# ------------------ Token-Bucket Rate Limiter ------------------
class TokenBucketRateLimiter:
    """
    Simple Token-Bucket rate limiter:
    - 'tokens_per_interval' tokens are replenished every 'interval' seconds,
      up to 'max_tokens'.
    - Each .acquire_token() call consumes 1 token, blocking if necessary.
    """
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

# Create a global rate limiter instance
rate_limiter = TokenBucketRateLimiter(tokens_per_interval=1, interval=7.0, max_tokens=1)

# ------------------ Kafka Utility Functions ------------------
def create_topic(topic_name: str, num_partitions: int = 25, replication_factor: int = 2) -> None:
    start_cpu_time: float = time.process_time()
    start_cpu_percent: float = psutil.cpu_percent()
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id='NBA_team_boxscores'
        )
        topic_list = [NewTopic(name=topic_name,
                               num_partitions=num_partitions,
                               replication_factor=replication_factor)]
        existing_topics = admin_client.list_topics()

        if topic_name not in existing_topics:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logger.info(
                f"Topic '{topic_name}' created successfully with "
                f"{num_partitions} partitions and replication factor {replication_factor}."
            )
        else:
            logger.info(f"Topic '{topic_name}' already exists.")
    except TopicAlreadyExistsError:
        logger.info(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        logger.error(f"Failed to create Kafka topic: {e}")
    finally:
        end_cpu_time: float = time.process_time()
        end_cpu_percent = psutil.cpu_percent()
        logger.info(
            f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
            f"CPU time for create_topic(): {end_cpu_time - start_cpu_time:.4f} seconds"
        )

def create_producer() -> KafkaProducer:
    start_cpu_time: float = time.process_time()
    start_cpu_percent: float = psutil.cpu_percent()
    try:
        producer = KafkaProducer(**PRODUCER_CONFIG)
        logger.info("Kafka Producer created successfully.")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka Producer: {e}")
        raise
    finally:
        end_cpu_time: float = time.process_time()
        end_cpu_percent = psutil.cpu_percent()
        logger.info(
            f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
            f"CPU time for create_producer(): {end_cpu_time - start_cpu_time:.4f} seconds"
        )

# ------------------ Build URL for Team Boxscores (Using Larger Counter & Multi-Day) ------------------
def build_boxscore_url(
    season: str,
    season_type: str,
    date_from: str,
    date_to: str
) -> str:
    """
    Similar to player boxscores, but PlayerOrTeam=T for teams.
    Using Counter=999999 to ensure we get all rows in one shot.
    """
    base_url = "https://stats.nba.com/stats/leaguegamelog"
    return (
        f"{base_url}?Counter=999999"
        f"&DateFrom={date_from}"
        f"&DateTo={date_to}"
        f"&Direction=DESC"
        f"&ISTRound="
        f"&LeagueID=00"
        f"&PlayerOrTeam=T"
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
    season: str,
    season_type: str,
    date_from: str,
    date_to: str,
    session: requests.Session,
    retries: int = 3,
    timeout: int = 240
) -> None:
    """
    Fetch team boxscore data for a given date range, then send to Kafka.
    """
    start_cpu_time: float = time.process_time()
    start_cpu_percent: float = psutil.cpu_percent()

    url = build_boxscore_url(season, season_type, date_from, date_to)

    # Check the cache first
    cached_data = NBADataCache.get(url)
    if cached_data is not None:
        logger.info(f"[CACHE] Using cached data for {url}")
        data = cached_data
    else:
        data = None
        attempt = 0
        while attempt < retries:
            # Throttle calls
            rate_limiter.acquire_token()
            try:
                response = session.get(url, headers=NBA_HEADERS, timeout=timeout)

                # If throttled or unavailable
                if response.status_code == 443 or response.status_code == 503:
                    logger.warning(f"Got HTTP {response.status_code} -> Throttled/connection issue. Closing session & retrying.")
                    # Close session, open a fresh one, wait, and re-try
                    session.close()
                    session = requests.Session()
                    time.sleep(120)
                    attempt += 1
                    continue

                response.raise_for_status()
                data = response.json()

                # Store in cache
                NBADataCache.set(url, data)
                break

            except requests.exceptions.RequestException as e:
                logger.error(f"Attempt {attempt + 1} failed for {url} with error: {e}")

                # If it's a connection-related error, close/recreate session & retry
                if isinstance(e, requests.exceptions.ConnectionError):
                    logger.error("Connection error encountered. Closing session & forcing a new one.")
                    session.close()
                    session = requests.Session()

                if attempt < retries - 1:
                    # Exponential-ish backoff
                    sleep_time = 2 ** attempt
                    logger.info(f"Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"All {retries} attempts failed for {url}.")
                    log_failure(season, season_type, date_from, date_to)
                    end_cpu_time = time.process_time()
                    end_cpu_percent = psutil.cpu_percent()
                    logger.info(
                        f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
                        f"CPU time for fetch_and_send_boxscore_data("
                        f"{season}, {season_type}, {date_from}-{date_to}): "
                        f"{end_cpu_time - start_cpu_time:.4f} seconds"
                    )
                    return
            attempt += 1

    if not data:
        logger.warning(f"No data returned for {url}")
        end_cpu_time = time.process_time()
        end_cpu_percent = psutil.cpu_percent()
        logger.info(
            f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
            f"CPU time for fetch_and_send_boxscore_data("
            f"{season}, {season_type}, {date_from}-{date_to}): "
            f"{end_cpu_time - start_cpu_time:.4f} seconds"
        )
        return

    # Validate JSON with Pydantic
    try:
        validated_data = LeagueGameLog(**data)
    except ValidationError as val_err:
        logger.error(
            f"Data validation error for {season}, {season_type}, {date_from}-{date_to}: {val_err}"
        )
        end_cpu_time = time.process_time()
        end_cpu_percent = psutil.cpu_percent()
        logger.info(
            f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
            f"CPU time (validation error) fetch_and_send_boxscore_data("
            f"{season}, {season_type}, {date_from}-{date_to}): "
            f"{end_cpu_time - start_cpu_time:.4f} seconds"
        )
        return

    # Send each row to Kafka
    for result_set in validated_data.resultSets:
        if result_set.name == "LeagueGameLog":
            for row in result_set.rowSet:
                row_dict = dict(zip(result_set.headers, row))
                row_dict["Season"] = season
                row_dict["SeasonType"] = season_type
                row_dict["DateFrom"] = date_from
                row_dict["DateTo"] = date_to

                # We'll use GAME_ID as key
                key_str = str(row_dict.get("GAME_ID", "NO_GAMEID"))

                try:
                    producer.send(
                        topic,
                        key=key_str.encode('utf-8'),
                        value=row_dict
                    )
                except (NotLeaderForPartitionError, KafkaTimeoutError) as kafka_err:
                    logger.error(
                        f"Producer error for GAME_ID {key_str}: {kafka_err}"
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to send message to Kafka for GAME_ID {key_str}: {e}"
                    )

    logger.info(
        f"Team boxscore data for {season}, {season_type}, DateRange {date_from}-{date_to} sent to Kafka successfully."
    )

    end_cpu_time = time.process_time()
    end_cpu_percent = psutil.cpu_percent()
    logger.info(
        f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
        f"CPU time for fetch_and_send_boxscore_data("
        f"{season}, {season_type}, {date_from}-{date_to}): "
        f"{end_cpu_time - start_cpu_time:.4f} seconds"
    )

# ------------------ Date Helpers ------------------
def get_season_start_end_dates(season: str) -> (datetime, datetime):
    """
    For a given season string like "1946-47", return approximate start/end dates.
    """
    start_year = int(season[:4])
    end_year = start_year + 1

    approximate_start = datetime(start_year, 10, 1)
    approximate_end   = datetime(end_year, 6, 30)

    return approximate_start, approximate_end

def daterange_chunked(start_date: datetime, end_date: datetime, chunk_days: int = 365):
    """
    Yields tuples of (chunk_start, chunk_end) in increments of 'chunk_days'.
    This drastically reduces total requests vs day-by-day calls.
    Increase chunk_days or fetch entire seasons in one shot if you want even fewer calls.
    """
    chunk_start = start_date
    while chunk_start <= end_date:
        chunk_end = chunk_start + timedelta(days=chunk_days - 1)
        if chunk_end > end_date:
            chunk_end = end_date
        yield (chunk_start, chunk_end)
        chunk_start = chunk_end + timedelta(days=1)

# ------------------ Orchestrate All Combinations (Speed Layer) ------------------
def process_all_boxscore_combinations(producer: KafkaProducer, topic: str, max_workers: int = 1) -> None:
    """
    We chunk the date range to drastically reduce the number of requests.
    max_workers=1 => strictly sequential calls.
    """
    start_cpu_time = time.process_time()
    start_cpu_percent = psutil.cpu_percent()

    tasks = []
    session = requests.Session()
    if PROXIES:
        session.proxies.update(PROXIES)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for season_str in seasons:
            for _, season_type_value in season_type_map.items():
                start_date, end_date = get_season_start_end_dates(season_str)

                # Using 365-day chunks to reduce total requests
                for chunk_start, chunk_end in daterange_chunked(start_date, end_date, chunk_days=365):
                    date_from_str = chunk_start.strftime("%Y-%m-%d")
                    date_to_str = chunk_end.strftime("%Y-%m-%d")
                    tasks.append(
                        executor.submit(
                            fetch_and_send_boxscore_data,
                            producer,
                            topic,
                            season_str,
                            season_type_value,
                            date_from_str,
                            date_to_str,
                            session
                        )
                    )

        for future in as_completed(tasks):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error processing team boxscore data: {e}")

    end_cpu_time = time.process_time()
    end_cpu_percent = psutil.cpu_percent()
    logger.info(
        f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
        f"CPU time for process_all_boxscore_combinations(): {end_cpu_time - start_cpu_time:.4f} seconds"
    )

# ------------------ Batch Layer to Re-run Failures ------------------
def run_batch_layer(producer: KafkaProducer, topic: str) -> None:
    """
    Re-runs any combinations that permanently failed in the real-time (speed) layer,
    as recorded in 'failures_log.json'.
    """
    if not os.path.exists(FAILURES_LOG):
        logger.info("No failures_log.json found. No batch re-runs needed.")
        return

    logger.info("Starting batch layer re-run of missing data from failures_log.json...")
    with open(FAILURES_LOG, 'r') as f:
        failures = json.load(f)

    # Clear the file so we don't repeatedly re-run the same fails
    open(FAILURES_LOG, 'w').close()

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

# ------------------ cProfile and Logging Functions ------------------
profiler = cProfile.Profile()

def log_stats_periodically(prof: cProfile.Profile, interval: int = 120) -> None:
    while True:
        time.sleep(interval)
        s = io.StringIO()
        ps = pstats.Stats(prof, stream=s).sort_stats("cumulative")
        ps.print_stats(10)
        logger.info(f"[Periodic CPU profiling stats - Last {interval} seconds]:\n{s.getvalue()}")

# ------------------ Main Function ------------------
def main(topic: str = "NBA_team_boxscores") -> None:
    try:
        start_cpu_time_main = time.process_time()
        start_cpu_percent_main = psutil.cpu_percent()

        producer = create_producer()
        create_topic(topic, num_partitions=25, replication_factor=2)

        # 1) Speed Layer
        process_all_boxscore_combinations(producer, topic, max_workers=1)

        # 2) Batch Layer
        run_batch_layer(producer, topic)

        end_cpu_time_main = time.process_time()
        end_cpu_percent_main = psutil.cpu_percent()
        logger.info(
            f"CPU usage start: {start_cpu_percent_main}%, end: {end_cpu_percent_main}%. "
            f"CPU time for entire main block: {end_cpu_time_main - start_cpu_time_main:.4f} seconds"
        )
        logger.info('All NBA team boxscore data has been collected and sent to Kafka (real-time + batch).')

    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
    finally:
        try:
            producer.close()
            logger.info("Kafka Producer closed successfully.")
        except Exception as e:
            logger.error(f"Error closing Kafka Producer: {e}")

if __name__ == "__main__":
    profiler.enable()

    t = threading.Thread(target=log_stats_periodically, args=(profiler,), daemon=True)
    t.start()

    main()

    profiler.disable()
    s_final = io.StringIO()
    p_final = pstats.Stats(profiler, stream=s_final).sort_stats("cumulative")
    p_final.print_stats()
    logger.info(f"[Final CPU profiling stats]:\n{s_final.getvalue()}")
