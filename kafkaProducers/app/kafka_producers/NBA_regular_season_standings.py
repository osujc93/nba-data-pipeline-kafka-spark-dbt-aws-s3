#!/usr/bin/env python3
import argparse
import json
import logging
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Union, Dict, Optional
import cProfile
import pstats
import io
import threading
import psutil  # For CPU usage logging
import os

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.errors import NotLeaderForPartitionError, KafkaTimeoutError
from pydantic import BaseModel, ValidationError

# Import the NBADataCache
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
    'linger_ms': 100,
    'batch_size': 1 * 512 * 512,   # 1 MB
    'max_request_size': 2 * 1024 * 1024,
    'compression_type': 'gzip',
    'buffer_memory': 256 * 1024 * 1024,
    'max_in_flight_requests_per_connection': 5
}

DEFAULT_TOPIC: str = "NBA_regular_season_standings"

# ------------------ Pydantic Models for NBA Standings Data ------------------
class NBAStandingsResultSet(BaseModel):
    name: str
    headers: List[str]
    rowSet: List[List[Union[str, int, float, None]]]


class LeagueStandings(BaseModel):
    resource: str
    parameters: Dict[str, Union[str, int]]
    resultSets: List[NBAStandingsResultSet]


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
    )
}

SEASON_TYPE: str = "Regular%20Season"
SECTIONS: List[str] = ["overall", "streaks", "ab", "margins", "vs", "calendar"]

# Seasons from 1970-71 to 2024-25
SEASONS: List[str] = [f"{year}-{str(year + 1)[-2:]}" for year in range(1970, 2025)]

FAILURES_LOG = "failures_log_season_standings.json"

# ------------------ ADDED: Optional Proxies ------------------
PROXIES = {
    # "http": "http://YOUR_PROXY:PORT",
    # "https": "http://YOUR_PROXY:PORT",
}

# ------------------ Token-Bucket Rate Limiter ------------------
class TokenBucketRateLimiter:
    def __init__(self, tokens_per_interval: int = 1, interval: float = 4.0, max_tokens: int = 1):
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

rate_limiter = TokenBucketRateLimiter(tokens_per_interval=1, interval=4.0, max_tokens=1)

# ------------------ Kafka Utility Functions ------------------
def create_topic(
    topic_name: str,
    num_partitions: int = 25,
    replication_factor: int = 2
) -> None:
    start_cpu_time: float = time.process_time()
    start_cpu_percent: float = psutil.cpu_percent(interval=None)
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id='NBA_regular_season_standings'
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
        end_cpu_percent: float = psutil.cpu_percent(interval=None)
        logger.info(
            f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
            f"CPU time for create_topic(): {end_cpu_time - start_cpu_time:.4f} seconds"
        )


def create_producer() -> KafkaProducer:
    start_cpu_time: float = time.process_time()
    start_cpu_percent: float = psutil.cpu_percent(interval=None)
    try:
        producer = KafkaProducer(**PRODUCER_CONFIG)
        logger.info("Kafka Producer created successfully.")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka Producer: {e}")
        raise
    finally:
        end_cpu_time: float = time.process_time()
        end_cpu_percent: float = psutil.cpu_percent(interval=None)
        logger.info(
            f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
            f"CPU time for create_producer(): {end_cpu_time - start_cpu_time:.4f} seconds"
        )

# ------------------ Batch Layer Helper ------------------
def log_failure(season: str, section: str) -> None:
    fail_entry = {
        "season": season,
        "section": section
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

# ------------------ NBA Standings Data Processing Functions ------------------
def build_nba_standings_url(season: str, section: str) -> str:
    """
    Original logic remains, but we can add &Counter=999999 if needed.
    For standings specifically, it's a different endpoint. We'll just append it anyway.
    """
    base_url: str = "https://stats.nba.com/stats/leaguestandingsv3"
    return (
        f"{base_url}?GroupBy=conf&LeagueID=00&Season={season}"
        f"&SeasonType={SEASON_TYPE}&Section={section}"
        f"&Counter=999999"  # <--- appended for consistency
    )

def fetch_and_send_standings_data(
    producer: KafkaProducer,
    topic: str,
    season: str,
    section: str,
    session: requests.Session,
    retries: int = 3,
    timeout: int = 120
) -> None:
    start_cpu_time: float = time.process_time()
    start_cpu_percent: float = psutil.cpu_percent(interval=None)

    url: str = build_nba_standings_url(season, section)

    cached_data = NBADataCache.get(url)
    if cached_data is not None:
        data = cached_data
        logger.info(f"[CACHE] Using cached standings data for Season: {season}, Section: {section}")
    else:
        data: Optional[Dict] = None
        for attempt in range(retries):
            rate_limiter.acquire_token()
            try:
                response = session.get(url, headers=NBA_HEADERS, timeout=timeout)

                # Handle 443/503 throttling
                if response.status_code in [443, 503]:
                    logger.warning(f"Got HTTP {response.status_code} -> Throttled. Sleeping 60s.")
                    time.sleep(60)
                    continue

                response.raise_for_status()
                data = response.json()
                NBADataCache.set(url, data)
                logger.info(f"Successfully fetched data for Season: {season}, Section: {section}")
                break
            except requests.exceptions.RequestException as e:
                logger.error(f"Attempt {attempt + 1} failed for {url} with error: {e}")
                if attempt < retries - 1:
                    sleep_time = 2 ** attempt
                    logger.info(f"Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"All {retries} attempts failed for {url}.")
                    log_failure(season, section)
                    end_cpu_time: float = time.process_time()
                    end_cpu_percent: float = psutil.cpu_percent(interval=None)
                    logger.info(
                        f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
                        f"CPU time for fetch_and_send_standings_data({season}, {section}): "
                        f"{end_cpu_time - start_cpu_time:.4f} seconds"
                    )
                    return

    if not data:
        logger.warning(f"No data returned for {url}")
        end_cpu_time: float = time.process_time()
        end_cpu_percent: float = psutil.cpu_percent(interval=None)
        logger.info(
            f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
            f"CPU time for fetch_and_send_standings_data({season}, {section}): "
            f"{end_cpu_time - start_cpu_time:.4f} seconds"
        )
        return

    try:
        validated_data = LeagueStandings(**data)
    except ValidationError as val_err:
        logger.error(
            f"Data validation error for Season: {season}, Section: {section}: {val_err}"
        )
        end_cpu_time: float = time.process_time()
        end_cpu_percent: float = psutil.cpu_percent(interval=None)
        logger.info(
            f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
            f"CPU time (validation error) fetch_and_send_standings_data({season}, {section}): "
            f"{end_cpu_time - start_cpu_time:.4f} seconds"
        )
        return

    for result_set in validated_data.resultSets:
        if result_set.name == "Standings":
            for row in result_set.rowSet:
                row_dict = dict(zip(result_set.headers, row))
                row_dict["Season"] = season
                row_dict["Section"] = section

                try:
                    team_id = row_dict.get("TeamID", "0")
                    producer.send(
                        topic,
                        key=str(team_id).encode('utf-8'),
                        value=row_dict
                    )
                except (NotLeaderForPartitionError, KafkaTimeoutError) as kafka_err:
                    logger.error(f"Producer error for TeamID {team_id}: {kafka_err}")
                except Exception as e:
                    logger.error(f"Failed to send message to Kafka for TeamID {team_id}: {e}")

    logger.info(
        f"Standings data for Season: {season}, Section: {section} sent to Kafka successfully."
    )

    end_cpu_time: float = time.process_time()
    end_cpu_percent: float = psutil.cpu_percent(interval=None)
    logger.info(
        f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
        f"CPU time for fetch_and_send_standings_data({season}, {section}): "
        f"{end_cpu_time - start_cpu_time:.4f} seconds"
    )

def process_all_standings_combinations(
    producer: KafkaProducer,
    topic: str,
    max_workers: int = 1
) -> None:
    """
    We'll keep the same approach: we do 1 request per (season, section).
    ADDED &Counter=999999 in the build_nba_standings_url function.
    """
    start_cpu_time: float = time.process_time()
    start_cpu_percent: float = psutil.cpu_percent(interval=None)

    tasks = []
    session = requests.Session()

    # Apply optional proxies if needed
    if PROXIES:
        session.proxies.update(PROXIES)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for season in SEASONS:
            for section in SECTIONS:
                tasks.append(
                    executor.submit(
                        fetch_and_send_standings_data,
                        producer,
                        topic,
                        season,
                        section,
                        session
                    )
                )

        for future in as_completed(tasks):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error processing NBA standings data: {e}")

    end_cpu_time: float = time.process_time()
    end_cpu_percent: float = psutil.cpu_percent(interval=None)
    logger.info(
        f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
        f"CPU time for process_all_standings_combinations(): "
        f"{end_cpu_time - start_cpu_time:.4f} seconds"
    )

# ------------------ Batch Layer to Re-run Failures ------------------
def run_batch_layer(producer: KafkaProducer, topic: str) -> None:
    if not os.path.exists(FAILURES_LOG):
        logger.info("No failures log found for season_standings. No batch re-runs needed.")
        return

    logger.info("Starting batch layer re-run from failures_log_season_standings.json...")
    with open(FAILURES_LOG, 'r') as f:
        failures = json.load(f)

    # Clear file
    open(FAILURES_LOG, 'w').close()

    session = requests.Session()
    if PROXIES:
        session.proxies.update(PROXIES)

    for fail_entry in failures:
        season = fail_entry['season']
        section = fail_entry['section']

        fetch_and_send_standings_data(
            producer=producer,
            topic=topic,
            season=season,
            section=section,
            session=session,
            retries=3,
            timeout=120
        )

    logger.info("Batch layer re-run for season_standings completed.")

# ------------------ cProfile and Logging Functions ------------------
profiler = cProfile.Profile()

def log_stats_periodically(prof: cProfile.Profile, interval: int = 120) -> None:
    while True:
        time.sleep(interval)
        s = io.StringIO()
        ps = pstats.Stats(prof, stream=s).sort_stats("cumulative")
        ps.print_stats(10)
        logger.info(
            f"[Periodic CPU profiling stats - Last {interval} seconds]:\n{s.getvalue()}"
        )

# ------------------ Main Function ------------------
def main(topic: str = DEFAULT_TOPIC) -> None:
    try:
        start_cpu_time_main: float = time.process_time()
        start_cpu_percent_main: float = psutil.cpu_percent(interval=None)

        producer = create_producer()
        create_topic(topic, num_partitions=25, replication_factor=2)

        # 1) Speed Layer
        process_all_standings_combinations(producer, topic, max_workers=1)

        # 2) Batch Layer
        run_batch_layer(producer, topic)

        end_cpu_time_main: float = time.process_time()
        end_cpu_percent_main: float = psutil.cpu_percent(interval=None)
        logger.info(
            f"CPU usage start: {start_cpu_percent_main}%, end: {end_cpu_percent_main}%. "
            f"CPU time for entire main block: {end_cpu_time_main - start_cpu_time_main:.4f} seconds"
        )
        logger.info('All NBA regular season standings data has been collected and sent to Kafka (speed + batch).')

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

    profiling_thread = threading.Thread(
        target=log_stats_periodically,
        args=(profiler,),
        daemon=True
    )
    profiling_thread.start()

    main()

    profiler.disable()
    s_final = io.StringIO()
    p_final = pstats.Stats(profiler, stream=s_final).sort_stats("cumulative")
    p_final.print_stats()
    logger.info(f"[Final CPU profiling stats]:\n{s_final.getvalue()}")
