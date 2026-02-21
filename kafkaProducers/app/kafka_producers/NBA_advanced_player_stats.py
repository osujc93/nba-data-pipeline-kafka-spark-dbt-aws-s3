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
    'batch_size': 1 * 1024 * 1024,
    'max_request_size': 10 * 1024 * 1024,
    'compression_type': 'gzip',
    'buffer_memory': 256 * 1024 * 1024,
    'max_in_flight_requests_per_connection': 5
}

# ------------------ Pydantic Models for NBA Player Data ------------------
class NBAPlayerResultSet(BaseModel):
    name: str
    headers: List[str]
    rowSet: List[List[Union[str, int, float, None]]]


class LeagueDashPlayerStats(BaseModel):
    resource: str
    parameters: dict
    resultSets: List[NBAPlayerResultSet]


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

per_modes: List[str] = [
    "PerGame", "Per100Possessions", "Totals", "Per100Plays", "Per48",
    "Per40", "Per36", "PerMinute", "PerPossession", "PerPlay", "MinutesPer"
]

# Example: from 1996-97 to 2024-25
seasons: List[str] = [f"{year}-{str(year+1)[-2:]}" for year in range(1996, 2025)]

valid_months_per_season_type: Dict[str, List[int]] = {
    "Regular%20Season": [1, 2, 3, 4, 5, 6, 7],
    "All%20Star": [5],
    "Pre%20Season": [1],
    "PlayIn": [7],
    "IST": [2, 3],
    "Playoffs": [7, 8, 9],
}

FAILURES_LOG = "failures_log.json"

# ------------------ ADDED: Optional Proxies ------------------
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
def create_topic(topic_name: str, num_partitions: int = 25, replication_factor: int = 2) -> None:
    start_cpu_time: float = time.process_time()
    start_cpu_percent: float = psutil.cpu_percent()
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id='NBA_advanced_player_stats'
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
        end_cpu_percent: float = psutil.cpu_percent()
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
        end_cpu_percent: float = psutil.cpu_percent()
        logger.info(
            f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
            f"CPU time for create_producer(): {end_cpu_time - start_cpu_time:.4f} seconds"
        )


# ------------------ NBA Player Data Processing Functions ------------------
def build_nba_player_url(
    season: str,
    season_type: str,
    # CHANGED: remove explicit "month" argument from function signature
    # month: int,
    per_mode: str,
    last_n_games: int
) -> str:
    """
    ADDED Counter=999999
    CHANGED Month=0 (entire season) so we do not call once per single month.
    """
    base_url: str = "https://stats.nba.com/stats/leaguedashplayerstats"
    return (
        f"{base_url}?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear="
        f"&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames={last_n_games}&LeagueID=00&Location=&MeasureType=Advanced"
        f"&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N"
        f"&PerMode={per_mode}&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N"
        f"&Rank=N&Season={season}&SeasonSegment=&SeasonType={season_type}"
        f"&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="
        f"&Counter=999999"  # <--- ADDED to avoid row limit cutoff
    )


def log_failure(
    season: str,
    season_type: str,
    # CHANGED: remove month param since we do Month=0
    per_mode: str,
    last_n_games: int
) -> None:
    """
    Append a failed combination to 'failures_log.json' so the batch layer can re-run it.
    """
    fail_entry = {
        "season": season,
        "season_type": season_type,
        "month": 0,  # forced to 0
        "per_mode": per_mode,
        "last_n_games": last_n_games
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


def fetch_and_send_player_data(
    producer: KafkaProducer,
    topic: str,
    season: str,
    season_type: str,
    # CHANGED: remove month param
    per_mode: str,
    session: requests.Session,
    last_n_games: int,
    retries: int = 3,
    timeout: int = 120
) -> None:
    start_cpu_time: float = time.process_time()
    start_cpu_percent: float = psutil.cpu_percent()

    # CHANGED: we always do Month=0, so we only do one bigger chunk for the entire season.
    url: str = build_nba_player_url(season, season_type, per_mode, last_n_games)

    # Check cache first
    cached_data = NBADataCache.get(url)
    if cached_data is not None:
        data = cached_data
        logger.info(
            f"[CACHE] Using cached player data for "
            f"{season}, {season_type}, month=0, per_mode={per_mode}, last_n_games={last_n_games}"
        )
    else:
        data = None
        for attempt in range(retries):
            rate_limiter.acquire_token()
            try:
                response = session.get(url, headers=NBA_HEADERS, timeout=timeout)

                # If 443 or 503
                if response.status_code in [443, 503]:
                    logger.warning(f"Got HTTP {response.status_code} -> Throttled. Sleeping 60s.")
                    time.sleep(60)
                    continue

                response.raise_for_status()
                data = response.json()
                NBADataCache.set(url, data)
                break
            except requests.exceptions.RequestException as e:
                logger.error(f"Attempt {attempt + 1} failed for {url} with error: {e}")
                if attempt < retries - 1:
                    sleep_time = 2 ** attempt
                    logger.info(f"Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"All {retries} attempts failed for {url}.")
                    # Log final failure so the batch layer can pick it up:
                    log_failure(season, season_type, per_mode, last_n_games)
                    end_cpu_time: float = time.process_time()
                    end_cpu_percent: float = psutil.cpu_percent()
                    logger.info(
                        f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
                        f"CPU time for fetch_and_send_player_data("
                        f"{season}, {season_type}, 0, {per_mode}, {last_n_games}): "
                        f"{end_cpu_time - start_cpu_time:.4f} seconds"
                    )
                    return

    if not data:
        logger.warning(f"No data returned for {url}")
        end_cpu_time: float = time.process_time()
        end_cpu_percent: float = psutil.cpu_percent()
        logger.info(
            f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
            f"CPU time for fetch_and_send_player_data("
            f"{season}, {season_type}, 0, {per_mode}, {last_n_games}): "
            f"{end_cpu_time - start_cpu_time:.4f} seconds"
        )
        return

    try:
        validated_data = LeagueDashPlayerStats(**data)
    except ValidationError as val_err:
        logger.error(
            f"Data validation error for {season}, {season_type}, 0, {per_mode}, {last_n_games}: {val_err}"
        )
        end_cpu_time: float = time.process_time()
        end_cpu_percent: float = psutil.cpu_percent()
        logger.info(
            f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
            f"CPU time (validation error) fetch_and_send_player_data("
            f"{season}, {season_type}, 0, {per_mode}, {last_n_games}): "
            f"{end_cpu_time - start_cpu_time:.4f} seconds"
        )
        return

    for result_set in validated_data.resultSets:
        if result_set.name == "LeagueDashPlayerStats":
            for row in result_set.rowSet:
                row_dict = dict(zip(result_set.headers, row))
                # We forcibly set Month=0 in the final data
                row_dict["Season"] = season
                row_dict["SeasonType"] = season_type
                row_dict["Month"] = 0
                row_dict["PerMode"] = per_mode
                row_dict["LastNGames"] = last_n_games

                try:
                    producer.send(
                        topic,
                        key=str(row_dict.get("PLAYER_ID", "0")).encode('utf-8'),
                        value=row_dict
                    )
                except (NotLeaderForPartitionError, KafkaTimeoutError) as kafka_err:
                    logger.error(
                        f"Producer error for PLAYER_ID {row_dict.get('PLAYER_ID', '0')}: {kafka_err}"
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to send message to Kafka for PLAYER_ID {row_dict.get('PLAYER_ID', '0')}: {e}"
                    )

    logger.info(
        f"Player data for {season}, {season_type}, Month=0, {per_mode}, "
        f"LastNGames={last_n_games} sent to Kafka successfully."
    )

    end_cpu_time: float = time.process_time()
    end_cpu_percent: float = psutil.cpu_percent()
    logger.info(
        f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
        f"CPU time for fetch_and_send_player_data("
        f"{season}, {season_type}, 0, {per_mode}, {last_n_games}): "
        f"{end_cpu_time - start_cpu_time:.4f} seconds"
    )


def process_all_player_combinations(producer: KafkaProducer, topic: str, max_workers: int = 1) -> None:
    """
    We set max_workers=1 for fully blocking I/O. 
    CHANGED: No longer iterate over months. We do Month=0 only (entire season).
    """
    start_cpu_time: float = time.process_time()
    start_cpu_percent: float = psutil.cpu_percent()

    tasks = []
    session = requests.Session()
    if PROXIES:
        session.proxies.update(PROXIES)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for season_str in seasons:
            for _, season_type_value in season_type_map.items():
                for pm in per_modes:
                    for last_n_games in range(16):
                        tasks.append(
                            executor.submit(
                                fetch_and_send_player_data,
                                producer,
                                topic,
                                season_str,
                                season_type_value,
                                pm,
                                session,
                                last_n_games
                            )
                        )

        for future in as_completed(tasks):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error processing NBA player data: {e}")

    end_cpu_time: float = time.process_time()
    end_cpu_percent: float = psutil.cpu_percent()
    logger.info(
        f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
        f"CPU time for process_all_player_combinations(): {end_cpu_time - start_cpu_time:.4f} seconds"
    )


# ------------------ Batch Layer to Re-run Failures ------------------
def run_batch_layer(producer: KafkaProducer, topic: str) -> None:
    """
    Re-runs any combinations that permanently failed in the real-time (speed) layer,
    as recorded in 'failures_log.json'. This ensures we fill in missing data eventually.
    """
    if not os.path.exists(FAILURES_LOG):
        logger.info("No failures_log.json found. No batch re-runs needed.")
        return

    logger.info("Starting batch layer re-run of missing data from failures_log.json...")
    with open(FAILURES_LOG, 'r') as f:
        failures = json.load(f)

    # Clear the file so we don't repeatedly re-run the same fails in a loop
    open(FAILURES_LOG, 'w').close()

    session = requests.Session()
    if PROXIES:
        session.proxies.update(PROXIES)

    for fail_entry in failures:
        season = fail_entry['season']
        season_type = fail_entry['season_type']
        pm = fail_entry['per_mode']
        last_n_games = fail_entry.get('last_n_games', 0)  # default 0 if not found

        fetch_and_send_player_data(
            producer=producer,
            topic=topic,
            season=season,
            season_type=season_type,
            per_mode=pm,
            session=session,
            last_n_games=last_n_games,
            retries=3,
            timeout=120
        )

    logger.info("Batch layer re-run completed.")


# ------------------ cProfile and Logging Functions ------------------
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
        logger.info(f"[Periodic CPU profiling stats - Last {interval} seconds]:\n{s.getvalue()}")


# ------------------ Main Function ------------------
def main(topic: str = "NBA_advanced_player_stats") -> None:
    """
    Speed layer: real-time fetch. Then triggers batch layer re-run for missed combos.
    """
    try:
        start_cpu_time_main: float = time.process_time()
        start_cpu_percent_main: float = psutil.cpu_percent()

        producer = create_producer()
        create_topic(topic, num_partitions=25, replication_factor=2)

        # 1) Speed Layer: process everything concurrently (with max_workers=1 => blocking)
        process_all_player_combinations(producer, topic, max_workers=1)

        # 2) Batch Layer: re-run any final fails
        run_batch_layer(producer, topic)

        end_cpu_time_main: float = time.process_time()
        end_cpu_percent_main: float = psutil.cpu_percent()
        logger.info(
            f"CPU usage start: {start_cpu_percent_main}%, end: {end_cpu_percent_main}%. "
            f"CPU time for entire main block: {end_cpu_time_main - start_cpu_time_main:.4f} seconds"
        )
        logger.info('All NBA player data has been collected and sent to Kafka (real-time + batch).')

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
