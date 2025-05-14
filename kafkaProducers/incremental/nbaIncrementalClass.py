import json
import logging
import os
import requests
import psycopg2
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Union, List, Optional
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NotLeaderForPartitionError, KafkaTimeoutError

from pydanticModels import LeagueGameLog
from tokenBucketRateLimiter import TokenBucketRateLimiter

logger = logging.getLogger(__name__)

# ------------------ Kafka Configuration ------------------
BOOTSTRAP_SERVERS: List[str] = [
    '172.20.10.2:9092',
    '172.20.10.3:9093',
    '172.20.10.4:9094'
]

PRODUCER_CONFIG: Dict[str, Union[str, int]] = {
    'bootstrap_servers': BOOTSTRAP_SERVERS,
    'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
    'key_serializer': lambda x: str(x).encode('utf-8'),
    'retries': 10,
    'max_block_ms': 120000,
    'request_timeout_ms': 120000,
    'acks': 'all',
    'linger_ms': 7700,
    'batch_size': 5 * 1024 * 1024,
    'max_request_size': 20 * 1024 * 1024,
    'compression_type': 'gzip',
    'buffer_memory': 512 * 1024 * 1024,
    'max_in_flight_requests_per_connection': 5
}

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

FAILURES_LOG = "failures_log_incremental.json"

# The season we use for incremental loads
CURRENT_SEASON = "2024-25"


class NBAIncrementalClass:
    """
    daily incremental NBA boxscore Kafka ingestion.
    """

    def __init__(
        self,
        pg_host: str = "postgres",
        pg_port: int = 5432,
        pg_db: str = "nelonba",
        pg_user: str = "nelonba",
        pg_password: str = "Password123456789",
        topic: str = "NBA_player_boxscores_incr"
    ):
        self.pg_host = pg_host
        self.pg_port = pg_port
        self.pg_db = pg_db
        self.pg_user = pg_user
        self.pg_password = pg_password
        self.topic = topic

        self.producer = None
        self.rate_limiter = TokenBucketRateLimiter(
            tokens_per_interval=1,
            interval=7.0,
            max_tokens=1
        )
        self.proxies = {}

    def create_topic(self, topic_name: str, num_partitions: int = 6, replication_factor: int = 3) -> None:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                client_id='NBA_player_boxscores_incremental'
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
                    f"{num_partitions} partitions, replication factor {replication_factor}."
                )
            else:
                logger.info(f"Topic '{topic_name}' already exists.")
        except TopicAlreadyExistsError:
            logger.info(f"Topic '{topic_name}' already exists.")
        except Exception as e:
            logger.error(f"Failed to create Kafka topic: {e}")

    def create_producer(self) -> None:
        try:
            self.producer = KafkaProducer(**PRODUCER_CONFIG)
            logger.info("Kafka Producer (incremental) created successfully.")
        except Exception as e:
            logger.error(f"Failed to create Kafka Producer: {e}")
            raise

    def get_last_ingestion_date(self) -> datetime:
        default_date = datetime(1946, 10, 1)
        conn = None
        try:
            conn = psycopg2.connect(
                dbname=self.pg_db,
                user=self.pg_user,
                password=self.pg_password,
                host=self.pg_host,
                port=self.pg_port
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

            # Attempt to fetch the latest entry
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
            if conn:
                conn.close()

    def update_last_ingestion_date(self, new_last_date: datetime) -> None:
        """
        Only call this if we actually ingested data. 
        """
        conn = None
        try:
            conn = psycopg2.connect(
                dbname=self.pg_db,
                user=self.pg_user,
                password=self.pg_password,
                host=self.pg_host,
                port=self.pg_port
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
            if conn:
                conn.close()

    @staticmethod
    def build_boxscore_url(
        season: str,
        season_type: str,
        date_from: str,
        date_to: str
    ) -> str:
        base_url = "https://stats.nba.com/stats/leaguegamelog"

        df_obj = datetime.strptime(date_from, "%Y-%m-%d")
        dt_obj = datetime.strptime(date_to, "%Y-%m-%d")

        df_quoted = quote(df_obj.strftime("%m/%d/%Y"), safe="")
        dt_quoted = quote(dt_obj.strftime("%m/%d/%Y"), safe="")

        return (
            f"{base_url}?Counter=1000"
            f"&DateFrom={df_quoted}"
            f"&DateTo={dt_quoted}"
            f"&Direction=DESC"
            f"&ISTRound="
            f"&LeagueID=00"
            f"&PlayerOrTeam=P"
            f"&Season={season}"
            f"&SeasonType={season_type}"
            f"&Sorter=DATE"
        )

    @staticmethod
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
        self,
        date_from: str,
        date_to: str,
        season_type: str,
        session: requests.Session,
        retries: int = 3,
        timeout: int = 240
    ) -> int:

        url = self.build_boxscore_url(
            season=CURRENT_SEASON,
            season_type=season_type,
            date_from=date_from,
            date_to=date_to
        )

        data = None
        attempt = 0
        total_rows_ingested = 0

        try:
            while attempt < retries:
                self.rate_limiter.acquire_token()
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
                    break
                except requests.exceptions.RequestException as e:
                    logger.error(f"Attempt {attempt+1} failed for {url} with error: {e}")
                    if attempt < retries - 1:
                        time.sleep(2 ** attempt)
                    else:
                        logger.error(f"All {retries} attempts failed for {url}.")
                        self.log_failure(date_from, date_to, season_type)
                        return 0  # No data ingested
                attempt += 1

            if not data:
                logger.warning(f"No data returned for {url}")
                return 0

            # Validate JSON with Pydantic
            try:
                validated_data = LeagueGameLog(**data)
            except Exception as val_err:
                logger.error(
                    f"[Incremental] Data validation error for "
                    f"date_from={date_from}, date_to={date_to}: {val_err}"
                )
                return 0

            # Send rows to Kafka
            for result_set in validated_data.resultSets:
                if result_set.name == "LeagueGameLog":
                    for row in result_set.rowSet:
                        row_dict = dict(zip(result_set.headers, row))
                        
                        # -------------------------------
                        # Include Season field, just like the first batch
                        row_dict["Season"] = CURRENT_SEASON
                        # -------------------------------

                        row_dict["DateFrom"] = date_from
                        row_dict["DateTo"] = date_to
                        row_dict["SeasonType"] = season_type

                        key_str = str(row_dict.get("GAME_ID", "NO_GAMEID"))
                        try:
                            if self.producer:
                                self.producer.send(
                                    self.topic,
                                    key=key_str.encode('utf-8'),
                                    value=row_dict
                                )
                                total_rows_ingested += 1
                            else:
                                logger.error("[Incremental] Producer is not initialized.")
                        except (NotLeaderForPartitionError, KafkaTimeoutError) as kafka_err:
                            logger.error(f"[Incremental] Producer error for GAME_ID {key_str}: {kafka_err}")
                        except Exception as e:
                            logger.error(
                                f"[Incremental] Failed to send message to Kafka for GAME_ID {key_str}: {e}"
                            )

            if total_rows_ingested > 0:
                logger.info(
                    f"[Incremental] Sent {total_rows_ingested} rows to Kafka for date range "
                    f"{date_from} - {date_to}, SeasonType={season_type}"
                )
            else:
                logger.info(
                    f"[Incremental] 0 rows found for date range {date_from} - {date_to}, SeasonType={season_type}"
                )

            return total_rows_ingested
        finally:
            pass

    def run_incremental_load(self) -> int:
        """
        Returns the number of rows ingested so we know whether to update the last_ingestion_date.
        """
        total_ingested_overall = 0
        try:
            last_date = self.get_last_ingestion_date()
            start_date = last_date + timedelta(days=1)
            end_date = datetime.today()

            if start_date > end_date:
                logger.info("[Incremental] No new dates to ingest.")
                return 0

            date_from_str = start_date.strftime("%Y-%m-%d")
            date_to_str = end_date.strftime("%Y-%m-%d")

            session = requests.Session()
            if self.proxies:
                session.proxies.update(self.proxies)

            tasks = []
            with ThreadPoolExecutor(max_workers=1) as executor:
                for _, stype_value in season_type_map.items():
                    future = executor.submit(
                        self.fetch_and_send_boxscore_data,
                        date_from_str,
                        date_to_str,
                        stype_value,
                        session
                    )
                    tasks.append(future)

                for f in as_completed(tasks):
                    try:
                        ingested_count = f.result()
                        total_ingested_overall += ingested_count
                    except Exception as exc:
                        logger.error(f"[Incremental] Error in thread: {exc}")

        finally:
            pass

        return total_ingested_overall

    def run_batch_layer_incremental(self) -> int:
        """
        Returns number of rows ingested from re-trying failures.
        """
        total_ingested_failures = 0
        try:
            if not os.path.exists(FAILURES_LOG):
                logger.info("[Incremental] No failures log found.")
                return 0

            with open(FAILURES_LOG, 'r') as f:
                failures = json.load(f)

            # Clear the file
            open(FAILURES_LOG, 'w').close()

            session = requests.Session()
            if self.proxies:
                session.proxies.update(self.proxies)

            tasks = []
            with ThreadPoolExecutor(max_workers=5) as executor:
                for fail_entry in failures:
                    date_from = fail_entry['date_from']
                    date_to = fail_entry['date_to']
                    season_type = fail_entry['season_type']
                    future = executor.submit(
                        self.fetch_and_send_boxscore_data,
                        date_from,
                        date_to,
                        season_type,
                        session
                    )
                    tasks.append(future)

                for f in as_completed(tasks):
                    try:
                        ingested_count = f.result()
                        total_ingested_failures += ingested_count
                    except Exception as exc:
                        logger.error(f"[Incremental][Batch] Error in thread: {exc}")

        finally:
            pass

        return total_ingested_failures

    def run(self) -> None:
        try:
            self.create_producer()
            self.create_topic(self.topic, num_partitions=6, replication_factor=3)

            # 1) Run incremental load
            ingested_main = self.run_incremental_load()

            # 2) Retry any failures
            ingested_failures = self.run_batch_layer_incremental()

            rows_ingested_total = ingested_main + ingested_failures
            if rows_ingested_total > 0:
                # We move last_date to "today" only if there's actually new data
                end_date = datetime.today()
                self.update_last_ingestion_date(end_date)
            else:
                logger.info("[Incremental] No new rows were ingested, so last_ingestion_date not updated.")

        except Exception as e:
            logger.error(f"[Incremental] Unexpected error: {e}")
        finally:
            if self.producer:
                self.producer.close()
                logger.info("[Incremental] Kafka Producer closed.")
