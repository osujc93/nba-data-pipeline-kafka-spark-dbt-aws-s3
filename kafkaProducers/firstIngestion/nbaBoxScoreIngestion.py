import json
import logging
import os
import time
import requests
import psycopg2
from datetime import datetime, timedelta
from typing import Any, List, Dict, Optional
from urllib.parse import quote
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import (
    KafkaTimeoutError,
    NotLeaderForPartitionError,
    TopicAlreadyExistsError
)

from pydanticModels import LeagueGameLog
from tokenBucketRateLimiter import TokenBucketRateLimiter

logger = logging.getLogger(__name__)

# ------------------ Kafka Configuration ------------------
BOOTSTRAP_SERVERS: List[str] = [
    '172.20.10.2:9092',
    '172.20.10.3:9093',
    '172.20.10.4:9094'
]

# ------------------ Producer Configuration ------------------
PRODUCER_CONFIG: Dict[str, Any] = {
    'bootstrap_servers': BOOTSTRAP_SERVERS,
    'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
    'key_serializer': lambda x: str(x).encode('utf-8'),
    'retries': 10,
    'max_block_ms': 120000,
    'request_timeout_ms': 120000,
    'acks': 'all',  # Wait for all ISR
    'linger_ms': 6900,
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

# Define the seasons we want (1946-47 to 2024-25 as an example)
seasons: List[str] = [f"{year}-{str(year+1)[-2:]}" for year in range(1946, 2025)]

FAILURES_LOG = "failures_log.json"

class NBABoxScoreIngestion:
    """
    Class-based refactoring of the Kafka ingestion script.
    """

    def __init__(self, topic: str = "NBA_player_boxscores"):
        self.topic = topic
        self.producer = None
        self.rate_limiter = TokenBucketRateLimiter(
            tokens_per_interval=1,
            interval=7.0,
            max_tokens=1
        )

    def create_topic(self, topic_name: str, num_partitions: int = 6,
                     replication_factor: int = 3) -> None:
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

    def create_producer(self) -> None:
        try:
            self.producer = KafkaProducer(**PRODUCER_CONFIG)
            logger.info("Kafka Producer created successfully.")
        except Exception as exc:
            logger.error(f"Failed to create Kafka Producer: {exc}")
            raise

    @staticmethod
    def build_boxscore_url(
        season: str,
        season_type: str,
        date_from: str,
        date_to: str
    ) -> str:
        base_url = "https://stats.nba.com/stats/leaguegamelog"

        # Convert "YYYY-MM-DD" to "MM/DD/YYYY" and URL-encode
        df_obj = datetime.strptime(date_from, "%Y-%m-%d")
        dt_obj = datetime.strptime(date_to, "%Y-%m-%d")

        df_quoted = quote(df_obj.strftime("%m/%d/%Y"), safe="")
        dt_quoted = quote(dt_obj.strftime("%m/%d/%Y"), safe="")

        full_url = (
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
        return full_url

    @staticmethod
    def log_failure(season: str, season_type: str, date_from: str, date_to: str) -> None:
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

    @staticmethod
    def store_boxscore_data_in_postgres(rows: List[Dict]) -> None:
        try:
            if not rows:
                return  

            conn = psycopg2.connect(
                dbname="nelonba",
                user="nelonba",
                password="Password123456789",
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
        self,
        season: str,
        season_type: str,
        date_from: str,
        date_to: str,
        session: requests.Session,
        retries: int = 3,
        timeout: int = 240
    ) -> Optional[datetime]:

        url = self.build_boxscore_url(season, season_type, date_from, date_to)
        max_game_date: Optional[datetime] = None

        data = None
        attempt = 0
        try:
            while attempt < retries:
                self.rate_limiter.acquire_token()
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
                        self.log_failure(season, season_type, date_from, date_to)
                        return None
                attempt += 1

            if not data:
                logger.warning("No data returned for %s", url)
                return None

            # Validate JSON with Pydantic
            try:
                validated_data = LeagueGameLog(**data)
            except Exception as val_err:
                logger.error(
                    "Data validation error for %s, %s, %s-%s: %s",
                    season, season_type, date_from, date_to, val_err
                )
                return None

            rows_for_postgres = []

            # Send each row to Kafka, track max GAME_DATE
            for result_set in validated_data.resultSets:
                if result_set.name == "LeagueGameLog":
                    for row in result_set.rowSet:
                        row_dict = dict(zip(result_set.headers, row))
                        # Add some extra fields:
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
                            if self.producer:
                                self.producer.send(
                                    self.topic,
                                    key=key_str.encode('utf-8'),
                                    value=row_dict
                                )
                            else:
                                logger.error("Producer is not initialized.")
                        except (NotLeaderForPartitionError, KafkaTimeoutError) as kafka_err:
                            logger.error("Producer error for GAME_ID %s: %s", key_str, kafka_err)
                        except Exception as exc:
                            logger.error("Failed to send message to Kafka for GAME_ID %s: %s", key_str, exc)

                        # Collect the row for Postgres insert
                        rows_for_postgres.append(row_dict)

            self.store_boxscore_data_in_postgres(rows_for_postgres)

            logger.info(
                "Boxscore data for %s, %s, DateRange %s-%s sent to Kafka successfully.",
                season, season_type, date_from, date_to
            )

            return max_game_date
        finally:
            pass

    @staticmethod
    def get_season_start_end_dates(season: str) -> (datetime, datetime):
        start_year = int(season[:4])
        end_year = start_year + 1

        approximate_start = datetime(start_year, 10, 1)
        approximate_end = datetime(end_year, 6, 30)

        return approximate_start, approximate_end

    @staticmethod
    def daterange_chunked(
        start_date: datetime,
        end_date: datetime,
        chunk_days: int = 730
    ):
        chunk_start = start_date
        while chunk_start <= end_date:
            chunk_end = chunk_start + timedelta(days=chunk_days - 1)
            if chunk_end > end_date:
                chunk_end = end_date
            yield chunk_start, chunk_end
            chunk_start = chunk_end + timedelta(days=1)

    def process_all_boxscore_combinations(
        self,
        max_workers: int = 1
    ) -> datetime:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        tasks = []
        last_processed_date = datetime(1900, 1, 1)
        session = requests.Session()

        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for season_str in seasons:
                    for _, season_type_value in season_type_map.items():
                        start_date, end_date = self.get_season_start_end_dates(season_str)
                        for chunk_start, chunk_end in self.daterange_chunked(
                            start_date,
                            end_date,
                        ):
                            future = executor.submit(
                                self.fetch_and_send_boxscore_data,
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
        finally:
            pass

        return last_processed_date

    def run_batch_layer(self) -> None:
        if not os.path.exists(FAILURES_LOG):
            logger.info("No failures_log.json found. No batch re-runs needed.")
            return

        logger.info("Starting batch layer re-run of missing data from failures_log.json...")
        with open(FAILURES_LOG, 'r', encoding='utf-8') as file:
            failures = json.load(file)

        # Clear the file so we don't repeatedly re-run the same fails in a loop
        open(FAILURES_LOG, 'w', encoding='utf-8').close()

        session = requests.Session()

        for fail_entry in failures:
            season = fail_entry['season']
            season_type = fail_entry['season_type']
            date_from = fail_entry['date_from']
            date_to = fail_entry['date_to']

            self.fetch_and_send_boxscore_data(
                season=season,
                season_type=season_type,
                date_from=date_from,
                date_to=date_to,
                session=session,
                retries=3,
                timeout=240
            )

        logger.info("Batch layer re-run completed.")

    @staticmethod
    def finalize_batch_in_postgres(final_ingested_date: datetime) -> None:
        try:
            conn = psycopg2.connect(
                dbname="nelonba",
                user="nelonba",
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

    def run(self) -> None:
        try:
            self.create_producer()
            self.create_topic(self.topic, num_partitions=6, replication_factor=3)

            final_date_ingested = self.process_all_boxscore_combinations(max_workers=1)

            self.run_batch_layer()

            self.finalize_batch_in_postgres(final_date_ingested)

            logger.info("All NBA boxscore data has been collected and sent to Kafka (real-time + batch).")

        except Exception as exc:
            logger.error("Unexpected error occurred: %s", exc)
        finally:
            if self.producer:
                try:
                    self.producer.close()
                    logger.info("Kafka Producer closed successfully.")
                except Exception as close_exc:
                    logger.error("Error closing Kafka Producer: %s", close_exc)
