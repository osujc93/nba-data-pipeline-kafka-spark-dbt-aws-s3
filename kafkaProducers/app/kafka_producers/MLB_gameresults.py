#!/usr/bin/env python3
import numpy as np
from datetime import datetime, timedelta
import json
import logging
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import pandas as pd
from pydantic import BaseModel, ValidationError
from typing import List, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
#          MATCHING BOOTSTRAP & PRODUCER CONFIGS (from nbaboxscores.py)
# -----------------------------------------------------------------------------
BOOTSTRAP_SERVERS = [
    '172.20.10.2:9092',
    '172.20.10.3:9093',
    '172.20.10.4:9094'
]

PRODUCER_CONFIG = {
    'bootstrap_servers': BOOTSTRAP_SERVERS,
    'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
    'key_serializer': lambda x: str(x).encode('utf-8'),
    'retries': 10,
    'max_block_ms': 120000,
    'request_timeout_ms': 120000,
    'acks': 'all',
    'linger_ms': 6,
    'batch_size': 5 * 1024 * 1024,
    'max_request_size': 20 * 1024 * 1024,
    'compression_type': 'gzip',
    'buffer_memory': 512 * 1024 * 1024,
    'max_in_flight_requests_per_connection': 5
}

# -----------------------------------------------------------------------------
#              TOKEN-BUCKET RATE LIMITER (same approach as nbaboxscores.py)
# -----------------------------------------------------------------------------
class TokenBucketRateLimiter:
    """
    Simple Token-Bucket rate limiter:
    - 'tokens_per_interval' tokens are replenished every 'interval' seconds,
      up to 'max_tokens'.
    - Each .acquire_token() call consumes 1 token, blocking if necessary.
    """
    def __init__(self, tokens_per_interval: int = 1, interval: float = 2.0, max_tokens: int = 1):
        self.tokens_per_interval = tokens_per_interval
        self.interval = interval
        self.max_tokens = max_tokens
        self.available_tokens = max_tokens
        self.last_refill_time = time.monotonic()
        try:
            import threading
            self.lock = threading.Lock()
        except ImportError:
            # Fallback if threading not available
            self.lock = None

    def _refill(self) -> None:
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
        while True:
            if self.lock:
                with self.lock:
                    self._refill()
                    if self.available_tokens > 0:
                        self.available_tokens -= 1
                        return
            else:
                # If no lock is available, do best-effort
                self._refill()
                if self.available_tokens > 0:
                    self.available_tokens -= 1
                    return
            time.sleep(0.05)

# Create a global rate-limiter instance
rate_limiter = TokenBucketRateLimiter(tokens_per_interval=1, interval=2.0, max_tokens=1)

# -----------------------------------------------------------------------------
#             CREATE TOPIC  (Matching partition/replication approach)
# -----------------------------------------------------------------------------
def create_topic(topic_name: str, num_partitions: int = 25, replication_factor: int = 2):
    """
    Create a Kafka topic if it doesn't already exist.
    Now defaulting replication_factor=2 to match the NBA approach.
    """
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id='MLB_games_results'
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

# -----------------------------------------------------------------------------
#             CREATE PRODUCER (matching config from nbaboxscores.py)
# -----------------------------------------------------------------------------
def create_producer() -> KafkaProducer:
    try:
        producer = KafkaProducer(**PRODUCER_CONFIG)
        logger.info("Kafka Producer created successfully.")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka Producer: {e}")
        raise

# -----------------------------------------------------------------------------
#              DEFINE Pydantic MODELS FOR DATA VALIDATION
# -----------------------------------------------------------------------------
class LeagueRecord(BaseModel):
    wins: int | None = None
    losses: int | None = None
    pct: str | None = None

class Team(BaseModel):
    id: int
    name: str
    season: int | None = None
    teamCode: str | None = None
    abbreviation: str | None = None
    teamName: str | None = None
    locationName: str | None = None
    firstYearOfPlay: int | None = None
    shortName: str | None = None
    franchiseName: str | None = None
    clubName: str | None = None
    active: bool | None = None
    venue: dict | None = None
    league: dict | None = None
    division: dict | None = None
    sport: dict | None = None

class TeamData(BaseModel):
    team: Team
    score: int | None = None
    isWinner: bool | None = None
    leagueRecord: LeagueRecord | None = None

class GameTeams(BaseModel):
    away: TeamData
    home: TeamData

class Game(BaseModel):
    gamePk: int
    season: str
    officialDate: str
    teams: GameTeams

class DateInfo(BaseModel):
    date: str
    totalItems: int
    totalEvents: int
    totalGames: int
    totalGamesInProgress: int
    games: List[Game]

class ScheduleData(BaseModel):
    dates: List[DateInfo]

# -----------------------------------------------------------------------------
#       CHECK GAME EXISTENCE (HEAD request) with Rate Limit + Retries
# -----------------------------------------------------------------------------
def check_game_existence(
    session: requests.Session,
    start_date: datetime,
    end_date: datetime,
    game_type: str,
    retries: int = 3,
    timeout: int = 120
) -> bool:
    """
    HEAD request to see if data exists for the given date range/game_type.
    Rate limited, and if 443/503, close session & wait 120.
    """
    start_formatted_date = start_date.strftime('%Y-%m-%d')
    end_formatted_date = end_date.strftime('%Y-%m-%d')
    url = (
        f'https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate={start_formatted_date}'
        f'&endDate={end_formatted_date}&timeZone=America/New_York&gameType={game_type}'
        f'&language=en&leagueId=104&leagueId=103&leagueId=160&leagueId=590'
        f'&hydrate=team,linescore(matchup,runners),xrefId,story,flags,statusFlags,broadcasts(all)'
        f',venue(location),decisions,person,probablePitcher,stats,game(content(media(epg),summary),tickets)'
        f',seriesStatus(useOverride=true)&sortBy=gameDate,gameStatus,gameType'
    )

    attempt = 0
    while attempt < retries:
        # Acquire token first
        rate_limiter.acquire_token()
        try:
            resp = session.head(url, timeout=timeout)
            # Check for 443/503 throttling
            if resp.status_code in (443, 503):
                logger.warning(
                    f"[HEAD] HTTP {resp.status_code} -> Throttled or connection issue. "
                    f"Closing session & retrying for {url}"
                )
                session.close()
                session = requests.Session()
                time.sleep(120)
                attempt += 1
                continue

            resp.raise_for_status()
            return resp.status_code == 200
        except requests.RequestException as e:
            logger.error(f"[HEAD] Attempt {attempt+1} failed ({url}): {e}")
            if isinstance(e, requests.exceptions.ConnectionError):
                session.close()
                session = requests.Session()

            if attempt < retries - 1:
                backoff = 2 ** attempt
                logger.info(f"[HEAD] Retrying in {backoff} seconds...")
                time.sleep(backoff)
        attempt += 1

    logger.error(f"[HEAD] All {retries} attempts failed for checking {url}.")
    return False

# -----------------------------------------------------------------------------
#         FETCH & SEND DATA (GET request) with Rate Limit + Retries
# -----------------------------------------------------------------------------
def fetch_and_send_data(
    producer: KafkaProducer,
    session: requests.Session,
    start_date: datetime,
    end_date: datetime,
    game_type: str,
    retries: int = 3,
    timeout: int = 120
):
    """
    Fetch data from the MLB API (GET) with the same approach:
    - Single-thread blocking,
    - Rate limit, 
    - Session closed if 443/503,
    - Retry with exponential backoff.
    """
    game_type_mapping = {
        'R': 'Regular season',
        'A': 'All-Star Game',
        'P': 'Postseason',
        'F': 'Wild Card',
        'D': 'Division Series',
        'L': 'League Championship Series',
        'W': 'World Series',
        'S': 'Spring Training'
    }

    # HEAD check first
    if not check_game_existence(session, start_date, end_date, game_type):
        logger.warning(
            f"No game data found from {start_date.strftime('%Y-%m-%d')} "
            f"to {end_date.strftime('%Y-%m-%d')} for game type {game_type}. Skipping."
        )
        return

    start_formatted_date = start_date.strftime('%Y-%m-%d')
    end_formatted_date = end_date.strftime('%Y-%m-%d')
    api_url = (
        f'https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate={start_formatted_date}'
        f'&endDate={end_formatted_date}&timeZone=America/New_York&gameType={game_type}'
        f'&language=en&leagueId=104&leagueId=103&leagueId=160&leagueId=590'
        f'&hydrate=team,linescore(matchup,runners),xrefId,story,flags,statusFlags,broadcasts(all)'
        f',venue(location),decisions,person,probablePitcher,stats,game(content(media(epg),summary),tickets)'
        f',seriesStatus(useOverride=true)&sortBy=gameDate,gameStatus,gameType'
    )

    data = None
    attempt = 0
    while attempt < retries:
        # Acquire token from rate limiter
        rate_limiter.acquire_token()
        try:
            resp = session.get(api_url, timeout=timeout)
            # Handle 443/503
            if resp.status_code in (443, 503):
                logger.warning(
                    f"[GET] HTTP {resp.status_code} -> Throttled or connection issue. "
                    f"Closing session & retrying {api_url}"
                )
                session.close()
                session = requests.Session()
                time.sleep(120)
                attempt += 1
                continue

            resp.raise_for_status()
            data = resp.json()
            break
        except requests.RequestException as e:
            logger.error(f"[GET] Attempt {attempt+1} failed ({api_url}): {e}")
            if isinstance(e, requests.exceptions.ConnectionError):
                session.close()
                session = requests.Session()

            if attempt < retries - 1:
                backoff = 2 ** attempt
                logger.info(f"[GET] Retrying in {backoff} seconds...")
                time.sleep(backoff)
        attempt += 1

    if not data:
        logger.warning(f"No data returned or all retries failed for {api_url}.")
        return

    # Validate
    try:
        validated_data = ScheduleData(**data)
    except ValidationError as val_err:
        logger.error(f"Data validation error occurred: {val_err}")
        return

    # Parse & send to Kafka if we have valid data
    if not validated_data.dates:
        logger.info(
            f"No games found from {start_formatted_date} to {end_formatted_date} "
            f"for game type {game_type}."
        )
        return

    for date_info in validated_data.dates:
        for game in date_info.games:
            game_data = {}
            for side in ['away', 'home']:
                team_data = getattr(game.teams, side)
                team_info = pd.json_normalize(team_data.team.dict())
                team_info[f'{side}_score'] = team_data.score
                team_info[f'{side}_isWinner'] = team_data.isWinner
                if team_data.leagueRecord:
                    team_info[f'{side}_leagueRecord_wins'] = team_data.leagueRecord.wins
                    team_info[f'{side}_leagueRecord_losses'] = team_data.leagueRecord.losses
                    team_info[f'{side}_leagueRecord_pct'] = team_data.leagueRecord.pct

                team_info.columns = [f'{side}_team_{col}' for col in team_info.columns]
                df_side = team_info
                game_data.update(df_side.to_dict(orient='records')[0])

            game_data['game_id'] = game.gamePk

            # Extract season, month, day from officialDate
            game_date = game.officialDate
            if game_date != 'N/A':
                season, month, day = game_date.split('-')
            else:
                season, month, day = 'N/A', 'N/A', 'N/A'
            game_data['season'] = season
            game_data['month'] = month
            game_data['day'] = day

            # Add game type
            game_data['game_type'] = game_type_mapping.get(game_type, 'Unknown')

            # Send to Kafka
            producer.send('MLB_games_results', key=str(game.gamePk).encode('utf-8'), value=game_data)

    logger.info(
        f"Data from {start_formatted_date} to {end_formatted_date} for game type {game_type} "
        f"sent to Kafka successfully."
    )

# -----------------------------------------------------------------------------
#                  MAIN SCRIPT LOGIC
# -----------------------------------------------------------------------------
#  1) Create Producer
producer = create_producer()

#  2) Create topic with matching approach (25 partitions, replication factor=2)
create_topic('MLB_games_results', num_partitions=25, replication_factor=2)

# User-defined year range
start_year = 1901
end_year = 2024

# MLB season typical start/end
season_start_month = 2
season_start_day = 19
season_end_month = 11
season_end_day = 22

# Various MLB game types
game_types = ['R', 'A', 'P', 'F', 'D', 'L', 'W', 'S']

# -----------------------------------------------------------------------------
#                  PROCESS A SINGLE YEAR (Now single-threaded)
# -----------------------------------------------------------------------------
def process_year(producer: KafkaProducer, year: int):
    """
    Processes all game data for the given year, chunked by 29 days,
    but in a single-thread, blocking approach (max_workers=1).
    """
    session = requests.Session()
    start_date = datetime(year, season_start_month, season_start_day)
    end_date = datetime(year, season_end_month, season_end_day)

    current_date = start_date
    while current_date <= end_date:
        batch_end_date = min(current_date + timedelta(days=29), end_date)

        # Instead of multi-threading, do one game_type at a time, single-thread.
        for game_type in game_types:
            fetch_and_send_data(
                producer=producer,
                session=session,
                start_date=current_date,
                end_date=batch_end_date,
                game_type=game_type,
                retries=3,
                timeout=120
            )

        # Move forward to next chunk
        current_date = batch_end_date + timedelta(days=1)

        # Sleep a random 1-3 seconds before next chunk
        lag = np.random.uniform(1, 3)
        logger.info(f"Waiting {round(lag, 1)} seconds before next request chunk.")
        time.sleep(lag)

    session.close()

# -----------------------------------------------------------------------------
#         PROCESS YEARS IN BATCHES (But each in a single thread)
# -----------------------------------------------------------------------------
def process_years_in_batches(start_year: int, end_year: int, batch_size: int = 10):
    """
    You can still chunk the years in 'batches', but run them in a single-threaded
    loop. If you truly need them strictly sequential, just remove concurrency
    entirely. Below we set max_workers=1 to keep it single-thread blocking.
    """
    years = list(range(start_year, end_year + 1))
    for i in range(0, len(years), batch_size):
        batch = years[i:i + batch_size]

        # We'll keep a "ThreadPoolExecutor" but set max_workers=1 for fully
        # blocking, effectively running one batch at a time in a single thread.
        with ThreadPoolExecutor(max_workers=1) as executor:
            future_to_year = {executor.submit(process_year, producer, y): y for y in batch}

            for fut in as_completed(future_to_year):
                y = future_to_year[fut]
                try:
                    fut.result()
                except Exception as e:
                    logger.error(f"Error processing year {y}: {e}")

# -----------------------------------------------------------------------------
#       RUN THE FULL PROCESS
# -----------------------------------------------------------------------------
process_years_in_batches(start_year, end_year, batch_size=10)
logger.info("All data has been collected and sent to Kafka.")

# Close the Kafka producer
producer.close()
