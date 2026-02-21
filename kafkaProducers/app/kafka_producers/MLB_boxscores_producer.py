#!/usr/bin/env python3
import argparse
import json
import logging
import requests
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
import pandas as pd

from pydantic import BaseModel, ValidationError, Field, field_validator, model_validator
from typing import Dict, Any, Union, List, Optional

# --- Profiling imports ---
import cProfile
import pstats
import time as pytime
import psutil  # for CPU usage

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------
#            MATCHING BOOTSTRAP & PRODUCER CONFIGS (from nbaboxscores.py)
# ------------------------------------------------------------------------
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

# ------------------------------------------------------------------------
#                     TOKEN-BUCKET RATE LIMITER
# ------------------------------------------------------------------------
class TokenBucketRateLimiter:
    """
    Simple Token-Bucket rate limiter:
    - 'tokens_per_interval' tokens are replenished every 'interval' seconds,
      up to 'max_tokens'.
    - Each .acquire_token() call consumes 1 token, blocking if necessary.
    """
    def __init__(self, tokens_per_interval: int = 1000, interval: float = 0.0001, max_tokens: int = 1000):
        self.tokens_per_interval = tokens_per_interval
        self.interval = interval
        self.max_tokens = max_tokens
        self.available_tokens = max_tokens
        self.last_refill_time = time.monotonic()
        try:
            import threading
            self.lock = threading.Lock()
        except ImportError:
            self.lock = None

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
            if self.lock:
                with self.lock:
                    self._refill()
                    if self.available_tokens > 0:
                        self.available_tokens -= 1
                        return
            else:
                # If no lock is available, do best effort
                self._refill()
                if self.available_tokens > 0:
                    self.available_tokens -= 1
                    return
            time.sleep(0.01)

rate_limiter = TokenBucketRateLimiter(tokens_per_interval=1000, interval=0.0001, max_tokens=1000)

# ------------------------------------------------------------------------
#                           TOPIC CREATION
# ------------------------------------------------------------------------
def create_topic(topic_name):
    start_cpu_time = pytime.process_time()
    start_cpu_percent = psutil.cpu_percent()
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id='MLB_boxscores_topic'
        )
        # Matching nbaboxscores.py: replication_factor=2
        topic_list = [NewTopic(name=topic_name, num_partitions=25, replication_factor=2)]
        existing_topics = admin_client.list_topics()

        if topic_name not in existing_topics:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logger.info(f"Topic '{topic_name}' created successfully.")
        else:
            logger.info(f"Topic '{topic_name}' already exists.")
    except TopicAlreadyExistsError:
        logger.info(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        logger.error(f"Failed to create Kafka topic: {e}")
    finally:
        end_cpu_time = pytime.process_time()
        end_cpu_percent = psutil.cpu_percent()
        logger.info(
            f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
            f"CPU time for create_topic(): {end_cpu_time - start_cpu_time:.4f} seconds"
        )

# ------------------------------------------------------------------------
#                        CREATE PRODUCER
# ------------------------------------------------------------------------
def create_producer():
    start_cpu_time = pytime.process_time()
    start_cpu_percent = psutil.cpu_percent()
    try:
        # Use our updated PRODUCER_CONFIG
        producer = KafkaProducer(**PRODUCER_CONFIG)
        logger.info("Kafka Producer created successfully.")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka Producer: {e}")
        raise
    finally:
        end_cpu_time = pytime.process_time()
        end_cpu_percent = psutil.cpu_percent()
        logger.info(
            f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
            f"CPU time for create_producer(): {end_cpu_time - start_cpu_time:.4f} seconds"
        )

# ------------------------------------------------------------------------
#              PYDANTIC MODELS FOR VALIDATING JSON STRUCTURE
# ------------------------------------------------------------------------
class PlayerStats(BaseModel):
    person_id: Optional[int] = None
    person_fullName: Optional[str] = None
    person_link: Optional[str] = None
    jerseyNumber: Optional[int] = None
    position_code: Optional[str] = None
    position_name: Optional[str] = None
    position_type: Optional[str] = None
    position_abbreviation: Optional[str] = None
    status_code: Optional[str] = None
    status_description: Optional[str] = None
    parentTeamId: Optional[int] = None
    battingOrder: Optional[int] = None
    batting_summary: Optional[str] = None
    batting_gamesPlayed: Optional[int] = None
    batting_flyOuts: Optional[int] = None
    batting_groundOuts: Optional[int] = None
    batting_airOuts: Optional[int] = None
    batting_runs: Optional[int] = None
    batting_doubles: Optional[int] = None
    batting_triples: Optional[int] = None
    batting_homeRuns: Optional[int] = None
    batting_strikeOuts: Optional[int] = None
    batting_baseOnBalls: Optional[int] = None
    batting_intentionalWalks: Optional[int] = None
    batting_hits: Optional[int] = None
    batting_hitByPitch: Optional[int] = None
    batting_atBats: Optional[int] = None
    batting_caughtStealing: Optional[int] = None
    batting_stolenBases: Optional[int] = None
    batting_stolenBasePercentage: Optional[float] = None
    batting_groundIntoDoublePlay: Optional[int] = None
    batting_groundIntoTriplePlay: Optional[int] = None
    batting_plateAppearances: Optional[int] = None
    batting_totalBases: Optional[int] = None
    batting_rbi: Optional[int] = None
    batting_leftOnBase: Optional[int] = None
    batting_sacBunts: Optional[int] = None
    batting_sacFlies: Optional[int] = None
    batting_catchersInterference: Optional[int] = None
    batting_pickoffs: Optional[int] = None
    batting_atBatsPerHomeRun: Optional[float] = None
    batting_popOuts: Optional[int] = None
    batting_lineOuts: Optional[int] = None
    fielding_gamesStarted: Optional[int] = None
    fielding_caughtStealing: Optional[int] = None
    fielding_stolenBases: Optional[int] = None
    fielding_stolenBasePercentage: Optional[float] = None
    fielding_assists: Optional[int] = None
    fielding_putOuts: Optional[int] = None
    fielding_errors: Optional[int] = None
    fielding_chances: Optional[int] = None
    fielding_fielding: Optional[float] = None
    fielding_passedBall: Optional[int] = None
    gameStatus_isCurrentBatter: Optional[bool] = None
    gameStatus_isCurrentPitcher: Optional[bool] = None
    gameStatus_isOnBench: Optional[bool] = None
    gameStatus_isSubstitute: Optional[bool] = None

class TeamStats(BaseModel):
    team_allStarStatus: Optional[str] = None
    team_id: Optional[int] = None
    team_name: Optional[str] = None
    team_link: Optional[str] = None
    teamStats_batting_flyOuts: Optional[int] = None
    teamStats_batting_groundOuts: Optional[int] = None
    teamStats_batting_airOuts: Optional[int] = None
    teamStats_batting_runs: Optional[int] = None
    teamStats_batting_doubles: Optional[int] = None
    teamStats_batting_triples: Optional[int] = None
    teamStats_batting_homeRuns: Optional[int] = None
    teamStats_batting_strikeOuts: Optional[int] = None
    teamStats_batting_baseOnBalls: Optional[int] = None
    teamStats_batting_intentionalWalks: Optional[int] = None
    teamStats_batting_hits: Optional[int] = None
    teamStats_batting_hitByPitch: Optional[int] = None
    teamStats_batting_avg: Optional[float] = None
    teamStats_batting_atBats: Optional[int] = None
    teamStats_batting_obp: Optional[float] = None
    teamStats_batting_slg: Optional[float] = None
    teamStats_batting_ops: Optional[float] = None
    teamStats_batting_caughtStealing: Optional[int] = None
    teamStats_batting_stolenBases: Optional[int] = None
    teamStats_batting_stolenBasePercentage: Optional[float] = None
    teamStats_batting_groundIntoDoublePlay: Optional[int] = None
    teamStats_batting_groundIntoTriplePlay: Optional[int] = None
    teamStats_batting_plateAppearances: Optional[int] = None
    teamStats_batting_totalBases: Optional[int] = None
    teamStats_batting_rbi: Optional[int] = None
    teamStats_batting_leftOnBase: Optional[int] = None
    teamStats_batting_sacBunts: Optional[int] = None
    teamStats_batting_sacFlies: Optional[int] = None
    teamStats_batting_catchersInterference: Optional[int] = None
    teamStats_batting_pickoffs: Optional[int] = None
    teamStats_batting_atBatsPerHomeRun: Optional[float] = None
    teamStats_batting_popOuts: Optional[int] = None
    teamStats_batting_lineOuts: Optional[int] = None
    teamStats_pitching_flyOuts: Optional[int] = None
    teamStats_pitching_groundOuts: Optional[int] = None
    teamStats_pitching_airOuts: Optional[int] = None
    teamStats_pitching_runs: Optional[int] = None
    teamStats_pitching_doubles: Optional[int] = None
    teamStats_pitching_triples: Optional[int] = None
    teamStats_pitching_homeRuns: Optional[int] = None
    teamStats_pitching_strikeOuts: Optional[int] = None
    teamStats_pitching_baseOnBalls: Optional[int] = None
    teamStats_pitching_intentionalWalks: Optional[int] = None
    teamStats_pitching_hits: Optional[int] = None
    teamStats_pitching_hitByPitch: Optional[int] = None
    teamStats_pitching_atBats: Optional[int] = None
    teamStats_pitching_obp: Optional[float] = None
    teamStats_pitching_caughtStealing: Optional[int] = None
    teamStats_pitching_stolenBases: Optional[int] = None
    teamStats_pitching_stolenBasePercentage: Optional[float] = None
    teamStats_pitching_numberOfPitches: Optional[int] = None
    teamStats_pitching_era: Optional[float] = None
    teamStats_pitching_inningsPitched: Optional[float] = None
    teamStats_pitching_saveOpportunities: Optional[int] = None
    teamStats_pitching_earnedRuns: Optional[int] = None
    teamStats_pitching_whip: Optional[float] = None
    teamStats_pitching_battersFaced: Optional[int] = None
    teamStats_pitching_outs: Optional[int] = None
    teamStats_pitching_completeGames: Optional[int] = None
    teamStats_pitching_shutouts: Optional[int] = None
    teamStats_pitching_pitchesThrown: Optional[int] = None
    teamStats_pitching_balls: Optional[int] = None
    teamStats_pitching_strikes: Optional[int] = None
    teamStats_pitching_strikePercentage: Optional[float] = None
    teamStats_pitching_hitBatsmen: Optional[int] = None
    teamStats_pitching_balks: Optional[int] = None
    teamStats_pitching_wildPitches: Optional[int] = None
    teamStats_pitching_pickoffs: Optional[int] = None
    teamStats_pitching_groundOutsToAirouts: Optional[float] = None
    teamStats_pitching_rbi: Optional[int] = None
    teamStats_pitching_pitchesPerInning: Optional[float] = None
    teamStats_pitching_runsScoredPer9: Optional[float] = None
    teamStats_pitching_homeRunsPer9: Optional[float] = None
    teamStats_pitching_inheritedRunners: Optional[int] = None
    teamStats_pitching_inheritedRunnersScored: Optional[int] = None
    teamStats_pitching_catchersInterference: Optional[int] = None
    teamStats_pitching_sacBunts: Optional[int] = None
    teamStats_pitching_sacFlies: Optional[int] = None
    teamStats_pitching_passedBall: Optional[int] = None
    teamStats_pitching_popOuts: Optional[int] = None
    teamStats_pitching_lineOuts: Optional[int] = None
    teamStats_fielding_caughtStealing: Optional[int] = None
    teamStats_fielding_stolenBases: Optional[int] = None
    teamStats_fielding_stolenBasePercentage: Optional[float] = None
    teamStats_fielding_assists: Optional[int] = None
    teamStats_fielding_putOuts: Optional[int] = None
    teamStats_fielding_errors: Optional[int] = None
    teamStats_fielding_chances: Optional[int] = None
    teamStats_fielding_passedBall: Optional[int] = None
    teamStats_fielding_pickoffs: Optional[int] = None

class GameDataStructure(BaseModel):
    gameData: Union[Dict[str, Any], List[Any]] = Field(default_factory=dict)
    liveData: Union[Dict[str, Any], List[Any]] = Field(default_factory=dict)
    alerts:   Union[Dict[str, Any], List[Any]] = Field(default_factory=dict)

# ------------------------------------------------------------------------
#           NEW MODEL TO FILTER OUT “NO DATA” TEAMS BOXSCORE
# ------------------------------------------------------------------------
class TeamsBoxscore(BaseModel):
    away_team_link: str
    home_team_link: str
    game_id: int
    season: str
    month: str
    day: str
    game_time: Optional[str] = None

    @model_validator(mode='after')
    def skip_no_data(cls, instance):
        """
        Reject an obviously empty game:
          - if game_id == 0
          - if both team links end with '/teams/null'
        """
        if instance.game_id == 0:
            raise ValueError("Invalid record: game_id=0 indicates no data.")
        if instance.away_team_link.endswith('teams/null') and instance.home_team_link.endswith('teams/null'):
            raise ValueError("Invalid record: both away_team_link and home_team_link are null.")
        return instance

# ------------------------------------------------------------------------
#                     FETCH GAME DATA WITH RETRIES
# ------------------------------------------------------------------------
def fetch_game_data(game_number, session: requests.Session, retries: int = 3, timeout: int = 240):
    """
    Fetch data from MLB API with a token-bucket rate limiter, handling 443/503 errors
    by closing the session and retrying after a 120-second sleep.
    """
    start_cpu_time = pytime.process_time()
    start_cpu_percent = psutil.cpu_percent()
    url = f"https://ws.statsapi.mlb.com/api/v1.1/game/{game_number}/feed/live?language=en"

    data = None
    attempt = 0
    while attempt < retries:
        # Acquire a token before each attempt to slow down requests
        rate_limiter.acquire_token()
        try:
            response = session.get(url, timeout=timeout)
            # If MLB has a 443 or 503 (throttling or connection issues), handle it
            if response.status_code in [443, 503]:
                logger.warning(
                    f"Got HTTP {response.status_code} -> Throttled/connection issue. "
                    f"Closing session & retrying for game {game_number}."
                )
                session.close()
                session = requests.Session()
                time.sleep(120)
                attempt += 1
                continue

            if response.status_code == 404:
                logger.warning(f"Game data for game {game_number} does not exist (404).")
                break  # No further retries will fix a 404

            response.raise_for_status()  # Raise for other 4xx/5xx
            data_json = response.json()
            validated_data = GameDataStructure(**data_json)
            data = validated_data.model_dump(by_alias=True)
            break

        except requests.RequestException as req_err:
            logger.error(f"Request error on attempt {attempt + 1} for game {game_number}: {req_err}")
            # If it's a connection-related error, close and force new session
            if isinstance(req_err, requests.exceptions.ConnectionError):
                session.close()
                session = requests.Session()

            if attempt < retries - 1:
                # Exponential-ish backoff (similar to nbaboxscores)
                backoff = 2 ** attempt
                logger.info(f"Retrying in {backoff} seconds...")
                time.sleep(backoff)
            else:
                logger.error(f"All {retries} attempts failed for game {game_number}.")

        except ValidationError as val_err:
            logger.error(f"Data validation error for game {game_number}: {val_err}")
            # If it's a validation error at the top level, no reason to retry
            break

        attempt += 1

    end_cpu_time_inner = pytime.process_time()
    end_cpu_percent_inner = psutil.cpu_percent()
    logger.info(
        f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent_inner}%. "
        f"CPU time for fetch_game_data({game_number}): {end_cpu_time_inner - start_cpu_time:.4f} seconds"
    )
    return data

# ------------------------------------------------------------------------
#                     PROCESS TEAMS BOXSCORE DATA
# ------------------------------------------------------------------------
def process_teams_boxscore_data(data):
    """
    Flatten and extract the relevant 'teams' boxscore data into a DataFrame.
    Note: We haven't yet validated that the boxscore isn't "all null"; that
    will happen once we convert to dict rows and pass each row to TeamsBoxscore.
    """
    start_cpu_time = pytime.process_time()
    start_cpu_percent = psutil.cpu_percent()
    try:
        game_id = data['gameData']['game'].get('pk')
        game_date = data['gameData']['datetime'].get('originalDate')
        game_time = data['gameData']['datetime'].get('time')

        if not game_date or game_date == 'N/A':
            season, month, day = 'N/A', 'N/A', 'N/A'
        else:
            season, month, day = game_date.split('-')

        # If boxscore->teams is present, flatten it
        if 'liveData' in data and 'boxscore' in data['liveData'] and 'teams' in data['liveData']['boxscore']:
            teams_boxscore_info = pd.json_normalize(data['liveData']['boxscore']['teams'], sep='_')
            teams_boxscore_info['game_id'] = game_id
            teams_boxscore_info['season'] = season
            teams_boxscore_info['month'] = month
            teams_boxscore_info['day'] = day
            teams_boxscore_info['game_time'] = game_time

            # Example: Validate team/player stats if desired
            for team in ['home', 'away']:
                # Validate per-player
                players_key = f'{team}_players'
                if players_key in data['liveData']['boxscore']['teams']:
                    players_info = pd.json_normalize(
                        data['liveData']['boxscore']['teams'][players_key],
                        sep='_'
                    ).to_dict(orient='records')
                    for player in players_info:
                        try:
                            _ = PlayerStats(**player)  # Validate
                        except ValidationError as val_err:
                            logger.error(f"Validation error in {team} player stats: {val_err}")

                # Validate teamStats
                team_stats_key = f'{team}_teamStats'
                if team_stats_key in data['liveData']['boxscore']['teams']:
                    team_stats_dict = data['liveData']['boxscore']['teams'][team_stats_key]
                    try:
                        _ = TeamStats(**team_stats_dict)
                    except ValidationError as val_err:
                        logger.error(f"Validation error in {team} team stats: {val_err}")

            return teams_boxscore_info
        else:
            logger.info("No 'teams' boxscore data available.")
            return None

    except KeyError as e:
        logger.error(f"Missing key in gameData: {e}")
        return None
    finally:
        end_cpu_time = pytime.process_time()
        end_cpu_percent = psutil.cpu_percent()
        logger.info(
            f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
            f"CPU time for process_teams_boxscore_data(): {end_cpu_time - start_cpu_time:.4f} seconds"
        )

# ------------------------------------------------------------------------
#                     SEND DATA TO KAFKA
# ------------------------------------------------------------------------
def send_data_to_kafka(producer, topic, records):
    start_cpu_time = pytime.process_time()
    start_cpu_percent = psutil.cpu_percent()
    for record in records:
        game_id = record.get('game_id', 'unknown')
        for attempt in range(10):
            try:
                producer.send(topic, key=game_id, value=record).get(timeout=60)
                break
            except UnknownTopicOrPartitionError as e:
                logger.error(f"UnknownTopicOrPartitionError: {e}")
                time.sleep(5)
            except Exception as e:
                logger.error(f"Error sending data to Kafka (attempt {attempt+1}): {e}")
                time.sleep(5)
    end_cpu_time = pytime.process_time()
    end_cpu_percent = psutil.cpu_percent()
    logger.info(
        f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
        f"CPU time for send_data_to_kafka(): {end_cpu_time - start_cpu_time:.4f} seconds"
    )

# ------------------------------------------------------------------------
#                    PROCESS BATCH  (Called by Executor)
# ------------------------------------------------------------------------
def process_batch(batch, topic):
    """
    Even though we can set max_workers > 1, each batch call is done in parallel.
    """
    start_cpu_time = pytime.process_time()
    start_cpu_percent = psutil.cpu_percent()
    # Create a producer each time we process a batch
    producer = create_producer()
    # Create a single session to be reused across these calls
    session = requests.Session()

    for game_number in batch:
        data = fetch_game_data(game_number, session=session, retries=3, timeout=240)
        if data:
            teams_boxscore_data_frame = process_teams_boxscore_data(data)
            if teams_boxscore_data_frame is not None:
                df_records = teams_boxscore_data_frame.to_dict(orient='records')
                valid_records = []
                for record in df_records:
                    # Convert single record to our new TeamsBoxscore model
                    try:
                        validated = TeamsBoxscore(**record)
                        # If valid, store the .dict() version
                        valid_records.append(validated.dict())
                    except ValidationError as ve:
                        logger.warning(f"Skipping invalid record for game {game_number}: {ve}")

                if valid_records:
                    send_data_to_kafka(producer, topic, valid_records)
                    logger.info(f"Successfully sent teams_boxscore data for game {game_number} to Kafka.")
                else:
                    logger.info(f"No valid teams_boxscore data for game {game_number} after validation.")
            else:
                logger.info(f"No teams_boxscore dataframe for game {game_number}.")
        else:
            logger.info(f"No data for game {game_number}.")

    producer.close()
    end_cpu_time = pytime.process_time()
    end_cpu_percent = psutil.cpu_percent()
    logger.info(
        f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
        f"CPU time for process_batch({batch}): {end_cpu_time - start_cpu_time:.4f} seconds"
    )

# ------------------------------------------------------------------------
#            STREAM TEAMS BOXSCORE DATA (You can set max_workers as desired)
# ------------------------------------------------------------------------
def stream_teams_boxscore_data(start_game_number, end_game_number, topic):
    start_cpu_time = pytime.process_time()
    start_cpu_percent = psutil.cpu_percent()
    max_workers = 60
    game_numbers = list(range(start_game_number, end_game_number + 1))

    # We'll chunk them so that each 'batch' is processed in parallel
    batch_size = (len(game_numbers) + max_workers - 1) // max_workers
    batches = [game_numbers[i:i + batch_size] for i in range(0, len(game_numbers), batch_size)]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {executor.submit(process_batch, batch, topic): batch for batch in batches}
        for future in as_completed(future_to_batch):
            batch = future_to_batch[future]
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error processing batch {batch}: {e}")

    end_cpu_time = pytime.process_time()
    end_cpu_percent = psutil.cpu_percent()
    logger.info(
        f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
        f"CPU time for stream_teams_boxscore_data(): {end_cpu_time - start_cpu_time:.4f} seconds"
    )

# ------------------------------------------------------------------------
#                   ARGUMENT PARSING
# ------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Kafka Producer for MLB Data")
    parser.add_argument('--start_game', type=int, default=44000,
                        help="Start game number (default: 44000)")
    parser.add_argument('--end_game', type=int, default=990000,
                        help="End game number (default: 990000)")
    parser.add_argument('--topic', type=str, default='MLB_boxscores_topic',
                        help="Kafka topic name (default: MLB_boxscores_topic)")
    return parser.parse_args()

# ------------------------------------------------------------------------
#                            MAIN
# ------------------------------------------------------------------------
def main():
    args = parse_args()
    create_topic(args.topic)
    stream_teams_boxscore_data(args.start_game, args.end_game, args.topic)

# ------------------------------------------------------------------------
#              RUNNING WITH cProfile (as in nbaboxscores.py)
# ------------------------------------------------------------------------
if __name__ == "__main__":
    with cProfile.Profile() as pr:
        main()

    stats = pstats.Stats(pr)
    stats.sort_stats(pstats.SortKey.TIME)
    stats.print_stats()
