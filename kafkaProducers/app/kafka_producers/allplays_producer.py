import os
import logging
import argparse
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
import pandas as pd
from pydantic import BaseModel, ValidationError
from typing import Optional, Dict, List, Any
import requests
import json

# --- Added for profiling ---
import cProfile
import pstats
# For function-level CPU time logging
import time as pytime
# For CPU usage logging
import psutil

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Updated Bootstrap Servers
bootstrap_servers = ['172.16.10.2:9092', '172.16.10.3:9093', '172.16.10.4:9094']

def create_topic(topic_name):
    """
    Creates a Kafka topic if it doesn't already exist.
    """
    start_cpu_time = pytime.process_time()
    start_cpu_percent = psutil.cpu_percent()
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='allplays_info'
        )
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

def create_producer():
    """
    Creates a Kafka producer with security configurations.
    """
    start_cpu_time = pytime.process_time()
    start_cpu_percent = psutil.cpu_percent()
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: str(x).encode('utf-8'),
            retries=5,
            max_block_ms=3600000,
            request_timeout_ms=3600000,
            acks='all',
            linger_ms=5,
            batch_size=2 * 1024 * 1024,
            max_request_size=1195925856,
            compression_type='lz4',
            buffer_memory=134217728,
            max_in_flight_requests_per_connection=5
        )
        logger.info("Kafka Producer created successfully with security configurations.")
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

# Initialize the producer globally
producer = create_producer()

# -----------------------------
# Pydantic Models (Revised)
# -----------------------------
class GameDataSub(BaseModel):
    """
    Mirrors the structure inside JSON's "gameData".
    """
    game: Optional[dict] = None
    datetime: Optional[dict] = None
    status: Optional[dict] = None
    teams: Optional[dict] = None
    players: Optional[dict] = None
    venue: Optional[dict] = None
    officialVenue: Optional[dict] = None
    weather: Optional[dict] = None
    gameInfo: Optional[dict] = None
    review: Optional[dict] = None
    flags: Optional[dict] = None
    alerts: Optional[list] = None
    probablePitchers: Optional[dict] = None

class LiveDataSub(BaseModel):
    """
    Mirrors the structure inside JSON's "liveData".
    """
    plays: Optional[dict] = None

class GameDataStructure(BaseModel):
    """
    Top-level structure matching:
    {
      "gameData": {...},
      "liveData": {...}
    }
    """
    gameData: Optional[GameDataSub] = None
    liveData: Optional[LiveDataSub] = None

def check_game_existence(game_number):
    """
    Check if game data for a specific game number exists.
    """
    start_cpu_time = pytime.process_time()
    start_cpu_percent = psutil.cpu_percent()
    url = f"https://ws.statsapi.mlb.com/api/v1.1/game/{game_number}/feed/live?language=en"
    try:
        response = requests.head(url, timeout=120)  # increased to 120s
        return response.status_code == 200
    except requests.RequestException as e:
        logger.error(f"Error checking game existence for game {game_number}: {e}")
        return False
    finally:
        end_cpu_time = pytime.process_time()
        end_cpu_percent = psutil.cpu_percent()
        logger.info(
            f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
            f"CPU time for check_game_existence({game_number}): {end_cpu_time - start_cpu_time:.4f} seconds"
        )

def fetch_game_data(game_number):
    """
    Fetch game data from the MLB API and validate it with Pydantic.
    """
    start_cpu_time = pytime.process_time()
    start_cpu_percent = psutil.cpu_percent()
    if not check_game_existence(game_number):
        logger.warning(f"Game data for game {game_number} does not exist. Skipping.")
        end_cpu_time_skip = pytime.process_time()
        end_cpu_percent_skip = psutil.cpu_percent()
        logger.info(
            f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent_skip}%. "
            f"CPU time for fetch_game_data({game_number}): {end_cpu_time_skip - start_cpu_time:.4f} seconds"
        )
        return None

    url = f"https://ws.statsapi.mlb.com/api/v1.1/game/{game_number}/feed/live?language=en"
    retries = 2
    backoff_factor = 0.5

    data_result = None
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=120)  # increased to 120s
            response.raise_for_status()
            data = response.json()

            # Validate data with Pydantic model
            validated_data = GameDataStructure(**data)
            data_result = validated_data
            break

        except requests.HTTPError as http_err:
            logger.error(f"HTTP error occurred for game {game_number}: {http_err}")
        except requests.RequestException as req_err:
            logger.error(f"Request error occurred for game {game_number}: {req_err}")
        except ValidationError as val_err:
            logger.error(f"Data validation error for game {game_number}: {val_err}")
        except Exception as err:
            logger.error(f"Other error occurred for game {game_number}: {err}")

        sleep_time = backoff_factor * (2 ** attempt) + random.uniform(0, 1)
        logger.info(f"Retrying in {sleep_time:.2f} seconds...")
        time.sleep(sleep_time)

    end_cpu_time = pytime.process_time()
    end_cpu_percent = psutil.cpu_percent()
    logger.info(
        f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
        f"CPU time for fetch_game_data({game_number}): {end_cpu_time - start_cpu_time:.4f} seconds"
    )
    return data_result

def process_allplays_data(model_obj: GameDataStructure):
    """
    Process 'allPlays' data from the validated Pydantic object,
    converting it into a form suitable for Kafka.
    """
    start_cpu_time = pytime.process_time()
    start_cpu_percent = psutil.cpu_percent()
    try:
        game_id = model_obj.gameData.game.get("pk")
        game_datetime_dict = model_obj.gameData.datetime or {}
        game_date = game_datetime_dict.get('originalDate', 'N/A')
        game_time = game_datetime_dict.get('time', 'N/A')

        if game_date != 'N/A':
            season, month, day = game_date.split('-')
        else:
            season, month, day = ('N/A', 'N/A', 'N/A')
    except Exception as e:
        logger.error(f"Missing or invalid keys in gameData: {e}")
        end_cpu_time = pytime.process_time()
        end_cpu_percent = psutil.cpu_percent()
        logger.info(
            f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
            f"CPU time for process_allplays_data(): {end_cpu_time - start_cpu_time:.4f} seconds"
        )
        return None

    live_data_plays = model_obj.liveData.plays if model_obj.liveData and model_obj.liveData.plays else {}
    all_plays_list = live_data_plays.get('allPlays', [])

    if not isinstance(all_plays_list, list):
        logger.info("No valid 'allPlays' list found.")
        end_cpu_time = pytime.process_time()
        end_cpu_percent = psutil.cpu_percent()
        logger.info(
            f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
            f"CPU time for process_allplays_data(): {end_cpu_time - start_cpu_time:.4f} seconds"
        )
        return None

    try:
        all_plays_df = pd.json_normalize(all_plays_list)
        all_plays_df['game_id'] = game_id
        all_plays_df['season'] = season
        all_plays_df['month'] = month
        all_plays_df['day'] = day
        all_plays_df['game_time'] = game_time

        return all_plays_df
    except Exception as e:
        logger.error(f"Error processing allPlays: {e}")
        return None
    finally:
        end_cpu_time = pytime.process_time()
        end_cpu_percent = psutil.cpu_percent()
        logger.info(
            f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
            f"CPU time for process_allplays_data(): {end_cpu_time - start_cpu_time:.4f} seconds"
        )

def send_data_to_kafka(producer, topic, records):
    """
    Send each record to the specified Kafka topic.
    """
    start_cpu_time = pytime.process_time()
    start_cpu_percent = psutil.cpu_percent()
    for record in records:
        for attempt in range(10):
            try:
                producer.send(topic, value=record).get(timeout=60)
                break
            except UnknownTopicOrPartitionError as e:
                logger.error(f"UnknownTopicOrPartitionError: {e}")
                time.sleep(5)
            except Exception as e:
                logger.error(f"Error sending data to Kafka: {e}")
                time.sleep(5)
    end_cpu_time = pytime.process_time()
    end_cpu_percent = psutil.cpu_percent()
    logger.info(
        f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
        f"CPU time for send_data_to_kafka(): {end_cpu_time - start_cpu_time:.4f} seconds"
    )

def process_batch(batch, topic):
    """
    Process a batch of game numbers, fetch, validate, transform, and send allPlays to Kafka.
    """
    start_cpu_time = pytime.process_time()
    start_cpu_percent = psutil.cpu_percent()
    for game_number in batch:
        data_obj = fetch_game_data(game_number)
        if data_obj:
            allplays_data_frame = process_allplays_data(data_obj)
            if allplays_data_frame is not None:
                # If NOT all values are null
                if not allplays_data_frame.isnull().all().all():
                    records = allplays_data_frame.to_dict(orient='records')
                    send_data_to_kafka(producer, topic, records)
                    logger.info(f"Successfully sent allPlays data for game {game_number} to Kafka.")
                else:
                    logger.info(f"Invalid allPlays data for game {game_number}. All values are null.")
            else:
                logger.info(f"No valid allPlays data for game {game_number}.")
        else:
            logger.info(f"No data for game {game_number}.")
    end_cpu_time = pytime.process_time()
    end_cpu_percent = psutil.cpu_percent()
    logger.info(
        f"CPU usage start: {start_cpu_percent}%, end: {end_cpu_percent}%. "
        f"CPU time for process_batch({batch}): {end_cpu_time - start_cpu_time:.4f} seconds"
    )

def stream_allplays_data(start_game_number, end_game_number, topic):
    """
    Streams 'allPlays' data for a range of game numbers into Kafka.
    """
    start_cpu_time = pytime.process_time()
    start_cpu_percent = psutil.cpu_percent()
    max_workers = 6
    game_numbers = list(range(start_game_number, end_game_number + 1))

    # Split game numbers into nearly equal-sized batches
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
        f"CPU time for stream_allplays_data(): {end_cpu_time - start_cpu_time:.4f} seconds"
    )

def parse_args():
    parser = argparse.ArgumentParser(description="Kafka Producer for MLB Data")
    parser.add_argument('--start_game', type=int, required=True, help="Start game number")
    parser.add_argument('--end_game', type=int, required=True, help="End game number")
    parser.add_argument('--topic', type=str, required=True, help="Kafka topic name")
    return parser.parse_args()

def main():
    args = parse_args()
    create_topic(args.topic)
    stream_allplays_data(args.start_game, args.end_game, args.topic)

if __name__ == "__main__":
    # Profile the entire run of main()
    with cProfile.Profile() as pr:
        main()

    # Print out profiling stats
    stats = pstats.Stats(pr)
    stats.sort_stats(pstats.SortKey.TIME)
    stats.print_stats()
