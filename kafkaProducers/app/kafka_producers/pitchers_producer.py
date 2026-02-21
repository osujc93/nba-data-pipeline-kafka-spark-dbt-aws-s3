import argparse
import json
import logging
import requests
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import pandas as pd
from pydantic import BaseModel, ValidationError, Field
from typing import Optional, Dict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bootstrap_servers = ['kafka1:9092', 'kafka2:9093', 'kafka3:9094']

# Create a Kafka topic if it doesn't already exist
def create_topic(topic_name):
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='pitchers_info'
        )
        topic_list = [NewTopic(name=topic_name, num_partitions=25, replication_factor=3)]
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

# Create a Kafka producer
def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: str(x).encode('utf-8'),
            retries=10,
            max_block_ms=3600000,
            request_timeout_ms=3600000,
            acks='all',
            linger_ms=5,
            batch_size=1024 * 1024,
            max_request_size=1195925856          
        )
        logger.info("Kafka Producer created successfully.")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka Producer: {e}")
        raise

# Pydantic models for data validation
class GameData(BaseModel):
    pk: int

class DateTimeInfo(BaseModel):
    originalDate: Optional[str] = None
    time: Optional[str] = None

class PitcherInfo(BaseModel):
    id: int
    fullName: Optional[str] = None
    link: Optional[str] = None

class ProbablePitchers(BaseModel):
    home: Optional[PitcherInfo] = None
    away: Optional[PitcherInfo] = None

class GameDataStructure(BaseModel):
    game: GameData
    datetime: DateTimeInfo
    probablePitchers: ProbablePitchers

class APIResponse(BaseModel):
    gameData: GameDataStructure

# Check if game data for a specific game number exists
def check_game_existence(game_number):
    url = f"https://ws.statsapi.mlb.com/api/v1.1/game/{game_number}/feed/live?language=en"
    response = requests.head(url, timeout=60)
    return response.status_code == 200

# Fetch game data from the MLB API
def fetch_game_data(game_number):
    if not check_game_existence(game_number):
        logger.warning(f"Game data for game {game_number} does not exist. Skipping.")
        return None

    url = f"https://ws.statsapi.mlb.com/api/v1.1/game/{game_number}/feed/live?language=en"
    retries = 10
    backoff_factor = 0.5

    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            data = response.json()

            # Validate data with Pydantic model
            validated_data = APIResponse(gameData=data['gameData'])
            return validated_data.model_dump()  
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

    return None

# Process the probable pitchers' data from the game data
def process_probable_pitchers_data(data):
    try:
        game_id = data['gameData']['game']['pk']
        game_date = data['gameData']['datetime'].get('originalDate', 'N/A')
        game_time = data['gameData']['datetime'].get('time', 'N/A')

        if game_date != 'N/A':
            season, month, day = game_date.split('-')
        else:
            season, month, day = 'N/A', 'N/A', 'N/A'

    except KeyError as e:
        message = f"Missing key in gameData: {e}"
        logger.error(message)
        return None

    if 'probablePitchers' in data['gameData']:
        try:
            pitchers_info = pd.json_normalize(data['gameData']['probablePitchers'])
            pitchers_info['game_id'] = game_id
            pitchers_info['season'] = season
            pitchers_info['month'] = month
            pitchers_info['day'] = day
            pitchers_info['game_time'] = game_time

            return pitchers_info
        except Exception as e:
            message = f"Error processing probablePitchers: {e}"
            logger.error(message)
            return None
    return None

# Stream probable pitchers' data for a range of games and send to Kafka
def stream_probable_pitchers_data(start_game_number, end_game_number, topic):
    max_workers = 10
    game_numbers = list(range(start_game_number, end_game_number + 1))

    def fetch_and_process_game_data_batch(batch):
        producer = create_producer()
        for game_number in batch:
            data = fetch_game_data(game_number)
            if data:
                pitchers_data_frame = process_probable_pitchers_data(data)
                if pitchers_data_frame is not None:
                    records = pitchers_data_frame.to_dict(orient='records')
                    for record in records:
                        send_to_kafka(producer, topic, record)
                    logger.info(f"Successfully sent probablePitchers data for game {game_number} to Kafka.")
                else:
                    logger.info(f"No valid probablePitchers data for game {game_number}.")
            else:
                logger.info(f"No data for game {game_number}.")

    batch_size = len(game_numbers) // max_workers
    batches = [game_numbers[i:i + batch_size] for i in range(0, len(game_numbers), batch_size)]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_and_process_game_data_batch, batch) for batch in batches]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error processing batch: {e}")

# Send processed data to Kafka
def send_to_kafka(producer, topic, data):
    game_id = data['game_id']
    try:
        producer.send(topic, key=str(game_id).encode('utf-8'), value=data)
        logger.info(f"Sent data for game ID {game_id} to Kafka.")
    except Exception as e:
        logger.error(f"Failed to send data for game ID {game_id} to Kafka: {e}")

# Parse command-line arguments
def parse_args():
    parser = argparse.ArgumentParser(description="Kafka Producer for MLB Data")
    parser.add_argument('--start_game', type=int, required=True, help="Start game number")
    parser.add_argument('--end_game', type=int, required=True, help="End game number")
    parser.add_argument('--topic', type=str, required=True, help="Kafka topic name")
    return parser.parse_args()

# Main function to execute the script
def main():
    args = parse_args()
    create_topic(args.topic)
    stream_probable_pitchers_data(args.start_game, args.end_game, args.topic)

if __name__ == "__main__":
    main()