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
from pydantic import BaseModel, ValidationError, Field, field_validator
from typing import Optional, Dict, Any, Union, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bootstrap_servers = ['kafka1:9092', 'kafka2:9093', 'kafka3:9094']

def create_topic(topic_name):
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='officials_info'
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
class GameDataStructure(BaseModel):
    game: Dict[str, Any] = Field(default_factory=dict)
    datetime: Dict[str, Any] = Field(default_factory=dict)

    @field_validator('game', 'datetime', mode='before')
    def validate_fields(cls, v):
        if isinstance(v, list):
            return {str(i): v[i] for i in range(len(v))}
        elif isinstance(v, dict):
            return v
        return {}

class APIResponse(BaseModel):
    gameData: GameDataStructure
    liveData: Dict[str, Any] = Field(default_factory=dict)

    @field_validator('liveData', mode='before')
    def validate_liveData(cls, v):
        if isinstance(v, list):
            return {str(i): v[i] for i in range(len(v))}
        elif isinstance(v, dict):
            return v
        return {}

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
            validated_data = APIResponse(**data)
            return validated_data.dict()  
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

# Process the officials' data from the game data
def process_officials_data(data):
    try:
        game_id = data['gameData']['game'].get('pk')
        game_date = data['gameData']['datetime'].get('originalDate', 'N/A')
        game_time = data['gameData']['datetime'].get('time', 'N/A')
        
        if game_date != 'N/A':
            season, month, day = game_date.split('-')
        else:
            season, month, day = 'N/A', 'N/A', 'N/A'

    except KeyError as e:
        logger.error(f"Missing key in gameData: {e}")
        return None

    if 'liveData' in data and 'boxscore' in data['liveData'] and 'officials' in data['liveData']['boxscore']:
        try:
            officials_info = pd.json_normalize(data['liveData']['boxscore']['officials'])
            officials_info['game_id'] = game_id
            officials_info['game_time'] = game_time
            officials_info['season'] = season
            officials_info['month'] = month
            officials_info['day'] = day

            return officials_info
        except Exception as e:
            logger.error(f"Error processing officials: {e}")
            return None
    return None

# Send processed data to Kafka
def send_data_to_kafka(producer, topic, records):
    for record in records:
        key = str(record['game_id'])
        for attempt in range(10):
            try:
                producer.send(topic, key=key.encode('utf-8'), value=record).get(timeout=60)
                break
            except UnknownTopicOrPartitionError as e:
                logger.error(f"UnknownTopicOrPartitionError: {e}")
                time.sleep(5)
            except Exception as e:
                logger.error(f"Error sending data to Kafka: {e}")
                time.sleep(5)

# Stream officials' data for a range of games and send to Kafka
def stream_officials_data(start_game_number, end_game_number, topic):
    max_workers = 10
    game_numbers = list(range(start_game_number, end_game_number + 1))
    
    # Split game numbers into batches
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

# Process a batch of game data
def process_batch(batch, topic):
    producer = create_producer()
    for game_number in batch:
        data = fetch_game_data(game_number)
        if data:
            officials_data_frame = process_officials_data(data)
            if officials_data_frame is not None:
                records = officials_data_frame.to_dict(orient='records')
                send_data_to_kafka(producer, topic, records)
                logger.info(f"Successfully sent officials data for game {game_number} to Kafka.")
            else:
                logger.info(f"No valid officials data for game {game_number}.")
        else:
            logger.info(f"No data for game {game_number}.")
    producer.close()

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
    stream_officials_data(args.start_game, args.end_game, args.topic)

if __name__ == "__main__":
    main()