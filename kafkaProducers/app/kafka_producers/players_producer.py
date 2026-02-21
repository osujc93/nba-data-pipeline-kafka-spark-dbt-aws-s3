"""
This script fetches, processes, and sends MLB players' data to a Kafka topic for a specified range of game numbers. It uses concurrent futures to handle multiple threads for efficient data processing. The script ensures the creation of necessary Kafka topics and manages Kafka producers and consumers.

It includes functions to create Kafka topics, produce and consume messages, check for the existence of game data, and process the game data. The script handles errors gracefully to ensure robustness and reliability.

The main function orchestrates the fetching of data for the specified game numbers, processes it, and sends it to Kafka. It uses a thread pool to manage concurrent tasks, ensuring efficient resource usage and parallel processing of the game data. This approach allows for scalable and maintainable data processing in a distributed environment.
"""

import argparse
import json
import logging
import requests
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
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
            client_id='players_info'
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
            max_block_ms=60000,
            request_timeout_ms=240000,
            acks='all',
            linger_ms=10,  
            batch_size=1024 * 1024,  
            max_request_size=10485760          
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

class Player(BaseModel):
    id: int
    fullName: Optional[str] = None

class GameDataStructure(BaseModel):
    game: GameData
    datetime: DateTimeInfo
    players: Dict[str, Player]

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
            validated_data = APIResponse(**data)
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

# Process the players' data from the game data
def process_players_data(data):
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

    if 'players' in data['gameData']:
        try:
            players_info = pd.json_normalize(data['gameData']['players'])
            players_info['game_id'] = game_id
            players_info['season'] = season
            players_info['month'] = month
            players_info['day'] = day
            players_info['game_time'] = game_time

            return players_info
        except Exception as e:
            message = f"Error processing players: {e}"
            logger.error(message)
            return None
    return None

# Fetch and process game data, then send to Kafka
def fetch_and_process_game_data(game_number, topic, producer):
    data = fetch_game_data(game_number)
    if data:
        players_data_frame = process_players_data(data)
        if players_data_frame is not None:
            records = players_data_frame.to_dict(orient='records')
            for record in records:
                send_to_kafka(producer, topic, record, game_number)
            logger.info(f"Successfully sent players data for game {game_number} to Kafka.")
        else:
            logger.info(f"No valid players data for game {game_number}.")
    else:
        logger.info(f"No data for game {game_number}.")

# Stream players' data for a range of games and send to Kafka
def stream_players_data(start_game_number, end_game_number, topic):
    max_workers = 10
    game_numbers = list(range(start_game_number, end_game_number + 1))

    # Create Kafka producer
    producer = create_producer()

    def process_batch(batch):
        for game_number in batch:
            fetch_and_process_game_data(game_number, topic, producer)

    # Split game numbers into batches
    batch_size = (len(game_numbers) + max_workers - 1) // max_workers
    batches = [game_numbers[i:i + batch_size] for i in range(0, len(game_numbers), batch_size)]

    # Use ThreadPoolExecutor to process batches concurrently
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_batch, batch) for batch in batches]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error processing batch: {e}")

# Send processed data to Kafka
def send_to_kafka(producer, topic, data, game_id):
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
    stream_players_data(args.start_game, args.end_game, args.topic)

if __name__ == "__main__":
    main()
