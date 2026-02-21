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
from pydantic import BaseModel, ValidationError, Field, validator
from typing import Optional, Dict, Any, Union, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bootstrap_servers = ['kafka1:9092', 'kafka2:9093', 'kafka3:9094']

# Create a Kafka topic if it doesn't already exist
def create_topic(topic_name):
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='text_descriptions'
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

# Helper function to convert time strings to seconds
def time_str_to_seconds(time_str):
    try:
        h, m, s = map(int, time_str.split(':'))
        return h * 3600 + m * 60 + s
    except ValueError:
        return None

# Pydantic models for data validation
class Tag(BaseModel):
    slug: Optional[str] = None
    type: Optional[str] = None
    title: Optional[str] = None
    gamePk: Optional[int] = None

    @validator('*', always=True)
    def set_default(cls, v):
        return v if v is not None else 'N/A'

class Article(BaseModel):
    contentDate: Optional[str] = None
    description: Optional[str] = None
    headline: Optional[str] = None
    slug: Optional[str] = None
    blurb: Optional[str] = None
    templateUrl: Optional[str] = None
    type: Optional[str] = None
    tags: Optional[List[Tag]] = None

    @validator('*', always=True)
    def set_default(cls, v):
        return v if v is not None else 'N/A'

class VideoContent(BaseModel):
    headline: Optional[str] = None
    duration: Optional[Union[int, str]] = None
    title: Optional[str] = None
    description: Optional[str] = None
    slug: Optional[str] = None
    blurb: Optional[str] = None
    date: Optional[str] = None
    contentDate: Optional[str] = None
    playbacks: Optional[List[Dict[str, str]]] = None
    tags: Optional[List[Tag]] = None

    @validator('duration', always=True)
    def parse_duration(cls, v):
        if isinstance(v, str):
            return time_str_to_seconds(v)
        return v

    @validator('*', always=True)
    def set_default(cls, v):
        return v if v is not None else 'N/A'

class GameContent(BaseModel):
    recapArticle: Optional[List[Article]] = None
    relatedArticles: Optional[List[Article]] = None
    videoContent: Optional[List[VideoContent]] = None

class Game(BaseModel):
    gamePk: int
    gameDate: Optional[str] = None
    content: Optional[GameContent] = None

    @validator('*', always=True)
    def set_default(cls, v):
        return v if v is not None else 'N/A'

class APIResponse(BaseModel):
    getGamesByGamePks: List[Game]

class VideoData(BaseModel):
    game_gamePk: Optional[int]
    game_gameDate: Optional[str]
    video_headline: Optional[str]
    video_title: Optional[str]
    video_description: Optional[str]
    video_blurb: Optional[str]

    @validator('*', always=True)
    def set_default(cls, v):
        return v if v is not None else 'N/A'

base_url = "https://data-graph.mlb.com/graphql/"    

# GraphQL query and operation name
operation_name = "getGamesByGamePks"
query = """
fragment mediaTags on Tag {
    ... on GameTag {
        slug
        type
        title
        gamePk
    }
    ... on TaxonomyTag {
        slug
        type
        title
    }
    ... on InternalTag {
        slug
        type
        title
    }
    ... on TeamTag {
        type
        team {
            id
        }
    }
}
fragment articleFields on Article {
    contentDate
    description
    headline
    slug
    blurb: summary
    templateUrl: thumbnail
    type
    tags {
        ...mediaTags
    }
}
query getGamesByGamePks($gamePks: [Int], $locale: Language, $gameRecapTags: [String]!, $relatedArticleTags: [String]!, $contentSource: ContentSource) {
    getGamesByGamePks(gamePks: $gamePks) {
        gamePk
        gameDate
        content {
            videoContent(locale: $locale) {
                headline
                duration
                title
                description
                slug
                id: slug
                blurb
                guid: playGuid
                date: contentDate
                contentDate
                preferredPlaybackScenarioURL(preferredPlaybacks: ["hlsCloud", "mp4Avc"])
                playbacks: playbackScenarios {
                    name: playback
                    url: location
                }
                thumbnail {
                    templateUrl
                }
                tags {
                    ...mediaTags
                }
            }
            ... on GameContent {
                recapArticle: articleContent(locale: $locale, tags: $gameRecapTags, limit: 1, contentSource: $contentSource) {
                    ...articleFields
                }
                relatedArticles: articleContent(locale: $locale, tags: $relatedArticleTags, excludeTags: $gameRecapTags, limit: 5) {
                    ...articleFields
                }
            }
        }
    }
}
"""

# Check if game data for a specific game ID exists
def check_game_existence(game_id):
    variables = {
        "gameRecapTags": ["game-recap"],
        "relatedArticleTags": ["storytype-article"],
        "gamePks": [game_id],
        "locale": "EN_US",
        "contentSource": "MLB"
    }

    # Construct the payload
    payload = {
        "operationName": operation_name,
        "query": query,
        "variables": variables
    }

    # Make the POST request
    response = requests.post(base_url, json=payload)
    return response.status_code == 200

# Function to send data to Kafka
def send_to_kafka(producer, topic_name, data, game_id):
    try:
        producer.send(topic_name, key=str(game_id).encode('utf-8'), value=data)
        logger.info(f"Sent data for game ID {game_id} to Kafka.")
    except Exception as e:
        logger.error(f"Failed to send data for game ID {game_id} to Kafka: {e}")

# Function to collect and send data
def collect_and_send_data(producer, topic_name, start_game_number, end_game_number):
    max_workers = 10
    game_numbers = list(range(start_game_number, end_game_number + 1))
    
    # Split game numbers into batches
    batch_size = (len(game_numbers) + max_workers - 1) // max_workers
    batches = [game_numbers[i:i + batch_size] for i in range(0, len(game_numbers), batch_size)]
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {executor.submit(process_batch, batch, topic_name, producer): batch for batch in batches}

        for future in as_completed(future_to_batch):
            batch = future_to_batch[future]
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error processing batch {batch}: {e}")

# Process a batch of game data
def process_batch(batch, topic_name, producer):
    for game_id in batch:
        if not check_game_existence(game_id):
            logger.warning(f"Game data for game ID {game_id} does not exist. Skipping.")
            continue

        variables = {
            "gameRecapTags": ["game-recap"],
            "relatedArticleTags": ["storytype-article"],
            "gamePks": [game_id],
            "locale": "EN_US",
            "contentSource": "MLB"
        }

        # Construct the payload
        payload = {
            "operationName": operation_name,
            "query": query,
            "variables": variables
        }

        # Make the POST request
        response = requests.post(base_url, json=payload)

        # Handle the response
        if response.status_code == 200:
            try:
                data = response.json()

                # Validate data with Pydantic model
                validated_data = APIResponse(getGamesByGamePks=data['data']['getGamesByGamePks'])
                validated_data_dict = validated_data.dict()  

                # Flatten videoContent
                video_df = pd.json_normalize(validated_data_dict['getGamesByGamePks'],
                                             record_path=['content', 'videoContent'],
                                             meta=['gamePk', 'gameDate'],
                                             record_prefix='video_',
                                             meta_prefix='game_')

                # Select and rename important columns
                columns = [
                    'game_gamePk', 'game_gameDate', 'video_headline', 'video_title', 'video_description',
                    'video_blurb'
                ]

                video_df = video_df[columns]
                video_df.columns = [
                    'game_id', 'gameDate', 'headline', 'title', 'description', 'blurb'
                ]

                # Parse the gameDate to extract season, month, and day
                video_df['gameDate'] = pd.to_datetime(video_df['gameDate'])
                video_df['season'] = video_df['gameDate'].dt.year
                video_df['month'] = video_df['gameDate'].dt.month
                video_df['day'] = video_df['gameDate'].dt.day

                # Remove duplicate rows
                video_df = video_df.drop_duplicates()

                video_df = video_df.drop(columns=['gameDate'])                

                # Send each row as a message to Kafka
                for index, row in video_df.iterrows():
                    send_to_kafka(producer, topic_name, row.to_dict(), game_id)

            except ValidationError as val_err:
                logger.error(f"Data validation error for game ID {game_id}: {val_err}")
            except Exception as e:
                logger.error(f"Error processing game ID {game_id}: {e}")
        else:
            logger.error(f"Query failed for game ID {game_id} with status code {response.status_code}: {response.text}")

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
    producer = create_producer()
    collect_and_send_data(producer, args.topic, args.start_game, args.end_game)
    producer.close()

if __name__ == "__main__":
    main()