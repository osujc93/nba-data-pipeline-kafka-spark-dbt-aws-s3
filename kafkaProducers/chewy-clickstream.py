#!/usr/bin/env python3
import argparse
import json
import logging
import requests
import time
from typing import List, Union, Dict
import threading
import os
from datetime import datetime, timedelta

import psycopg2

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.errors import NotLeaderForPartitionError, KafkaTimeoutError
from typing import List, Optional, Union
from pydantic import BaseModel, ValidationError

from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
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
    'linger_ms': 0,  # <-- changed to 0 to minimize latency
    'batch_size': 5 * 1024 * 1024,
    'max_request_size': 20 * 1024 * 1024,
    'compression_type': 'gzip',
    'buffer_memory': 512 * 1024 * 1024,
    'max_in_flight_requests_per_connection': 5
}

# ------------------ Pydantic Models for Validation ------------------
class ClickstreamMetadata(BaseModel):
    is_first_visit: bool
    time_spent_on_page_ms: int
    promotion_code: Optional[str]
    page_load_time_ms: int
    customer_support_chat_opened: bool

class ClickstreamEvent(BaseModel):
    event_id: str
    timestamp: str
    user_id: str
    session_id: str
    event_type: str
    page_url: str
    page_name: Optional[str]
    referrer_url: Optional[str]
    product_id: Optional[str]
    product_name: Optional[str]
    brand_name: Optional[str]
    category_name: Optional[str]
    price: Optional[float]
    quantity: Optional[int]
    user_login_status: Optional[str]
    loyalty_status: Optional[str]
    pet_preference: Optional[str]
    marketing_campaign_id: Optional[str]
    device_type: str
    browser_name: str
    app_version: Optional[str]
    os_version: str
    ip_address: str
    geo_location: str
    language_pref: str
    search_query: Optional[str]
    search_results_count: Optional[int]
    filters_applied: List[str]
    cart_id: Optional[str]
    cart_value_before_event: Optional[float]
    cart_value_after_event: Optional[float]
    metadata: ClickstreamMetadata

class ClickstreamApiResponse(BaseModel):
    page: int
    limit: int
    total_clickstream: int
    total_pages: int
    data: List[ClickstreamEvent]

# ------------------ Helper Functions ------------------
def create_topic(topic_name: str, num_partitions: int = 25, replication_factor: int = 2) -> None:
    admin_client = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id='FakeClickstreamProducer'
    )
    topic_list = [NewTopic(name=topic_name,
                           num_partitions=num_partitions,
                           replication_factor=replication_factor)]
    existing_topics = admin_client.list_topics()
    try:
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

def create_producer() -> KafkaProducer:
    producer = None
    try:
        producer = KafkaProducer(**PRODUCER_CONFIG)
        logger.info("Kafka Producer created successfully (clickstream).")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka Producer: {e}")
        raise

def fetch_page_data(page: int, session: requests.Session) -> Optional[ClickstreamApiResponse]:
    """
    Fetches a single page of clickstream events and validates it.
    The limit parameter is removed to allow unlimited data.
    """
    url = f"http://fake-ecommerce-api:5001/clickstream?page={page}"
    try:
        response = session.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        logger.error(f"Error fetching from {url}: {e}")
        return None

    try:
        validated = ClickstreamApiResponse(**data)
        return validated
    except ValidationError as val_err:
        logger.error(f"Data validation error for page={page}: {val_err}")
        return None

def fetch_and_send_clickstream(producer: KafkaProducer, topic: str, session: requests.Session) -> None:
    """
    Continuously poll the fake e-commerce API for pages of clickstream events,
    with no explicit limit and minimal latency.
    """
    page = 1

    while True:
        first_page_data = fetch_page_data(page, session)
        if not first_page_data:
            logger.error(f"Could not fetch/validate page {page}.")
            # Removed time.sleep to avoid latency
            continue

        total_pages = first_page_data.total_pages
        for evt in first_page_data.data:
            key_str = evt.event_id
            try:
                producer.send(topic, key=key_str, value=evt.dict())
            except (NotLeaderForPartitionError, KafkaTimeoutError) as kafka_err:
                logger.error(f"Producer error for event_id {evt.event_id}: {kafka_err}")
            except Exception as e:
                logger.error(f"Failed to send clickstream to Kafka for event_id {evt.event_id}: {e}")

        logger.info(f"Sent page={page} with {len(first_page_data.data)} clickstream events to Kafka (topic={topic}).")

        # Fetch additional pages concurrently if needed
        pages_to_fetch = []
        next_page = page + 1
        while next_page <= total_pages and len(pages_to_fetch) < 2:
            pages_to_fetch.append(next_page)
            next_page += 1

        if pages_to_fetch:
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_to_page = {
                    executor.submit(fetch_page_data, p, session): p
                    for p in pages_to_fetch
                }
                for future in as_completed(future_to_page):
                    p = future_to_page[future]
                    result = future.result()
                    if not result:
                        logger.error(f"Failed to fetch/validate page {p}")
                        continue
                    for evt in result.data:
                        key_str = evt.event_id
                        try:
                            producer.send(topic, key=key_str, value=evt.dict())
                        except (NotLeaderForPartitionError, KafkaTimeoutError) as kafka_err:
                            logger.error(f"Producer error for event_id {evt.event_id}: {kafka_err}")
                        except Exception as e:
                            logger.error(f"Failed to send clickstream to Kafka for event_id {evt.event_id}: {e}")

                    logger.info(f"Sent page={p} with {len(result.data)} clickstream events to Kafka (topic={topic}).")

            page = next_page
        else:
            if page >= total_pages:
                logger.info("Reached the final page or no data.")
                # Removed time.sleep(10) to avoid latency
                page = 1
            else:
                page += 1

        # Removed time.sleep(1) to avoid latency

def finalize_batch_in_postgres() -> None:
    """
    We do a simple marker in Postgres for demonstration.
    """
    try:
        conn = psycopg2.connect(
            dbname="nelonba",
            user="nelonba",
            password="Password123456789",
            host="postgres",
            port=5432
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clickstream_ingestion_metadata (
                id SERIAL PRIMARY KEY,
                last_ingestion_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("""INSERT INTO clickstream_ingestion_metadata DEFAULT VALUES;""")
        logger.info("[FINALIZE] Inserted a row into clickstream_ingestion_metadata for demonstration.")
    except Exception as e:
        logger.error(f"[FINALIZE] Error inserting final ingestion date for clickstream: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

def main(topic: str = "FakeClickstreamEvents") -> None:
    producer = None
    try:
        producer = create_producer()
        create_topic(topic, num_partitions=24, replication_factor=3)

        session = requests.Session()
        # Loop forever: poll data from /clickstream, produce to Kafka
        fetch_and_send_clickstream(producer, topic, session)

    except KeyboardInterrupt:
        logger.info("Interrupted by user; shutting down.")
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
    finally:
        finalize_batch_in_postgres()
        if producer:
            try:
                producer.close()
                logger.info("Kafka Producer closed successfully.")
            except Exception as e:
                logger.error(f"Error closing Kafka Producer: {e}")

if __name__ == "__main__":
    main()
