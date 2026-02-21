#!/usr/bin/env python3
import argparse
import json
import logging
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    'linger_ms': 0,  # <-- changed to 0 for minimal latency
    'batch_size': 5 * 1024 * 1024,
    'max_request_size': 20 * 1024 * 1024,
    'compression_type': 'gzip',
    'buffer_memory': 512 * 1024 * 1024,
    'max_in_flight_requests_per_connection': 5
}

# ------------------ Pydantic Models for Validation ------------------
class BillingAddress(BaseModel):
    city: str
    state: str
    street: str
    zipcode: str

class ShippingAddress(BaseModel):
    city: str
    state: str
    street: str
    zipcode: str

class StatusHistoryItem(BaseModel):
    status: str
    status_id: str
    timestamp: str

class LineItemProduct(BaseModel):
    category: str
    name: str
    out_of_stock: bool
    price: float
    product_id: str
    stock: int

class LineItem(BaseModel):
    free_qty: int
    product: LineItemProduct
    quantity: int

class ShipmentItemProduct(BaseModel):
    category: str
    name: str
    out_of_stock: bool
    price: float
    product_id: str
    stock: int

class ShipmentItem(BaseModel):
    free_qty: int
    product: ShipmentItemProduct
    quantity: int

class Shipment(BaseModel):
    items: List[ShipmentItem]
    shipment_id: str
    shipment_status: str
    shipment_timestamp: Optional[Union[str, None]]
    tracking_number: Optional[Union[str, None]]

class Customer(BaseModel):
    address: str
    city: str
    customer_id: str
    email: str
    name: str
    phone: str
    state: str
    zipcode: str

class EcommOrder(BaseModel):
    billing_address: BillingAddress
    coupon_codes: List[str]
    currency: str
    customer: Customer
    delivered: str
    delivery_date: Optional[str]
    env_fee: float
    exchange_rate: float
    expected_delivery_date: Optional[str]
    fraud_flag: bool
    fulfillment_center: str
    line_items: List[LineItem]
    loyalty_points_used: int
    loyalty_tier: str
    order_id: str
    order_total: float
    partial_refund_amount: float
    payment_info: str
    payment_method: str
    payment_status: str
    pet_name: str
    refunded: str
    risk_score: int

    shipments: List[Shipment]
    shipping_address: ShippingAddress
    shipping_cost: float
    shipping_provider: Optional[str]
    shipping_speed: str
    shipping_status: str
    status_history: List[StatusHistoryItem]
    subscription_frequency: Optional[str]
    subscription_id: Optional[str]
    subtotal_after_coupon: float
    subtotal_before_promos: float
    tax_amount: float
    tax_rate: float

    timestamp: str
    tracking_number: Optional[str]

class OrdersApiResponse(BaseModel):
    data: List[EcommOrder]
    page: int
    total_pages: int

FAILURES_LOG = "failures_log.json"

# ------------------ Helper Functions ------------------
def create_topic(topic_name: str, num_partitions: int = 24, replication_factor: int = 3) -> None:
    admin_client = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id='FakeEcommOrders'
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
        logger.info("Kafka Producer created successfully.")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka Producer: {e}")
        raise

def fetch_page_orders(page: int, session: requests.Session) -> Optional[OrdersApiResponse]:
    """
    Fetch a single page of orders and validate with pydantic. 
    The limit parameter is removed to allow unlimited data.
    """
    url = f"http://fake-ecommerce-api:5001/orders?page={page}"
    try:
        response = session.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        logger.error(f"Error fetching from {url}: {e}")
        return None

    try:
        return OrdersApiResponse(**data)
    except ValidationError as val_err:
        logger.error(f"Data validation error for page={page}: {val_err}")
        return None

def fetch_and_send_orders_data(producer: KafkaProducer, topic: str, session: requests.Session) -> None:
    """
    Continuously poll the fake e-commerce API for pages of Orders and produce them to Kafka,
    with no explicit limit and minimal latency.
    """
    page = 1

    while True:
        first_page_result = fetch_page_orders(page, session)
        if not first_page_result:
            logger.error(f"Could not fetch/validate page={page}.")
            # Removed the time.sleep call to minimize latency
            continue

        total_pages = first_page_result.total_pages
        for order in first_page_result.data:
            try:
                key_str = str(order.order_id)
                producer.send(topic, key=key_str, value=order.dict())
            except (NotLeaderForPartitionError, KafkaTimeoutError) as kafka_err:
                logger.error(f"Producer error for order_id {order.order_id}: {kafka_err}")
            except Exception as e:
                logger.error(f"Failed to send message to Kafka for order_id {order.order_id}: {e}")

        logger.info(f"Sent page={page} with {len(first_page_result.data)} orders to Kafka (topic={topic}).")

        # Fetch additional pages concurrently, if needed
        pages_to_fetch = []
        next_page = page + 1
        while next_page <= total_pages and len(pages_to_fetch) < 2:
            pages_to_fetch.append(next_page)
            next_page += 1

        if pages_to_fetch:
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_to_page = {
                    executor.submit(fetch_page_orders, p, session): p
                    for p in pages_to_fetch
                }
                for future in as_completed(future_to_page):
                    p = future_to_page[future]
                    result = future.result()
                    if not result:
                        logger.error(f"Failed to fetch/validate page={p}")
                        continue
                    for order in result.data:
                        try:
                            key_str = str(order.order_id)
                            producer.send(topic, key=key_str, value=order.dict())
                        except (NotLeaderForPartitionError, KafkaTimeoutError) as kafka_err:
                            logger.error(f"Producer error for order_id {order.order_id}: {kafka_err}")
                        except Exception as e:
                            logger.error(f"Failed to send message to Kafka for order_id {order.order_id}: {e}")

                    logger.info(f"Sent page={p} with {len(result.data)} orders to Kafka (topic={topic}).")

            page = next_page
        else:
            if page >= total_pages:
                logger.info("Reached the final page or no data.")
                # Removed the time.sleep(10) call to minimize latency
                page = 1
            else:
                page += 1

        # Removed the time.sleep(1) call to minimize latency

def finalize_batch_in_postgres() -> None:
    """
    Show how we'd finalize something in Postgres.
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
            CREATE TABLE IF NOT EXISTS ecommerce_ingestion_metadata (
                id SERIAL PRIMARY KEY,
                last_ingestion_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("""INSERT INTO ecommerce_ingestion_metadata DEFAULT VALUES;""")
        logger.info("[FINALIZE] Inserted a row into ecommerce_ingestion_metadata for demonstration.")
    except Exception as e:
        logger.error(f"[FINALIZE] Error inserting final ingestion date: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

def main(topic: str = "FakeEcommOrders") -> None:
    producer = None
    try:
        producer = create_producer()
        create_topic(topic, num_partitions=24, replication_factor=3)

        session = requests.Session()
        fetch_and_send_orders_data(producer, topic, session)

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
