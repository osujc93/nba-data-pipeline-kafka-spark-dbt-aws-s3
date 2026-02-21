# kafka_producer_singleton.py

import threading
import json
import logging
from kafka import KafkaProducer

# Adjust bootstrap_servers, etc., to match your environment:
BOOTSTRAP_SERVERS = [
    '172.16.10.2:9092',
    '172.16.10.3:9093',
    '172.16.10.4:9094'
]

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class KafkaProducerSingleton:
    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        if KafkaProducerSingleton._instance is not None:
            raise Exception("Use KafkaProducerSingleton.get_instance() instead of instantiating directly.")

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: str(x).encode('utf-8'),
                retries=5,
                max_block_ms=3600000,
                request_timeout_ms=3600000,
                acks='all',
                linger_ms=5,
                batch_size=5 * 1024 * 1024,
                max_request_size=1195925856,
                compression_type='lz4',
                buffer_memory=134217728,
                max_in_flight_requests_per_connection=5
            )
            logger.info("Kafka Producer singleton created successfully.")
        except Exception as e:
            logger.error(f"Failed to create Kafka Producer: {e}")
            raise

    @classmethod
    def get_instance(cls):
        with cls._lock:
            if cls._instance is None:
                instance = KafkaProducerSingleton()
                cls._instance = instance
            return cls._instance.producer
