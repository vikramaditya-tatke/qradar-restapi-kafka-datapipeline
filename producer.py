from confluent_kafka import Producer
from settings import settings

from pipeline_logger import logger


def create_producer():
    kafka_config = {
        "bootstrap.servers": f"{settings.kafka_host}:{settings.kafka_port}",
        # "sasl_mechanism": "PLAIN",
        # "sasl_plain_username": "Vikram",
        # "sasl_plain_password": "Vikram123",
    }
    producer = Producer(kafka_config)
    return producer
